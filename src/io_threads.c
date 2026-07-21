/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "io_threads.h"
#include "queues.h"
#include <sys/resource.h>

#define IO_MPSC_QUEUE_SIZE 16384
#define IO_SPMC_QUEUE_SIZE 4096
#define IO_SPSC_QUEUE_SIZE 4096

static _Thread_local int thread_id = 0;
static _Thread_local mpscTicket io_thread_ticket = {0};
/* Backlog of responses when io_shared_outbox is full. Should be rare. */
static _Thread_local list *pending_io_responses = NULL;
static pthread_t io_threads[IO_THREADS_MAX_NUM] = {0};
static pthread_mutex_t io_threads_mutex[IO_THREADS_MAX_NUM];
static int cur_epoll_thread = 0;
// Main -> IO: Shared Queue (Single Producer Multi Consumer) where all IO threads pull jobs from
static spmcQueue io_shared_inbox = {0};
// IO -> Main: Response Channel (Multi Producer Single Consumer) used by IO threads to send results back to main-thread
static mpscQueue io_shared_outbox = {0};
// Main -> IO (Thread-Specific) for tasks that must run on specific IO thread where IO threads check their private inbox before the shared queue
static spscQueue io_private_inbox[IO_THREADS_MAX_NUM] = {0};
static size_t io_jobs_submitted;
static _Atomic(size_t) io_jobs_finished;
static int io_threads_initialized = 0;
_Atomic long long used_active_time_io_thread[IO_THREADS_MAX_NUM] = {0};

/* Job Types for Tagged Pointers
 * We use the lower 3 bits of the pointer to store the job type.
 * Requires data pointers to be 8-byte aligned (standard for zmalloc/ptrs). */
#define JOB_TAG_MASK 0x7
#define JOB_PTR_MASK (~(uintptr_t)JOB_TAG_MASK)

static inline void *tagJob(void *ptr, int type) {
    return (void *)((uintptr_t)ptr | type);
}

static inline void untagJob(void *tagged_ptr, void **ptr, int *type) {
    *type = (int)((uintptr_t)tagged_ptr & JOB_TAG_MASK);
    *ptr = (void *)((uintptr_t)tagged_ptr & JOB_PTR_MASK);
}

/* ===================== Parallel read run =====================
 *
 * A "run" is a group of clients whose read-only commands are executed at once,
 * across the IO threads and the main thread, and then completed on the main
 * thread in the order the clients arrived.
 *
 * Work is assigned to a thread by slot (slot % active_io_threads_num), so all
 * commands touching a given slot land on the same thread and no two threads
 * ever touch the same dict. Slots that map to thread 0 are the main thread's
 * own share, which it executes while the IO threads work.
 *
 * The main thread does nothing else between dispatching and joining. That is
 * what makes the run safe: cron, eviction, defrag and incremental rehashing all
 * mutate arbitrary slots, and none of them can overlap the run. Only commands
 * accepted by commandCanRunOnIOThread() are ever put in a run. */
typedef struct ioRunEntry {
    client *c;
    int tid; /* Thread that executes the proc; 0 is the main thread. */
} ioRunEntry;

static ioRunEntry *io_run = NULL;
static size_t io_run_count = 0;
static size_t io_run_capacity = 0;
static _Atomic(size_t) io_run_completed = 0;
/* Frozen clock shared by every command in the run. Written by the main thread
 * before dispatch, read by the IO threads during it. */
static mstime_t io_run_cmd_time_snapshot = 0;

/* Handler prototypes */
void ioThreadReadQueryFromClient(client *c);
void ioThreadWriteToClient(client *c);
void ioThreadFreeArgv(robj **argv);
void ioThreadPoll(aeEventLoop *el);
void ioThreadExecuteCommand(client *c);
static void ioThreadAccept(client *c);

int inMainThread(void) {
    return thread_id == 0;
}

int getCurTid(void) {
    return thread_id;
}

void commitIOJobs(void) {
    for (int i = 1; i < server.active_io_threads_num; i++) {
        spscCommit(&io_private_inbox[i]);
    }
}

/* Jobs sent but not yet processed by IO threads. */
static size_t getPendingIOThreadsJobs(void) {
    return io_jobs_submitted - atomic_load_explicit(&io_jobs_finished, memory_order_acquire);
}

/* Read/write jobs awaiting response from IO threads. */
static int getPendingIOResponsesCount(void) {
    return server.stat_io_writes_pending + server.stat_io_reads_pending;
}

/* Drains the I/O threads queue by waiting for all jobs to be processed.
 * This function must be called from the main thread. */
void drainIOThreadsQueue(void) {
    serverAssert(inMainThread());
    commitIOJobs();
    while (getPendingIOThreadsJobs()) {
        atomic_thread_fence(memory_order_acquire);
    }
}

/* Returns if there is an IO operation in progress for the given client. */
int clientHasPendingIO(client *c) {
    return c->io_read_state != CLIENT_IDLE || c->io_write_state != CLIENT_IDLE;
}

/* Wait until the IO-thread is done with the client */
void waitForClientIO(client *c) {
    /* No need to wait if the client was not offloaded to the IO thread. */
    if (c->io_read_state == CLIENT_IDLE && c->io_write_state == CLIENT_IDLE) return;

    /* Wait for read operation to complete if pending. */
    while (c->io_read_state == CLIENT_PENDING_IO) {
        atomic_thread_fence(memory_order_acquire);
    }

    /* Wait for write operation to complete if pending. */
    while (c->io_write_state == CLIENT_PENDING_IO) {
        atomic_thread_fence(memory_order_acquire);
    }

    /* Final memory barrier to ensure all changes are visible */
    atomic_thread_fence(memory_order_acquire);
}

void IOThreadsBeforeSleep(long long current_time) {
    if (server.io_threads_num == 1) return;
    serverAssert(inMainThread());

    commitIOJobs();

    if (server.io_threads_always_active) {
        /* active_all_io_threads state is for debug purposes: deactivate all threads before sleep if no pending jobs,
         * and reactivate all after sleep. We can't leave it active all the time as it will consume much CPU that will interfere with tests */
        if (server.active_io_threads_num > 1 && getPendingIOThreadsJobs() == 0) {
            for (int i = 1; i < server.active_io_threads_num; i++) {
                pthread_mutex_lock(&io_threads_mutex[i]);
            }
            server.active_io_threads_num = 1;
        }
    }

    /* If threads are not active, track main-thread active time for ignition decision */
    if (server.active_io_threads_num == 1) {
        static long long last_measurement_time = 0;
        if (current_time - last_measurement_time < 50000) return; /* Sample once in 50ms */
        last_measurement_time = current_time;
        trackInstantaneousMetric(STATS_METRIC_MAIN_THREAD_ACTIVE_TIME, server.stat_active_time, current_time, 1000000);
    }
}

#define IO_COOLDOWN_MS 1000
#define IO_SAMPLE_RATE_MS 10
#define IO_IGNITION_EVENTS 4
/* Start using I/O threads when the main thread is active for more than the below
 * defined percentage of the time. This number is picked somewhat arbitrarily but
 * needed to be low enough to make sure we start the next thread quickly while not
 * starting too many threads unnecessarily to avoid contention. */
#define IO_IGNITION_MAIN_THREAD_ACTIVE_PERCENT 30
#define BATCH_SIZE 32

void IOThreadsAfterSleep(int numevents) {
    if (server.io_threads_num == 1) return;
    serverAssert(inMainThread());
    /* Always Active Policy */
    if (server.io_threads_always_active) {
        if (numevents > 0 && server.active_io_threads_num < server.io_threads_num) {
            for (int i = server.active_io_threads_num; i < server.io_threads_num; i++) {
                pthread_mutex_unlock(&io_threads_mutex[i]);
            }
            server.active_io_threads_num = server.io_threads_num;
        }
        return;
    }

    mstime_t now = server.mstime;
    static long long last_scale_time = 0;

    /* Ignition Policy */
    if (server.active_io_threads_num == 1) {
        int should_ignite = 0;
        float main_thread_active_time = (float)getInstantaneousMetric(STATS_METRIC_MAIN_THREAD_ACTIVE_TIME) / 10000.0;
        /* Ignite IO threads when main-thread active time exceeds the threshold (30%) */
        should_ignite = (main_thread_active_time > (float)IO_IGNITION_MAIN_THREAD_ACTIVE_PERCENT);
        if (should_ignite) {
            pthread_mutex_unlock(&io_threads_mutex[1]);
            server.active_io_threads_num++;
            last_scale_time = now;
            serverLog(LL_DEBUG, "IO threads ignition: increased to %d", server.active_io_threads_num);
        }
        return;
    }

    static mstime_t last_sample_time = 0;
    static size_t spmc_size_sum = 0;
    static size_t sample_count = 0;

    /* Scaling Up/Down Policy */
    if (now - last_sample_time < IO_SAMPLE_RATE_MS) return;
    last_sample_time = now;

    size_t q_size = spmcSize(&io_shared_inbox);
    spmc_size_sum += q_size;
    sample_count++;

    trackInstantaneousMetric(STATS_METRIC_IO_WAIT, spmc_size_sum, sample_count, 1);

    /* Decision (Every STATS_METRIC_SAMPLES Samples) */
    if (sample_count % STATS_METRIC_SAMPLES != 0) return;

    size_t avg_q_size = getInstantaneousMetric(STATS_METRIC_IO_WAIT);
    size_t active = server.active_io_threads_num;
    size_t target = active;

    /* Calculate Target */
    if (avg_q_size > 1 && active < (size_t)server.io_threads_num) {
        target++;
    } else if (avg_q_size == 0 && (now - last_scale_time > IO_COOLDOWN_MS)) {
        if (target > 1) target--;
    }

    /* Scale Up */
    if (target > active) {
        for (size_t i = active; i < target; i++) {
            pthread_mutex_unlock(&io_threads_mutex[i]);
        }
        last_scale_time = now;
        server.active_io_threads_num = target;
        serverLog(LL_DEBUG, "IO threads increased from %zu to %zu", active, target);
    }
    /* Scale Down*/
    else if (target < active) {
        int tid = active - 1;

        /* Don't suspend if work remains in the specific thread's queue... */
        if (!spscIsEmpty(&io_private_inbox[tid])) return;
        /* ...or if we are dropping to 1 thread but the global queue still has work */
        if (target == 1 && !spmcIsEmpty(&io_shared_inbox)) return;

        pthread_mutex_lock(&io_threads_mutex[tid]);
        server.active_io_threads_num--;
        serverLog(LL_DEBUG, "IO threads decreased from %zu to %d", active, server.active_io_threads_num);
    }
}

/* This function performs polling on the given event loop and updates the server's
 * IO fired events count and poll state. */
void ioThreadPoll(aeEventLoop *el) {
    struct timeval tvp = {0, 0};
    int num_events = aePoll(el, &tvp);
    server.io_ae_fired_events = num_events;
    atomic_store_explicit(&server.io_poll_state, AE_IO_STATE_DONE, memory_order_release);
}

static void flushPendingIOResponses(int blocking) {
    if (!pending_io_responses) return;
    listIter li;
    listNode *ln;
    listRewind(pending_io_responses, &li);

    while ((ln = listNext(&li))) {
        void *job = listNodeValue(ln);
        int pushed = 0;

        /* Try to enqueue. If blocking is set, retry until success. */
        do {
            pushed = mpscEnqueue(&io_shared_outbox, job, &io_thread_ticket);
            if (pushed || !blocking || server.crashed) break; /* On server crash we kill the IO threads, no point in sending back jobs to the main-thread. */
            atomic_thread_fence(memory_order_acquire);
        } while (true);

        if (pushed) {
            listDelNode(pending_io_responses, ln);
        } else {
            return;
        }
    }

    /* List is fully drained */
    listRelease(pending_io_responses);
    pending_io_responses = NULL;
}

/* Define a cleanup function that will clean all thread resources */
void cleanupThreadResources(void *dummy) {
    UNUSED(dummy);

    /* Blocking flush: ensure all pending jobs are sent before thread dies */
    flushPendingIOResponses(1);

    /* Free the shared query buffer */
    freeSharedQueryBuf();
}

static void *IOThreadMain(void *myid) {
    /* The ID is the thread ID number (from 1 to server.io_threads_num-1). ID 0 is the main thread. */
    long id = (long)myid;
    char thdname[32];

    snprintf(thdname, sizeof(thdname), "io_thd_%ld", id);
    valkey_set_thread_title(thdname);
    serverSetCpuAffinity(server.server_cpulist);
    initSharedQueryBuf();
    pthread_cleanup_push(cleanupThreadResources, NULL);

    thread_id = (int)id;
    void *batch_jobs[BATCH_SIZE];
    int processed = 0;
    monotime work_start_time = 0;
    while (1) {
        /* Cancellation point so that pthread_cancel() from main thread is honored. */
        pthread_testcancel();
        size_t batch_count = 0;
        monotime prev_work_start_time = work_start_time;
        work_start_time = getMonotonicUs();
        if (processed != 0) {
            atomic_fetch_add_explicit(&used_active_time_io_thread[id],
                                      work_start_time - prev_work_start_time,
                                      memory_order_relaxed);
        }
        processed = 0;
        /* PRIORITY 1: Drain Private SPSC Queue (Batch Processing) */
        while ((batch_count = spscDequeueBatch(&io_private_inbox[id], batch_jobs, BATCH_SIZE)) > 0) {
            for (size_t i = 0; i < batch_count; i++) {
                void *data;
                int type;
                untagJob(batch_jobs[i], &data, &type);

                switch (type) {
                case JOB_REQ_FREE_ARGV:
                    ioThreadFreeArgv((robj **)data);
                    break;
                case JOB_REQ_POLL:
                    ioThreadPoll((aeEventLoop *)data);
                    break;
                case JOB_REQ_EXECUTE_CMD:
                    ioThreadExecuteCommand((client *)data);
                    break;
                default:
                    serverPanic("Invalid SPSC job type: %d", type);
                }
            }
            processed += batch_count;
        }

        /* PRIORITY 2: Shared Global Queue (SPMC)
         * Only checked after SPSC is drained. */
        void *tagged_job = spmcDequeue(&io_shared_inbox);
        if (tagged_job) {
            void *data;
            int type;
            untagJob(tagged_job, &data, &type);

            switch (type) {
            case JOB_REQ_READ_CLIENT:
                ioThreadReadQueryFromClient((client *)data);
                break;
            case JOB_REQ_WRITE_CLIENT:
                ioThreadWriteToClient((client *)data);
                break;
            case JOB_REQ_FREE_OBJ:
                decrRefCount(data);
                break;
            case JOB_REQ_ACCEPT:
                ioThreadAccept((client *)data);
                break;
            case JOB_REQ_POLL:
                ioThreadPoll((aeEventLoop *)data);
                break;
            default:
                serverPanic("Invalid SPMC job type: %d", type);
            }
            processed++;
        }

        if (processed) {
            atomic_fetch_add_explicit(&io_jobs_finished, processed, memory_order_release);
        }

        /* If both queues were empty (no processing done), wait for signal. */
        if (processed == 0) {
            if (unlikely(pending_io_responses)) {
                flushPendingIOResponses(0);
            } else {
                /* If it is locked. We should block until main thread unlocks it. */
                pthread_mutex_lock(&io_threads_mutex[id]);
                pthread_mutex_unlock(&io_threads_mutex[id]);
            }
        }
    }
    pthread_cleanup_pop(0);
    return NULL;
}

long long getIOThreadActiveTimeMicroseconds(int id) {
    return atomic_load_explicit(&used_active_time_io_thread[id], memory_order_relaxed);
}

static void createIOThread(int id) {
    serverAssert(server.io_threads_num > 0);
    serverAssert(id > 0 && id < server.io_threads_num);

    /* Initialize the private SPSC queue for this thread */
    spscInit(&io_private_inbox[id], IO_SPSC_QUEUE_SIZE);

    pthread_t tid;
    pthread_mutex_init(&io_threads_mutex[id], NULL);
    pthread_mutex_lock(&io_threads_mutex[id]); /* Thread will be stopped. */
    int err = pthread_create(&tid, NULL, IOThreadMain, (void *)(long)id);
    if (err) {
        serverLog(LL_WARNING, "Fatal: Can't initialize IO thread, pthread_create failed with: %s", strerror(err));
        exit(1);
    }
    io_threads[id] = tid;
}

/* Terminates the IO thread specified by id. */
static void shutdownIOThread(int id) {
    int err;
    pthread_t tid = io_threads[id];
    if (tid == pthread_self()) return;
    if (tid == 0) return;

    /* Only unlock mutex for inactive threads. Active threads are already unlocked. */
    if (id >= server.active_io_threads_num) {
        pthread_mutex_unlock(&io_threads_mutex[id]);
    }
    pthread_cancel(tid);

    if ((err = pthread_join(tid, NULL)) != 0) {
        serverLog(LL_WARNING, "IO thread(tid:%lu) can not be joined: %s", (unsigned long)tid, strerror(err));
    } else {
        serverLog(LL_NOTICE, "IO thread(tid:%lu) terminated", (unsigned long)tid);
    }
    pthread_mutex_destroy(&io_threads_mutex[id]);
    spscFree(&io_private_inbox[id]);
}

void killIOThreads(void) {
    for (int j = 1; j < server.io_threads_num; j++) { /* We don't kill thread 0, which is the main thread. */
        shutdownIOThread(j);
    }
}

int updateIOThreads(const char **err) {
    serverAssert(inMainThread());

    int prev_threads_num = 1;
    for (int i = IO_THREADS_MAX_NUM - 1; i > 0; i--) {
        if (io_threads[i]) {
            prev_threads_num = i + 1;
            break;
        }
    }
    if (prev_threads_num == server.io_threads_num) return 1;

    /* DEADLOCK PREVENTION:
     * Check if the pending workload fits in the return queue.
     * If the number of pending jobs is greater than the capacity of the Global MPSC queue,
     * the worker threads might fill the queue and block. If we enter drainIOThreadsQueue
     * in that state, we will deadlock (Main thread waits for worker, Worker waits for queue space). */
    size_t pending = getPendingIOResponsesCount();

    if (pending > io_shared_outbox.queue_size) {
        if (err) *err = "Can't update IO threads under load, try again later";
        return 0;
    }

    serverLog(LL_NOTICE, "Changing number of IO threads from %d to %d.", prev_threads_num, server.io_threads_num);
    drainIOThreadsQueue();

    /* Set active threads to 1, will be adjusted based on workload later. */
    for (int i = 1; i < server.active_io_threads_num; i++) {
        pthread_mutex_lock(&io_threads_mutex[i]);
    }
    server.active_io_threads_num = 1;

    if (server.io_threads_num > prev_threads_num) {
        initIOThreads(prev_threads_num);
    } else {
        for (int i = prev_threads_num - 1; i >= server.io_threads_num; i--) {
            /* Unblock inactive thread. */
            pthread_mutex_unlock(&io_threads_mutex[i]);
            shutdownIOThread(i);
            io_threads[i] = 0;
        }
    }
    return 1;
}

/* Initialize the data structures needed for I/O threads. */
void initIOThreads(int prev_threads_num) {
    /* Don't spawn any thread if the user selected a single thread:
     * we'll handle I/O directly from the main thread. */
    if (server.io_threads_num == 1) return;

    serverAssert(server.io_threads_num <= IO_THREADS_MAX_NUM);

    if (!io_threads_initialized) {
        server.active_io_threads_num = 1; /* We start with threads not active. */
        server.io_poll_state = AE_IO_STATE_NONE;
        server.io_ae_fired_events = 0;
        spmcInit(&io_shared_inbox, IO_SPMC_QUEUE_SIZE);
        mpscInit(&io_shared_outbox, IO_MPSC_QUEUE_SIZE);
        io_jobs_submitted = 0;
        atomic_init(&io_jobs_finished, 0);
        prefetchCommandsBatchInit();
        io_threads_initialized = 1;
    }

    /* Spawn and initialize the I/O threads. */
    for (int i = prev_threads_num; i < server.io_threads_num; i++) {
        createIOThread(i);
    }
}

int trySendReadToIOThreads(client *c) {
    if (server.active_io_threads_num <= 1) return C_ERR;
    /* If IO thread is already reading, return C_OK to make sure the main thread will not handle it. */
    if (c->io_read_state != CLIENT_IDLE) return C_OK;
    if (c->io_write_state == CLIENT_PENDING_IO) return C_OK;
    /* For simplicity, don't offload replica clients reads as read traffic from replica is negligible */
    if (getClientType(c) == CLIENT_TYPE_REPLICA) return C_ERR;
    /* With Lua debug client we may call connWrite directly in the main thread */
    if (c->flag.lua_debug) return C_ERR;
    /* For simplicity let the main-thread handle the blocked clients */
    if (c->flag.blocked || c->flag.unblocked) return C_ERR;
    if (c->flag.close_asap) return C_ERR;

    c->read_flags = canParseCommand(c) ? 0 : READ_FLAGS_DONT_PARSE;
    c->read_flags |= authRequired(c) ? READ_FLAGS_AUTH_REQUIRED : 0;
    c->read_flags |= isReplicatedClient(c) ? READ_FLAGS_REPLICATED : 0;

    c->io_read_state = CLIENT_PENDING_IO;
    connSetPostponeUpdateState(c->conn, 1);

    if (unlikely(spmcEnqueue(&io_shared_inbox, tagJob(c, JOB_REQ_READ_CLIENT)) == false)) {
        c->read_flags = 0;
        c->io_read_state = CLIENT_IDLE;
        connSetPostponeUpdateState(c->conn, 0);
        return C_ERR;
    }

    io_jobs_submitted++;
    server.stat_io_reads_pending++;
    c->flag.pending_read = 1;
    return C_OK;
}

/* This function attempts to offload the client's write to an I/O thread.
 * Returns C_OK if the client's writes were successfully offloaded to an I/O thread,
 * or C_ERR if the client is not eligible for offloading. */
int trySendWriteToIOThreads(client *c) {
    if (server.active_io_threads_num <= 1) return C_ERR;
    /* The I/O thread is already writing for this client. */
    if (c->io_write_state != CLIENT_IDLE) return C_OK;
    if (c->io_read_state == CLIENT_PENDING_IO) return C_ERR;
    /* Nothing to write */
    if (!clientHasPendingReplies(c)) return C_ERR;
    /* For simplicity, avoid offloading non-online replicas */
    if (getClientType(c) == CLIENT_TYPE_REPLICA && c->repl_data->repl_state != REPLICA_STATE_ONLINE) return C_ERR;
    /* We can't offload debugged clients as the main-thread may read at the same time  */
    if (c->flag.lua_debug) return C_ERR;

    int is_replica = getClientType(c) == CLIENT_TYPE_REPLICA;
    clientReplyBlock *block = NULL;
    if (is_replica) {
        c->io_last_reply_block = listLast(server.repl_buffer_blocks);
        replBufBlock *o = listNodeValue(c->io_last_reply_block);
        c->io_last_bufpos = o->used;
    } else {
        /* Save the last block of the reply list to io_last_reply_block and the used
         * position to io_last_bufpos. The I/O thread will write only up to
         * io_last_bufpos, regardless of the c->bufpos value. This is to prevent I/O
         * threads from reading data that might be invalid in their local CPU cache. */
        c->io_last_reply_block = listLast(c->reply);
        if (c->io_last_reply_block) {
            block = (clientReplyBlock *)listNodeValue(c->io_last_reply_block);
            c->io_last_bufpos = block->used;
        } else {
            c->io_last_bufpos = (size_t)c->bufpos;
        }
    }

    serverAssert(c->bufpos > 0 || c->io_last_bufpos > 0 || is_replica);

    /* The main-thread will update the client state after the I/O thread completes the write. */
    connSetPostponeUpdateState(c->conn, 1);
    c->write_flags = is_replica ? WRITE_FLAGS_IS_REPLICA : 0;
    c->io_write_state = CLIENT_PENDING_IO;
    void *job = tagJob(c, JOB_REQ_WRITE_CLIENT);
    if (unlikely(spmcEnqueue(&io_shared_inbox, job) == false)) {
        c->io_write_state = CLIENT_IDLE;
        connSetPostponeUpdateState(c->conn, 0);
        c->write_flags = 0;
        c->io_last_reply_block = NULL;
        c->io_last_bufpos = 0;
        return C_ERR;
    }
    /* Force new header after successful enqueue so the main thread doesn't
     * extend a header the I/O thread is currently reading. */
    if (!is_replica) {
        if (block) {
            if (block->flag.buf_encoded) block->last_header = NULL;
        } else {
            if (c->flag.buf_encoded) c->last_header = NULL;
        }
    }
    if (c->flag.pending_write) {
        listUnlinkNode(server.clients_pending_write, &c->clients_pending_write_node);
        c->flag.pending_write = 0;
    }

    io_jobs_submitted++;
    server.stat_io_writes_pending++;
    return C_OK;
}

/* Internal function to free the client's argv in an IO thread. */
void ioThreadFreeArgv(robj **argv) {
    int last_arg = 0;
    for (int i = 0;; i++) {
        robj *o = argv[i];
        if (o == NULL) {
            continue;
        }

        /* The main-thread set the refcount to 0 to indicate that this is the last argument to free */
        if (objectGetRefcount(o) == 0) {
            last_arg = 1;
            o->refcount = 1;
        }

        decrRefCount(o);

        if (last_arg) {
            break;
        }
    }

    zfree(argv);
}

/* This function attempts to offload the client's argv to an IO thread.
 * Returns C_OK if the client's argv were successfully offloaded to an IO thread,
 * C_ERR otherwise. */
int tryOffloadFreeArgvToIOThreads(client *c, int argc, robj **argv) {
    if (server.active_io_threads_num <= 1 || argc == 0) {
        return C_ERR;
    }

    int target_id = c->cur_tid;
    if (target_id < 1 || target_id >= server.active_io_threads_num) {
        target_id = (c->id % (server.active_io_threads_num - 1)) + 1;
    }

    if (spscIsFull(&io_private_inbox[target_id])) {
        return C_ERR;
    }

    int last_arg_to_free = -1;

    /* Prepare the argv */
    for (int j = 0; j < argc; j++) {
        if (argv[j]->refcount > 1) {
            decrRefCount(argv[j]);
            /* Set argv[j] to NULL to avoid double free */
            argv[j] = NULL;
        } else {
            last_arg_to_free = j;
        }
    }

    /* If no argv to free, free the argv array at the main thread */
    if (last_arg_to_free == -1) {
        zfree(argv);
        return C_OK;
    }

    /* We set the refcount of the last arg to free to 0 to indicate that
     * this is the last argument to free. With this approach, we don't need to
     * send the argc to the IO thread and we can send just the argv ptr. */
    argv[last_arg_to_free]->refcount = 0;
    void *job = tagJob(argv, JOB_REQ_FREE_ARGV);
    /* We pass false to enqueue the job without committing the queue index immediately.
     * This allows us to batch multiple free jobs together and
     * commit them in a single operation later in the event loop. This reduces the overhead
     * of memory barriers and cache line bouncing associated
     * with updating the queue's write pointer per job. */
    spscEnqueue(&io_private_inbox[target_id], job, false);
    io_jobs_submitted++;

    return C_OK;
}

/* This function attempts to offload the free of an object to an IO thread.
 * Returns C_OK if the object was successfully offloaded to an IO thread,
 * C_ERR otherwise.*/
int tryOffloadFreeObjToIOThreads(robj *obj) {
    if (server.active_io_threads_num <= 1) {
        return C_ERR;
    }

    if (obj->refcount > 1) return C_ERR;

    if (obj->encoding != OBJ_ENCODING_RAW || obj->type != OBJ_STRING) return C_ERR;

    void *job = tagJob(obj, JOB_REQ_FREE_OBJ);
    if (unlikely(spmcEnqueue(&io_shared_inbox, job) == false)) return C_ERR;
    io_jobs_submitted++;
    server.stat_io_freed_objects++;
    return C_OK;
}

/* This function retrieves the results of the IO Thread poll.
 * returns the number of fired events if the IO thread has finished processing poll events, 0 otherwise. */
static int getIOThreadPollResults(aeEventLoop *eventLoop) {
    int io_state;
    io_state = atomic_load_explicit(&server.io_poll_state, memory_order_acquire);
    if (io_state == AE_IO_STATE_POLL) {
        /* IO thread is still processing poll events. */
        return 0;
    }

    /* IO thread is done processing poll events. */
    serverAssert(io_state == AE_IO_STATE_DONE);
    server.stat_poll_processed_by_io_threads++;
    server.io_poll_state = AE_IO_STATE_NONE;

    /* Remove the custom poll proc. */
    aeSetCustomPollProc(eventLoop, NULL);
    aeSetPollProtect(eventLoop, 0);
    return server.io_ae_fired_events;
}

void trySendPollJobToIOThreads(void) {
    if (server.active_io_threads_num <= 1) {
        return;
    }

    /* If there are no pending jobs, let the main thread do the poll-wait by itself. */
    if (getPendingIOResponsesCount() == 0) {
        return;
    }

    /* If the IO thread is already processing poll events, don't send another job. */
    if (server.io_poll_state != AE_IO_STATE_NONE) {
        return;
    }

    void *job = tagJob(server.el, JOB_REQ_POLL);

    server.io_poll_state = AE_IO_STATE_POLL;
    aeSetPollProtect(server.el, 1);

    /* Use SPMC to minimize polling overhead. At high thread counts, use private SPSC queues for lower latency. */
    if (server.active_io_threads_num <= 9) {
        if (unlikely(spmcEnqueue(&io_shared_inbox, job) == false)) {
            server.io_poll_state = AE_IO_STATE_NONE;
            aeSetPollProtect(server.el, 0);
            return;
        }
    } else {
        cur_epoll_thread = ((cur_epoll_thread) % (server.active_io_threads_num - 1)) + 1;
        if (unlikely(spscIsFull(&io_private_inbox[cur_epoll_thread]))) {
            server.io_poll_state = AE_IO_STATE_NONE;
            aeSetPollProtect(server.el, 0);
            return;
        }
        spscEnqueue(&io_private_inbox[cur_epoll_thread], job, true);
    }

    aeSetCustomPollProc(server.el, getIOThreadPollResults);
    io_jobs_submitted++;
}

void sendToMainThread(void *data, int type) {
    if (unlikely(pending_io_responses)) {
        flushPendingIOResponses(0);
    }
    void *job = tagJob(data, type);
    if (unlikely(pending_io_responses || !mpscEnqueue(&io_shared_outbox, job, &io_thread_ticket))) {
        /* Failed to push new job: initialize list if needed and save job */
        if (pending_io_responses == NULL) {
            pending_io_responses = listCreate();
        }
        listAddNodeTail(pending_io_responses, job);
    }
}

static void ioThreadAccept(client *c) {
    connAccept(c->conn, NULL);
    atomic_thread_fence(memory_order_release);
    c->io_read_state = CLIENT_COMPLETED_IO;
    sendToMainThread(c, JOB_RES_READ_CLIENT);
}

/*
 * Attempts to offload an Accept operation (currently used for TLS accept) for a client
 * connection to I/O threads.
 *
 * Returns:
 *   C_OK  - If the accept operation was successfully queued for processing
 *   C_ERR - If the connection is not eligible for offloading
 *
 * Parameters:
 *   conn - The connection object to perform the accept operation on
 */
int trySendAcceptToIOThreads(connection *conn) {
    if (server.io_threads_num <= 1) {
        return C_ERR;
    }

    if (!(conn->flags & CONN_FLAG_ALLOW_ACCEPT_OFFLOAD)) {
        return C_ERR;
    }

    client *c = connGetPrivateData(conn);
    if (c->io_read_state != CLIENT_IDLE) {
        return C_OK;
    }

    if (server.active_io_threads_num <= 1) {
        return C_ERR;
    }

    c->io_read_state = CLIENT_PENDING_IO;
    c->flag.pending_read = 1;
    connSetPostponeUpdateState(c->conn, 1);

    void *job = tagJob(c, JOB_REQ_ACCEPT);
    if (unlikely(spmcEnqueue(&io_shared_inbox, job) == false)) {
        c->io_read_state = CLIENT_IDLE;
        c->flag.pending_read = 0;
        connSetPostponeUpdateState(c->conn, 0);
        return C_ERR;
    }

    server.stat_io_reads_pending++;
    server.stat_io_accept_offloaded++;
    io_jobs_submitted++;
    return C_OK;
}

/* Function to handle read jobs */
static void handleReadJobs(client **read_jobs, int read_count) {
    server.stat_io_reads_pending -= read_count;
    serverAssert(server.stat_io_reads_pending >= 0);
    /* process each client */
    for (int i = 0; i < read_count; i++) {
        client *c = read_jobs[i];
        processClientIOReadsDone(c);
    }

    /* Process commands in batch if we processed any reads */
    if (read_count) {
        server.stat_io_reads_processed += read_count;
        processClientsCommandsBatch();
    }
}

/* Function to handle write jobs */
static void handleWriteJobs(client **write_jobs, int write_count) {
    server.stat_io_writes_pending -= write_count;
    serverAssert(server.stat_io_writes_pending >= 0);

    for (int i = 0; i < write_count; i++) {
        client *c = write_jobs[i];
        server.stat_io_writes_processed++;
        processClientIOWriteDone(c);
    }
}

#define JOB_BATCH_SIZE (16)
int processIOThreadsResponses(void) {
    /* We don't check for threads number  since some threads may return jobs then deactivate/shut-down */

    /* Quick check if any pending operations exist */
    if (getPendingIOResponsesCount() == 0) return 0;

    int total_processed = 0;
    void *jobs[JOB_BATCH_SIZE];
    client *read_jobs[JOB_BATCH_SIZE];
    client *write_jobs[JOB_BATCH_SIZE];

    /* Loop until we consume all pending jobs */
    while (1) {
        int received_responses = 0;
        int dequeued_count = 0;
        int read_count = 0;
        int write_count = 0;

        /* Try to dequeue JOB_BATCH_SIZE */
        while (received_responses < JOB_BATCH_SIZE) {
            dequeued_count = mpscDequeueBatch(&io_shared_outbox, jobs, JOB_BATCH_SIZE - received_responses);

            /* Stop if we can't get more jobs from the queue. */
            if (dequeued_count == 0) break;

            received_responses += dequeued_count;
            total_processed += dequeued_count;

            for (int i = 0; i < dequeued_count; i++) {
                void *data;
                int job_type;
                untagJob(jobs[i], &data, &job_type);
                client *c = (client *)data;
                if (job_type == JOB_RES_READ_CLIENT) {
                    serverAssert(c->io_read_state == CLIENT_COMPLETED_IO);
                    read_jobs[read_count++] = c;
                } else if (job_type == JOB_RES_WRITE_CLIENT) {
                    serverAssert(c->io_write_state == CLIENT_COMPLETED_IO);
                    write_jobs[write_count++] = c;
                } else {
                    serverPanic("Unknown job type %d", job_type);
                }
            }
        }

        if (read_count) handleReadJobs(read_jobs, read_count);
        if (write_count) handleWriteJobs(write_jobs, write_count);

        /* If the queue was empty at the last try - don't try again */
        if (dequeued_count == 0) return total_processed;
    }
}

/* ===================== Parallel read run =====================
 * See the comment on ioRunEntry at the top of this file. */

/* Execute one client's command proc. Runs on an IO thread, or on the main
 * thread for its own share of the run.
 *
 * The client, its reply buffer and its slot's dict are the only things this may
 * touch. Anything server-wide that a read-only command would normally bump goes
 * into c->io_stats, which the main thread folds in during completion. */
static void executeCommandProc(client *c) {
    client *prev_client = server_current_client;
    server_current_client = c;

    /* Divert the server-wide counters, and freeze the clock: the command runs
     * outside of the main thread's execution unit. Both are thread-locals, so
     * this only affects the thread running this proc. */
    server_deferred_stats = &c->io_stats;
    server_io_cmd_time_snapshot = io_run_cmd_time_snapshot;

    callInvoke(c, &c->io_call_ctx);

    server_io_cmd_time_snapshot = 0;
    server_deferred_stats = NULL;
    server_current_client = prev_client;
}

/* IO thread entry point for a dispatched command. Only jobs that were actually
 * dispatched bump io_run_completed: the main thread's own share must not, or it
 * would satisfy its own wait and start completing clients that the IO threads
 * are still executing. */
void ioThreadExecuteCommand(client *c) {
    executeCommandProc(c);

    /* Release: publishes this thread's writes to the client's reply buffer to
     * the main thread, which acquires the counter in ioThreadRunJoin(). */
    atomic_fetch_add_explicit(&io_run_completed, 1, memory_order_release);
}

/* Add a client whose command's proc has been deferred to the current run. */
void ioThreadRunAdd(client *c) {
    serverAssert(inMainThread());
    serverAssert(c->flag.io_pending_exec);

    if (io_run_count == io_run_capacity) {
        io_run_capacity = io_run_capacity ? io_run_capacity * 2 : 16;
        io_run = zrealloc(io_run, sizeof(ioRunEntry) * io_run_capacity);
    }
    io_run[io_run_count].c = c;
    io_run[io_run_count].tid = 0;
    io_run_count++;
}

size_t ioThreadRunPending(void) {
    return io_run_count;
}

/* Drop a client from the pending run. Called from unlinkClient(), so that a
 * client freed while the run is being completed is not touched again. */
void removeClientFromParallelReadRun(client *c) {
    for (size_t i = 0; i < io_run_count; i++) {
        if (io_run[i].c == c) {
            io_run[i].c = NULL;
            return;
        }
    }
}

/* Finish one client: fold in what its proc did on the IO thread, run the second
 * half of call(), and then let the rest of its pipeline proceed. */
static void ioThreadRunCompleteClient(client *c) {
    if (c->flag.io_pending_exec) {
        c->flag.io_pending_exec = 0;

        client *prev_client = server_current_client;
        server_current_client = c;

        /* Re-arm the error counter so that the epilogue's failed-call check
         * sees exactly the errors this command produced, and no others that
         * may have been recorded since its prologue ran. */
        incrCommandStatsOnError(NULL, 0);
        applyDeferredStats(c);

        callEpilogue(c, &c->io_call_ctx);
        commandProcessed(c);
        if (c->conn) updateClientMemUsageAndBucket(c);

        server_current_client = prev_client;

        /* The proc filled the reply buffer with the pending-write queue
         * suppressed, since that list is main-thread state. Queue it now. */
        if (clientHasPendingReplies(c)) putClientInPendingWriteQueue(c);
    }

    /* Anything else this client pipelined behind the deferred command. */
    if (processInputBuffer(c) != C_ERR) beforeNextClient(c);
}

/* Execute the pending run and complete its clients. Returns once every command
 * in the run has been executed and completed; the main thread does nothing else
 * in between, which is what keeps the IO threads' slots disjoint from cron,
 * eviction and rehashing. */
void ioThreadRunJoin(void) {
    if (io_run_count == 0) return;
    serverAssert(inMainThread());

    /* Freeze the clock for the whole run, the way an execution unit does for a
     * single command, so every command in it observes the same time. */
    enterExecutionUnit(1, ustime());
    exitExecutionUnit();
    io_run_cmd_time_snapshot = server.cmd_time_snapshot;

    atomic_store_explicit(&io_run_completed, 0, memory_order_relaxed);

    server.stat_io_cmd_runs++;

    /* Dispatch by slot. A slot whose thread is busy (full queue) stays with the
     * main thread rather than waiting for room. */
    size_t dispatched = 0;
    for (size_t i = 0; i < io_run_count; i++) {
        client *c = io_run[i].c;
        io_run[i].tid = 0;
        if (c == NULL || !c->flag.io_pending_exec) continue;

        int tid = c->slot % server.active_io_threads_num;
        if (tid == 0 || spscIsFull(&io_private_inbox[tid])) continue;

        spscEnqueue(&io_private_inbox[tid], tagJob(c, JOB_REQ_EXECUTE_CMD), false);
        io_run[i].tid = tid;
        io_jobs_submitted++;
        dispatched++;
    }
    if (dispatched) commitIOJobs();
    server.stat_io_cmds_executed += dispatched;

    /* The main thread's own share, executed while the IO threads work. */
    for (size_t i = 0; i < io_run_count; i++) {
        client *c = io_run[i].c;
        if (c == NULL || io_run[i].tid != 0 || !c->flag.io_pending_exec) continue;
        executeCommandProc(c);
    }

    /* Join: every dispatched command must have finished before the main thread
     * touches any of these clients again. */
    while (atomic_load_explicit(&io_run_completed, memory_order_acquire) < dispatched) {
        /* The IO threads are spinning on their inboxes; this is short. */
    }

    /* Complete the clients in the order they arrived, so that stats,
     * propagation and monitor feeds are ordered as if the commands had run
     * inline. Clear the run first: completing a client resumes its pipeline,
     * which can re-enter this code via processEventsWhileBlocked. */
    size_t count = io_run_count;
    io_run_count = 0;
    for (size_t i = 0; i < count; i++) {
        client *c = io_run[i].c;
        if (c == NULL) continue;
        ioThreadRunCompleteClient(c);
    }
}
