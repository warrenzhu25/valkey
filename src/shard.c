/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * Execution-shard thread/loop scaffolding. See shard.h.
 */

#include "server.h"
#include "shard.h"
#include "slot_shard.h"
#include "anet.h"
#include "cluster.h" /* aggregateClientOutputBuffer */
#include "module.h"

#include <unistd.h>
#include <signal.h>

shard *server_shards = NULL;

static int shard_threads_active = 0;
_Thread_local int shard_current_id = 0;
static _Thread_local unsigned long long tl_stat_shard_remote_commands = 0;
static _Thread_local unsigned long long tl_stat_shard_remote_queue_us = 0;
static _Thread_local unsigned long long tl_stat_shard_remote_execution_us = 0;
static _Thread_local unsigned long long tl_stat_shard_remote_delivery_us = 0;
static int shard_barrier_excluded_worker = -1;
static int *shard_main_call_waiting = NULL;
static pthread_mutex_t clients_index_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Capacity of each shard's inbox/results ring. Power of two; sized generously so a
 * burst of in-flight REMOTE jobs does not hit the full-inbox backpressure path in
 * the common case. */
#define SHARD_QUEUE_SIZE 4096
#define SHARD_REMOTE_BATCH_MAX 32
#define SHARD_INBOX_BATCH_SIZE 256

static void shardDrainInbox(shard *self);

/* Escalation barrier state (shard.h). One barrier at a time; only the main thread calls
 * Begin/End, and the workers only park. */
static pthread_mutex_t barrier_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t barrier_cond = PTHREAD_COND_INITIALIZER;
static int barrier_active = 0; /* main wants all workers parked */
static int barrier_parked = 0; /* workers currently parked */

/* No-op read handler: the self-pipe carries only a wake signal, so drain and
 * discard. Its only purpose is to give the idle loop an fd to poll and a way for
 * another thread to break that poll (for shutdown). */
static void shardWakeReadHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el);
    UNUSED(mask);
    char buf[256];
    while (read(fd, buf, sizeof(buf)) > 0) { /* drain */
    }
    shard *self = privdata;
    if (self != NULL && self->id == 0) shardDrainInbox(self);
}

/* A persistent socket-less client for executing commands off the coordinator's real
 * client. It is a fake client -- so addReply* produces plain RESP bytes with no
 * copy-avoid encoding -- but carries a dummy conn and the CACHED_RESPONSE id so
 * prepareClientToWrite() accumulates the reply instead of discarding it (the same trick
 * createCachedResponseClient() uses). Never registered on a loop, never polled. */
static client *shardCreateExecutor(void) {
    client *c = createClient(NULL);
    c->flag.fake = 1;
    c->flag.deny_blocking = 1;
    /* shard_executor makes reads keep expired keys (never delete/propagate) and makes
     * prepareClientToWrite() buffer the reply without scheduling a socket write, so a
     * worker never touches the global clients_pending_write list. No conn needed. */
    c->flag.shard_executor = 1;
    return c;
}

static void shardFreeExecutor(client *c) {
    if (c == NULL) return;
    freeClient(c);
}

typedef enum shardMessageType {
    SHARD_MSG_ADOPT_CLIENT,
    SHARD_MSG_CALL,
    SHARD_MSG_EXEC,
    SHARD_MSG_RESULT,
    SHARD_MSG_TX_PREPARE,
    SHARD_MSG_TX_ACK,
    SHARD_MSG_TX_COMMIT,
} shardMessageType;

typedef struct shardRemoteBatch {
    uint64_t client_id;
    struct client *client;
    int count;
    int pending_results;
    struct {
        sds head;
        list *blocks;
        struct serverCommand *cmd;
        int slot;
        unsigned long long input_bytes;
        size_t qb_applied;
    } entry[SHARD_REMOTE_BATCH_MAX];
} shardRemoteBatch;

typedef struct shardExecJob {
    shardRemoteBatch *batch;
    int coordinator_shard;
    int count;
    monotime enqueue_time;
    struct {
        int result_index;
        int dbid, slot, resp, argc;
        struct serverCommand *cmd;
        robj **argv; /* argv ownership moved from the coordinator */
        int argv_is_static;
        mstime_t cmd_time; /* the coordinator's command-time snapshot, for expiry */
        unsigned long long input_bytes;
        size_t qb_applied;
    } entry[SHARD_REMOTE_BATCH_MAX];
} shardExecJob;

typedef struct shardResult {
    shardRemoteBatch *batch;
    /* Reply hand-off without the extra aggregate copy. `head` is a copy of the executor's
     * small inline buffer; `blocks` are its overflow reply-list nodes, *moved* (not copied)
     * from the executor. Both are plain RESP bytes -- the executor is a fake client so it is
     * never buf_encoded -- and are freed by the coordinator after it appends them to the
     * real client via addReplyProto (which applies that client's own encoding). */
    int count;
    monotime enqueue_time;
    struct {
        int result_index;
        sds head;
        list *blocks;
        struct serverCommand *cmd;
        int slot;
        unsigned long long input_bytes;
        size_t qb_applied;
    } entry[SHARD_REMOTE_BATCH_MAX];
} shardResult;

typedef struct shardCallJob {
    client *client;
    int flags;
    int coordinator_shard;
    int done;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
} shardCallJob;

typedef struct shardMessage {
    shardMessageType type;
    union {
        client *client;
        shardCallJob *call;
        shardExecJob *job;
        shardResult *result;
        struct shardTxState *tx;
    } data;
} shardMessage;

/* Distributed transaction state for VLL protocol */
typedef struct shardTxState {
    int coordinator_shard;
    int target_shard;
    int target_slot;
    void *payload; /* Temp */
} shardTxState;


static void shardArm(shard *self);

/* Worker beforeSleep: run any queued REMOTE reads, then park if a barrier is active, then
 * poll. Draining before parking empties the inbox so no coordinator waits across a barrier;
 * running a read (on this shard's own slots) before parking is safe because the coordinator
 * that requested the barrier is still waiting for this worker to park. */
static void shardWorkerBeforeSleep(aeEventLoop *el) {
    shard *self = shardForEventLoop(el);
    if (self) shardDrainInbox(self);
    if (listLength(shardCurrentUnblockedClients())) processUnblockedClients();
    handleClientsWithPendingWrites();
    freeClientsInAsyncFreeQueue();
    shardWorkerParkIfNeeded();
    shardFlushAllDeferredMessages();
    /* Last thing before poll: arm the wake flag so producers can coalesce their wakes. */
    if (self) shardArm(self);
}

static void *shardThreadMain(void *arg) {
    shard *s = arg;
    char name[32];
    snprintf(name, sizeof(name), "shard_%d", s->id);
    valkey_set_thread_title(name);
    shard_current_id = s->id;

    /* Route signals to the main thread, matching bio and I/O threads. */
    sigset_t sigset;
    sigfillset(&sigset);
    pthread_sigmask(SIG_BLOCK, &sigset, NULL);
    makeThreadKillable();

    if (server.server_cpulist && !strcasecmp(server.server_cpulist, "auto")) {
        serverBindThreadToNumaCore(s->id);
    } else {
        serverSetCpuAffinity(server.server_cpulist);
    }
    initSharedQueryBuf();

    /* Idle for now: only the wake pipe is registered on this loop. When a shard
     * owns clients and slots (later steps), this same loop serves them. */
    aeMain(s->el);
    freeSharedQueryBuf();
    return NULL;
}

void shardWorkerParkIfNeeded(void) {
    pthread_mutex_lock(&barrier_mutex);
    if (barrier_active) {
        barrier_parked++;
        pthread_cond_broadcast(&barrier_cond); /* tell the main thread we parked */
        while (barrier_active) {
            pthread_mutex_unlock(&barrier_mutex);
            sched_yield();
            pthread_mutex_lock(&barrier_mutex);
        }
        /* Account our departure so shardBarrierEnd() can wait for every parked worker to
         * leave before it returns; that is what makes back-to-back barriers safe. */
        barrier_parked--;
        pthread_cond_broadcast(&barrier_cond);
    }
    pthread_mutex_unlock(&barrier_mutex);
}

static int shardBarrierBeginExcluding(int excluded_worker) {
    int workers = shard_threads_active;
    if (workers == 0) return 0; /* shard-threads 1: nothing to quiesce. */
    int caller = shardCurrentId();

    pthread_mutex_lock(&barrier_mutex);
    barrier_active = 1;
    barrier_parked = 0;
    int target_parked = workers;
    for (int i = 1; i <= workers; i++) {
        if (i == caller || i == excluded_worker || (shard_main_call_waiting && shard_main_call_waiting[i]))
            target_parked--;
    }

    /* Break every worker out of aePoll so it reaches its beforeSleep and parks. */
    for (int i = 1; i <= workers; i++) {
        if (i == caller || i == excluded_worker) continue;
        if (shard_main_call_waiting && shard_main_call_waiting[i]) continue;
        char b = 'b';
        if (write(server_shards[i].wake_pipe[1], &b, 1) < 0) { /* best-effort */
        }
    }

    while (barrier_parked < target_parked) {
        pthread_mutex_unlock(&barrier_mutex);
        sched_yield();
        pthread_mutex_lock(&barrier_mutex);
    }
    pthread_mutex_unlock(&barrier_mutex);
    return workers;
}

int shardBarrierBegin(void) {
    return shardBarrierBeginExcluding(shard_barrier_excluded_worker);
}

void shardBarrierEnd(void) {
    pthread_mutex_lock(&barrier_mutex);
    barrier_active = 0;
    pthread_cond_broadcast(&barrier_cond); /* release the parked workers */
    /* Wait for every parked worker to actually leave before returning. The barrier is only
     * ever entered from the main thread, so the next Begin runs after this End; without this
     * wait a back-to-back Begin would reset barrier_parked to 0 while workers are still in the
     * release path, and those workers -- seeing barrier_active set again -- would never
     * re-count for the new barrier, hanging Begin forever. */
    while (barrier_parked > 0) {
        pthread_mutex_unlock(&barrier_mutex);
        sched_yield();
        pthread_mutex_lock(&barrier_mutex);
    }
    pthread_mutex_unlock(&barrier_mutex);
}

void shardInit(void) {
    int n = server.shard_threads_num;
    server_shards = zcalloc(sizeof(shard) * n);

    /* Shard 0 is the main thread; it reuses the global event loop. */
    server_shards[0].id = 0;
    server_shards[0].el = server.el;
    server_shards[0].thread = pthread_self();
    server_shards[0].clients = server.clients;
    server_shards[0].clients_pending_write = server.clients_pending_write;
    server_shards[0].unblocked_clients = server.unblocked_clients;
    server_shards[0].clients_to_close = server.clients_to_close;
    server_shards[0].client_count = listLength(server.clients);
    server_shards[0].commandstats = zcalloc(sizeof(shardCommandStats) * USER_COMMAND_BITS_COUNT);
    mpscInit(&server_shards[0].inbox, SHARD_QUEUE_SIZE);
    atomic_init(&server_shards[0].needs_wake, 0);
    shard_main_call_waiting = zcalloc(sizeof(int) * n);

    if (n == 1) return; /* Default: no extra threads, provably today's behavior. */

    /* Shard 0 (the main thread) needs an executor for slots it owns, and a wake pipe on
     * server.el so a worker can break the main loop's poll to deliver a REMOTE result. */
    server_shards[0].executor = shardCreateExecutor();
    if (pipe(server_shards[0].wake_pipe) == -1) serverPanic("Failed creating shard 0 wake pipe");
    anetNonBlock(NULL, server_shards[0].wake_pipe[0]);
    anetNonBlock(NULL, server_shards[0].wake_pipe[1]);
    if (aeCreateFileEvent(server.el, server_shards[0].wake_pipe[0], AE_READABLE, shardWakeReadHandler,
                          &server_shards[0]) == AE_ERR)
        serverPanic("Failed registering shard 0 wake handler");

    for (int i = 1; i < n; i++) {
        shard *s = &server_shards[i];
        s->id = i;
        s->executor = shardCreateExecutor();
        s->clients = listCreate();
        s->clients_pending_write = listCreate();
        s->unblocked_clients = listCreate();
        s->clients_to_close = listCreate();
        s->commandstats = zcalloc(sizeof(shardCommandStats) * USER_COMMAND_BITS_COUNT);
        mpscInit(&s->inbox, SHARD_QUEUE_SIZE);
        atomic_init(&s->needs_wake, 0);
        s->el = aeCreateEventLoop(server.maxclients + CONFIG_FDSET_INCR);
        if (s->el == NULL) serverPanic("Failed creating event loop for shard %d", i);
        aeSetBeforeSleepProc(s->el, shardWorkerBeforeSleep);
        if (pipe(s->wake_pipe) == -1) serverPanic("Failed creating wake pipe for shard %d", i);
        anetNonBlock(NULL, s->wake_pipe[0]);
        anetNonBlock(NULL, s->wake_pipe[1]);
        if (aeCreateFileEvent(s->el, s->wake_pipe[0], AE_READABLE, shardWakeReadHandler, s) == AE_ERR)
            serverPanic("Failed registering wake handler for shard %d", i);
        int err = pthread_create(&s->thread, NULL, shardThreadMain, s);
        if (err != 0) serverPanic("Failed spawning shard thread %d: %s", i, strerror(err));
        shard_threads_active++;
    }
    serverLog(LL_NOTICE, "Execution shards: %d threads (1 main + %d worker).", n, n - 1);
}

void shardKillThreads(void) {
    if (server_shards == NULL) return;
    int n = server.shard_threads_num;
    for (int i = 1; i < n; i++) {
        shard *s = &server_shards[i];
        if (s->el == NULL) continue;
        /* Stop the loop, then wake it out of aeApiPoll so it observes the stop. */
        aeStop(s->el);
        char b = 'x';
        if (write(s->wake_pipe[1], &b, 1) < 0) { /* best-effort wake */
        }
        pthread_join(s->thread, NULL);
        aeDeleteFileEvent(s->el, s->wake_pipe[0], AE_READABLE);
        aeDeleteEventLoop(s->el);
        close(s->wake_pipe[0]);
        close(s->wake_pipe[1]);
        s->el = NULL;
        shard_threads_active--;
    }
    /* Shard 0's wake pipe lives on server.el (the main loop); tear it down here too. */
    if (server_shards[0].wake_pipe[0] > 0) {
        aeDeleteFileEvent(server.el, server_shards[0].wake_pipe[0], AE_READABLE);
        close(server_shards[0].wake_pipe[0]);
        close(server_shards[0].wake_pipe[1]);
        server_shards[0].wake_pipe[0] = server_shards[0].wake_pipe[1] = 0;
    }
    for (int i = 0; i < n; i++) {
        shardFreeExecutor(server_shards[i].executor);
        server_shards[i].executor = NULL;
        zfree(server_shards[i].commandstats);
        server_shards[i].commandstats = NULL;
        if (i > 0) {
            listRelease(server_shards[i].clients);
            listRelease(server_shards[i].clients_pending_write);
            listRelease(server_shards[i].unblocked_clients);
            listRelease(server_shards[i].clients_to_close);
        }
        mpscFree(&server_shards[i].inbox);
    }
    zfree(shard_main_call_waiting);
    shard_main_call_waiting = NULL;
}

int shardThreadsActive(void) {
    return shard_threads_active;
}

int shardCurrentId(void) {
    return shard_current_id;
}

shard *shardForEventLoop(aeEventLoop *el) {
    if (server_shards == NULL) return NULL;
    for (int i = 0; i < server.shard_threads_num; i++) {
        if (server_shards[i].el == el) return &server_shards[i];
    }
    return NULL;
}

static shard *shardCurrent(void) {
    if (server_shards == NULL) return NULL;
    return &server_shards[shard_current_id];
}

list *shardCurrentClients(void) {
    shard *s = shardCurrent();
    return s ? s->clients : server.clients;
}

list *shardCurrentClientsPendingWrite(void) {
    shard *s = shardCurrent();
    return s ? s->clients_pending_write : server.clients_pending_write;
}

list *shardCurrentUnblockedClients(void) {
    shard *s = shardCurrent();
    return s ? s->unblocked_clients : server.unblocked_clients;
}

list *shardCurrentClientsToClose(void) {
    shard *s = shardCurrent();
    return s ? s->clients_to_close : server.clients_to_close;
}

static shard *shardForClient(client *c) {
    if (server_shards == NULL || c == NULL || c->conn == NULL || c->conn->el == NULL) return shardCurrent();
    shard *s = shardForEventLoop(c->conn->el);
    return s ? s : shardCurrent();
}

list *shardClientClientsPendingWrite(client *c) {
    shard *s = shardForClient(c);
    return s ? s->clients_pending_write : server.clients_pending_write;
}

list *shardClientUnblockedClients(client *c) {
    shard *s = shardForClient(c);
    return s ? s->unblocked_clients : server.unblocked_clients;
}

list *shardClientClientsToClose(client *c) {
    shard *s = shardForClient(c);
    return s ? s->clients_to_close : server.clients_to_close;
}

size_t shardAllClientCount(void) {
    if (server_shards == NULL) return listLength(server.clients);
    size_t count = 0;
    for (int i = 0; i < server.shard_threads_num; i++) {
        count += server_shards[i].client_count;
    }
    return count;
}

int shardSelectForNewClient(void) {
    if (server_shards == NULL || server.shard_threads_num == 1) return 0;
    static unsigned int next_shard = 0;
    int best = next_shard++ % server.shard_threads_num;
    size_t best_count = server_shards[best].client_count;
    for (int i = 0; i < server.shard_threads_num; i++) {
        if (server_shards[i].client_count < best_count) {
            best = i;
            best_count = server_shards[i].client_count;
        }
    }
    return best;
}

void shardLinkClient(client *c) {
    shard *s = shardCurrent();
    if (s == NULL) s = &server_shards[0];
    listAddNodeTail(s->clients, c);
    c->client_list_node = listLast(s->clients);
    s->client_count++;
    uint64_t id = htonu64(c->id);
    pthread_mutex_lock(&clients_index_mutex);
    raxInsert(server.clients_index, (unsigned char *)&id, sizeof(id), c, NULL);
    pthread_mutex_unlock(&clients_index_mutex);
}

void shardUnlinkClient(client *c) {
    shard *s = (server_shards && c->conn && c->conn->el) ? shardForEventLoop(c->conn->el) : shardCurrent();
    if (s == NULL) s = &server_shards[0];
    if (c->client_list_node) {
        uint64_t id = htonu64(c->id);
        pthread_mutex_lock(&clients_index_mutex);
        raxRemove(server.clients_index, (unsigned char *)&id, sizeof(id), NULL);
        pthread_mutex_unlock(&clients_index_mutex);
        listDelNode(s->clients, c->client_list_node);
        c->client_list_node = NULL;
        serverAssert(s->client_count > 0);
        s->client_count--;
    }
}

client *shardLookupClientByID(uint64_t id) {
    id = htonu64(id);
    void *c = NULL;
    pthread_mutex_lock(&clients_index_mutex);
    raxFind(server.clients_index, (unsigned char *)&id, sizeof(id), &c);
    pthread_mutex_unlock(&clients_index_mutex);
    return c;
}

void shardAssertClientOnCurrentLoop(client *c) {
    if (c == NULL || c->conn == NULL || server_shards == NULL) return;
    shard *s = shardForEventLoop(c->conn->el);
    serverAssert(s == NULL || s->id == shardCurrentId());
}

static shardCommandStats *shardCommandStatsForCurrent(struct serverCommand *cmd) {
    shard *s = shardCurrent();
    if (s == NULL || s->commandstats == NULL || cmd == NULL) return NULL;
    serverAssert(cmd->id >= 0 && cmd->id < USER_COMMAND_BITS_COUNT);
    return &s->commandstats[cmd->id];
}

void shardIncrCommandStats(struct serverCommand *cmd, long long duration) {
    shardCommandStats *stats = shardCommandStatsForCurrent(cmd);
    if (stats == NULL) return;
    stats->calls++;
    stats->microseconds += duration;
}

void shardIncrCommandFailedCalls(struct serverCommand *cmd) {
    shardCommandStats *stats = shardCommandStatsForCurrent(cmd);
    if (stats == NULL) return;
    stats->failed_calls++;
}

void shardIncrCommandRejectedCalls(struct serverCommand *cmd) {
    shardCommandStats *stats = shardCommandStatsForCurrent(cmd);
    if (stats == NULL) return;
    stats->rejected_calls++;
}

shardCommandStats shardGetCommandStats(struct serverCommand *cmd) {
    shardCommandStats total = {0};
    if (server_shards == NULL || cmd == NULL) return total;
    serverAssert(cmd->id >= 0 && cmd->id < USER_COMMAND_BITS_COUNT);
    for (int i = 0; i < server.shard_threads_num; i++) {
        shardCommandStats *stats = &server_shards[i].commandstats[cmd->id];
        total.microseconds += stats->microseconds;
        total.calls += stats->calls;
        total.rejected_calls += stats->rejected_calls;
        total.failed_calls += stats->failed_calls;
    }
    return total;
}

void shardResetCommandStats(void) {
    if (server_shards == NULL) return;
    for (int i = 0; i < server.shard_threads_num; i++) {
        memset(server_shards[i].commandstats, 0, sizeof(shardCommandStats) * USER_COMMAND_BITS_COUNT);
    }
}


#define SHARD_BATCH_CACHE_SIZE 128
static _Thread_local shardRemoteBatch *batch_cache_tls[SHARD_BATCH_CACHE_SIZE];
static _Thread_local int batch_cache_count = 0;

static shardRemoteBatch *shardRemoteBatchAlloc(void) {
    if (batch_cache_count > 0) {
        shardRemoteBatch *batch = batch_cache_tls[--batch_cache_count];
        memset(batch, 0, sizeof(*batch));
        return batch;
    }
    return zcalloc(sizeof(shardRemoteBatch));
}

static void shardRemoteBatchFree(shardRemoteBatch *batch) {
    if (batch_cache_count < SHARD_BATCH_CACHE_SIZE) {
        batch_cache_tls[batch_cache_count++] = batch;
    } else {
        zfree(batch);
    }
}

#define SHARD_MSG_CACHE_SIZE 8192
static _Thread_local shardMessage *msg_cache[SHARD_MSG_CACHE_SIZE];
static _Thread_local int msg_cache_count = 0;

static shardMessage *shardMessageAlloc(void) {
    if (msg_cache_count > 0) {
        return msg_cache[--msg_cache_count];
    }
    size_t job_size = sizeof(shardExecJob) > sizeof(shardResult) ? sizeof(shardExecJob) : sizeof(shardResult);
    return zmalloc(sizeof(shardMessage) + job_size);
}

static void shardMessageFree(shardMessage *msg) {
    if (msg_cache_count < SHARD_MSG_CACHE_SIZE) {
        msg_cache[msg_cache_count++] = msg;
    } else {
        zfree(msg);
    }
}

static void shardWake(shard *s);

/* Consumer side: arm the wake flag just before blocking in poll, then re-check the
 * inbox. Pairs with the fence+exchange in shardEnqueueMessage as a Dekker handshake:
 * the seq_cst fence guarantees that of {this arm, a racing enqueue} at least one side
 * observes the other, so the consumer never sleeps through a pending message. If a
 * message landed during arming, disarm and poke our own pipe so poll returns at once
 * and the next beforeSleep drains it in full. */
static void shardArm(shard *self) {
    atomic_store_explicit(&self->needs_wake, 1, memory_order_relaxed);
    atomic_thread_fence(memory_order_seq_cst);
    if (!mpscIsEmpty(&self->inbox)) {
        atomic_store_explicit(&self->needs_wake, 0, memory_order_relaxed);
        shardWake(self);
    }
}


#define SHARD_DEFERRED_OUTBOX_MAX 32
static _Thread_local struct {
    shardMessage *msgs[SHARD_DEFERRED_OUTBOX_MAX];
    int count;
} deferred_outbox[256];

void shardFlushDeferredOutbox(int target_id) {
    if (target_id < 0 || target_id >= 256) return;
    int count = deferred_outbox[target_id].count;
    if (count == 0) return;

    shard *target = &server_shards[target_id];
    size_t enqueued = 0;
    while (enqueued < count) {
        mpscTicket ticket = {0};
        while (!mpscEnqueue(&target->inbox, deferred_outbox[target_id].msgs[enqueued], &ticket)) {
            shardWake(target);
            usleep(100);
        }
        enqueued++;
    }
    deferred_outbox[target_id].count = 0;

    atomic_thread_fence(memory_order_seq_cst);
    if (atomic_exchange_explicit(&target->needs_wake, 0, memory_order_relaxed)) shardWake(target);
}

void shardFlushAllDeferredMessages(void) {
    if (server_shards == NULL) return;
    for (int i = 0; i < server.shard_threads_num; i++) {
        shardFlushDeferredOutbox(i);
    }
}

static void shardEnqueueMessage(shard *target, shardMessage *msg) {
    int target_id = target->id;
    deferred_outbox[target_id].msgs[deferred_outbox[target_id].count++] = msg;
    if (deferred_outbox[target_id].count == SHARD_DEFERRED_OUTBOX_MAX) {
        shardFlushDeferredOutbox(target_id);
    }
}

static void shardEnqueueMessageImmediate(shard *target, shardMessage *msg) {
    mpscTicket ticket = {0};
    while (!mpscEnqueue(&target->inbox, msg, &ticket)) {
        /* Inbox full: the consumer must run to drain it, so always wake and back off. */
        shardWake(target);
        usleep(100);
    }
    /* Wake the consumer only if it has armed itself for sleep. The seq_cst fence orders
     * the enqueue above before this read (the producer half of the handshake in shardArm),
     * so a wakeup is never dropped; when the consumer is running this skips the write()
     * syscall, which is the whole point under load. */
    atomic_thread_fence(memory_order_seq_cst);
    if (atomic_load_explicit(&target->needs_wake, memory_order_relaxed)) {
        if (atomic_exchange_explicit(&target->needs_wake, 0, memory_order_relaxed)) shardWake(target);
    }
}

void shardAdoptClient(client *c) {
    /* `createClient` binds the socket to the main thread's event loop by default.
     * When threading is active, we must decouple it from the main thread immediately.
     * Later, when the designated shard adopts it, `connSetReadHandler` will correctly
     * attach the file descriptors to that shard's exclusive event loop context. */
    if (server_shards != NULL) {
        connSetReadHandler(c->conn, NULL);
    }

    int target = shardSelectForNewClient();
    if (target == 0) {
        connGetPrivateData(c->conn);
        c->conn->el = server_shards[0].el;
        connSetReadHandler(c->conn, readQueryFromClient);
        shardLinkClient(c);
        if (connAccept(c->conn, clientAcceptHandler) == C_ERR) {
            if (connGetState(c->conn) == CONN_STATE_ERROR)
                serverLog(LL_WARNING, "Error accepting a client connection: %s (addr=%s laddr=%s)",
                          connGetLastError(c->conn), getClientPeerId(c), getClientSockname(c));
            freeClient(connGetPrivateData(c->conn));
        }
        return;
    }

    shardMessage *msg = shardMessageAlloc();
    msg->type = SHARD_MSG_ADOPT_CLIENT;
    msg->data.client = c;
    shardEnqueueMessageImmediate(&server_shards[target], msg);
}

static int shardCommandSlot(client *c) {
    if (c->cmd == NULL) return -1;
    uint64_t f = c->cmd->flags;
    if (f & (CMD_MODULE | CMD_BLOCKING)) return -1;
    if (c->flag.multi) return -1;
    if (c->slot >= 0) return c->slot;

    getKeysResult keys;
    initGetKeysResult(&keys);
    int numkeys = getKeysFromCommand(c->cmd, c->argv, c->argc, &keys);
    if (numkeys <= 0) return -1;

    int slot = -1;
    for (int i = 0; i < numkeys; i++) {
        robj *key = c->argv[keys.keys[i].pos];
        int keyslot = keyHashSlot(objectGetVal(key), sdslen(objectGetVal(key)));
        if (slot == -1) {
            slot = keyslot;
        } else if (slot != keyslot) {
            slot = -1;
            break;
        }
    }
    getKeysFreeResult(&keys);
    c->slot = slot;
    return slot;
}

/* ------------------------------------------------------------------------------------
 * REMOTE read execution. The coordinator (always the main thread here -- workers own no
 * client sockets) hands a safe single-slot read to the owning shard's thread, which runs
 * it against its slots and hands back the reply bytes. argv in, RESP bytes out; neither
 * thread touches the other's client, socket, or reply buffer.
 * ------------------------------------------------------------------------------------ */

/* Break shard s's loop out of poll so it runs its beforeSleep (drain inbox / results). */
static void shardWake(shard *s) {
    char b = 'w';
    if (write(s->wake_pipe[1], &b, 1) < 0) { /* best-effort; the pipe is only a wake signal */
    }
}

static int shardDebugCommandBypassesCallBarrier(client *c) {
    if (c->cmd == NULL || c->cmd->proc != debugCommand || c->argc < 2) return 0;
    char *subcmd = objectGetVal(c->argv[1]);
    return (!strcasecmp(subcmd, "slot-shard") && c->argc == 3) ||
           (!strcasecmp(subcmd, "current-shard") && c->argc == 2) ||
           (!strcasecmp(subcmd, "shard-barrier") && c->argc == 2);
}

static int shardDebugCommandRunsOnCurrentShard(client *c) {
    if (c->cmd == NULL || c->cmd->proc != debugCommand || c->argc != 2) return 0;
    return !strcasecmp(objectGetVal(c->argv[1]), "current-shard");
}

/* True when a command that reached the barrier path still resolves to one or more keys.
 * Single-slot commands are already routed to their owner (shardDispatch), so any command
 * arriving here with keys is multi-slot: its keys span more than one slot's hashtable. */
static int shardCommandIsMultiSlot(client *c) {
    if (c->cmd == NULL) return 0;
    if (c->cmd->flags & (CMD_MODULE | CMD_BLOCKING)) return 0; /* classified by their own flags */
    getKeysResult keys;
    initGetKeysResult(&keys);
    int numkeys = getKeysFromCommand(c->cmd, c->argv, c->argc, &keys);
    getKeysFreeResult(&keys);
    return numkeys > 0;
}

static int shardCallJobNeedsBarrier(client *c) {
    if (c->cmd == NULL) return 1;
    uint64_t f = c->cmd->flags;
    if (f & (CMD_WRITE | CMD_MAY_REPLICATE | CMD_MODULE | CMD_BLOCKING)) return 1;
    /* Whole-keyspace scanners read hashtables across arbitrary slots, so they must run with
     * the workers quiesced. CMD_TOUCHES_ARBITRARY_KEYS covers SCAN/RANDOMKEY; KEYS iterates
     * every slot but is not flagged, so name it explicitly. */
    if (f & CMD_TOUCHES_ARBITRARY_KEYS) return 1;
    if (c->cmd->proc == keysCommand) return 1;
    if (c->cmd->proc == replconfCommand || c->cmd->proc == syncCommand) return 0;
    if (f & CMD_ADMIN) return c->cmd->proc != configGetCommand;
    /* A read that reached the barrier path is multi-slot (single-slot reads are routed to
     * their owner), so it touches several slots' hashtables -- some worker-owned. */
    if (shardCommandIsMultiSlot(c)) return 1;
    return 0;
}

static void shardMoveClientCommandToJob(client *c, shardExecJob *job, int result_index) {
    int idx = job->count++;
    job->entry[idx].result_index = result_index;
    job->entry[idx].dbid = c->db->id;
    job->entry[idx].slot = c->slot;
    job->entry[idx].resp = c->resp;
    job->entry[idx].cmd = c->cmd;
    job->entry[idx].argc = c->argc;
    job->entry[idx].cmd_time = server_cmd_time_snapshot;
    job->entry[idx].input_bytes = c->net_input_bytes_curr_cmd;
    job->entry[idx].qb_applied = c->qb_applied;
    serverAssert(c->original_argv == NULL);
    job->entry[idx].argv = c->argv;
    job->entry[idx].argv_is_static = isArgvStatic(c, c->argv);
    c->argv = c->argv_static;
    c->argc = 0;
    c->argv_len = 16;
    c->argv_len = 0;
    c->argv_len_sum = 0;
}

static int shardCanBatchParsedCommand(client *c, parsedCommand *p) {
#ifdef LOG_REQ_RES
    return 0;
#endif
    if (moduleHasCommandFilters()) return 0;
    if (!(p->read_flags & READ_FLAGS_PARSING_COMPLETED) || p->argc == 0) return 0;
    if (p->cmd == NULL || (p->read_flags & (READ_FLAGS_COMMAND_NOT_FOUND | READ_FLAGS_BAD_ARITY |
                                            READ_FLAGS_CROSSSLOT | READ_FLAGS_NO_KEYS)))
        return 0;
    if (c->flag.multi || (p->cmd->flags & (CMD_MODULE | CMD_BLOCKING))) return 0;
    if (slotToShard(p->slot) == shardCurrentId()) return 0;

    struct serverCommand *saved_cmd = c->cmd;
    struct serverCommand *saved_lastcmd = c->lastcmd;
    struct serverCommand *saved_realcmd = c->realcmd;
    robj **saved_argv = c->argv;
    int saved_argc = c->argc;
    int saved_read_flags = c->read_flags;
    int saved_slot = c->slot;

    c->cmd = c->lastcmd = c->realcmd = p->cmd;
    c->argv = p->argv;
    c->argc = p->argc;
    c->read_flags = p->read_flags;
    c->slot = p->slot;

    int can_batch = 1;
    if (authRequired(c) && !(p->cmd->flags & CMD_NO_AUTH)) can_batch = 0;
    int acl_errpos;
    if (can_batch && ACLCheckAllPerm(c, &acl_errpos) != ACL_OK) can_batch = 0;
    if (can_batch && server.cluster_enabled && !mustObeyClient(c)) {
        int error_code;
        clusterNode *n = getNodeByQuery(c, &error_code);
        if (n == NULL || !clusterNodeIsMyself(n)) can_batch = 0;
    }

    c->cmd = saved_cmd;
    c->lastcmd = saved_lastcmd;
    c->realcmd = saved_realcmd;
    c->argv = saved_argv;
    c->argc = saved_argc;
    c->read_flags = saved_read_flags;
    c->slot = saved_slot;
    return can_batch;
}

static void shardMoveParsedCommandToJob(client *c, parsedCommand *p, shardExecJob *job, int result_index, size_t qb_applied) {
    int idx = job->count++;
    job->entry[idx].result_index = result_index;
    job->entry[idx].dbid = c->db->id;
    job->entry[idx].slot = p->slot;
    job->entry[idx].resp = c->resp;
    job->entry[idx].cmd = p->cmd;
    job->entry[idx].argc = p->argc;
    job->entry[idx].cmd_time = server_cmd_time_snapshot;
    job->entry[idx].input_bytes = p->input_bytes;
    job->entry[idx].qb_applied = qb_applied;
    if (isArgvStatic(c, p->argv)) {
        job->entry[idx].argv = zmalloc(sizeof(robj *) * p->argc);
        memcpy(job->entry[idx].argv, p->argv, sizeof(robj *) * p->argc);
        job->entry[idx].argv_is_static = 0;
    } else {
        job->entry[idx].argv = p->argv;
        job->entry[idx].argv_is_static = 0;
    }
    p->argv = p->argv_static;
    p->argc = 0;
    p->argv_len = 16;
}

/* Coordinator side: suspend c, hand its command to `owner`'s thread. Returns C_OK; the
 * reply is delivered later by shardMainDrainResults(). */

/* Entry point from msetGenericCommand when keys map to multiple shards. */
int shardTxBegin(client *c, int *target_slots, int num_slots) {
    /* 1. Ensure target slots are sorted by the owning Shard ID ascending to prevent deadlock. */
    // Sort logic here...

    int coordinator_shard = shardCurrentId();

    /* 2. Dispatch PREPARE messages to all target shards sequentially. */
    for (int i = 0; i < num_slots; i++) {
        int target_shard = slotToShard(target_slots[i]);

        shardMessage *msg = shardMessageAlloc();
        msg->type = SHARD_MSG_TX_PREPARE;

        shardTxState *tx = zcalloc(sizeof(shardTxState));
        tx->coordinator_shard = coordinator_shard;
        tx->target_shard = target_shard;
        tx->target_slot = target_slots[i];

        msg->data.tx = tx;

        shardEnqueueMessageImmediate(&server_shards[target_shard], msg);
    }

    /* 3. Block client awaiting Tx resolving */
    blockClient(c, BLOCKED_SHARD);
    return C_OK;
}

static int shardRemoteBegin(client *c, shard *owner, int flags) {
    UNUSED(flags);
    shardRemoteBatch *batch = shardRemoteBatchAlloc();
    int coordinator_shard = shardCurrentId();
    int job_count = 1;

    int active_workers[SHARD_REMOTE_BATCH_MAX];
    shardExecJob *active_jobs[SHARD_REMOTE_BATCH_MAX];
    shardMessage *active_msgs[SHARD_REMOTE_BATCH_MAX];

    shardMessage *msg = shardMessageAlloc();
    shardExecJob *job = (shardExecJob *)(msg + 1);
    job->batch = batch;
    job->coordinator_shard = coordinator_shard;
    job->count = 0;
    active_workers[0] = owner->id;
    active_jobs[0] = job;
    active_msgs[0] = msg;
    /* Move argv ownership to the job instead of deep-copying it. The coordinator client is
     * about to block and will not touch argv again: on normal completion the reply comes
     * back before resetClient() runs, and on disconnect freeClient() -> resetClient() sees
     * a NULL argv and frees nothing. So the owning worker becomes the *sole* owner and frees
     * the objects on its own thread -- no cross-thread refcount race, and none of the per-arg
     * allocation/copy the deep copy did. Freshly parsed argv objects are private, un-encoded
     * strings here (REMOTE excludes MULTI/scripts/modules and runs before any pre-call
     * rewrite), so there is nothing to decode. The worker frees each job entry's argv. */
    shardMoveClientCommandToJob(c, job, 0);

    int command_count = 1;
    size_t qb_applied = c->qb_applied;
    cmdQueue *queue = &c->cmd_queue;
    while (command_count < SHARD_REMOTE_BATCH_MAX && queue->off < queue->len) {
        parsedCommand *p = &queue->cmds[queue->off];
        if (!shardCanBatchParsedCommand(c, p)) break;

        int command_owner = slotToShard(p->slot);
        shardExecJob *target_job = NULL;
        for (int i = 0; i < job_count; i++) {
            if (active_workers[i] == command_owner) {
                target_job = active_jobs[i];
                break;
            }
        }
        if (target_job == NULL) {
            shardMessage *next_msg = shardMessageAlloc();
            target_job = (shardExecJob *)(next_msg + 1);
            target_job->batch = batch;
            target_job->coordinator_shard = coordinator_shard;
            target_job->count = 0;
            active_workers[job_count] = command_owner;
            active_jobs[job_count] = target_job;
            active_msgs[job_count] = next_msg;
            job_count++;
        }

        qb_applied += p->input_bytes;
        shardMoveParsedCommandToJob(c, p, target_job, command_count, qb_applied);
        command_count++;
        queue->off++;
        if (queue->off == queue->len) queue->off = queue->len = 0;
    }

    batch->client_id = c->id;
    batch->client = c;
    batch->count = command_count;
    batch->pending_results = job_count;
    if (command_count > 1) {
        server.stat_shard_remote_batches++;
        server.stat_shard_remote_batched_commands += command_count;
    }
    if (job_count > 1) server.stat_shard_remote_fanout_batches++;

    blockClient(c, BLOCKED_SHARD); /* pending_command stays 0: resume finalizes, no re-exec */

    /* Pin the client for the round trip so shardProcessResult can use the pointer directly
     * instead of a mutex-guarded id lookup on every completion. The client is touched only by
     * this (coordinator) thread, so if it disconnects mid-flight freeClient() sees flag.protected
     * and defers to the async free queue (which also skips protected clients) -- the pointer stays
     * valid until the result clears the pin. A command-issuing client is never already protected
     * (protectClient() strips its read handler), so this cannot clobber an existing pin. */
    serverAssert(!c->flag.protected);
    c->flag.protected = 1;

    monotime enqueue_time = getMonotonicUs();
    for (int i = 0; i < job_count; i++) {
        active_jobs[i]->enqueue_time = enqueue_time;
        active_msgs[i]->type = SHARD_MSG_EXEC;
        active_msgs[i]->data.job = active_jobs[i];
        shardEnqueueMessage(&server_shards[active_workers[i]], active_msgs[i]);
    }
    return C_OK;
}

static void shardProcessCallJob(shardCallJob *job) {
    int bypass_barrier = shardDebugCommandBypassesCallBarrier(job->client);
    int needs_barrier = !bypass_barrier && shardCallJobNeedsBarrier(job->client);
    if (needs_barrier) {
        int parked = shardBarrierBeginExcluding(job->coordinator_shard);
        UNUSED(parked);
    }
    shard_barrier_excluded_worker = job->coordinator_shard;
    call(job->client, job->flags);
    shard_barrier_excluded_worker = -1;
    if (needs_barrier) shardBarrierEnd();

    pthread_mutex_lock(&job->mutex);
    job->done = 1;
    pthread_cond_signal(&job->cond);
    pthread_mutex_unlock(&job->mutex);
}

static int shardMainCallSync(client *c, int flags) {
    int coordinator = shardCurrentId();
    shardCallJob job;
    job.client = c;
    job.flags = flags;
    job.coordinator_shard = coordinator;
    job.done = 0;
    pthread_mutex_init(&job.mutex, NULL);
    pthread_cond_init(&job.cond, NULL);

    shardMessage *msg = shardMessageAlloc();
    msg->type = SHARD_MSG_CALL;
    msg->data.call = &job;

    pthread_mutex_lock(&barrier_mutex);
    while (barrier_active) {
        barrier_parked++;
        pthread_cond_broadcast(&barrier_cond);
        while (barrier_active) {
            pthread_mutex_unlock(&barrier_mutex);
            sched_yield();
            pthread_mutex_lock(&barrier_mutex);
        }
        /* Match shardWorkerParkIfNeeded(): account our departure so End can drain to zero. */
        barrier_parked--;
        pthread_cond_broadcast(&barrier_cond);
    }
    shard_main_call_waiting[coordinator] = 1;
    pthread_mutex_unlock(&barrier_mutex);

    shardEnqueueMessageImmediate(&server_shards[0], msg);

    pthread_mutex_lock(&job.mutex);
    while (!job.done) pthread_cond_wait(&job.cond, &job.mutex);
    pthread_mutex_unlock(&job.mutex);

    /* Clear our call-waiting flag and, if a barrier became active while we were excluded from
     * it as a call-waiting worker, park before returning to our event loop -- otherwise we
     * could resume executing commands (touching the keyspace) concurrently with the barriered
     * command still running on the main thread. barrier_mutex serialises this against
     * shardBarrierBeginExcluding(): either it sees our flag still set and excludes us -- in
     * which case we observe barrier_active here and park -- or we clear the flag first and it
     * includes us in the normal park protocol. Mirrors the prologue park above. */
    pthread_mutex_lock(&barrier_mutex);
    shard_main_call_waiting[coordinator] = 0;
    while (barrier_active) {
        barrier_parked++;
        pthread_cond_broadcast(&barrier_cond);
        while (barrier_active) {
            pthread_mutex_unlock(&barrier_mutex);
            sched_yield();
            pthread_mutex_lock(&barrier_mutex);
        }
        barrier_parked--;
        pthread_cond_broadcast(&barrier_cond);
    }
    pthread_mutex_unlock(&barrier_mutex);

    pthread_cond_destroy(&job.cond);
    pthread_mutex_destroy(&job.mutex);
    return C_OK;
}

/* Owner side (runs on the worker thread from its beforeSleep): run each queued command on
 * the executor and post the reply back to the coordinator. */
static void shardProcessExecJob(shard *self, shardMessage *msg, monotime execution_start) {
    shardExecJob *job = msg->data.job;
    client *x = self->executor;


    // We will reuse `msg` for the result!
    // We must cache coordinator_shard, batch, and job->count because they might be overwritten.
    int coordinator_shard = job->coordinator_shard;
    shardRemoteBatch *batch_cache = job->batch;
    int job_count_cache = job->count;

    shardResult *res = (shardResult *)(msg + 1);
    // Be careful, res and job overlap. We only write to res AFTER reading job fields for that entry!

    tl_stat_shard_remote_commands += job_count_cache;
    tl_stat_shard_remote_queue_us += execution_start - job->enqueue_time;


    for (int i = 0; i < job_count_cache; i++) {
        // Step 1: Read all fields from job
        int result_index = job->entry[i].result_index;
        int dbid = job->entry[i].dbid;
        uint8_t resp = job->entry[i].resp;
        int slot = job->entry[i].slot;
        struct serverCommand *cmd = job->entry[i].cmd;
        robj **argv = job->entry[i].argv;
        int argc = job->entry[i].argc;
        size_t input_bytes = job->entry[i].input_bytes;
        size_t qb_applied = job->entry[i].qb_applied;
        mstime_t cmd_time = job->entry[i].cmd_time;
        int is_static = job->entry[i].argv_is_static; // CACHE IT HERE BEFORE OVERWRITING!

        x->db = server.db[dbid];
        x->resp = resp;
        x->slot = slot;
        x->cmd = x->lastcmd = x->realcmd = cmd;
        x->argv = argv;
        x->argc = argc;
        x->net_input_bytes_curr_cmd = input_bytes;
        x->qb_applied = qb_applied;
        x->flag.argv_borrowed = 1;

        server_current_client = x;
        server_cmd_time_snapshot = cmd_time;

        call(x, CMD_CALL_FULL);


        server_current_client = NULL;

        // Step 2: Write result fields to `res`
        // Since res->entry[i] fits completely within the same byte span as job->entry[i],
        // and its fields are written AFTER we read from job->entry[i], this is safe.
        res->entry[i].result_index = result_index;
        res->entry[i].head = (x->bufpos > 0) ? sdsnewlen(x->buf, x->bufpos) : NULL;
        res->entry[i].blocks = listLength(x->reply) ? x->reply : NULL;
        if (res->entry[i].blocks) {
            x->reply = listCreate(); // Leave old list for main thread, make a fresh one
            listSetFreeMethod(x->reply, freeClientReplyValue);
        }
        res->entry[i].cmd = cmd;
        res->entry[i].slot = slot;
        res->entry[i].input_bytes = input_bytes;
        res->entry[i].qb_applied = qb_applied;

        for (int j = 0; j < x->argc; j++) decrRefCount(x->argv[j]);
        if (!is_static) zfree(x->argv);
        x->argc = 0;
        x->argv = x->argv_static;
        x->argv_len = 16;

        if (x->original_argv) {
            for (int j = 0; j < x->original_argc; j++) decrRefCount(x->original_argv[j]);
            if (!is_static) zfree(x->original_argv);
            x->original_argc = 0;
            x->original_argv = NULL;
        }

        resetClient(x); // Safe to call now, it clears remaining reply strings/state safely

        x->lastcmd = x->realcmd = NULL;
        x->bufpos = 0;
        x->reply_bytes = 0;
        x->last_header = NULL;
    }

    // Now that the loop is done, we can write global fields to `res`
    res->batch = batch_cache;
    res->count = job_count_cache;
    res->enqueue_time = getMonotonicUs();

    tl_stat_shard_remote_execution_us += res->enqueue_time - execution_start;

    msg->type = SHARD_MSG_RESULT;
    msg->data.result = res;
    shardEnqueueMessage(&server_shards[coordinator_shard], msg);
}

/* Complete a remote command without entering the generic blocked-client dispatch. Remote
 * commands have no blocked keys or module state to release; they only need their response
 * recorded, their client state reset, and their buffered input scheduled for processing. */
static void shardCompleteRemoteClient(client *c) {
    serverAssert(c->flag.blocked && c->bstate && c->bstate->btype == BLOCKED_SHARD);
    reqresAppendResponse(c);
    resetClient(c);

    if (!c->flag.module) atomic_fetch_sub_explicit(&server.blocked_clients, 1, memory_order_relaxed);
    atomic_fetch_sub_explicit(&server.blocked_clients_by_type[BLOCKED_SHARD], 1, memory_order_relaxed);
    c->flag.blocked = 0;
    c->bstate->btype = BLOCKED_NONE;
    c->bstate->unblock_on_nokey = 0;
    queueClientForReprocessing(c);
}

static void shardProcessResult(shardMessage *msg, monotime delivery_time) {
    shardResult *res = msg->data.result;
    tl_stat_shard_remote_delivery_us += delivery_time - res->enqueue_time;
    shardRemoteBatch *batch = res->batch;
    for (int i = 0; i < res->count; i++) {
        int idx = res->entry[i].result_index;
        serverAssert(idx >= 0 && idx < batch->count);
        batch->entry[idx].head = res->entry[i].head;
        batch->entry[idx].blocks = res->entry[i].blocks;
        batch->entry[idx].cmd = res->entry[i].cmd;
        batch->entry[idx].slot = res->entry[i].slot;
        batch->entry[idx].input_bytes = res->entry[i].input_bytes;
        batch->entry[idx].qb_applied = res->entry[i].qb_applied;
    }
    if (--batch->pending_results > 0) return;

    /* The client was pinned (flag.protected) by shardRemoteBegin, so the pointer is still valid
     * -- no id lookup, no clients_index_mutex on this hot path. Release the pin only after every
     * participating owner has returned. If the client disconnected mid-flight it is now
     * close_asap and the async free queue will reclaim it, so skip delivery. */
    client *c = batch->client;
    serverAssert(c->id == batch->client_id);
    c->flag.protected = 0;
    if (!c->flag.close_asap && c->flag.blocked && c->bstate && c->bstate->btype == BLOCKED_SHARD) {
        for (int i = 0; i < batch->count; i++) {
            c->cmd = c->lastcmd = c->realcmd = batch->entry[i].cmd;
            c->slot = batch->entry[i].slot;
            c->net_input_bytes_curr_cmd = batch->entry[i].input_bytes;
            c->qb_applied = batch->entry[i].qb_applied;

            /* Emit the inline-buffer head first, then the overflow blocks, in reply order. Each
             * addReplyProto applies c's own reply encoding; feeding it in pieces yields the same
             * wire bytes as one call would. */
            if (batch->entry[i].head)
                addReplyProto(c, batch->entry[i].head, sdslen(batch->entry[i].head));
            if (batch->entry[i].blocks) {
                listIter li;
                listNode *ln;
                listRewind(batch->entry[i].blocks, &li);
                while ((ln = listNext(&li)) != NULL) {
                    clientReplyBlock *b = listNodeValue(ln);
                    if (b->used) addReplyProto(c, b->buf, b->used);
                }
            }

            if (i != batch->count - 1) {
                c->duration = 0;
                reqresAppendResponse(c);
                resetClient(c);
            }
        }
        shardCompleteRemoteClient(c);
    }
    for (int i = 0; i < batch->count; i++) {
        if (batch->entry[i].head) sdsfree(batch->entry[i].head);
        if (batch->entry[i].blocks) listRelease(batch->entry[i].blocks);
    }
    shardRemoteBatchFree(batch);
}

static void shardProcessAdoptClient(shard *self, client *c) {
    c->conn->el = self->el;
    connSetReadHandler(c->conn, readQueryFromClient);
    shardLinkClient(c);
    if (connAccept(c->conn, clientAcceptHandler) == C_ERR) {
        if (connGetState(c->conn) == CONN_STATE_ERROR)
            serverLog(LL_WARNING, "Error accepting a client connection: %s (addr=%s laddr=%s)",
                      connGetLastError(c->conn), getClientPeerId(c), getClientSockname(c));
        freeClient(connGetPrivateData(c->conn));
    }
}


/* =========================================================================
 * VLL Intent Dispatch / Execution Handlers (Scaffolding)
 * ========================================================================= */
static void shardProcessTxPrepare(shard *self, shardMessage *msg) {
    UNUSED(self);
    UNUSED(msg);
    /* TODO: Set locking bit on self->tx_locked_slots, enqueue ACK to tx->coordinator */
}

static void shardProcessTxAck(shardMessage *msg) {
    UNUSED(msg);
    /* TODO: Tally ACKS. If == expected_acks, dispatch TX_COMMIT payload to shards */
}

static void shardProcessTxCommit(shard *self, shardMessage *msg) {
    UNUSED(self);
    UNUSED(msg);
    /* TODO: Execute command logic bypass, unset lock bit, drain self->deferred_tx_queue */
}

static void shardDrainInbox(shard *self) {
    void *items[SHARD_INBOX_BATCH_SIZE];
    size_t n;
    kvstoreBatchBegin();
    while ((n = mpscDequeueBatch(&self->inbox, items, SHARD_INBOX_BATCH_SIZE)) > 0) {
        monotime batch_now = getMonotonicUs();

        // PASS 0: Prefetch the shardMessage structures
        for (size_t i = 0; i < n; i++) {
            valkey_prefetch(items[i]);
        }

        // PASS 1: Prefetch the command arguments (argv[1]) for EXEC messages
        for (size_t i = 0; i < n; i++) {
            shardMessage *msg = items[i];
            if (msg->type == SHARD_MSG_EXEC) {
                shardExecJob *job = msg->data.job;
                int job_count_cache = job->count;
                for (int j = 0; j < job_count_cache; j++) {
                    int argc = job->entry[j].argc;
                    if (argc >= 2) {
                        valkey_prefetch(job->entry[j].argv[1]);
                    }
                }
            }
        }

        // PASS 2: Evaluate strings and Prefetch Dictionary Buckets
        for (size_t i = 0; i < n; i++) {
            shardMessage *msg = items[i];
            if (msg->type == SHARD_MSG_EXEC) {
                shardExecJob *job = msg->data.job;
                int job_count_cache = job->count;
                for (int j = 0; j < job_count_cache; j++) {
                    int argc = job->entry[j].argc;
                    robj **argv = job->entry[j].argv;
                    if (argc >= 2) {
                        int dbid = job->entry[j].dbid;
                        int slot = job->entry[j].slot;
                        struct serverCommand *cmd = job->entry[j].cmd;

                        if (argv[1]->type == OBJ_STRING && argv[1]->encoding != OBJ_ENCODING_INT && (cmd->flags & (CMD_WRITE | CMD_READONLY))) {
                            hashtable *ht = kvstoreGetHashtable(server.db[dbid]->keys, slot);
                            if (ht) {
                                void *key_ptr = objectGetVal(argv[1]);
                                hashtablePrefetchBucket(ht, key_ptr);
                            }
                        }
                    }
                }
            }
        }

        for (size_t i = 0; i < n; i++) {
            shardMessage *msg = items[i];
            int should_free = 1;
            if (msg->type == SHARD_MSG_ADOPT_CLIENT) {
                shardProcessAdoptClient(self, msg->data.client);
            } else if (msg->type == SHARD_MSG_CALL) {
                shardProcessCallJob(msg->data.call);
            } else if (msg->type == SHARD_MSG_EXEC) {
                shardProcessExecJob(self, msg, batch_now);
                should_free = 0; // msg is reused for SHARD_MSG_RESULT
            } else if (msg->type == SHARD_MSG_RESULT) {
                shardProcessResult(msg, batch_now);
            } else if (msg->type == SHARD_MSG_TX_PREPARE) {
                shardProcessTxPrepare(self, msg);
            } else if (msg->type == SHARD_MSG_TX_ACK) {
                shardProcessTxAck(msg);
            } else if (msg->type == SHARD_MSG_TX_COMMIT) {
                shardProcessTxCommit(self, msg);
            }
            if (should_free) shardMessageFree(msg);
        }
    }

    if (tl_stat_shard_remote_commands) {
        atomic_fetch_add_explicit(&server.stat_shard_remote_commands, tl_stat_shard_remote_commands, memory_order_relaxed);
        tl_stat_shard_remote_commands = 0;
    }
    if (tl_stat_shard_remote_queue_us) {
        atomic_fetch_add_explicit(&server.stat_shard_remote_queue_us, tl_stat_shard_remote_queue_us, memory_order_relaxed);
        tl_stat_shard_remote_queue_us = 0;
    }
    if (tl_stat_shard_remote_execution_us) {
        atomic_fetch_add_explicit(&server.stat_shard_remote_execution_us, tl_stat_shard_remote_execution_us, memory_order_relaxed);
        tl_stat_shard_remote_execution_us = 0;
    }
    if (tl_stat_shard_remote_delivery_us) {
        atomic_fetch_add_explicit(&server.stat_shard_remote_delivery_us, tl_stat_shard_remote_delivery_us, memory_order_relaxed);
        tl_stat_shard_remote_delivery_us = 0;
    }
    kvstoreBatchEnd();
}

void shardDrainCurrentInbox(void) {
    if (server_shards == NULL) return;
    shardDrainInbox(&server_shards[shardCurrentId()]);
}

void shardMainDrainResults(void) {
    shardDrainCurrentInbox();
}

void shardMainArmWake(void) {
    /* Only meaningful once workers exist: at shard-threads 1 shard 0's wake pipe is
     * never created, and nothing ever posts to its inbox. */
    if (server_shards == NULL || shard_threads_active == 0) return;
    shardArm(&server_shards[0]);
}

int shardDispatch(client *c, int flags) {
    /* shard-threads 1: identity path, a provable no-op versus calling call() directly. */
    if (server.shard_threads_num == 1) {
        call(c, flags);
        return C_OK;
    }

    if (shardDebugCommandRunsOnCurrentShard(c)) {
        call(c, flags);
        return C_OK;
    }

    /* shard-threads > 1. Single-slot commands run on the slot owner. If the
     * connection lives elsewhere, execute on the owner's socket-less executor and
     * deliver the RESP bytes back to the coordinator. Keyless/global, multi-slot,
     * MULTI, blocking, and module commands stay on shard 0 under the barrier. */
    int slot = shardCommandSlot(c);
    if (slot >= 0) {
        int owner = slotToShard(slot);
        if (owner == shardCurrentId()) {
            call(c, flags); /* LOCAL: current shard owns the slot */
            return C_OK;
        }
        return shardRemoteBegin(c, &server_shards[owner], flags); /* REMOTE */
    }

    if (c->cmd->proc == msetCommand) {
        int target_slots[512];
        int num_slots = 0;
        
        /* MSET format: MSET key value [key value ...] */
        for (int i = 1; i < c->argc; i += 2) {
            if (num_slots >= 512) break; // Defensive bound
            char *key_val = (char*)objectGetVal(c->argv[i]);
            target_slots[num_slots++] = keyHashSlot(key_val, sdslen(key_val));
        }
        
        return shardTxBegin(c, target_slots, num_slots);
    }


    if (shardCurrentId() != 0) {
        return shardMainCallSync(c, flags);
    }

    if (shardCallJobNeedsBarrier(c)) {
        int parked = shardBarrierBegin();
        UNUSED(parked);
        call(c, flags);
        shardBarrierEnd();
    } else {
        call(c, flags);
    }
    return C_OK;
}
