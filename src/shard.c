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

#include <unistd.h>
#include <signal.h>

shard *server_shards = NULL;

static int shard_threads_active = 0;

/* Capacity of each shard's inbox/results ring. Power of two; sized generously so a
 * burst of in-flight REMOTE jobs does not hit the full-inbox backpressure path in
 * the common case. */
#define SHARD_QUEUE_SIZE 4096

/* Escalation barrier state (shard.h). One barrier at a time; only the main thread calls
 * Begin/End, and the workers only park. */
static pthread_mutex_t barrier_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  barrier_cond = PTHREAD_COND_INITIALIZER;
static int barrier_active = 0;       /* main wants all workers parked */
static int barrier_parked = 0;       /* workers currently parked */

/* No-op read handler: the self-pipe carries only a wake signal, so drain and
 * discard. Its only purpose is to give the idle loop an fd to poll and a way for
 * another thread to break that poll (for shutdown). */
static void shardWakeReadHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el);
    UNUSED(privdata);
    UNUSED(mask);
    char buf[256];
    while (read(fd, buf, sizeof(buf)) > 0) { /* drain */
    }
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

static void shardWorkerDrainInbox(shard *self);

/* Worker beforeSleep: run any queued REMOTE reads, then park if a barrier is active, then
 * poll. Draining before parking empties the inbox so no coordinator waits across a barrier;
 * running a read (on this shard's own slots) before parking is safe because the coordinator
 * that requested the barrier is still waiting for this worker to park. */
static void shardWorkerBeforeSleep(aeEventLoop *el) {
    shard *self = NULL;
    for (int i = 1; i < server.shard_threads_num; i++) {
        if (server_shards[i].el == el) {
            self = &server_shards[i];
            break;
        }
    }
    if (self) shardWorkerDrainInbox(self);
    shardWorkerParkIfNeeded();
}

static void *shardThreadMain(void *arg) {
    shard *s = arg;
    char name[32];
    snprintf(name, sizeof(name), "shard_%d", s->id);
    valkey_set_thread_title(name);

    /* Route signals to the main thread, matching bio and I/O threads. */
    sigset_t sigset;
    sigfillset(&sigset);
    pthread_sigmask(SIG_BLOCK, &sigset, NULL);
    makeThreadKillable();

    serverSetCpuAffinity(server.server_cpulist);

    /* Idle for now: only the wake pipe is registered on this loop. When a shard
     * owns clients and slots (later steps), this same loop serves them. */
    aeMain(s->el);
    return NULL;
}

void shardWorkerParkIfNeeded(void) {
    pthread_mutex_lock(&barrier_mutex);
    if (barrier_active) {
        barrier_parked++;
        pthread_cond_broadcast(&barrier_cond); /* tell the main thread we parked */
        while (barrier_active) pthread_cond_wait(&barrier_cond, &barrier_mutex);
        barrier_parked--;
    }
    pthread_mutex_unlock(&barrier_mutex);
}

int shardBarrierBegin(void) {
    int workers = shard_threads_active;
    if (workers == 0) return 0; /* shard-threads 1: nothing to quiesce. */

    pthread_mutex_lock(&barrier_mutex);
    barrier_active = 1;
    pthread_mutex_unlock(&barrier_mutex);

    /* Break every worker out of aePoll so it reaches its beforeSleep and parks. */
    for (int i = 1; i <= workers; i++) {
        char b = 'b';
        if (write(server_shards[i].wake_pipe[1], &b, 1) < 0) { /* best-effort */
        }
    }

    pthread_mutex_lock(&barrier_mutex);
    while (barrier_parked < workers) pthread_cond_wait(&barrier_cond, &barrier_mutex);
    pthread_mutex_unlock(&barrier_mutex);
    return workers;
}

void shardBarrierEnd(void) {
    pthread_mutex_lock(&barrier_mutex);
    barrier_active = 0;
    pthread_cond_broadcast(&barrier_cond); /* release the parked workers */
    pthread_mutex_unlock(&barrier_mutex);
}

void shardInit(void) {
    int n = server.shard_threads_num;
    server_shards = zcalloc(sizeof(shard) * n);

    /* Shard 0 is the main thread; it reuses the global event loop. */
    server_shards[0].id = 0;
    server_shards[0].el = server.el;
    server_shards[0].thread = pthread_self();

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
        spscInit(&s->inbox, SHARD_QUEUE_SIZE);
        spscInit(&s->results, SHARD_QUEUE_SIZE);
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
    }
}

int shardThreadsActive(void) {
    return shard_threads_active;
}

/* A command that can be routed to its slot's owner and run on an executor client
 * instead of the coordinator's client. Conservative: single resolved slot, read-only,
 * non-blocking, not inside MULTI. Read-only means the command has no client-visible
 * state beyond its reply, so running it on a different (executor) client is transparent.
 * In standalone c->slot is always -1, so this is false until virtual slots (Phase 3). */
static int shardIsSafeLocalRead(client *c) {
    if (c->slot < 0 || c->cmd == NULL) return 0;
    uint64_t f = c->cmd->flags;
    if (!(f & CMD_READONLY)) return 0;
    if (f & CMD_BLOCKING) return 0;
    if (c->flag.multi) return 0;
    return 1;
}

/* ------------------------------------------------------------------------------------
 * REMOTE read execution. The coordinator (always the main thread here -- workers own no
 * client sockets) hands a safe single-slot read to the owning shard's thread, which runs
 * it against its slots and hands back the reply bytes. argv in, RESP bytes out; neither
 * thread touches the other's client, socket, or reply buffer.
 * ------------------------------------------------------------------------------------ */

typedef struct shardExecJob {
    uint64_t              client_id;  /* coordinator client id, validated on return (§ disconnect) */
    int                   dbid, slot, resp, argc;
    struct serverCommand *cmd;
    robj                **argv;       /* deep copies owned by the job; freed by the owner */
    mstime_t              cmd_time;    /* the coordinator's command-time snapshot, for expiry */
} shardExecJob;

typedef struct shardResult {
    uint64_t client_id;
    sds      reply;                   /* reply bytes; freed by the coordinator */
} shardResult;

/* Break shard s's loop out of poll so it runs its beforeSleep (drain inbox / results). */
static void shardWake(shard *s) {
    char b = 'w';
    if (write(s->wake_pipe[1], &b, 1) < 0) { /* best-effort; the pipe is only a wake signal */
    }
}

/* Coordinator side: suspend c, hand its command to `owner`'s thread. Returns C_OK; the
 * reply is delivered later by shardMainDrainResults(). Falls back to running locally under
 * a barrier if the owner's inbox is full (backpressure), so a slow owner never blocks the
 * coordinator. */
static int shardRemoteBegin(client *c, shard *owner, int flags) {
    if (spscIsFull(&owner->inbox)) {
        int parked = shardBarrierBegin();
        UNUSED(parked);
        call(c, flags);
        shardBarrierEnd();
        return C_OK;
    }

    shardExecJob *job = zmalloc(sizeof(*job));
    job->client_id = c->id;
    job->dbid = c->db->id;
    job->slot = c->slot;
    job->resp = c->resp;
    job->cmd = c->cmd;
    job->argc = c->argc;
    job->cmd_time = server_cmd_time_snapshot;
    /* Deep-copy argv: the coordinator client (and its argv) may be freed while the job is
     * in flight, and robj refcounts are not atomic across threads. The copies are owned
     * solely by the job and freed by the owner. Correctness-first; borrowing with
     * lifetime tracking is a later optimization. */
    job->argv = zmalloc(sizeof(robj *) * c->argc);
    for (int i = 0; i < c->argc; i++) {
        robj *dec = getDecodedObject(c->argv[i]);
        job->argv[i] = createStringObject(objectGetVal(dec), sdslen(objectGetVal(dec)));
        decrRefCount(dec);
    }

    blockClient(c, BLOCKED_SHARD); /* pending_command stays 0: resume finalizes, no re-exec */
    spscEnqueue(&owner->inbox, job, /*commit=*/true);
    shardWake(owner);
    return C_OK;
}

/* Owner side (runs on the worker thread from its beforeSleep): run each queued read on the
 * executor and post the reply back to the coordinator. */
static void shardWorkerDrainInbox(shard *self) {
    void *items[64];
    size_t n;
    while ((n = spscDequeueBatch(&self->inbox, items, 64)) > 0) {
        for (size_t i = 0; i < n; i++) {
            shardExecJob *job = items[i];
            client *x = self->executor;

            x->db = server.db[job->dbid];
            x->resp = job->resp;
            x->slot = job->slot;
            x->cmd = x->lastcmd = x->realcmd = job->cmd;
            x->argv = job->argv;
            x->argc = job->argc;
            x->flag.executing_command = 1;

            /* Thread-local frame: the read path (getKeySlot cache, expiry clock) reads
             * these, and 2b-iii-1 made them per-thread and the read path shared-state-free. */
            server_current_client = x;
            server_executing_client = x;
            server_cmd_time_snapshot = job->cmd_time;

            job->cmd->proc(x); /* proc() directly, like the AOF loader: no call() global accounting */

            server_current_client = NULL;
            server_executing_client = NULL;
            x->flag.executing_command = 0;

            sds reply = aggregateClientOutputBuffer(x);

            /* Reset the executor for reuse; free the job's argv copies (owned here). */
            for (int j = 0; j < job->argc; j++) decrRefCount(job->argv[j]);
            zfree(job->argv);
            x->argv = NULL;
            x->argc = 0;
            x->cmd = x->lastcmd = x->realcmd = NULL;
            x->slot = -1;
            x->bufpos = 0;
            if (listLength(x->reply)) listEmpty(x->reply);
            x->reply_bytes = 0;

            shardResult *res = zmalloc(sizeof(*res));
            res->client_id = job->client_id;
            res->reply = reply;
            zfree(job);

            /* results ring: this owner is the sole producer, the main thread the sole
             * consumer. On overflow, drop the result (the coordinator's read will time out
             * or the client disconnects); sized so this should not happen in practice. */
            if (!spscIsFull(&self->results)) {
                spscEnqueue(&self->results, res, /*commit=*/true);
                shardWake(&server_shards[0]);
            } else {
                sdsfree(res->reply);
                zfree(res);
            }
        }
    }
}

/* Coordinator side (main thread, from beforeSleep): deliver finished REMOTE reads. */
void shardMainDrainResults(void) {
    if (server_shards == NULL) return;
    for (int i = 1; i < server.shard_threads_num; i++) {
        shard *w = &server_shards[i];
        void *items[64];
        size_t n;
        while ((n = spscDequeueBatch(&w->results, items, 64)) > 0) {
            for (size_t k = 0; k < n; k++) {
                shardResult *res = items[k];
                client *c = lookupClientByID(res->client_id);
                /* Drop if the client disconnected mid-hop, or is no longer BLOCKED_SHARD. */
                if (c && c->flag.blocked && c->bstate && c->bstate->btype == BLOCKED_SHARD) {
                    addReplyProto(c, res->reply, sdslen(res->reply));
                    unblockClient(c, 1);
                }
                sdsfree(res->reply);
                zfree(res);
            }
        }
    }
}

int shardDispatch(client *c, int flags) {
    /* shard-threads 1: identity path, a provable no-op versus calling call() directly. */
    if (server.shard_threads_num == 1) {
        call(c, flags);
        return C_OK;
    }

    /* shard-threads > 1. A safe single-slot read runs on its slot's owner:
     *  - owner is shard 0 (the main thread): run inline, no hop.
     *  - owner is a worker: hand it over (REMOTE); it executes on the owner's thread.
     * Everything else -- writes, keyless/global, multi-slot, MULTI, scripts -- takes the
     * escalation barrier so no shard is executing against its slots while it runs on main. */
    if (shardIsSafeLocalRead(c)) {
        int owner = slotToShard(c->slot);
        if (owner == 0) {
            call(c, flags); /* LOCAL: main owns the slot */
            return C_OK;
        }
        return shardRemoteBegin(c, &server_shards[owner], flags); /* REMOTE */
    }

    int parked = shardBarrierBegin();
    UNUSED(parked);
    call(c, flags);
    shardBarrierEnd();
    return C_OK;
}
