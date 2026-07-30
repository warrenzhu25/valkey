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
static _Thread_local int shard_current_id = 0;
static int shard_barrier_excluded_worker = -1;
static int *shard_main_call_waiting = NULL;
static pthread_mutex_t clients_index_mutex = PTHREAD_MUTEX_INITIALIZER;

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

typedef enum shardMessageType {
    SHARD_MSG_ADOPT_CLIENT,
    SHARD_MSG_CALL,
    SHARD_MSG_EXEC,
    SHARD_MSG_RESULT,
} shardMessageType;

typedef struct shardExecJob {
    uint64_t              client_id;  /* coordinator client id, validated on return (§ disconnect) */
    int                   coordinator_shard;
    int                   dbid, slot, resp, argc;
    struct serverCommand *cmd;
    robj                **argv;       /* deep copies owned by the job; freed by the owner */
    mstime_t              cmd_time;    /* the coordinator's command-time snapshot, for expiry */
} shardExecJob;

typedef struct shardResult {
    uint64_t client_id;
    sds      reply;                   /* reply bytes; freed by the coordinator */
} shardResult;

typedef struct shardCallJob {
    client *client;
    int     flags;
    int     coordinator_shard;
    int     done;
    pthread_mutex_t mutex;
    pthread_cond_t  cond;
} shardCallJob;

typedef struct shardMessage {
    shardMessageType type;
    union {
        client *client;
        shardCallJob *call;
        shardExecJob *job;
        shardResult *result;
    } data;
} shardMessage;

static void shardDrainInbox(shard *self);
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

    serverSetCpuAffinity(server.server_cpulist);
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
        while (barrier_active) pthread_cond_wait(&barrier_cond, &barrier_mutex);
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

    while (barrier_parked < target_parked) pthread_cond_wait(&barrier_cond, &barrier_mutex);
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

static void shardEnqueueMessage(shard *target, shardMessage *msg) {
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
    if (atomic_exchange_explicit(&target->needs_wake, 0, memory_order_relaxed)) shardWake(target);
}

void shardAdoptClient(client *c) {
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

    shardMessage *msg = zmalloc(sizeof(*msg));
    msg->type = SHARD_MSG_ADOPT_CLIENT;
    msg->data.client = c;
    shardEnqueueMessage(&server_shards[target], msg);
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

static int shardCallJobNeedsBarrier(client *c) {
    if (c->cmd == NULL) return 1;
    uint64_t f = c->cmd->flags;
    if (f & (CMD_WRITE | CMD_MAY_REPLICATE | CMD_MODULE | CMD_BLOCKING)) return 1;
    if (c->cmd->proc == replconfCommand || c->cmd->proc == syncCommand) return 0;
    if (f & CMD_ADMIN) return c->cmd->proc != configGetCommand;
    return 0;
}

/* Coordinator side: suspend c, hand its command to `owner`'s thread. Returns C_OK; the
 * reply is delivered later by shardMainDrainResults(). */
static int shardRemoteBegin(client *c, shard *owner, int flags) {
    UNUSED(flags);
    shardExecJob *job = zmalloc(sizeof(*job));
    job->client_id = c->id;
    job->coordinator_shard = shardCurrentId();
    job->dbid = c->db->id;
    job->slot = c->slot;
    job->resp = c->resp;
    job->cmd = c->cmd;
    job->argc = c->argc;
    job->cmd_time = server_cmd_time_snapshot;
    /* Move argv ownership to the job instead of deep-copying it. The coordinator client is
     * about to block and will not touch argv again: on normal completion the reply comes
     * back before resetClient() runs, and on disconnect freeClient() -> resetClient() sees
     * a NULL argv and frees nothing. So the owning worker becomes the *sole* owner and frees
     * the objects on its own thread -- no cross-thread refcount race, and none of the per-arg
     * allocation/copy the deep copy did. Freshly parsed argv objects are private, un-encoded
     * strings here (REMOTE excludes MULTI/scripts/modules and runs before any pre-call
     * rewrite), so there is nothing to decode. The worker frees job->argv exactly as before. */
    serverAssert(c->original_argv == NULL);
    job->argv = c->argv;

    blockClient(c, BLOCKED_SHARD); /* pending_command stays 0: resume finalizes, no re-exec */

    /* Detach argv from the client now that it is blocked, before the job is published to
     * the owner: from here the worker is the sole owner. resetClient() at finalize (and on
     * disconnect) then frees a NULL argv, a no-op. */
    c->argv = NULL;
    c->argc = 0;
    c->argv_len = 0;
    c->argv_len_sum = 0;

    shardMessage *msg = zmalloc(sizeof(*msg));
    msg->type = SHARD_MSG_EXEC;
    msg->data.job = job;
    shardEnqueueMessage(owner, msg);
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

    shardMessage *msg = zmalloc(sizeof(*msg));
    msg->type = SHARD_MSG_CALL;
    msg->data.call = &job;

    pthread_mutex_lock(&barrier_mutex);
    while (barrier_active) {
        barrier_parked++;
        pthread_cond_broadcast(&barrier_cond);
        while (barrier_active) pthread_cond_wait(&barrier_cond, &barrier_mutex);
    }
    shard_main_call_waiting[coordinator] = 1;
    pthread_mutex_unlock(&barrier_mutex);

    shardEnqueueMessage(&server_shards[0], msg);

    pthread_mutex_lock(&job.mutex);
    while (!job.done) pthread_cond_wait(&job.cond, &job.mutex);
    pthread_mutex_lock(&barrier_mutex);
    shard_main_call_waiting[coordinator] = 0;
    pthread_mutex_unlock(&barrier_mutex);
    pthread_mutex_unlock(&job.mutex);

    pthread_cond_destroy(&job.cond);
    pthread_mutex_destroy(&job.mutex);
    return C_OK;
}

/* Owner side (runs on the worker thread from its beforeSleep): run each queued command on
 * the executor and post the reply back to the coordinator. */
static void shardProcessExecJob(shard *self, shardExecJob *job) {
    client *x = self->executor;

    x->db = server.db[job->dbid];
    x->resp = job->resp;
    x->slot = job->slot;
    x->cmd = x->lastcmd = x->realcmd = job->cmd;
    x->argv = job->argv;
    x->argc = job->argc;
    x->flag.argv_borrowed = 1;

    server_current_client = x;
    server_cmd_time_snapshot = job->cmd_time;

    call(x, CMD_CALL_FULL);

    server_current_client = NULL;

    sds reply = aggregateClientOutputBuffer(x);

    freeClientArgv(x);
    freeClientOriginalArgv(x);
    x->flag.argv_borrowed = 0;
    for (int j = 0; j < job->argc; j++) decrRefCount(job->argv[j]);
    zfree(job->argv);
    x->lastcmd = x->realcmd = NULL;
    x->slot = -1;
    x->bufpos = 0;
    if (listLength(x->reply)) listEmpty(x->reply);
    x->reply_bytes = 0;

    shardResult *res = zmalloc(sizeof(*res));
    res->client_id = job->client_id;
    res->reply = reply;
    int coordinator_shard = job->coordinator_shard;
    zfree(job);

    shardMessage *msg = zmalloc(sizeof(*msg));
    msg->type = SHARD_MSG_RESULT;
    msg->data.result = res;
    shardEnqueueMessage(&server_shards[coordinator_shard], msg);
}

static void shardProcessResult(shardResult *res) {
    client *c = lookupClientByID(res->client_id);
    if (c && c->flag.blocked && c->bstate && c->bstate->btype == BLOCKED_SHARD) {
        addReplyProto(c, res->reply, sdslen(res->reply));
        unblockClient(c, 1);
    }
    sdsfree(res->reply);
    zfree(res);
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

static void shardDrainInbox(shard *self) {
    void *items[64];
    size_t n;
    while ((n = mpscDequeueBatch(&self->inbox, items, 64)) > 0) {
        for (size_t i = 0; i < n; i++) {
            shardMessage *msg = items[i];
            if (msg->type == SHARD_MSG_ADOPT_CLIENT) {
                shardProcessAdoptClient(self, msg->data.client);
            } else if (msg->type == SHARD_MSG_CALL) {
                shardProcessCallJob(msg->data.call);
            } else if (msg->type == SHARD_MSG_EXEC) {
                shardProcessExecJob(self, msg->data.job);
            } else if (msg->type == SHARD_MSG_RESULT) {
                shardProcessResult(msg->data.result);
            }
            zfree(msg);
        }
    }
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
