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
    c->conn = zcalloc(sizeof(connection));
    c->id = CLIENT_ID_CACHED_RESPONSE;
    return c;
}

static void shardFreeExecutor(client *c) {
    if (c == NULL) return;
    zfree(c->conn);
    c->conn = NULL;
    freeClient(c);
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

void shardInit(void) {
    int n = server.shard_threads_num;
    server_shards = zcalloc(sizeof(shard) * n);

    /* Shard 0 is the main thread; it reuses the global event loop. */
    server_shards[0].id = 0;
    server_shards[0].el = server.el;
    server_shards[0].thread = pthread_self();

    if (n == 1) return; /* Default: no extra threads, provably today's behavior. */

    /* Shard 0 can own slots too, so it needs an executor even though it has no
     * separate thread. */
    server_shards[0].executor = shardCreateExecutor();

    for (int i = 1; i < n; i++) {
        shard *s = &server_shards[i];
        s->id = i;
        s->executor = shardCreateExecutor();
        spscInit(&s->inbox, SHARD_QUEUE_SIZE);
        spscInit(&s->results, SHARD_QUEUE_SIZE);
        s->el = aeCreateEventLoop(server.maxclients + CONFIG_FDSET_INCR);
        if (s->el == NULL) serverPanic("Failed creating event loop for shard %d", i);
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

/* Run coordinator c's command on `owner`'s executor client and return the reply as a
 * flat RESP sds. Executes on the CALLING thread (the coordinator) for now: this proves
 * the execute-on-executor -> detach -> reattach path is byte-identical before a later
 * step moves the execution onto the owner's thread. The executor borrows c's argv
 * read-only; the caller still owns and frees it. `flags` is processCommand's CMD_CALL_FULL,
 * so a read that lazily expires a key still propagates the DEL normally. */
static sds shardExecReadOnExecutor(shard *owner, client *c, int flags) {
    client *x = owner->executor;
    client *prev_current = server_current_client;
    client *prev_executing = server_executing_client;

    x->db = c->db;
    x->resp = c->resp;
    x->slot = c->slot;
    x->cmd = x->lastcmd = x->realcmd = c->cmd;
    x->argv = c->argv;
    x->argc = c->argc;
    x->argv_len = c->argc;
    x->flag.executing_command = 1;

    /* Mirror the normal frame: current_client == executing_client == the client that
     * runs the command, so getKeySlot()'s cache and anything reading current_client
     * see the executor during execution. */
    server_current_client = x;
    call(x, flags);

    server_current_client = prev_current;
    server_executing_client = prev_executing;

    sds bytes = aggregateClientOutputBuffer(x);

    /* Reset the executor for reuse without touching c's borrowed argv. */
    x->argv = NULL;
    x->argc = 0;
    x->argv_len = 0;
    x->cmd = x->lastcmd = x->realcmd = NULL;
    x->flag.executing_command = 0;
    x->slot = -1;
    x->bufpos = 0;
    if (listLength(x->reply)) listEmpty(x->reply);
    x->reply_bytes = 0;
    return bytes;
}

int shardDispatch(client *c, int flags) {
    /* shard-threads 1: identity path, a provable no-op versus calling call() directly. */
    if (server.shard_threads_num == 1) {
        call(c, flags);
        return C_OK;
    }

    /* shard-threads > 1. Safe single-slot reads run on the owning shard's executor
     * client; everything else (writes, keyless/global, multi-slot, MULTI, scripts)
     * runs on the coordinator exactly as today. In this step the executor still runs
     * on the calling thread -- a later step moves it onto the owner's thread (REMOTE),
     * which is where the parallelism appears. */
    if (shardIsSafeLocalRead(c)) {
        shard *owner = &server_shards[slotToShard(c->slot)];
        sds bytes = shardExecReadOnExecutor(owner, c, flags);
        addReplyProto(c, bytes, sdslen(bytes));
        sdsfree(bytes);
        return C_OK;
    }

    call(c, flags);
    return C_OK;
}
