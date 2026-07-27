/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * Execution-shard thread/loop scaffolding. See shard.h.
 */

#include "server.h"
#include "shard.h"
#include "anet.h"

#include <unistd.h>
#include <signal.h>

shard *server_shards = NULL;

static int shard_threads_active = 0;

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

    for (int i = 1; i < n; i++) {
        shard *s = &server_shards[i];
        s->id = i;
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
}

int shardThreadsActive(void) {
    return shard_threads_active;
}
