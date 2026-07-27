/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * Execution-shard threads and their event loops.
 *
 * An execution shard is a thread plus, eventually, the client sockets and hash
 * slots it owns exclusively. Shard 0 is the main thread and reuses the global
 * event loop (server.el); shards 1..N-1 each run their own aeEventLoop on their
 * own thread. This file is the thread/loop *scaffolding* only -- it stands the
 * threads up and tears them down cleanly. Client ownership, connection placement,
 * and command dispatch are separate, later steps; until they land, the shard
 * threads run idle loops and no command touches them.
 *
 * At the default `shard-threads 1` no thread is spawned and this is a no-op,
 * which is what keeps the default build bit-for-bit today's behavior.
 *
 * See notes/proposal-slot-per-thread.md §18 (per-thread event loops).
 */

#ifndef SHARD_H
#define SHARD_H

#include "ae.h"

#include <pthread.h>

typedef struct shard {
    int          id;           /* 0 .. server.shard_threads_num - 1 */
    pthread_t    thread;       /* valid only for id > 0 */
    aeEventLoop *el;           /* id 0: server.el; id > 0: this shard's own loop */
    int          wake_pipe[2]; /* self-pipe [read, write] to break this shard's poll (id > 0) */
} shard;

/* array[server.shard_threads_num]; NULL until shardInit(). Read-mostly after init. */
extern shard *server_shards;

/* Build the shard table and spawn the id>0 shard threads, each running an idle
 * event loop. Spawns nothing when shard_threads_num == 1. Call from InitServerLast(),
 * after server.el exists. */
void shardInit(void);

/* Stop, wake, and join every id>0 shard thread, then free their loops and pipes.
 * Idempotent. Call at the committed shutdown point, before exit(). */
void shardKillThreads(void);

/* Number of id>0 shard threads currently spawned (0 at shard-threads 1). For INFO. */
int shardThreadsActive(void);

#endif /* SHARD_H */
