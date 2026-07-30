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

/* Include server.h (not <pthread.h> directly): it pulls in pthread, ae, and the client
 * type in the right order. Including <pthread.h> here instead drags in <mach/mach.h> on
 * macOS after serverassert.h's `panic` macro is defined, which then clobbers mach's
 * `void panic()` declaration. */
#include "server.h"
#include "queues.h"

struct client;

typedef struct shardCommandStats {
    long long microseconds;
    long long calls;
    long long rejected_calls;
    long long failed_calls;
} shardCommandStats;

typedef struct shard {
    int          id;           /* 0 .. server.shard_threads_num - 1 */
    pthread_t    thread;       /* valid only for id > 0 */
    aeEventLoop *el;           /* id 0: server.el; id > 0: this shard's own loop */
    int          wake_pipe[2]; /* self-pipe [read, write] to break this shard's poll (id > 0) */
    list        *clients;
    list        *clients_pending_write;
    list        *unblocked_clients;
    list        *clients_to_close;
    size_t       client_count;
    /* Multi-producer, single-consumer transport into this shard's event loop. */
    mpscQueue    inbox;
    /* Socket-less client this shard executes commands on, so execution never touches the
     * coordinator's real client. Its reply is detached as bytes and handed back. Created
     * only when shard_threads_num > 1. See shardDispatch. */
    struct client *executor;
    shardCommandStats *commandstats;
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
int shardCurrentId(void);
shard *shardForEventLoop(aeEventLoop *el);
list *shardCurrentClients(void);
list *shardCurrentClientsPendingWrite(void);
list *shardCurrentUnblockedClients(void);
list *shardCurrentClientsToClose(void);
list *shardClientClientsPendingWrite(struct client *c);
list *shardClientUnblockedClients(struct client *c);
list *shardClientClientsToClose(struct client *c);
size_t shardAllClientCount(void);
int shardSelectForNewClient(void);
void shardLinkClient(struct client *c);
void shardUnlinkClient(struct client *c);
client *shardLookupClientByID(uint64_t id);
void shardAdoptClient(struct client *c);
void shardAssertClientOnCurrentLoop(struct client *c);
void shardIncrCommandStats(struct serverCommand *cmd, long long duration);
void shardIncrCommandFailedCalls(struct serverCommand *cmd);
void shardIncrCommandRejectedCalls(struct serverCommand *cmd);
shardCommandStats shardGetCommandStats(struct serverCommand *cmd);
void shardResetCommandStats(void);

/* The escalation barrier (notes/proposal-slot-per-thread.md §6, §14.5). When the main
 * thread must run a command that could touch a worker-owned slot (writes, keyless/global,
 * multi-slot, MULTI, scripts -- anything not routed to its owner as a safe read), it
 * quiesces every worker first so no shard is executing against its slots concurrently.
 *
 * shardBarrierBegin() blocks until every worker has parked and returns the number parked;
 * the caller then runs its command with the whole keyspace to itself; shardBarrierEnd()
 * releases the workers. A no-op (returns 0 immediately) at shard-threads 1. */
int  shardBarrierBegin(void);
void shardBarrierEnd(void);

/* Called from each worker loop's beforeSleep: park here while a barrier is active. */
void shardWorkerParkIfNeeded(void);

/* Called from the main loop's beforeSleep: deliver finished REMOTE reads to their clients. */
void shardMainDrainResults(void);
void shardDrainCurrentInbox(void);

/* The one hot integration point: called from processCommand in place of call(). At
 * shard-threads 1 it is exactly `call(c, flags)` — a provable no-op. At >1 it is where
 * LOCAL / REMOTE / BARRIER routing goes; until that lands it still runs on the calling
 * thread, so worker threads stay idle and behavior is unchanged. Returns C_OK. */
int shardDispatch(struct client *c, int flags);

#endif /* SHARD_H */
