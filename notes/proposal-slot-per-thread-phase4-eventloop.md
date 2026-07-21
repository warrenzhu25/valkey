# Proposal — Slot-per-thread Phase 4 (part 1): per-thread event loops

**Status: pre-issue draft.** This is the concrete, line-anchored design of the **prerequisite
half of Phase 4**: turning Valkey's single global event loop into one loop per execution shard,
so that a shard thread owns *both* a set of client sockets and a slice of slots. It is the code
form of [proposal-slot-per-thread.md](proposal-slot-per-thread.md) §5b — the section that doc
calls "the part with no Valkey analog," and "arguably a bigger lift than the slot-ownership and
journal machinery combined."

[Phase 4 (part 2)](proposal-slot-per-thread-phase4.md) is the LOCAL/REMOTE/BARRIER dispatch
branch and the REMOTE continuation. It explicitly assumes this note's work "exists or lands
alongside." **This note is that work.** Read them as one phase split by subsystem: part 1 owns
the loop and the connection, part 2 owns the command.

Prerequisites: [Phase 2](proposal-slot-per-thread-phase2.md) (`slot_to_shard[]`,
`shard-threads`) and [Phase 3](proposal-slot-per-thread-phase3.md) (virtual slots, so standalone
has a slot to route on).

All `file:line` anchors were checked against this checkout.

---

## 1. Scope

> "Per-thread event loop + multi-threaded single-key reads, no migration. Stand up Model B
> (§5b): de-globalize `server.el` into `conn->el`, per-shard `aeEventLoop`s, `SO_REUSEPORT`
> accept, round-robin placement." (parent §10.4)

**In scope:** the shard thread and its `aeEventLoop`; `conn->el`; connection placement at accept;
splitting `beforeSleep` into a global part and a per-shard part; making the client registries and
the hot-path globals safe for N loops; the cross-thread wake fd.

**Out of scope:** the dispatch branch and the REMOTE round trip (part 2); connection migration
([Phase 4a](proposal-slot-per-thread-phase4a.md)); writes, journal, propagation (Phase 5);
per-shard expiry/eviction (Phase 6). At the end of this phase a shard thread runs its own loop
and serves its own clients — but every command still executes exactly as today, because part 2
supplies the branch that makes ownership matter.

That split is deliberate: **this phase should be landable and testable with `shard-threads > 1`
and still produce byte-identical behavior**, because until part 2 lands, every command that
reaches a shard thread whose slot it does not own simply takes the barrier. Slow, correct,
and a very small correctness surface to debug the threading against.

## 2. The starting reality

Valkey has exactly one event loop and the connection layer is welded to it:

- `server.el = aeCreateEventLoop(server.maxclients + CONFIG_FDSET_INCR)` (`src/server.c:3004`);
  handlers installed at `src/server.c:3106-3107`; `aeMain(server.el)` at `src/server.c:7841`.
- The main thread is the sole acceptor: `createSocketAcceptHandler` (`src/server.c:2727`)
  registers the listener fd on `server.el` (`src/server.c:2731`), called from
  `src/server.c:3216` and `:7042`.
- `struct connection` (`src/connection.h:159-171`) has `fd`, `flags`, handlers — and **no loop**.
  Every socket operation therefore hardcodes the global: `src/socket.c:122,142,239,240,253,254,
  277,362`, mirrored in `src/tls.c:1201-1208,1250-1260` and `src/rdma.c:932,936,1208,1229,1252,
  1269,1281,1674`.
- Timers hang off it too: `serverCron` and `clientsTimeProc` (`src/server.c:3087`, `:3093`).
- **The existing I/O-thread pool does not change this.** `IOThreadMain` (`src/io_threads.c:282`)
  is a worker that pulls jobs from queues; even the poll offload `ioThreadPoll`
  (`src/io_threads.c:235`) polls *the one loop* on the main thread's behalf and stores the result
  in `server.io_poll_state`. One loop, one connection owner. This is parent §5b's **Model A**,
  and it is precisely why Model A cannot produce a LOCAL fast path: the socket owner and the data
  owner are different threads by construction, so every command is a REMOTE hop.

## 3. Verified anchors (this checkout)

- **Loop API is already per-instance.** `aeCreateEventLoop(int setsize)` (`src/ae.c:76`),
  `aeMain` (`src/ae.c:540`), `aeSetBeforeSleepProc` (`src/ae.c:551`), `aeSetAfterSleepProc`
  (`src/ae.c:555`). Creating N loops costs nothing structurally; the work is removing the
  `server.el` assumption.
- **`beforeSleep`** (`src/server.c:1854`) — the function to split. Notable contents:
  `trySendPollJobToIOThreads()` (`:1858`), the `ProcessingEventsWhileBlocked` early-return
  subset (`:1868-1883`, itself calling `handleClientsWithPendingWrites` at `:1873`),
  `processIOThreadsResponses()` (`:1886`), `connTypeProcessPendingData()` (`:1890`),
  `clusterBeforeSleep()`, `flushAppendOnlyFile(0)`, `handleClientsWithPendingWrites()` (`:1982`),
  `freeClientsInAsyncFreeQueue()`, `incrementalTrimReplicationBacklog(...)`, `evictClients()`,
  the `EL_DURATION_*` accounting, `aeSetDontWait(server.el, dont_sleep)` (`:2040`),
  `IOThreadsBeforeSleep(...)`, and `moduleReleaseGIL()` last. `afterSleep` at `src/server.c:2056`.
- **Client registries are global.** On creation: `listAddNodeTail(server.clients, c)`
  (`src/networking.c:212`), `c->client_list_node = listLast(server.clients)` (`:216`),
  `raxInsert(server.clients_index, &id, ...)` (`:218`). On free: `raxRemove` + `listDelNode`
  (`src/networking.c:1997-1998`). Lookup by id: `raxFind(server.clients_index, ...)`
  (`src/networking.c:2426`). Iterated by `clientsCron` (`src/server.c:1227-1235`, which skips
  clients with `c->io_read_state != CLIENT_IDLE` at `:1236`) and by `CLIENT LIST`-style walks
  (`src/networking.c:4548-4566`, `:5385`).
- **Per-loop client lists that must become per-shard.** `server.clients_pending_write` —
  `putClientInPendingWriteQueue` (`src/networking.c:406`), linked at `:422`, unlinked at
  `:2045-2046`, drained by `handleClientsWithPendingWrites` (`:3318-3340`);
  `server.unblocked_clients` (`src/networking.c:2055-2057`); `server.clients_to_close`
  (`src/networking.c:2250-2251`, drained at `:2386-2414`).
- **Accept path.** `acceptCommonHandler(connCreateAcceptedSocket(cfd, NULL), flags, cip)`
  (`src/socket.c:333`; `connCreateAcceptedSocket` at `src/socket.c:97`;
  `connCreateAccepted` inline at `src/connection.h:463`) → `createClient(connection *conn)`
  (`src/networking.c:285`). The existing `trySendAcceptToIOThreads` (`src/io_threads.c:793`)
  offloads *post-accept handshake work* to an I/O worker; it does not move the listener.
- **`SO_REUSEPORT` is not used today.** `anet.c` sets only `SO_REUSEADDR` —
  `anetSetReuseAddr` (`src/anet.c:383`), applied at `:434` and `:615`; listen path
  `anetListen` (`src/anet.c:552`), `_anetTcpServer` (`src/anet.c:594`), `anetTcpServer`
  (`src/anet.c:632`, declared `src/anet.h:57`).
- **Memory accounting is already thread-aware.** `src/zmalloc.c:98-116`: per-thread
  `used_memory_thread[]` indexed by a TLS thread index (atomic variant on architectures without
  safe unaligned word writes), plus `used_memory_for_additional_threads` for threads beyond
  `MAX_THREADS_NUM`. Shard threads allocating and freeing is already accounted correctly —
  one less thing to build.
- **Shared objects are immortal.** `OBJ_SHARED_REFCOUNT` (`src/server.h:775`) marks objects
  never destroyed; `makeObjectShared` (`src/object.c:161-163`) sets it, and both incr and decr
  paths skip such objects (`src/object.c:146`, `:650`, `:675`). So `shared.*` replies can be
  referenced from N threads with **no atomic refcount** — the single biggest latent hazard in
  multi-threading the reply path turns out to already be handled.
- **Transport for control messages.** `spscQueue` (`src/queues.h:112`), `spscInit` (`:128`),
  `spscEnqueue` (`:136`), `spscDequeueBatch` (`:140`); `mpscQueue`/`mpscEnqueue` (`:45`, `:67`).

## 4. Design

### 4.1 The decision that makes everything else tractable: shard 0 *is* the main thread

```text
   thread 0  (main)                    thread 1              thread N-1
   ┌───────────────────────────┐      ┌──────────────┐      ┌──────────────┐
   │ aeEventLoop = server.el   │      │ shard[1].el  │      │ shard[N-1].el│
   │  • listener fds           │      │  • its client│      │  • its client│
   │  • its own client sockets │      │    sockets   │      │    sockets   │
   │  • cluster bus, repl link │      │              │      │              │
   │  • serverCron, clientsCron│      │  (no globals)│      │  (no globals)│
   │  • AOF flush, backlog trim│      │              │      │              │
   │  • module GIL, BIO joins   │      │              │      │              │
   │ owns slots [0, 16384/N)   │      │ owns slots …│      │ owns slots … │
   └───────────────────────────┘      └──────────────┘      └──────────────┘
```

`server_shards[0].el == server.el`, and shard 0 runs on the main thread. Consequences:

- **Everything not yet ported keeps working unchanged**, because it still runs on the loop it
  always ran on: cluster bus, the replication link to a primary, replica output, module timers,
  `serverCron`, AOF flushing, `evictClients`, backlog trimming, `freeClientsInAsyncFreeQueue`.
- `shard-threads 1` is then *literally* today's server: one shard, one loop, and that loop is
  `server.el`. The no-op guarantee is structural rather than a code path that has to be kept in
  sync.
- The scary global work is concentrated on one known thread instead of being racy everywhere,
  which makes the thread-safety audit (§4.6) an enumeration rather than a sweep.

The cost is that shard 0 is asymmetric: it carries all the housekeeping *plus* its slot slice, so
it saturates first. Mitigation is a Phase-6+ concern (give shard 0 a smaller slot range, or move
housekeeping to a dedicated non-shard thread). Note it in `INFO`; do not design around it yet.

### 4.2 Change 1 — de-globalize the loop into `conn->el`

```c
/* src/connection.h:159 */
struct connection {
    ConnectionType *type;
    ConnectionState state;
    aeEventLoop *el;        /* NEW: the loop this connection is registered on */
    int last_errno;
    ...
};
```

Then every `server.el` in the connection layer becomes `conn->el`:
`src/socket.c:122,142,239,240,253,254,277`, `src/tls.c:1201-1208,1250-1260`, and
`src/rdma.c:932,936,1252` (the RDMA CM-channel and listener registrations at `:1208,1229,1281,
1674` and `src/socket.c:362` are *listener* fds, which stay on shard 0's loop — see §4.3).

Three rules keep this honest:

1. **`conn->el` is assigned exactly once, at creation**, by whoever creates the connection:
   accepted connections get their placed shard's loop (§4.3); outgoing connections (replication
   link, cluster bus, module connections) get `server.el` — i.e. shard 0 — because nothing in
   this program moves them.
2. **A connection's fd is only ever registered/unregistered on `conn->el`, only ever from the
   thread that owns that loop.** `aeCreateFileEvent`/`aeDeleteFileEvent` are not thread-safe;
   a cross-thread arm must go through a control message (that is Phase 4a's whole problem).
   Add a debug-build assertion `serverAssert(conn->el == thisShard()->el)` at the top of each
   socket-layer entry point — this single assert is what will catch the ownership bugs that
   would otherwise present as random `epoll` corruption.
3. `NULL` `conn->el` is legal and means "not registered" (the AOF-loader fake client
   `createClient(NULL)`, `src/aof.c:1491`, and part 2's per-shard executor clients).

This is the largest mechanical diff in the phase, on the hottest path in the server. It deserves
its own commit and its own full-suite run before anything else lands on top of it, at
`shard-threads 1`, where it must be provably inert.

### 4.3 Change 2 — accept and placement

**Recommendation: keep a single acceptor on shard 0 and hand the new connection to its shard.
Do not open per-shard `SO_REUSEPORT` listeners in this phase.** This revises parent §5b Change 2,
which recommends `SO_REUSEPORT`; the reasons are worth stating because the parent's argument (no
cross-thread handoff at accept) is real:

- **The handoff has to exist anyway.** Phase 4a must move *live* connections between loops. A
  brand-new connection is the degenerate case of exactly that: no partial querybuf, no pending
  replies, no blocking state, nothing in flight. Building the general mechanism and using it
  first on its easiest input is a better order than building two mechanisms.
- **`SO_REUSEPORT` does not cover the listener set.** Valkey listens on TCP, TCP6, **Unix
  sockets**, TLS, and optionally RDMA (`connListener`, `src/connection.h:176`). `SO_REUSEPORT`
  is a TCP/UDP socket option; Unix-socket and RDMA listeners would still need a handoff path, so
  the handoff path exists regardless — and then two placement mechanisms must be kept in
  agreement.
- **Rebinding gets complicated.** `CONFIG SET port` / `CONFIG SET bind` tear down and re-create
  listeners (`src/server.c:7042` re-registers accept handlers); with N kernel-balanced listeners
  that becomes an N-way dance, and a partially-rebound state is a listening socket on a stale
  loop.
- **Accept is not the bottleneck for the target workload.** This design targets long-lived
  connections issuing many commands. If a connection-churn benchmark later shows the single
  acceptor serializing, `SO_REUSEPORT` becomes a bounded optimization on top (add the option to
  `src/anet.c` next to `anetSetReuseAddr`, `:383`, and register one accept handler per shard) —
  and by then the placement policy is already written.

**Placement policy at accept.** At accept time the client's slots are unknown, so: round-robin
over shards, skewed by current client count (least-loaded of two random shards is the cheap,
well-behaved choice). Record the choice; do not attempt to be clever. Correcting a bad placement
is Phase 4a's job, and **until Phase 4a exists a misplaced client pays a REMOTE hop on every
command forever** — which is why parent §10 gates the two separately and why this phase's
benchmarks must use deliberately placed clients (§7.6) and say so when publishing numbers.

Mechanically: `acceptCommonHandler` (`src/socket.c:333`) runs on shard 0, creates the
`connection` and the `client` (`createClient`, `src/networking.c:285`) but **does not register
the fd**; it picks a target shard, sets `conn->el`, and posts a `SHARD_REQ_ADOPT` control
message to that shard's inbox. The target shard, in its own loop, arms the read handler and
adds the client to its local registries. Until the adopt message is processed the fd is
registered nowhere, which is safe (data waits in the socket buffer) as long as adoption is
never dropped — assert on inbox-full and fall back to keeping the client on shard 0.

### 4.4 Change 3 — splitting `beforeSleep`

`beforeSleep` (`src/server.c:1854`) is where the "one thread does everything" assumption is most
concentrated. Split it in two, by asking of each item: *does it touch only this shard's clients?*

**`shardBeforeSleep` — runs on every shard loop, including shard 0:**

- drain the shard inbox (adopt messages; in part 2, `SHARD_REQ_EXEC` and barrier messages)
- drain the results queue (part 2 §3.4) and resume `BLOCKED_SHARD` clients
- `processUnblockedClients` over **this shard's** unblocked list
- `handleClientsWithPendingWrites()` over **this shard's** pending-write list
  (`src/networking.c:3318`)
- `freeClientsInAsyncFreeQueue()` over **this shard's** close list
- this shard's `aeSetDontWait` decision (`src/server.c:2040`) and its own duration accounting

**`beforeSleep` — stays on shard 0 only:** `clusterBeforeSleep()`, `flushAppendOnlyFile(0)` and
the `fsynced_reploff` bookkeeping, `incrementalTrimReplicationBacklog(...)`, `evictClients()`,
`moduleReleaseGIL()`, and the `EL_DURATION_*`/`stat_active_time` aggregation.

Two items need a decision rather than a bucket:

- **`processIOThreadsResponses()` / `trySendPollJobToIOThreads()` / `IOThreadsBeforeSleep()`.**
  The existing I/O-thread pool assumes it is feeding the single loop. Making it feed N loops is a
  second threading redesign inside this one. **Decision: `io-threads` and `shard-threads` are
  mutually exclusive in this phase** — reject the combination at config load with a clear error,
  keep the I/O-thread calls on shard 0 only, and let `shard-threads > 1` imply `io-threads 1`.
  They are, after all, two answers to the same question; making them compose is a later,
  separately-justified project (see [proposal-readonly-io-execution.md](proposal-readonly-io-execution.md)
  for the design that keeps the I/O-thread pool and moves execution into it instead).
- **`ProcessingEventsWhileBlocked`** (`src/server.c:1868`). Re-entering the loop from inside a
  long command (RDB load, `DEBUG SLEEP`, module blocking) currently runs a reduced beforeSleep.
  Under N loops it must re-enter **only the calling shard's** loop, never another's. Keep the
  early-return subset, scope it to the current shard, and assert the current thread owns the loop
  being pumped.

### 4.5 Client registries: per-shard lists, global index under barrier

`server.clients` and `server.clients_index` (`src/networking.c:212-218`) are read by `CLIENT
LIST`, `CLIENT KILL`, `CLIENT NO-EVICT`, `clientsCron` (`src/server.c:1227`), `INFO clients`, and
the maxclients check (`src/networking.c:1878`). N threads creating and freeing clients cannot
share them unlocked.

The split that avoids a lock on the hot path:

- **Per-shard**: `shard->clients` (list + the client's `client_list_node`),
  `shard->clients_pending_write`, `shard->unblocked_clients`, `shard->clients_to_close`.
  All four are touched only by the owning thread, on the hot path, with no synchronization.
- **Global**: nothing on the hot path. `CLIENT LIST`/`CLIENT KILL`/`CLIENT INFO`/`clientsCron`
  become **barrier operations** (parent §6): quiesce every shard, walk every shard's list, act,
  release. They are administrative and already escalate under parent §6's rule, so this costs
  nothing that was not already conceded — and it removes the need for a global registry lock
  entirely.
- **Client-id lookup** (`raxFind(server.clients_index, ...)`, `src/networking.c:2426`) is used by
  `CLIENT KILL ID`, `CLIENT UNPAUSE`, tracking invalidation, and module APIs. Keep one rax, but
  make it **barrier-protected** like the above: writers (create/free) enqueue an index update to
  shard 0 rather than mutating it inline, and readers run under barrier. If a profile later shows
  tracking invalidation needing lock-free id lookup, that is a targeted fix, not a reason to
  lock the registry now.
- **`maxclients`** must become a global atomic count (or a barrier-time recount), since accept
  runs on shard 0 but frees happen on every shard. An atomic increment per connection lifecycle
  event is free at that rate.

### 4.6 The thread-safety audit — the enumeration this phase actually rests on

Every global reachable from the client path must be classified before a second thread runs it.
The ones that matter, and their verdicts:

| Global | Verdict |
|---|---|
| `shared.*` reply objects | **Safe as-is.** `OBJ_SHARED_REFCOUNT` (`src/server.h:775`) makes them immortal; refcount ops skip them (`src/object.c:146,675`). No atomics needed. |
| `zmalloc` accounting | **Safe as-is.** Per-thread counters (`src/zmalloc.c:98-116`). |
| `server.stat_*` counters (`stat_numcommands`, `stat_keyspace_hits`, …) | **Must become per-shard**, summed on read in `INFO`. Making them atomic instead would put a contended RMW on the hot path for numbers nobody reads per-command. |
| `server.dirty` | Per-shard; aggregated. Read at `src/server.c:3907` and `:3983` inside `call()`. Full treatment in Phase 5. |
| `server.also_propagate` (`src/server.c:3450,3471,3702`) | Per-shard. Phase 5. In this phase, only shard 0 propagates, because everything that writes takes the barrier. |
| `server.current_client` | **Must become thread-local.** It is read from deep inside the keyspace layer — e.g. the slot cache in `getKeySlot` (`src/db.c:252-257`). A shared `current_client` across N executing threads is an immediate correctness bug. |
| `server.mstime` / `server.unixtime` cached clocks | Written by `serverCron` on shard 0, read everywhere. Word-sized reads of a monotonically-updated cache — tolerable, but make the type explicit (`_Atomic` with relaxed ordering) rather than relying on it accidentally. |
| Command table (`server.commands` dict) | Read-only after startup **except** `COMMAND DOCS`-style introspection and module command registration. Module registration must take the barrier. |
| Config values (`server.maxmemory`, `server.proto_max_bulk_len`, …) | Read on the hot path, written by `CONFIG SET` on shard 0. `CONFIG SET` becomes a barrier operation — cheap and rare. |
| `server.blocked_clients` / `blocked_clients_by_type` | Per-shard counters, summed for `INFO`. |
| Latency monitor, slowlog, `commandlog` | Per-shard buffers merged on read, or barrier-protected appends. Slowlog ordering across shards becomes approximate — document it. |
| Keyspace notifications (`notifyKeyspaceEvent`) | Publishes to pub/sub, which is global. In this phase, only shard 0 executes commands that notify (everything else barriers). Phase 5 must order notifications through the journal. |
| Module GIL (`moduleReleaseGIL`) | Stays on shard 0. Every module command takes the barrier (parent §8), so no module code runs on a shard thread in this phase. |

The audit is not optional garnish — it *is* the phase. Budget for it explicitly: TSan on the
full test suite at `shard-threads 4` is the acceptance criterion (§8.4), and a clean TSan run is
worth more than any number of hand-reviewed diffs.

### 4.7 The cross-thread wake

Each shard's loop needs an `eventfd` (or self-pipe) registered on its own `el` with a no-op read
handler, so another thread can force `aeApiPoll` to return. Part 2 §3.5 specifies it for delivering
REMOTE results; it is introduced here because **adoption messages need it first** — a shard
sitting in `aeApiPoll` with no clients yet would otherwise not notice its first connection.
Coalesce writes (wake only on an empty→non-empty transition) so a burst of messages costs one
syscall.

## 5. Configuration

- `shard-threads N` gains meaning: N threads are actually spawned (`initServer`,
  `src/server.c:2924`), each with its own loop. Default stays **1**, which is today's server
  exactly (§4.1).
- Changing it at runtime is **not** supported in this phase — reject `CONFIG SET shard-threads`
  when the server has clients, or make it startup-only. Phase 2 made it a live re-partition of a
  plain array; re-partitioning while threads own slots requires a barrier plus data-ownership
  handoff, and there is no reason to build that before Phase 4a's migration machinery exists.
- `io-threads > 1` together with `shard-threads > 1` is rejected at config load (§4.4).
- `INFO server`: report `shard_threads` (Phase 2 added the field) and, per shard, client count
  and slot range — operators cannot reason about placement without it.

## 6. Files touched

- `src/shard.{c,h}` — **extend** (created in part 2 / parent §14.2): `shardThreadMain`,
  `shardBeforeSleep`, per-shard registries, the wake fd, `SHARD_REQ_ADOPT`, `thisShard()`,
  placement policy, thread spawn/join.
- `src/connection.h` — add `aeEventLoop *el` to `struct connection` (`:159`); the
  `connCreateAccepted` inline (`:463`) takes/sets it.
- `src/socket.c` — replace `server.el` with `conn->el` at `:122,142,239,240,253,254,277`; keep
  `:362` (listener teardown) on shard 0. `acceptCommonHandler` (`:333`) does placement instead of
  registration.
- `src/tls.c` — same substitution at `:1201-1208,1250-1260`.
- `src/rdma.c` — same at `:932,936,1252`; CM-channel/listener registrations (`:1208,1229,1281,
  1674`) stay on shard 0.
- `src/server.c` — `initServer` (`:2924`) spawns shard threads and creates their loops;
  `beforeSleep` (`:1854`) splits per §4.4; `clientsCron` (`:1227`) walks per-shard lists under
  barrier; `main`'s `aeMain(server.el)` (`:7841`) becomes shard 0's loop and joins the others on
  shutdown; per-shard stat aggregation for `INFO`.
- `src/networking.c` — `createClient` (`:285`) registers into the owning shard's lists rather
  than the globals (`:212-218`); `freeClient` teardown (`:1997-1998`, `:2045-2046`, `:2055-2057`,
  `:2250-2251`) likewise; `handleClientsWithPendingWrites` (`:3318`) takes the shard's list;
  `putClientInPendingWriteQueue` (`:406`) targets the current shard.
- `src/config.c` — `shard-threads` becomes startup-only (or client-gated); reject the
  `io-threads` combination.
- `src/io_threads.c` — no functional change; its entry points are called only from shard 0.
- `tests/unit/shard-eventloop.tcl` — **new**: §7.
- `src/unit/test_shard_placement.cpp` — **new**: placement policy as a pure function.

## 7. Test plan

1. **`shard-threads 1` is inert.** Full suite passes unchanged; `perf` shows no regression vs
   `main`. This is the gate for landing §4.2 alone.
2. **Adoption.** With `shard-threads 4`, connect 1000 clients; every client is adopted by exactly
   one shard, sum of per-shard client counts equals `INFO clients:connected_clients`, and each
   client's fd is registered on exactly one loop.
3. **Everything still works, just placed.** With `shard-threads 4` and part 2 not yet landed (all
   commands barrier), run the full existing suite. Behavior must be byte-identical; only latency
   changes. **This is the strongest test in the phase** — it exercises N loops against the
   entire command surface with a trivially correct execution model.
4. **Disconnect/teardown from every shard.** Kill clients on non-zero shards under load; no leaks
   (`INFO clients`, valgrind/ASan), no fd registered on a dead loop, `clients_to_close` drains on
   the right thread.
5. **Administrative commands under barrier.** `CLIENT LIST`/`CLIENT KILL`/`CLIENT INFO`/`CONFIG
   SET` with clients spread across shards return complete, consistent results.
6. **Placement benchmark, honestly labelled.** Read throughput with clients deliberately placed
   on the shard owning their keys, vs. round-robin placement. The gap between the two *is* the
   value of Phase 4a, and publishing it is how that phase gets justified.
7. **Re-entrancy.** `DEBUG SLEEP`, RDB load, and a blocked module command on a non-zero shard:
   `ProcessingEventsWhileBlocked` pumps only that shard's loop.
8. **Shutdown.** `SHUTDOWN` and `SIGTERM` join all shard threads cleanly; no loop is left polling
   an fd that has been closed.

## 8. Verification

1. `make -C src` and a CMake build.
2. `./runtest` at default config — zero behavior change.
3. `./runtest` with `--config shard-threads 4` (and the cluster suite likewise) — same results.
4. **TSan build, `shard-threads 4`, full suite.** Zero races. This is the acceptance criterion
   for §4.6; treat any finding as a blocker, not a bug to file.
5. ASan/UBSan run of the same.
6. `valkey-benchmark` for §7.6, publishing both placed and round-robin numbers with the core
   count named.
7. Manual: `INFO server` shows per-shard client counts and slot ranges; `CONFIG SET io-threads 4`
   with `shard-threads 4` is refused with a clear message.

## 9. Honest risks

- **This is the phase that can quietly break everything.** `conn->el` touches the hottest path in
  the server, and the failure mode of a mis-registered fd is a hang or corruption, not a test
  failure. The debug assertion in §4.2 rule 2 and the TSan gate are the two things standing
  between this design and a long tail of irreproducible bugs.
- **Shard 0 asymmetry.** It carries cron, AOF, cluster bus, replication, and module GIL on top of
  its slots. At high shard counts it becomes the limiter, and the benchmark will show sub-linear
  scaling for reasons that have nothing to do with the slot model.
- **Losing composability with `io-threads`.** Rejecting the combination is the honest call, but it
  means a deployment tuned on I/O threads cannot try shard threads incrementally — it is a
  configuration cliff, and it should be named as such in the release notes.
- **No migration yet.** Round-robin placement means a client whose hot slot lives elsewhere pays
  a REMOTE hop forever (parent §5a). Any benchmark published from this phase without saying
  whether clients were placed is misleading.
- **Blast radius on the connection layer.** Three transports (socket, TLS, RDMA) each carry their
  own `server.el` uses. RDMA in particular has connection-manager fds with their own lifecycle;
  keeping them on shard 0 is the conservative call, and RDMA should be tested explicitly
  (`runtest-rdma`) rather than assumed.

## 10. References

- Parent design: [proposal-slot-per-thread.md](proposal-slot-per-thread.md) §5b (Model A vs
  Model B — this note is its implementation), §5a (Dragonfly's connection layer), §10.4.
- Part 2 of this phase: [Phase 4: dispatch + REMOTE continuation](proposal-slot-per-thread-phase4.md).
- Next: [Phase 4a: connection migration](proposal-slot-per-thread-phase4a.md).
- The alternative that keeps one loop: [proposal-readonly-io-execution.md](proposal-readonly-io-execution.md).
- Background: [01-server-lifecycle-and-event-loop.md](01-server-lifecycle-and-event-loop.md),
  [03-clients-and-networking.md](03-clients-and-networking.md),
  [09-threading-and-io-model.md](09-threading-and-io-model.md), `design-docs/io-threads.md`.
- Code: `src/ae.c:76,540,551`, `src/server.c:1854,2040,2727,2731,3004,3087,3106,3216,7841`,
  `src/connection.h:159,463`, `src/socket.c:97,122,333`, `src/networking.c:212,285,406,3318`,
  `src/io_threads.c:235,282,793`, `src/anet.c:383`, `src/zmalloc.c:98`, `src/object.c:161`.
