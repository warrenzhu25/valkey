# Proposal — Slot-per-thread command execution

**Status: pre-issue draft.** Companion to
[proposal-dragonfly-inspired-perf.md](proposal-dragonfly-inspired-perf.md), which
ranks this as "Stage 4" and gates it on a Stage 0 measurement. This document is the
detailed design for that stage. It is not a commitment to build it.

Every `file:line` anchor in this document was checked against this checkout at authoring time.
Line numbers drift; if an anchor looks wrong, grep for the function name.

## Contents

**Part I — the design** (why this is credible, what it costs, and what would kill it)

| § | | § | |
|---|---|---|---|
| [1](#1-the-idea-in-one-paragraph) | The idea in one paragraph | [8](#8-the-rest-of-the-system) | The rest of the system |
| [2](#2-why-this-is-credible-for-valkey-specifically) | Why this is credible for Valkey | [9](#9-configuration) | Configuration |
| [3](#3-goals--non-goals) | Goals / non-goals | [10](#10-phasing) | Phasing — **the map of Part III** |
| [4](#4-ownership-model) | Ownership model | [11](#11-honest-risks) | Honest risks |
| [5](#5-execution-paths) | Execution paths (5a, 5b) | [12](#12-what-would-make-me-abandon-this) | What would make me abandon this |
| [6](#6-the-escalation-barrier) | The escalation barrier | [13](#13-code-anchors) | Code anchors |
| [7](#7-replication-and-aof-ordering--the-crux) | Replication & AOF ordering — **the crux** | | |

**Part II** — [14. Implementation guide (ready-to-code)](#14-implementation-guide-ready-to-code):
the data structures, the dispatch seam, the journal, the barrier, and the build order.

**Part III — the phases in detail.** Each is a self-contained implementation design with its own
file list, test plan, and gate.

| § | Phase | What it lands |
|---|---|---|
| [15](#15-phase-1-the-ordering-model-harness) | Phase 1 | The ordering-model harness — the one step that can kill the project |
| [16](#16-phase-2-the-ownership-map-and-config) | Phase 2 | `slot_to_shard[]` + `shard-threads`, as a provable no-op |
| [17](#17-phase-3-virtual-slots-for-standalone) | Phase 3 | Virtual slots for standalone — one routing path, and the end of the rehash spike |
| [18](#18-phase-4-part-1-per-thread-event-loops) | Phase 4 pt 1 | Per-thread event loops: `conn->el`, placement, splitting `beforeSleep` |
| [19](#19-phase-4-part-2-dispatch-and-the-remote-continuation) | Phase 4 pt 2 | The LOCAL/REMOTE/BARRIER branch and the REMOTE continuation |
| [20](#20-phase-4a-connection-migration) | Phase 4a | Connection migration — making arbitrary clients go LOCAL |
| [21](#21-phase-5-writes-journals-and-the-sequencer) | Phase 5 | Writes, per-shard journals, the sequencer |
| [22](#22-phase-6-per-shard-expiry-and-eviction) | Phase 6 | Per-shard expiry (cheap) and eviction (measure first) |

Phase 7 — replacing the barrier with VLL — has its own note:
[proposal-vll-transactions.md](proposal-vll-transactions.md).

---

# Part I — the design

## 1. The idea in one paragraph

Valkey's keyspace is already partitioned by hash slot: `kvstore` is an array of
hash tables, one per slot (`src/kvstore.c:294`). Today one thread executes every
command against all of them. This proposal assigns **each slot to a thread that owns
it exclusively**, so that commands touching that slot execute on that thread with no
locks and no shared mutable state. Commands that cannot be satisfied by a single
shard escalate to a barrier and run exactly as they do today.

## 2. Why this is credible for Valkey specifically

Three properties of the current code make this far less invasive than it sounds:

1. **The partition already exists.** `kvstore` is per-slot. Shard ownership is a
   mapping over an array that is already there.
2. **Cluster mode already forbids cross-slot commands.** A multi-key command whose keys
   span slots is rejected outright with `-CROSSSLOT` (`src/cluster.c:1314`), and
   `MULTI`/`EXEC` is slot-unified at EXEC time and discarded if it straddles
   (`src/cluster.c:1071-1083`). **In cluster mode, sharding by slot takes away nothing
   users have today.** This is the single most important fact in the design.
3. **Routing is already computed off the main thread.** `clusterSlotByCommand()` is
   documented as side-effect-free and safe to call from I/O threads
   (`src/cluster.c:980`), and it is already called during off-thread parsing
   (`src/server.c:4276`). The value a shard router needs is already being produced in
   the right place.

## 3. Goals / non-goals

**Goals**

- Near-linear throughput scaling with cores for single-slot commands.
- Zero behavior change at `shard-threads 1` (the default). The feature must be
  invisible until switched on.
- No new user-visible restriction in cluster mode.
- Preserve today's replication guarantees (see §7 — this is the hard part).

**Non-goals**

- A VLL/Calvin-style distributed transaction manager. The escalation barrier (§6)
  covers multi-shard work correctly, if not optimally. VLL is a later optimization,
  behind the same interface, only if profiling demands it — designed out in
  [proposal-vll-transactions.md](proposal-vll-transactions.md).
- Fibers, io_uring, or a new hash table. Orthogonal; see the companion doc.
- Making a single hot *key* faster. Nothing here helps that, and §11 is honest about it.

## 4. Ownership model

```text
             slot 0 ..... 16383            (already exists: kvstore)
             +---+---+---+---+---+---+
db->keys     | 0 | 1 | 2 | 3 |...|16383|   array of hashtables
             +---+---+---+---+---+---+
               |   |   |   |       |
   slot_to_shard[] maps each slot to exactly one owner
               |   |   |   |       |
          +----v---v-+ +-v---v-----v----+
          | shard 0  | |    shard 1     |   ... shard N-1
          | thread   | |    thread      |
          +----------+ +----------------+
```

- A **shard** is a thread plus the set of slots it owns. Ownership is exclusive:
  the owning thread reads and writes those hashtables without locks.
- `slot_to_shard[16384]` is a plain array, read-mostly. It changes only under a
  barrier (§6), which makes it safe to read lock-free from any thread.
- **Standalone mode gets virtual slots.** Today standalone sets `slot_count_bits = 0`
  (`src/server.c:2894`) — one hashtable per db. This proposal makes standalone use the
  same 16384-way `kvstore`, hashing keys to *virtual* slots used purely for routing,
  with no cluster semantics attached. This unifies the two modes on one code path and,
  as a free side effect, fixes standalone's giant-rehash problem (see the companion
  doc's dashtable discussion).

## 5. Execution paths

Each client has a **home thread** (its socket, its input buffer, its reply buffer). The
home thread acts as the command's **coordinator**. It never touches another shard's
data directly.

```text
  Parse command  (slot = clusterSlotByCommand)
        │
        ▼   route on slot ownership:
  ┌─────────────────────────────────────────────────────────────────────┐
  │ slot owned by my thread   →  LOCAL:   execute inline, zero hops       │
  │ single slot, another shard →  REMOTE:  one hop to owner; owner        │
  │                                        executes, returns result       │
  │ multi-slot (standalone),   →  BARRIER: quiesce all shards, run on     │
  │ or global / MULTI / Lua /             the coordinator exactly as      │
  │ module                                Valkey does today               │
  └─────────────────────────────────────────────────────────────────────┘
        │  (all three paths produce a write to journal)
        ▼
  journal record + commit id
        │
        ▼
  Sequencer: merge shard journals in commit-id order → repl backlog / AOF
```

**LOCAL** is the case that matters. With `shard-threads` equal to the core count and a
client distribution that mirrors the slot distribution, most commands should land here
and pay *nothing* — no queue, no atomic, no cache-line bounce. Client-to-shard affinity
should be a first-class tuning goal, not an afterthought: a cluster-aware client already
knows which node owns a slot, and the same idea applies within the process.

**REMOTE** costs one queue hop each way. The existing `io_threads` queue primitives
(`src/queues.c`, SPSC/SPMC/MPSC) are the right transport — this is what they are for.
The owning shard executes and hands back a result; the coordinator formats the reply,
so reply buffers stay single-owner and per-client reply ordering is preserved for free
(the coordinator processes a client's commands sequentially, as today).

### 5a. Connection management — how Dragonfly does it, and what Valkey would owe

The coordinator model above is the skeleton; Dragonfly's connection layer fills in the
mechanics, and two of them are load-bearing enough to name explicitly. (Vendor-sourced
from Dragonfly's helio/`Connection` design; struct names **(approx)**, the model is the
point.)

- **A thread is both a connection host and a data shard.** Dragonfly's proactor threads
  each own a set of client sockets *and* a keyspace slice. When a command runs, the
  connection's thread is the coordinator and the owning shard may be itself (LOCAL) or a
  peer (REMOTE) — exactly the split above. A connection is pinned to one **home thread** on
  accept (Dragonfly picks it for NIC/CPU locality) and all of its socket I/O, parsing, and
  **reply formatting happen only there** — which is *why* reply buffers stay single-owner:
  a shard executing a remote hop hands back a result, never touching a socket it doesn't own.

- **Fibers are what make the REMOTE hop non-blocking.** Each connection is a **fiber**, not
  a thread; one proactor multiplexes thousands of them cooperatively. When a coordinator
  awaits a remote shard, its fiber *yields* and the thread runs another connection
  meanwhile. So "one queue hop each way" costs a fiber suspend/resume, not a blocked thread.
  **Valkey has no fiber runtime.** The coordinator here would instead register a
  continuation and return to the event loop (the reply is assembled in a callback when the
  shard acks) — more callback plumbing than a fiber yield, and the piece of this proposal
  with no existing Valkey analog to lean on.

- **Connection migration is how "affinity" (above) becomes real.** Dragonfly can *move a
  connection to a different thread* when its traffic overwhelmingly targets one shard, so
  its commands become LOCAL — and to rebalance load across proactors. This is the runtime
  mechanism behind the "client-to-shard affinity as a first-class goal" line above:
  **without migration, a badly-placed client pays a REMOTE hop on every command**, and
  static placement alone won't fix a client whose hot slot lives on another thread. A Valkey
  port that wants the LOCAL-path win in practice — not just in benchmarks with hand-placed
  clients — has to build connection migration, and moving a live connection between event
  loops (its socket, partial input buffer, pending replies, blocking state) is genuinely
  fiddly. Treat it as part of the scope, not a later polish.

- **Pipeline squashing** amortizes the hop for pipelined clients: Dragonfly batches a
  pipeline and dispatches grouped hops per shard once, rather than one hop per command —
  the natural mitigation for §11's "cross-thread hop is a real per-command tax" at the low
  end.

### 5b. Event-loop and connection ownership — the part with no Valkey analog

§5/§5a describe *what* the coordinator model is; this section is *how* it lands on Valkey's
actual event-loop code, because that is where the real work of this proposal concentrates —
arguably a bigger lift than the slot-ownership and journal machinery combined.

**The starting reality.** Valkey today has exactly **one** event loop, and the connection
layer is hardwired to it:

- One global loop: `server.el = aeCreateEventLoop(...)` (`src/server.c:3004`).
- The main thread is the **sole acceptor** — the listener fd's accept handler is registered
  on `server.el` (`src/server.c:2731`); no `SO_REUSEPORT`.
- Connections carry **no loop of their own**: `struct connection` (`src/connection.h:159`)
  has an fd and handlers but no `el`, and every socket op hardcodes `server.el`
  (`src/socket.c:122,142,239,240,253,254,277`).
- The existing I/O-thread model (chapter 09 of the notes) does **not** change this:
  `ioThreadPoll(aeEventLoop *el)` (`src/io_threads.c:235`) offloads the poll of the *single*
  loop to a worker; workers pull parsed jobs from queues. Still one loop, one connection
  owner.

So a shard thread must acquire **two** independent ownerships, solved differently: *data*
ownership of its slots (§4 — a plain `slot_to_shard[]` array + exclusive hashtable access,
the easy half) and *connection* ownership of a running event loop with its own client sockets
(this section — the hard half).

**Two models, and the choice matters.**

- **Model A — worker-pull.** Shard threads are like today's io-thread workers: block on an
  inbox, pull `SHARD_REQ_EXEC` jobs, execute, post results back; the main thread stays the
  sole acceptor and owns every socket. Least invasive — reuses the queue primitives, touches
  the connection layer not at all. **But it has no LOCAL path:** the main thread owns the
  socket while a shard owns the data, so *every* command crosses main→shard→main, even a
  `GET` the coordinator could have served. This is REMOTE-for-everything, and it does not
  achieve this proposal's goal. Model A is a dead end for the fast path; it is documented
  here only to be explicitly rejected.

- **Model B — proactor-per-thread (Dragonfly's real design, and what §5 requires).** Each
  shard thread runs its **own `aeEventLoop`** and owns *both* a set of client sockets *and* a
  slice of slots:

  ```c
  void *shardThreadMain(void *arg) {
      shard *s = arg;
      s->el = aeCreateEventLoop(maxclients_per_shard);   /* its OWN loop */
      aeSetBeforeSleepProc(s->el, shardBeforeSleep);     /* drain inbox, flush this shard's replies */
      aeMain(s->el);                                     /* this thread's forever loop */
  }
  ```

  A command that arrives on a socket this thread owns *and* whose slot this thread owns runs
  **inline, zero hops** — the LOCAL fast path the whole proposal exists for. Only a key on
  another shard costs a queue hop. This is why §5 says the home thread owns "its socket, its
  input buffer, its reply buffer." **Model B is the design; the rest of this section is its
  cost.**

**Change 1 — de-globalize the event loop (the single biggest connection-layer edit).**
`aeCreateEventLoop` is already per-call, so N loops is free; the work is removing the
`server.el` assumption:

- Add `aeEventLoop *el;` to `struct connection` (`src/connection.h:159`).
- Replace **every** `server.el` in `src/socket.c` (`:122,142,239,240,253,254,277`, and the
  TLS mirror in `src/tls.c`) with `conn->el`. Mechanical, but it is the hottest I/O path in
  the server, so it needs care and its own test pass.
- Each shard loop gets its own `beforeSleep` (`shardBeforeSleep`: drain the shard inbox,
  run the sequencer's local portion, flush *this shard's* clients' pending-write list) and
  its own timer for per-shard cron work (per-shard expiry/eviction, §8).

**Change 2 — connection placement (accept).** Two options:

- **Per-thread listeners via `SO_REUSEPORT`** *(recommended here; **§18.4.3 revises this** in
  favour of a single acceptor + handoff, and says why)* — open the listener N times with
  `SO_REUSEPORT`, register each accept handler on a different shard's `el`, and let the
  **kernel** balance accepts across threads. No cross-thread handoff at accept time. Requires
  adding `SO_REUSEPORT` to `src/anet.c` (unused today) and registering N accept handlers
  instead of the single one at `src/server.c:3216`.
- **Single acceptor + handoff** — keep the main thread accepting, then migrate each new
  connection to a chosen shard (Change 4). Simpler, but the accept becomes a serialization
  point under high connection churn.

Placement policy: at accept you don't yet know the client's slots, so start round-robin /
least-loaded and correct mistakes via migration.

**Change 3 — ownership semantics.** The home thread owns the socket, the input buffer, the
parse, and the **reply buffer**. This is what keeps replies correct for free: a REMOTE hop
returns *result data*; the home thread formats and writes the reply, so a shard never touches
a socket it doesn't own and per-client reply ordering is automatic (the home thread runs one
client's commands in order — chapter 03's two-stage reply, now on a per-shard `beforeSleep`).

**Change 4 — connection migration (the genuinely fiddly part, §5a).** Moving a live
connection from thread A's loop to thread B's must atomically relocate: the fd's registration
(`aeDeleteFileEvent(A->el, fd)` on A, then `aeCreateFileEvent(B->el, fd)` on B — each executed
*on its own thread* via a control message), the partial `c->querybuf`, unflushed replies
(`c->buf`/`c->reply`), and any blocking / in-flight-REMOTE state. Protocol: **quiesce** the
connection (finish or checkpoint the in-flight command), hand the whole `client` struct's
ownership to B via a control message, then re-arm the fd on B's loop — never touching a
`client` from two threads at once. Without migration a misplaced client pays a REMOTE hop on
*every* command forever and static placement cannot fix a client whose hot slot lives on
another thread — so migration is scope, not polish.

**Phasing note.** Split the two hardest pieces: land Model B's per-thread event loop with
**read-only single-key** sharding and *no* migration first (accept round-robin, tolerate
REMOTE hops, prove the per-thread loop scales on placed clients — maps to §10 step 4), then
add migration as a separate, independently-gated step (§10 step 4.5) to make arbitrary clients
go LOCAL.

## 6. The escalation barrier

Anything that cannot be expressed as "one shard, one command" escalates:

- multi-slot commands (standalone only — cluster rejects them already)
- `MULTI`/`EXEC`, Lua scripts, functions
- module command invocations (initially all of them)
- `FLUSHALL`, `SWAPDB`, `DEBUG`, `SCAN`-family fallbacks, `RANDOMKEY`
- slot→shard rebalancing, and cluster slot migration

Protocol: the coordinator broadcasts a barrier request; each shard finishes its current
command, acks, and parks; the coordinator then runs the command **with the entire
keyspace visible, on one thread, exactly as Valkey does today**; then releases.

This is deliberately dumb. Its virtue is that **every hard command keeps running through
today's code, unchanged and already tested.** It converts a research project into an
engineering project. It is strictly slower than VLL for multi-shard work, and that is an
accepted, measurable, and *later-fixable* cost.

`SCAN` deserves a note: it need not take a barrier. The `kvstore` cursor already encodes
the table index, so `SCAN` already walks table by table. A shard-aware `SCAN` that fans
out and merges is close to free, and should be done rather than escalated.

## 7. Replication and AOF ordering — the crux

**This is the part that decides whether the design is viable.** Everything else is
mechanical.

Today the replication stream is a single totally-ordered command stream produced by one
thread (`propagateNow`, `src/server.c:3626` → `replicationFeedReplicas`,
`src/replication.c:579`), and replicas apply it single-threaded. N shard threads
producing writes concurrently have no such order.

**The naive fix and why it is not enough.** One is tempted to say: concurrent commands
touch disjoint keys, disjoint writes commute, therefore *any* merge order is a valid
serialization and we can merge shard journals in arrival order. The final state is
indeed identical. **But it silently weakens a guarantee that exists today**: if one
client issues `SET a 1` then `SET b 1` on different shards, an arrival-order merge lets a
replica observe `b=1` without `a=1`. Today that is impossible. Per-client causality
across keys is a real property that real applications depend on, and quietly dropping it
would be the kind of change that produces bug reports for years.

**The design.** Preserve a total order that is consistent with execution:

- A single **commit-id** counter (one atomic). A shard stamps a journal record with the
  next commit id **when it finishes executing** the command.
- Each shard appends its records to its own local journal ring — no contention.
- A **sequencer** drains the shard rings and emits records into the replication backlog
  and AOF **in commit-id order**, using a small reorder buffer to fill gaps.

Per-key order is preserved because one shard owns a key and its ring is FIFO. Per-client
order is preserved because the coordinator does not dispatch a client's command *k+1*
until *k* has completed — so *k* has already taken a lower commit id. The sequencer
assigns replication offsets at merge time, which keeps `master_repl_offset`, PSYNC, and
`WAIT` working on their existing contract.

> **This same journal is what makes a forkless snapshot coherent across shards.** Per
> [10-dragonfly-snapshot-model.md](10-dragonfly-snapshot-model.md), a per-shard snapshot
> establishes only a *per-shard* cut, not a global instant — so full sync is
> "each shard's point-in-time base + the journal after that shard's cut." The commit-id
> sequencer above **is** that journal layer. In other words, once slot-per-thread has
> the sequencer, the cross-shard consistency half of forkless snapshotting is already
> paid for; only the per-entry version stamp + serialize-before-mutate hook remain.

The cost is one shared atomic per write plus a reorder buffer that can stall on a gap.
Both are batchable. **A relaxed arrival-order mode could be offered as an explicit
opt-in** for users who genuinely don't need cross-key causality — but it must be opt-in
and loudly documented, never the default.

**Prototype this before writing any shard-threading code.** A standalone harness — N
concurrent disjoint-key writers, a sequencer, a replay check that the merged stream
reconstructs an identical keyspace *and* never violates per-client order — is cheap, and
if it doesn't hold up the whole proposal is dead. Learning that in a week beats learning
it in a year.

## 8. The rest of the system

| Area | Today | Under slot-per-thread |
|---|---|---|
| **Blocking** (`BLPOP`) | `server.ready_keys` + `handleClientsBlockedOnKeys()` (`src/blocked.c:383`) | Ready-key lists become per-shard. The shard that makes a key ready notifies the blocked client's coordinator via the existing queues. |
| **Expiry** | `activeExpireCycle` sweeps all slots | Per-shard cycles over owned slots. Naturally parallel; the lazy-expire path is already local to the key's shard. |
| **Eviction** | Global sampling + eviction pool (`src/evict.c:404`) | Per-shard eviction against a global `maxmemory` atomic with per-shard slack, to avoid a shared counter on the hot path. |
| **Pub/Sub** | Global | Channels are not keys; route through the coordinator. Sharded pub/sub maps to slots naturally. |
| **Keyspace notifications** | Emitted inline | Emitted by the owning shard; ordering follows the commit-id stream. |
| **`SCAN`/`DBSIZE`/`KEYS`** | Walk the kvstore | Fan out per shard, merge. `kvstore`'s cursor already encodes the table index. |
| **Modules** | Single-threaded, GIL | **Hardest surface.** Initially: every module command escalates. An opt-in "shard-safe" flag can come later. |
| **Cluster slot migration** | `design-docs/atomic-slot-migration.md` | Slot ownership now also means *thread* ownership; migration must take a barrier to reassign. |
| **Replica applying from primary** | `obey_client` bypasses slot checks (`src/server.c:4455`) | The apply path can touch any slot. Apply on a coordinator under barrier, or route per-record by slot. Needs its own design. |

## 9. Configuration

- `shard-threads N` — default `1`, which **must** be bit-for-bit today's behavior.
  The feature ships dormant.
- Slots are assigned to shards contiguously at startup. `slot_to_shard[]` is rebalanceable
  under a barrier, which is cheap: moving a slot between threads is moving a hashtable
  pointer, not moving data. This is the mitigation for a hot *slot* (§11).

## 10. Phasing

Each step is independently shippable and independently valuable. **Every step now has its own
line-anchored design note**; this list is the map, those notes are the territory.

1. **Prove the ordering model.** The §7 harness. No server changes. Do this first; it is
   the only step that can kill the project. →
   [Phase 1](#15-phase-1-the-ordering-model-harness)
2. **Introduce `slot_to_shard[]` with `shard-threads 1`.** Pure refactor. Every command
   is LOCAL. Should be a runtime no-op and fully testable against the existing suite. →
   [Phase 2](#16-phase-2-the-ownership-map-and-config)
3. **Virtual slots for standalone.** Unifies the routing path; independently fixes
   standalone's rehash spike. Watch `SCAN` cursor semantics and `RANDOMKEY`. →
   [Phase 3](#17-phase-3-virtual-slots-for-standalone)
4. **Per-thread event loop + multi-threaded single-key reads, no migration.** Stand up
   Model B (§5b): de-globalize `server.el` into `conn->el`, per-shard `aeEventLoop`s,
   round-robin placement. Read commands only — no journal, no propagation — a much smaller
   correctness surface. Tolerate REMOTE hops for badly-placed clients. This alone is most of
   the read-heavy win and proves the per-thread loop scales. Split by subsystem into
   [part 1: the loops](#18-phase-4-part-1-per-thread-event-loops) and
   [part 2: dispatch + the REMOTE continuation](#19-phase-4-part-2-dispatch-and-the-remote-continuation).
   *(Part 1 revises this section's `SO_REUSEPORT` recommendation — see its §4.3.)*
   4a. **Connection migration** (§5b Change 4) — move a live connection to the shard its
   traffic targets, so arbitrary clients go LOCAL. Independently gated because live migration
   (fd re-arm across loops, partial buffers, blocking state) is the fiddliest single piece. →
   [Phase 4a](#20-phase-4a-connection-migration)
5. **Single-key writes** + per-shard journals + the sequencer. →
   [Phase 5](#21-phase-5-writes-journals-and-the-sequencer)
6. **Per-shard expiry and eviction.** →
   [Phase 6](#22-phase-6-per-shard-expiry-and-eviction) *(which argues for splitting these two: expiry
   is a clean win, eviction should stay on the barrier until measured)*
7. *(Only if measured)* Replace the barrier with VLL-style per-shard transaction queues —
   [proposal-vll-transactions.md](proposal-vll-transactions.md).

If the project stalls after step 4, Valkey still has multi-threaded reads and a fixed
standalone rehash. That is a good place to be stranded.

## 11. Honest risks

- **Hot slots.** Slots are not uniformly hot. One hot slot pegs one thread while the rest
  idle, capping throughput at one core for that workload — where today's single thread at
  least served everything at uniform cost. Slot rebalancing (§9) helps a hot *slot*;
  **nothing here helps a hot** ***key***. This is the sharpest regression class and it must
  be measured against real traffic, not uniform benchmarks.
- **The cross-thread hop is a real per-command tax.** Today the client and its data are on
  the same thread. A REMOTE command pays a queue hop each way. **At low core counts or low
  concurrency, this design is slower than today.** It must not be the default, and its
  break-even point must be published.
- **Standalone multi-key commands regress.** `MGET a b c` spanning shards runs inline today
  and takes a barrier under this design. Cluster mode gets scaling nearly for free;
  standalone does not. Cluster mode should be the first target.
- **Modules.** The API promises single-threaded execution. Escalating every module command
  is correct but could make module-heavy workloads *slower* than today.
- **Blast radius.** This touches networking, propagation, replication, expiry, eviction,
  blocking, cluster, and modules. It is a multi-year program, not a feature.

## 12. What would make me abandon this

Written down in advance, so it can't be rationalized away later:

- Stage 0 shows keyspace work is a **minority** of main-thread time at saturation
  (Amdahl caps the whole thing).
- The §7 harness shows per-client causality cannot be preserved at acceptable cost.
- Real-traffic slot distributions turn out to be hot enough that hot-slot capping negates
  the scaling in practice.

Any one of these means Valkey's existing I/O-threads bet was the right call, and the
honest conclusion is to stop.

## 13. Code anchors

| Thing | Where |
|---|---|
| Per-slot kvstore | `src/kvstore.c:294` (`kvstoreCreate`); `slot_count_bits` `src/server.c:2894` |
| Slot routing (I/O-thread safe) | `src/cluster.c:981` (`clusterSlotByCommand`), called at `src/server.c:4282` (`prepareCommandGeneric`) |
| Client slot field | `src/server.h:1378` (`c->slot`, -1 = none); reset at `src/server.c:4304` |
| `-CROSSSLOT` / MULTI slot unification | `src/cluster.c:1314`, `src/cluster.c:1071-1083` |
| Cluster redirect gate in dispatch | `src/server.c:4455` (`getNodeByQuery`, `obey_client` at `:4419`) |
| Command dispatch | `src/server.c:4315` (`processCommand`), `:3875` (`call`) |
| Propagation | `src/server.c:3626` (`propagateNow`), `src/server.c:3680` (`alsoPropagate`), flushed by `propagatePendingCommands` `:3746` in `postExecutionUnitOperations` `:3799` |
| Replication feed | `src/replication.c:579` (`replicationFeedReplicas`) |
| Blocking / ready keys | `src/blocked.c:383` (`handleClientsBlockedOnKeys`) |
| Eviction | `src/evict.c:404` (`performEvictions`) |
| Queue primitives to reuse | `src/queues.c` (SPSC/SPMC/MPSC), `src/io_threads.c` |
| Single global event loop (to de-globalize, §5b) | `src/server.c:3004` (`server.el`); sole acceptor registered `:2731`, `:3216` |
| Connection struct (add `el`, §5b) | `src/connection.h:159`; hardcoded `server.el` in `src/socket.c:122,142,239,240,253,254,277` |
| Current poll offload (not per-thread loops) | `src/io_threads.c:235` (`ioThreadPoll`) |

---

# Part II — the implementation guide

## 14. Implementation guide (ready-to-code)

This section turns the design above into concrete artifacts: the data structures, the new
files and function signatures, the exact integration diffs, and a build order. It assumes
Phase 2 (`slot_to_shard[]` at `shard-threads 1`) as the first landing and builds up.

### 14.1 New files

| File | Contents |
|---|---|
| `src/shard.h` / `src/shard.c` | The shard table, ownership map, shard-thread main loop, dispatch API. |
| `src/shard_journal.h` / `src/shard_journal.c` | Per-shard journal ring + the commit-id sequencer (§7). |
| `tests/unit/shard-ordering.c` (or a standalone harness under `tests/helpers/`) | The §7 replay/ordering proof (Phase 1). |

Everything else is edits to existing files (§14.6).

### 14.2 Core data structures (`src/shard.h`)

```c
/* One shard = one thread that owns (a) a slice of slots and (b) a set of client
 * connections. Model B / proactor-per-thread (§5b). */
typedef struct shard {
    int             id;                 /* 0 .. server.shard_threads-1 */
    pthread_t       thread;             /* the shard thread; id 0 may be the main thread */
    aeEventLoop    *el;                 /* THIS shard's own event loop (§5b Change 1) */
    list           *clients;            /* connections homed on this thread */
    list           *clients_pending_write; /* this shard's reply-flush list (per-shard beforeSleep) */
    spscQueue       inbox;              /* coordinator -> this shard (REMOTE reqs, barrier, results) */
    shardJournal   *journal;            /* this shard's commit ring (§14.4) */
    _Atomic uint8_t barrier_state;      /* RUNNING / PARKED (see §6 protocol) */
    /* Per-shard subsystem state migrated off globals: */
    list           *ready_keys;         /* was server.ready_keys (blocking, §8) */
    long long       expire_cursor;      /* per-shard active-expire cursor (§8) */
    long long       mem_slack;          /* per-shard maxmemory slack (§8 eviction) */
} shard;

/* Global ownership map. Read-mostly; mutated only under a barrier (§6, §9). */
extern shard   *server_shards;          /* array[server.shard_threads] */
extern uint16_t slot_to_shard[16384];   /* slot -> shard id; plain array, lock-free read */

/* Job passed on a shard inbox. Reuse the tagged-pointer trick from io_threads.c:38. */
typedef enum { SHARD_REQ_EXEC, SHARD_REQ_BARRIER_ENTER, SHARD_REQ_BARRIER_LEAVE } shardReqType;
typedef struct shardExecJob {
    client   *coordinator;   /* who to hand the result back to */
    robj    **argv; int argc;
    int       slot;
    uint64_t  seq;           /* filled by the owner at commit time (§14.4) */
} shardExecJob;
```

`slot_to_shard` being a plain `uint16_t[16384]` (32 KB) read without a lock is safe because
it changes only under a full barrier when every shard is parked — no reader and writer ever
race. This is the same discipline the design already states in §4.

### 14.3 Dispatch — the one hot integration point

The router lives at the top of command execution, replacing the straight-line call into
`call()`. Today `processCommand` (`src/server.c:4315`) ends by invoking `call(c, …)` on the
main thread. Under sharding, `processCommandAndResetClient` (`src/networking.c:3932`) instead
routes by the already-computed `c->slot`:

```c
/* New: src/shard.c — called from processCommand once all gate checks pass. */
int shardDispatch(client *c) {
    /* shard-threads 1: identity path, must be a runtime no-op vs today. */
    if (server.shard_threads == 1) { call(c, CMD_CALL_FULL); return C_OK; }

    int slot = c->slot;                       /* clusterSlotByCommand already ran */
    if (slot < 0 || commandNeedsBarrier(c)) return shardBarrierRun(c);   /* §6 / §14.5 */

    int owner = slot_to_shard[slot];
    if (owner == myShardId()) {               /* LOCAL: the fast path, zero hops */
        call(c, CMD_CALL_FULL);
        return C_OK;
    }
    /* REMOTE: one hop to the owner; result comes back as a continuation (§5a). */
    shardExecJob *job = shardExecJobNew(c);
    c->flag.awaiting_shard = 1;               /* new client flag; suspend this client */
    spscEnqueue(&server_shards[owner].inbox, tagJob(job, SHARD_REQ_EXEC));
    return C_OK;                              /* no reply yet; delivered on ack */
}
```

`commandNeedsBarrier(c)` is a predicate over `c->cmd->flags` and argv: true for multi-slot
keysets (standalone), `CMD_CALL`-recursive commands (`EXEC`, `EVAL`, `FCALL`), module
commands (initially), and the global set in §6. Give the command table a new flag
`CMD_SHARD_SAFE` and default it **off**; a command is barrier-free only if it's flagged safe
*and* single-slot.

**The REMOTE continuation** is the piece with no Valkey analog (§5a). Concretely: the owner
shard runs `call()` against its data, captures the reply into a detachable buffer, and posts
a `SHARD_RES_DONE` job back to the coordinator's inbox carrying `(client*, reply-bytes,
seq)`. The coordinator, in its event loop, drains results, appends the bytes to the client's
real reply buffer (`_addReplyProtoToList`, chapter 03), clears `awaiting_shard`, and resumes
that client's next queued command. Because the coordinator processes one client's commands
strictly in order, per-client reply ordering is preserved with no extra machinery.

### 14.4 The journal + sequencer (`src/shard_journal.c`) — §7 made concrete

```c
typedef struct journalRec {
    uint64_t  seq;           /* global commit id, from the shared atomic */
    int       dbid, slot, target;
    robj    **argv; int argc;   /* the *deterministic* form (post-rewrite, ch.02) */
} journalRec;

typedef struct shardJournal {          /* one per shard; single-producer ring */
    journalRec *ring; size_t head, tail, cap;
} shardJournal;

extern _Atomic uint64_t server_commit_id;   /* THE single shared atomic (§7) */
```

Where it hooks in: today `call()` accumulates propagation and `propagatePendingCommands`
(`src/server.c:3746`) writes it to the backlog+AOF at unit end. Under sharding, the owner
shard instead does:

1. At the moment execution finishes (inside its `postExecutionUnitOperations` equivalent),
   `seq = atomic_fetch_add(&server_commit_id, 1)`.
2. Append the deterministic argv to *its own* `journal->ring` stamped with `seq`. No lock —
   single producer.

The **sequencer** runs on the main/coordinator thread each `beforeSleep`:

```c
/* Drain all shard rings and emit to backlog+AOF in seq order. */
void shardSequencerFlush(void) {
    static uint64_t expected = 0;             /* next seq to emit */
    /* Min-heap over the head record of each shard ring, keyed by seq. */
    for (;;) {
        journalRec *r = sequencerPeekMin();    /* lowest seq across all ring heads */
        if (!r || r->seq != expected) break;   /* gap: wait for the missing shard */
        replicationFeedReplicas(r->dbid, r->argv, r->argc);   /* existing feed, src/replication.c:579 */
        feedAppendOnlyFileIfEnabled(r);
        sequencerPopMin();
        expected++;
    }
}
```

The reorder buffer is exactly "wait until `expected` is present at some ring head." Per-key
order holds because one shard owns a key and its ring is FIFO; per-client order holds because
the coordinator never dispatched command *k+1* before *k* committed, so *k* took the lower
`seq`. `master_repl_offset` is assigned here at emit time — PSYNC/`WAIT` contracts unchanged.

> Phase-1 note: this `shardSequencerFlush` + a set of fake in-memory "shards" is *exactly*
> the §7 harness. Build it standalone first (`tests/`), feed it N disjoint-key writer
> streams, and assert the merged output replays to an identical keyspace **and** never shows
> a client's write *k+1* without *k*. If that assertion can't be met cheaply, stop (§12).

### 14.5 The barrier (`src/shard.c`) — §6 made concrete

```c
int shardBarrierRun(client *c) {
    /* 1. Broadcast ENTER to every shard; each finishes its current command and parks. */
    for (int i = 0; i < server.shard_threads; i++)
        if (i != myShardId()) spscEnqueue(&server_shards[i].inbox, tagJob(NULL, SHARD_REQ_BARRIER_ENTER));
    shardWaitAllParked();                 /* spin/wait on each barrier_state == PARKED */

    /* 2. Drain every journal ring through the sequencer so the pre-barrier stream is
     *    fully ordered before the barrier command runs (barrier commands see a quiesced,
     *    totally-ordered keyspace). */
    shardSequencerFlush();

    /* 3. Run the command on this coordinator with the WHOLE keyspace visible — today's
     *    code path, unchanged. This is the crux of "convert research project to
     *    engineering project." */
    call(c, CMD_CALL_FULL);

    /* 4. Release. */
    for (int i = 0; i < server.shard_threads; i++)
        if (i != myShardId()) spscEnqueue(&server_shards[i].inbox, tagJob(NULL, SHARD_REQ_BARRIER_LEAVE));
    return C_OK;
}
```

A shard's worker loop checks its inbox between commands; on `BARRIER_ENTER` it sets
`barrier_state = PARKED` and blocks on `BARRIER_LEAVE`. This reuses the io-threads parking
idiom (mutex held by the waker, chapter 09).

### 14.6 File-by-file change list

| File | Change |
|---|---|
| `src/shard.{c,h}` | **New.** §14.2 structures, `shardDispatch`, `shardBarrierRun`, worker loop, `slot_to_shard` + ownership init/rebalance. |
| `src/shard_journal.{c,h}` | **New.** §14.4 ring + `shardSequencerFlush`, `server_commit_id`. |
| `src/server.c` | `processCommand` (`:4315`) tail calls `shardDispatch(c)` instead of `call()` directly. `beforeSleep` (`:1854`) calls `shardSequencerFlush()` before `flushAppendOnlyFile`. `initServer` (`:2924`) spawns shard threads + inits ownership. Add `server.shard_threads`. |
| `src/server.h` | `c->flag.awaiting_shard`; `struct valkeyServer` gains `shard_threads`, `server_shards`. New command flag `CMD_SHARD_SAFE`. |
| `src/config.c` | `createIntConfig("shard-threads", NULL, DEBUG_CONFIG \| MODIFIABLE_CONFIG, 1, SHARD_THREADS_MAX, server.shard_threads, 1, INTEGER_CONFIG, NULL, updateShardThreads)` — mirrors `io-threads` at `:3457`; default **1**. |
| `src/networking.c` | `processCommandAndResetClient` (`:3932`) understands `awaiting_shard` (don't reset/return-to-loop until the result arrives). Per-shard result-drain + reply-flush in `shardBeforeSleep`. |
| `src/connection.h` | **§5b Change 1.** Add `aeEventLoop *el;` to `struct connection` (`:159`); connections bind to their home shard's loop, not the global one. |
| `src/socket.c` (+ `src/tls.c`) | **§5b Change 1.** Replace every hardcoded `server.el` (`:122,142,239,240,253,254,277`) with `conn->el`. The hottest I/O path — own test pass. |
| `src/server.c` (accept) | **§5b Change 2**, **revised by §18.4.3 to a single acceptor + handoff.** The `SO_REUSEPORT` variant (N accept handlers, one per shard `el`, plus `SO_REUSEPORT` in `src/anet.c`) becomes a later optimization for connection-churn-heavy workloads. |
| `src/shard.c` (migration) | **§5b Change 4.** `shardMigrateConnection(client*, dstShard)`: quiesce, hand `client` ownership via control message, re-arm fd on the destination `el`. Phase 4a. |
| `src/blocked.c` | `ready_keys` becomes per-shard (§8); wake goes through the coordinator queue. |
| `src/expire.c`, `src/evict.c` | Per-shard cursors / per-shard `maxmemory` slack (§8). Phase 6. |
| `src/kvstore.c` / `src/server.c:2894` | Standalone uses the 16384-way kvstore (virtual slots, §4). Phase 3. |
| Command JSON (`src/commands/*.json`) | Add `SHARD_SAFE` to the trivially-safe single-key commands (`GET`, `SET`, `INCR`, …) as Phases 4–5 enable them. Regenerate `commands.def`. |

### 14.7 Build order (maps to §10 phasing, with the concrete gate per step)

1. **§7 harness** ([Phase 1](#15-phase-1-the-ordering-model-harness)) — `shard_journal.c` + fake
   shards only. Gate: merged replay is identical *and* per-client-causal. **No server changes.**
2. **`slot_to_shard[]` + `shardDispatch` with `shard-threads 1`**
   ([Phase 2](#16-phase-2-the-ownership-map-and-config)) — identity path
   (`call()` inline). Gate: entire existing test suite passes unchanged; `perf` shows no
   regression vs `main` (the added branch is one predictable compare).
3. **Virtual slots for standalone** ([Phase 3](#17-phase-3-virtual-slots-for-standalone)) — flip
   `slot_count_bits`. Gate: `SCAN`/`RANDOMKEY` semantics unchanged (they already ride the
   kvstore cursor); empty-instance RSS delta measured and accepted.
4. **Per-thread event loop + single-key *reads*, no migration** (§5b Model B) —
   [part 1](#18-phase-4-part-1-per-thread-event-loops): `conn->el`, per-shard
   `aeEventLoop`, accept + placement; [part 2](#19-phase-4-part-2-dispatch-and-the-remote-continuation):
   LOCAL/REMOTE for read commands; **no journal**. Gate: linearizable per-key reads;
   throughput scales on a read benchmark with *placed* clients; no regression at
   `shard-threads 1`; clean TSan run at `shard-threads 4`.
   4a. **Connection migration** ([Phase 4a](#20-phase-4a-connection-migration), §5b Change 4)
   — `shardMigrateConnection`. Gate: a client whose hot slot is on another thread is migrated
   and goes LOCAL; no reply loss or reordering across the migration; blocking commands survive
   a migration.
5. **Single-key *writes* + journal + sequencer**
   ([Phase 5](#21-phase-5-writes-journals-and-the-sequencer)) — wire §14.4. Gate: replica stays
   bit-identical; `WAIT`/PSYNC offsets correct under concurrent writers.
6. **Per-shard expiry + eviction** ([Phase 6](#22-phase-6-per-shard-expiry-and-eviction), §8).
7. *(only if measured)* VLL replaces the barrier — [proposal-vll-transactions.md](proposal-vll-transactions.md).

Steps 1–2 are safe to land in `main` behind the default (`shard-threads 1`) with zero
behavior change, which is what makes this a real incremental program rather than a branch.

---

# Part III — the phases in detail

Sections 15–22 are the per-phase implementation designs: what each step actually changes, file
by file, with its own test plan and its own gate. They were written as separate notes and are
merged here so the whole program reads as one document.

**Reading the cross-references.** Inside a phase, `§x.y` means that phase's own subsection.
References to the core design above are written plainly (`§7`, `§14.5`). References to another
phase name it (`Phase 5 §4.6`). Every phase's "Verified anchors" section was checked against
this checkout at authoring time.

## 15. Phase 1: the ordering-model harness

This is the concrete design of **Phase 1** of §10 — the standalone experiment that
proves (or kills) the commit-id ordering model of §7 **before any server code is
written**.

It is the only phase whose deliverable is an *answer*, not a feature. §12 names two of
its three abandonment criteria in terms of this harness; this note makes those criteria
executable.

### 1. Scope

> "Prove the ordering model. The §7 harness. No server changes. Do this first; it is the only
> step that can kill the project." (§10.1)

The claim under test, restated precisely:

> N threads executing commands against disjoint slot sets, each stamping a record with
> `atomic_fetch_add(&commit_id, 1)` **at the moment execution completes** and appending it to
> its own single-producer ring, can be merged by a single sequencer into one totally-ordered
> stream that (a) replays to the same keyspace as the concurrent execution, (b) never violates
> per-client causal order, and (c) does so at a cost that does not eat the scaling win.

**In scope:** the `shard_journal` module (rings + shared commit counter + sequencer with reorder
buffer), a gtest suite that asserts the three properties above, a randomized differential
harness, and a microbenchmark that publishes the cost.

**Out of scope:** anything in `server.c`. No `slot_to_shard[]` (Phase 2), no threads in the
server, no propagation wiring (Phase 5). The harness fakes shards with plain pthreads and fakes
the keyspace with a hash map.

**Not throwaway.** The parent doc calls Phase 1 a validation experiment, and §14.7 calls
it "`shard_journal.c` + fake shards only." That framing is right about the *gate* but wastes the
code. Build the **real** `src/shard_journal.{c,h}` now — it has no dependency on shards,
threads, or the command table, only on `queues.h` and a record type — and drive it from tests.
Phase 5 then wires the same module into `propagateNow` instead of reimplementing it. The only
throwaway parts are the fake shard threads and the fake keyspace, which belong in the test file.

### 2. Verified anchors (this checkout)

- **The single-threaded stream this must reproduce.** `call()` (`src/server.c:3875`) accumulates
  propagation via `alsoPropagate` (`src/server.c:3680` → `serverOpArrayAppend`
  `src/server.c:3450`); `propagatePendingCommands` (`src/server.c:3746`) wraps a multi-op unit in
  `MULTI`/`EXEC` and calls `propagateNow` (`src/server.c:3626`) per op, from
  `postExecutionUnitOperations` (`src/server.c:3799`). `propagateNow` fans out to
  `replicationFeedReplicas` (`src/replication.c:579`) and `feedAppendOnlyFile`
  (`src/aof.c:1446`).
- **Where the replication offset is assigned.** `feedReplicationBuffer`
  (`src/replication.c:449`) advances `server.primary_repl_offset` as it copies bytes
  (`src/replication.c:476`, `:507`); the no-replica/no-backlog shortcut still bumps it by 1
  (`src/replication.c:597`). The offset is therefore a property of **emit order**, not execution
  order — which is exactly why the sequencer can own it.
- **`WAIT` reads that same counter.** `waitCommand` (`src/replication.c:5086`) →
  `replicationCountAcksByOffset` (`src/replication.c:5052`); unblock at
  `src/replication.c:5164`.
- **Queue transport already in tree.** `spscQueue` (`src/queues.h:112`), `spscInit` (`:128`),
  `spscEnqueue` (`:136`), `spscDequeueBatch` (`:140`); `mpscQueue` (`:45`) / `mpscEnqueue`
  (`:67`); `spmcQueue` (`:82`). Already exercised by `src/unit/test_queues.cpp`.
- **Unit-test infrastructure.** `src/unit/Makefile:14` globs every `*.cpp` in the directory into
  the gtest binary, and `src/unit/CMakeLists.txt` does the same — a new `test_*.cpp` needs no
  registration. Precedents that link real server modules: `src/unit/test_queues.cpp`,
  `src/unit/test_kvstore.cpp`.
- **Per-thread memory accounting already exists** (`src/zmalloc.c:98-116`: per-thread
  `used_memory_thread[]` indexed by a TLS thread index, with an atomic fallback) — so allocating
  journal records on shard threads is already accounted correctly. Not a Phase-1 blocker; noted
  because it is one fewer thing to design.

### 3. What "correct" means — the three assertions

Let each fake client issue a sequence of commands; each command executes on the shard owning its
key. Define `seq(r)` as the record's commit id and `emit(r)` as its position in the sequencer's
output.

**P1 — State equivalence.** Replaying the emitted stream single-threaded onto an empty keyspace
yields a keyspace byte-identical to the one the concurrent execution produced. This is the
property replicas depend on.

**P2 — Per-client causality.** For any client `k` and its consecutive commands `c_i`, `c_{i+1}`:
`seq(c_i) < seq(c_{i+1})`, and therefore `emit(c_i) < emit(c_{i+1})`. **This is the property §7
says an arrival-order merge silently destroys**, and the whole reason the commit-id
counter exists. It holds only if the coordinator does not dispatch `c_{i+1}` until `c_i` has
taken its id — the harness must model that dependency explicitly (see §4.2), because a harness
that lets a client fire commands concurrently is testing a weaker system than Valkey ships.

**P3 — Per-key order.** For two writes to the same key, `emit` order equals execution order.
Free, given single-owner slots and FIFO rings — but assert it anyway, because it is the property
that breaks first if slot ownership is ever violated.

Two structural invariants fall out and should be asserted directly, since they localize failures
much faster than P1 does:

**P4 — Dense, gapless, exactly-once.** The emitted sequence of `seq` values is `0, 1, 2, …` with
no gap, no duplicate, and no reordering.

**P5 — Bounded stall.** The sequencer never blocks forever on a missing id. The reorder buffer
depth and the wall-time a record spends waiting for a lower id are both bounded and measured
(§5).

### 4. Design

#### 4.1 The module under test — `src/shard_journal.{c,h}`

```c
/* shard_journal.h — no dependency on shards, threads, or the command table. */

typedef struct journalRec {
    uint64_t  seq;            /* global commit id */
    int       dbid;           /* -1 = do not emit SELECT (propagateNow's convention) */
    int       slot;           /* owning slot, or -1 */
    int       target;         /* PROPAGATE_AOF | PROPAGATE_REPL */
    int       argc;
    robj    **argv;           /* the deterministic, post-rewrite form (§14.4) */
} journalRec;

typedef struct shardJournal shardJournal;   /* opaque: one SPSC ring per shard */

/* Lifecycle */
void          shardJournalInit(int num_shards);
shardJournal *shardJournalOf(int shard_id);

/* Producer side — called on the owning shard thread, no lock. */
uint64_t shardJournalCommit(shardJournal *j, journalRec *rec);  /* stamps seq, appends */

/* Consumer side — called on exactly one thread (the sequencer). */
typedef void (*shardJournalEmitFn)(const journalRec *rec, void *privdata);
size_t   shardJournalFlush(shardJournalEmitFn emit, void *privdata);  /* emits in seq order */
uint64_t shardJournalEmittedUpTo(void);   /* highest contiguously-emitted seq (for WAIT, Phase 5) */
```

`shardJournalCommit` does exactly two things:

```c
rec->seq = atomic_fetch_add_explicit(&server_commit_id, 1, memory_order_relaxed);
spscEnqueue(&j->ring, rec, /*commit=*/true);
```

The `memory_order_relaxed` is deliberate and is itself part of what the harness proves: the
counter needs *uniqueness and monotonicity*, not ordering of surrounding memory, because the
sequencer synchronizes through the SPSC ring's own release/acquire pair. If the randomized
harness (§4.3) ever shows a P4 violation under relaxed ordering, tighten to `acq_rel` and
re-measure the cost — that delta is a headline number for the report.

`shardJournalFlush` is the reorder buffer:

```c
static uint64_t expected;   /* next seq to emit */
for (;;) {
    journalRec *r = peekMinAcrossRingHeads();   /* lowest seq among the N ring heads */
    if (!r || r->seq != expected) break;        /* gap: a shard hasn't committed yet */
    emit(r, privdata);
    popFrom(r->shard); expected++;
}
```

With N shards, `peekMinAcrossRingHeads` over a small N is a linear scan (N ≤ core count); a
min-heap only pays off past ~16 shards. Implement the linear scan, benchmark both, and let §5
decide. **The gap case is the interesting one**: the sequencer cannot emit `expected` until the
shard that took that id appends it. That is the stall P5 bounds.

#### 4.2 The fake shards and the dependency the harness must not cheat on

```text
   writer 0 ─┐                         ┌─ shard 0 (owns slots [0, 4096) )   ─ ring 0 ─┐
   writer 1 ─┼─ command generator ─────┼─ shard 1 (owns slots [4096, 8192))─ ring 1 ─┼─ sequencer
   writer 2 ─┤   (deterministic seed)  ├─ shard 2 ...                       ─ ring 2 ─┤     │
   writer 3 ─┘                         └─ shard 3 ...                       ─ ring 3 ─┘     ▼
                                                                              emitted stream
   each shard also applies the command to its own slice of the "concurrent keyspace"
                                                                                   │
   replay emitted stream single-threaded into a fresh keyspace  ────────────────────┴─► compare (P1)
```

- A **writer** is a fake client. Its commands are generated up front from a seeded RNG so a
  failing run is replayable from the seed alone.
- **The dependency that matters:** a writer submits command `i+1` only after command `i` has
  returned its `seq`. That models Valkey's real contract — the coordinator holds the client's
  querybuf while the command is in flight (parent Phase 4 §1: `BLOCKED_SHARD` rides the blocking
  framework, so command `k+1` is not even parsed until `k` resumes). A harness that fires a
  writer's commands concurrently would trivially "pass" P2 by vacuity and prove nothing.
- The **concurrent keyspace** is sharded the same way the shards are, so shard threads never
  touch each other's map — no locks, matching the real design.
- The **replay keyspace** is a single plain map, written by one thread from the emitted stream.

#### 4.3 Randomized differential harness

The gtest cases (§5) pin specific behaviors; the randomized harness is what actually finds the
bug. Parameters swept: shard count (1, 2, 4, 8, `nproc`), writer count (1 … 4×shards), key space
size (tiny — to force same-key contention within a shard — through large), command mix
(`SET`/`INCR`/`DEL`/`APPEND`, all deterministic on replay), and artificial per-command latency
skew (one shard made 10× slower, to maximize reorder-buffer depth).

Every run asserts P1–P5 and, on failure, dumps the seed plus the emitted stream. Run it under
**TSan** and **ASan** in CI-length sweeps; a data race here is a fatal finding, not a warning.

#### 4.4 The negative test that must exist

The obvious "optimization" of the shared atomic is **batched id allocation**: a shard reserves
64 ids at once and hands them out locally, cutting the atomic to one per 64 commands. It is
wrong, and the harness must encode *why* so nobody re-derives it two years from now:

> Shard A reserves `[100, 164)`, shard B reserves `[164, 228)`. A client's command `k` runs on B
> and takes 164; its next command `k+1` runs on A and takes 101. `seq(k+1) < seq(k)` — **P2
> violated**, and the replica can observe the second write without the first.

Write this as a test that turns batching **on** and asserts P2 **fails**, so the property is
executable rather than folklore. Then, if the atomic turns out to be the bottleneck (§5), the
search for a replacement starts from a documented constraint: *any* scheme must give a single
client's successive commands increasing ids **across shards**, which rules out per-shard id
space without a cross-shard fence.

Two replacements worth prototyping only if §5 says the atomic is too expensive:

- **Coordinator-assigned ids.** The coordinator (not the owner) stamps the id when the result
  comes back. Preserves P2 by construction, but breaks P3 for a hot key hit by two coordinators —
  needs the owner to also enforce per-key order, which is more machinery, not less.
- **Per-shard counters + a client-carried Lamport clock.** Each command carries
  `max(client_clock, shard_clock)+1`. Preserves P2, gives a partial order that the sequencer
  linearizes, and removes the shared cache line. Strictly more complex; only earn it with a
  measurement.

### 5. What to measure (this is half the deliverable)

The gate is not just "the tests pass" — §11 demands a published break-even, and §12 says a cost
that "eats the scaling" is an abandonment criterion. Report:

| Metric | Why it decides something |
|---|---|
| `atomic_fetch_add` throughput at 1/2/4/8/16 threads (ops/s, and ns/op at each level) | The hard ceiling on total write rate. If it saturates below the single-thread command rate today, the design is dead as written. |
| Sequencer cost per record (ns), linear scan vs min-heap, at N = 2…32 | Sequencer is single-threaded; if it costs more per record than `propagateNow` does today, it has just replaced one bottleneck with another. |
| Reorder-buffer depth: mean / p99 / max, under balanced and 10×-skewed shards | The memory and latency cost of the gap-wait. |
| Stall time: wall-clock a record waits for a lower id, p99 / max | This is added replication latency. It is the number `WAIT` users will feel. |
| End-to-end merged throughput vs. a single-threaded baseline producing the same stream | The actual scaling answer at the journal layer, independent of the keyspace work. |

Publish these as a table in the phase report, with the machine and core count named — §11
says the break-even must be published, and this is where that number is born.

### 6. Files touched

- `src/shard_journal.h`, `src/shard_journal.c` — **new**: `journalRec`, per-shard SPSC rings,
  `server_commit_id`, `shardJournalCommit`, `shardJournalFlush`, `shardJournalEmittedUpTo`.
  Depends only on `queues.h`, `zmalloc.h`, and the `robj` type.
- `src/Makefile` (object list, near `:514`) and `cmake/Modules/SourceFiles.cmake` (near `:14`) —
  build wiring, **both** required.
- `src/unit/test_shard_journal.cpp` — **new**: P1–P5 gtest cases, the fake-shard harness, and the
  negative batching test. Auto-discovered (`src/unit/Makefile:14`); no registration.
- `src/unit/test_shard_journal_bench.cpp` *(or a `--benchmark` flag on the above)* — **new**:
the §5 measurements.

**Not touched:** `server.c`, `replication.c`, `aof.c`, `networking.c`, `config.c`. Phase 1 adds a
module and tests and changes no runtime behavior — it is safe to land in `main` on its own,
which is what makes the measurement reproducible by others later.

### 7. Test plan

**Deterministic gtest cases (`src/unit/test_shard_journal.cpp`):**

1. `SingleShardIsIdentity` — N=1: emitted order equals commit order, no reorder buffer ever
   engages. (The `shard-threads 1` no-op guarantee, at the journal layer.)
2. `EmittedSequenceIsDenseAndUnique` — P4 under 8 shards × 100k records.
3. `PerKeyOrderPreserved` — P3: many writes to one key from one shard, interleaved with other
   shards' traffic.
4. `PerClientCausalityPreserved` — P2: writers with the §4.2 dependency; assert
   `seq` strictly increases per writer and `emit` order agrees.
5. `ReplayReconstructsKeyspace` — P1: concurrent keyspace vs replay keyspace, byte-compare.
6. `SequencerStallsThenDrains` — P5: hold one shard's ring artificially; assert the sequencer
   emits nothing past the gap, then emits everything in order once released, and that buffer
   depth is bounded by the number of in-flight commands.
7. `BatchedIdAllocationViolatesCausality` — **the negative test of §4.4**: enable batching,
   assert P2 fails. Documents the constraint by executing it.
8. `ArrivalOrderMergeViolatesCausality` — the same for the "just merge in arrival order"
shortcut §7 rejects. Makes the rejected alternative a test, not a paragraph.

**Randomized (§4.3):** seeded sweep, asserted against P1–P5, run under TSan and ASan.

### 8. Verification

1. `make -C src && make -C src test-unit UNIT_TEST_PATTERN='ShardJournal*'`.
2. CMake build to confirm the second source list is wired: configure + build + run the gtest
   target.
3. TSan: rebuild with `SANITIZER=thread` and run the randomized sweep; **zero** reported races.
4. ASan/UBSan run of the same sweep.
5. Long sweep (≥10 min, ≥8 shards) with a recorded seed list; any failure must be replayable from
   its seed alone.
6. Produce the §5 table.

### 9. Gate — and what "no" looks like

**Proceed to Phase 2 only if:** P1–P5 hold across the randomized sweep under TSan, *and* the §5
numbers show the shared atomic plus sequencer costing meaningfully less per write than the
keyspace work they are ordering (Stage 0, [proposal-stage0-measurement.md](proposal-stage0-measurement.md),
says what that work costs today).

**Stop if** (§12, made concrete here):

- P2 cannot be preserved without a cost that erases the scaling — i.e. the only schemes that
  preserve per-client causality cost more than they save. §7 already refuses to make
  arrival-order the default, so "we could just relax it" is not an escape hatch at this gate.
- `atomic_fetch_add` on one cache line saturates below the write rate a single thread achieves
  today. Then N shards cannot out-produce one thread no matter how fast the keyspace work gets.
- p99 stall time from gap-waiting is large enough to be a visible replication-latency
  regression, and no bounded-reorder variant fixes it.

Writing the "no" down before running the experiment is the point of doing the experiment first.

### 10. References

- Core design: §7 (the ordering
  crux), §10.1 (phasing), §12 (abandonment criteria), §14.4 (the journal sketch this note
  refines).
- Consumer of this module: [Phase 5](#21-phase-5-writes-journals-and-the-sequencer) (writes + propagation).
- Snapshot dependency: [10-dragonfly-snapshot-model.md](10-dragonfly-snapshot-model.md) — the
  same journal is what makes a per-shard snapshot cut globally coherent.
- Measurement context: [proposal-stage0-measurement.md](proposal-stage0-measurement.md).
- Code: `src/server.c:3626,3680,3746,3799,3875` (propagation today), `src/replication.c:449,579`
  (feed + offset), `src/aof.c:1446`, `src/queues.h` (transport), `src/unit/Makefile:14` (test
  discovery).

## 16. Phase 2: the ownership map and config

This is the concrete, line-anchored
implementation of **Phase 2** of §10 — the first real in-server
increment of slot-per-thread execution. It introduces the slot→shard ownership map and the
`shard-threads` config as a **pure, no-op refactor**: no threads, no behavior change at the
default `shard-threads 1`.

### 1. Scope

Phase 1 (the ordering-model harness, §15) validates the ordering model outside the server.
Phase 2 is the first code that lands in it:

> "Introduce `slot_to_shard[]` with `shard-threads 1`. Pure refactor. Every command is
> LOCAL. Should be a runtime no-op and fully testable against the existing suite." (§10.2)

The keyspace is already partitioned by hash slot — `kvstore` is per-slot
(`src/server.c:2902-2904`). Phase 2 adds the **ownership map** on top: an array assigning
each of the 16384 slots to an owning execution shard, plus the `shard-threads` config and a
routing accessor. With `shard-threads` at its default of 1, every slot maps to shard 0, so
every command is LOCAL and runs exactly as today. This is the seam later phases
(multi-threaded reads, per-shard journals) widen; here it must be a **provable no-op**.

**Out of scope:** any threading; LOCAL/REMOTE/BARRIER dispatch (Phase 4+); virtual slots
for standalone (Phase 3); the journal/sequencer (Phase 5); client-to-shard affinity and
connection migration. Phase 2 is the data structure + config + accessors only.

**Naming.** "Shard" is overloaded — in cluster topology a *shard* is a primary plus its
replicas. This feature's shard is an *execution shard* (a thread + the slots it owns). To
keep them distinct, the new module and symbols use the `slotShard*` prefix and the term
"slot-shard"; only the user-facing config keeps the proposal's committed name
`shard-threads`.

### 2. Verified anchors (this checkout)

- `CLUSTER_SLOTS` = `1 << CLUSTER_SLOT_MASK_BITS` = 16384 (`src/cluster.h:10`).
- Client slot field: `int slot;` (`src/server.h:1378`) — "the slot the client is executing
  against, -1 if none". Computed only in cluster mode by `clusterSlotByCommand()` inside
  `prepareCommandGeneric` (`src/server.c:4276`), reset to -1 in `unprepareCommand`
  (`src/server.c:4304`). In standalone it stays -1 (virtual slots are Phase 3).
- Config int-registration precedent — `io-threads` at `src/config.c:3457`:
  `createIntConfig("io-threads", NULL, DEBUG_CONFIG | MODIFIABLE_CONFIG, 1,
  IO_THREADS_MAX_NUM, server.io_threads_num, 1, INTEGER_CONFIG, NULL, updateIOThreads)`.
  Backing fields `io_threads_num`/`active_io_threads_num` at `src/server.h:1840-1841`.
- `createDatabase` (`src/server.c:2893`) — per-slot kvstore already exists; **not modified
  here**.
- `initServerConfig` (`src/server.c:2308`) seeds config-field defaults; `initServer`
  (`src/server.c:2924`) does subsystem init after config load — the init call site.
- INFO "# Server" block: `src/server.c:6182-6213`, `io_threads_active` printed at `:6212` —
  the spot for a `shard_threads` field.
- Build source lists are explicit in **both** systems: `src/Makefile:514` (`kvstore.o` in
  the object list) and `cmake/Modules/SourceFiles.cmake:14` (`kvstore.c`). A new module must
  be added to both.
- Unit tests link the whole server lib (`libvalkey.a` / `valkeylib-gtest` from
  `VALKEY_SERVER_SRCS`) and auto-glob `src/unit/*.cpp` (`src/unit/Makefile:15`,
  `src/unit/CMakeLists.txt:42,64`) — a new module is automatically visible to a new test
  file with no extra registration.

### 3. Design

#### 3.1 New module — `src/slot_shard.h` / `src/slot_shard.c`

The map and partition math in one small, unit-testable place.

```c
/* slot_shard.h */
#define SLOT_SHARD_MAX CLUSTER_SLOTS   /* upper bound on shard count (one shard per slot) */

/* (Re)partition all CLUSTER_SLOTS slots across `num_shards` execution shards into
 * contiguous balanced ranges. num_shards is clamped to [1, SLOT_SHARD_MAX]. Idempotent;
 * safe to call again when `shard-threads` changes. */
void slotShardInit(int num_shards);

/* Owning execution shard of `slot` (0 <= slot < CLUSTER_SLOTS). */
int slotToShard(int slot);

/* Current number of execution shards (mirrors server.shard_threads_num after init). */
int slotShardCount(void);
```

**No `clientHomeShard(client *c)` here** — an earlier draft of this section had one, defined as
"the shard that will run this client's command, returning 0 when `c->slot < 0`." That is a trap,
found while implementing this phase. `c->slot < 0` means *keyless, global, or (pre-Phase-3)
standalone* — commands that must take the **escalation barrier** (§6), not commands that run on
shard 0. An accessor returning 0 for both collapses the distinction at exactly the point where
dispatch decides it, and the barrier case disappears silently. Phase 4 defines the routing
accessor alongside the branch that consumes it (§19.3.1, where `slot < 0` routes to
`shardBarrierRun`), so the two cases stay separate. Phase 2 ships the map and the partition math
only.

Core implementation — a file-static map plus a balanced contiguous partition:

```c
/* slot_shard.c */
static uint16_t slot_to_shard[CLUSTER_SLOTS]; /* 32 KB, always fully sized */
static int shard_count = 1;

void slotShardInit(int num_shards) {
    if (num_shards < 1) num_shards = 1;
    if (num_shards > SLOT_SHARD_MAX) num_shards = SLOT_SHARD_MAX;
    shard_count = num_shards;
    for (int s = 0; s < CLUSTER_SLOTS; s++)
        slot_to_shard[s] = (uint16_t)((long long)s * num_shards / CLUSTER_SLOTS);
}
int slotToShard(int slot) { return slot_to_shard[slot]; }
int slotShardCount(void) { return shard_count; }
```

The partition `slot s → s * N / CLUSTER_SLOTS` produces N contiguous slot ranges, balanced
to within one slot. N=1 → all slots to shard 0 (today's behavior); N=CLUSTER_SLOTS →
identity. `uint16_t` holds shard ids up to 65535, above the 16384 max.

#### 3.2 Config — `src/config.c` (near `:3457`) and `src/server.h` (near `:1840`)

- Add `int shard_threads_num;` to the config-field region of `struct valkeyServer`
  (`src/server.h`, beside `io_threads_num` at `:1840`).
- Register beside `io-threads`:
  ```c
  createIntConfig("shard-threads", NULL, MODIFIABLE_CONFIG, 1, SLOT_SHARD_MAX,
                  server.shard_threads_num, 1, INTEGER_CONFIG, NULL, updateShardThreads),
  ```
  Apply callback re-partitions the map so `CONFIG SET` is honest about the data structure:
  ```c
  static int updateShardThreads(const char **err) {
      UNUSED(err);
      slotShardInit(server.shard_threads_num);
      return 1;
  }
  ```
  **Dormant by design:** the value changes the map but has *no execution effect yet* —
  nothing dispatches by shard until Phase 4. Document this in the config comment and the
  `valkey.conf` reference so operators are not misled into thinking it scales anything
  today.

#### 3.3 Init — `src/server.c` `initServer()` (`:2924`)

Call `slotShardInit(server.shard_threads_num)` early in `initServer()`, alongside the other
in-memory subsystem initializations (config-field defaults are already in effect). Add
`#include "slot_shard.h"` to `server.c`.

#### 3.4 Observability — INFO + a DEBUG helper (so the map is testable end-to-end)

- INFO "# Server" (`src/server.c:6212`, next to `io_threads_active`):
  `"shard_threads:%i\r\n", server.shard_threads_num`.
- `DEBUG SLOT-SHARD <slot>` in `src/debug.c` — returns the owning shard for a slot via
  `slotToShard`. This is the call site that makes `slotToShard` live (non-dead) code and lets a
  TCL test verify the partition against `CLUSTER KEYSLOT` end-to-end. Keep it minimal, mirroring
  an existing simple `DEBUG` subcommand's arg-parsing/reply shape. `getSlotOrReply`
  (`src/cluster_legacy.c:7237`, declared in `cluster.h`) already validates and range-checks a
  slot argument and is mode-agnostic, so the subcommand is a handful of lines.

**Honest scope statement.** Phase 2 deliberately does *not* branch command execution on the
shard id — with one home thread and one shard, every command is LOCAL, so there is nothing
to route to yet. The deliverable is the ownership map + config + accessors, made live via
the INFO field and the DEBUG helper and proven by tests. The LOCAL/REMOTE dispatch split is
Phase 4, when a second shard exists — and per §3.1, the *routing* accessor belongs to that
phase too, not this one.

### 4. Files touched

- `src/slot_shard.h`, `src/slot_shard.c` — **new**: map, partition, accessors.
- `src/server.h` — `int shard_threads_num;` config field.
- `src/config.c` — `shard-threads` int config + `updateShardThreads` apply callback.
- `src/server.c` — `#include "slot_shard.h"`; `slotShardInit()` call in `initServer`;
  `shard_threads` INFO field; `shard_threads_num` default in `initServerConfig` if the
  config framework does not already seed it.
- `src/debug.c` — `DEBUG SLOT-SHARD <slot>` subcommand.
- `src/Makefile` (add `slot_shard.o` near `:514`) and `cmake/Modules/SourceFiles.cmake`
  (add `slot_shard.c` near `:14`) — build wiring.
- `src/unit/test_slot_shard.cpp` — **new**: unit tests (auto-discovered, no registration).
- `tests/unit/shard-threads.tcl` — **new**: config + INFO + no-op behavior tests.
- `valkey.conf` — document `shard-threads` (dormant, default 1).

**Not touched:** `kvstore.c`, `db.c`, the command execution path in `server.c` (no dispatch
branching), replication, cluster routing.

### 5. Test plan

**Unit (`src/unit/test_slot_shard.cpp`)** — pure partition/accessor logic, no server boot:

1. `ShardThreadsOneMapsAllSlotsToZero` — `slotShardInit(1)`; every slot 0..16383 → 0;
   `slotShardCount()==1`.
2. `PartitionIsContiguousAndBalanced` — for N in {2,3,7,64}: every slot maps into [0,N);
   shard id is non-decreasing as slot increases (contiguous); largest and smallest shard's
   slot counts differ by at most 1.
3. `IdentityWhenShardPerSlot` — `slotShardInit(CLUSTER_SLOTS)`; slot s → s.
4. `ClampsOutOfRange` — `slotShardInit(0)` and a value > `SLOT_SHARD_MAX` both clamp to the
   valid range with no out-of-bounds writes.
5. `ReinitRepartitions` — init 4 then init 1; the map fully reverts to all-zero (proves
   `CONFIG SET` re-partition is clean, no stale entries).

**Integration (`tests/unit/shard-threads.tcl`)** — real server, proves no-op + wiring:

6. `CONFIG GET shard-threads` defaults to 1; `INFO server` shows `shard_threads:1`.
7. `CONFIG SET shard-threads 4` succeeds; INFO updates to 4; a normal SET/GET/DEL/MGET
   workload still behaves identically (the no-op guarantee at >1 shard).
8. Cluster mode: `DEBUG SLOT-SHARD [CLUSTER KEYSLOT foo]` agrees with the expected partition
   for the configured `shard-threads`, verifying the map end-to-end.
9. Regression smoke: with default config, a representative existing suite passes unchanged.

### 6. Verification

1. Build both ways: `make -C src` and a CMake build, confirming the new module compiles and
   links in each (both source lists updated).
2. `make -C src test-unit UNIT_TEST_PATTERN='SlotShard*'` — the partition unit tests.
3. `./runtest --single unit/shard-threads` — config/INFO/no-op integration tests.
4. Run a slice of the existing suite (e.g. `./runtest --single unit/type/string --single
   unit/keyspace`) with default config to confirm **zero behavior change**.
5. Manually: start `valkey-server`, `CONFIG GET shard-threads` → 1, `INFO server | grep
   shard_threads`, `CONFIG SET shard-threads 8`, confirm normal commands still work and the
   DEBUG helper reports the expected owning shard for a known slot.

### 7. References

- Parent design and phasing: §4
  (ownership model), §9 (configuration), §10 (phasing).
- Code: `src/server.c` (createDatabase, initServer, prepareCommandGeneric, INFO),
  `src/config.c` (int config precedent), `src/cluster.h` (`CLUSTER_SLOTS`), `src/debug.c`.

## 17. Phase 3: virtual slots for standalone

This is the concrete, line-anchored design of
**Phase 3** of §10 — making standalone
mode use the same per-slot `kvstore` partition cluster mode already uses, so that one routing
path serves both modes.

It assumes [Phase 2](#16-phase-2-the-ownership-map-and-config) (`slot_to_shard[]` + `shard-threads`)
has landed. It is still **pre-threading**: nothing dispatches by shard until Phase 4. But unlike
Phase 2, this phase is *not* a no-op — it changes a real data structure on the hot path, and it
carries an independent win that justifies it even if the threading program stops here.

### 1. Scope

> "Virtual slots for standalone. Unifies the routing path; independently fixes standalone's
> rehash spike. Watch `SCAN` cursor semantics and `RANDOMKEY`." (§10.3)

Today `createDatabase` (`src/server.c:2893`) builds each database's three kvstores with
`slot_count_bits = 0` in standalone and `CLUSTER_SLOT_MASK_BITS` (14 → 16384 tables) only when
`server.cluster_enabled` (`src/server.c:2894-2899`). Phase 3 gives standalone the partitioned
kvstore too, hashing keys to **virtual slots** — the same `keyHashSlot` value, used purely as a
table index and a routing key, with **no cluster semantics attached**.

**In scope:** the kvstore fan-out for standalone, the key→index seam, `SCAN`/`RANDOMKEY`
behavior under a multi-table kvstore in standalone, and the memory-overhead decision.

**Out of scope:** threading and dispatch (Phase 4); the journal (Phase 5); any user-visible
cluster behavior in standalone — **no `-CROSSSLOT`, no `-MOVED`, no slot ownership, no
`CLUSTER` command changes**. A standalone server after this phase must be behaviorally
indistinguishable from one before it, except in memory and rehash latency.

### 2. Why this phase pays for itself

Two reasons, and the second one is the honest justification if the threading program stalls:

1. **One routing path.** Phase 4's dispatch reads `c->slot`. Today that field is populated only
   in cluster mode — `clusterSlotByCommand()` inside `prepareCommandGeneric`
   (`src/server.c:4276`), reset to -1 by `unprepareCommand` (`src/server.c:4304`). Without Phase
   3, standalone would need either a second routing mechanism or a permanent "standalone always
   takes the barrier" carve-out, which means **standalone gets no scaling at all**. §11
   already warns standalone is the weaker case; this is the phase that stops it being the
   *hopeless* case.

2. **It fixes standalone's giant-rehash problem outright.** A standalone database is one
   `hashtable` per kvstore. Growing past a power of two rehashes *the whole table*: at 100M keys
   that is a multi-hundred-megabyte allocation and an incremental rehash that drags on every
   command touching the table. Cluster mode does not have this problem, because 16384 tables
   means each rehash is ~1/16384 the size — and `kvstore` already tracks rehashing per table
   (`rehashing` list and `resize_cursor` in `struct _kvstore`, `src/kvstore.c:60`). Phase 3 hands
   standalone that property for free. **This is shippable value with no threads involved**, and
   it is the thing to point at if someone asks what Phase 3 buys before Phase 4 exists.

### 3. Verified anchors (this checkout)

- **The branch to change.** `createDatabase` (`src/server.c:2893`):
  ```c
  int slot_count_bits = 0;
  int flags = KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND;
  if (server.cluster_enabled) {
      flags |= KVSTORE_FREE_EMPTY_HASHTABLES;
      slot_count_bits = CLUSTER_SLOT_MASK_BITS;
  }
  ```
  followed by three kvstores built from it: `db->keys`, `db->expires`,
  `db->keys_with_volatile_items` (`src/server.c:2902-2904`).
- **Databases are created lazily.** `createDatabaseIfNeeded` (`src/server.c:2917`) allocates on
  first use; `initServer` eagerly creates only db 0 (`src/server.c:3012`). This bounds the
  memory cost of §5 to databases actually used.
- **The key→index seam.** `getKVStoreIndexForKey` (`src/db.c:233`):
  `return server.cluster_enabled ? getKeySlot(key) : 0;` (`src/db.c:234`). One line, one place.
- **`getKeySlot` asserts cluster mode** (`src/db.c:240-241`: `serverAssert(server.cluster_enabled)`)
  and caches via `server.current_client->slot` when `flag.executing_command` is set and the
  client is not one we must obey (`src/db.c:252-257`), falling back to `keyHashSlot`
  (`src/cluster.c:58`). The `mustObeyClient` backfill at `src/db.c:260-262` exists because
  `getNodeByQuery()` never runs for primary/AOF-sourced commands.
- **`CLUSTER_SLOTS` = 16384** = `1 << CLUSTER_SLOT_MASK_BITS` (`src/cluster.h:10`);
  `MAX_HASHTABLES_BITS` = 16 (`src/kvstore.c:56`), so 14 is comfortably legal.
- **kvstore allocation cost.** In `kvstoreCreate` (`src/kvstore.c:294`): `kvs->hashtables =
  zcalloc(sizeof(hashtable *) * kvs->num_hashtables)` and `kvs->hashtable_size_index =
  kvs->num_hashtables > 1 ? zcalloc(sizeof(unsigned long long) * (kvs->num_hashtables + 1)) :
  NULL`. The tables themselves are lazy under `KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND`
  (`src/kvstore.c:314`).
- **The `SCAN` cursor encodes the table index.** `hashtableCursorToKvstoreCursor`
  (`src/kvstore.c:141`): `return (ht_cursor << kvs->num_hashtables_bits) | didx;` and the inverse
  at `src/kvstore.c:146-147`. **Changing `num_hashtables_bits` changes the cursor encoding.**
- **`kvstoreScan` already supports a table range.** `src/kvstore.c:476`, documented at
  `:472-475`: `first_idx == -1 && last_idx == -1` scans all; `>= 0` scans `[first_idx,
  last_idx]`. Cluster `SCAN` already uses it (`src/db.c:1355`), while the whole-store scan path
  passes `-1, -1` (`src/db.c:2338`).
- **`RANDOMKEY`** → `dbRandomKey` (`src/db.c:442`), reached from `randomkeyCommand`
  (`src/db.c:973`). Fair sampling across tables already exists and is used by eviction:
  `kvstoreGetFairRandomHashtableIndex` (`src/evict.c:116`), plus
  `kvstoreHashtableSampleEntries` (`src/kvstore.h:74`) and `kvstoreHashtableRandomEntry`
  (`src/kvstore.h:72`).
- **RDB carries no slot info on the save side in this checkout.** `RDB_OPCODE_SLOT_INFO` is
  defined at `src/rdb.h:146` ("Foreign slot info, safe to ignore") and appears only on the load
  path (`src/rdb.c:3274`). So the index is recomputed from the key at load time and **no RDB
  format change is implied by this phase**.
- **Dead declaration worth deleting while here:** `int calculateKeySlot(sds key);`
  (`src/server.h:3553`) has no definition anywhere in `src/`.

### 4. Design

#### 4.1 The change itself

```text
   standalone TODAY                      standalone AFTER Phase 3
   ────────────────                      ────────────────────────
   db->keys ─► [ one hashtable ]         db->keys ─► [ 0 ][ 1 ][ 2 ] ... [ 16383 ]
               all keys, one rehash                   ▲                     (lazy: allocated
               of the whole table                     │                      on first key)
                                          index = keyHashSlot(key) & 16383
                                          — a *virtual* slot: a table index and a
                                            routing key, with no cluster meaning
```

Two edits carry it:

```c
/* src/server.c:2893 createDatabase — slots always, cluster or not. */
int slot_count_bits = CLUSTER_SLOT_MASK_BITS;
int flags = KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND | KVSTORE_FREE_EMPTY_HASHTABLES;
```

```c
/* src/db.c:233 getKVStoreIndexForKey — one path for both modes. */
int getKVStoreIndexForKey(sds key) {
    return getKeySlot(key);
}
```

and `getKeySlot` (`src/db.c:240`) drops its `serverAssert(server.cluster_enabled)`. Everything
else in that function — the `current_client->slot` cache, the `mustObeyClient` backfill —
becomes *more* valuable in standalone, not less, because it is now on every keyed command.

#### 4.2 The one thing to be strict about: virtual ≠ cluster

`c->slot` is currently populated only by `clusterSlotByCommand()` (`src/server.c:4276`), which
is a cluster-mode routing function that also produces `-CROSSSLOT` and drives redirects. Phase 3
must **not** start calling it in standalone. Instead:

- `getKeySlot` computes and caches the virtual slot on the key-access path, exactly as it does
  today in cluster mode. That is enough to populate `c->slot` for Phase 4's dispatch, because
  dispatch runs after the command's keys are known.
- Multi-key commands spanning slots stay **legal** in standalone. They will take the escalation
  barrier in Phase 4 (§6, §11) — a performance property, never an error. Nothing in Phase
  3 may introduce a rejection path.
- `CLUSTER KEYSLOT` (`src/cluster.c:876`) and every other `CLUSTER` subcommand keep their current
  cluster-only behavior. The virtual slot is an internal index that happens to use the same hash.

Write these three as tests (§7), because "standalone silently started rejecting `MGET a b`"
is the exact regression this phase risks.

#### 4.3 `SCAN`: the cursor encoding changes — and why that is acceptable

Because the cursor is `(ht_cursor << num_hashtables_bits) | didx` (`src/kvstore.c:141`), moving
standalone from 0 bits to 14 bits **changes the numeric cursors a standalone server hands out**.
The consequences, in order of how much they matter:

- **The `SCAN` guarantee is unaffected.** The contract is that elements present for the whole
  iteration are returned at least once, and no error is returned for a stale cursor. That is a
  property of the per-table reverse-binary iteration plus the table walk, both of which cluster
  mode already relies on with 16384 tables. Standalone inherits a mode that is already in
  production.
- **A cursor is opaque and single-run.** Valkey never promised a cursor survives a restart, a
  version change, or a failover. A cursor minted before the upgrade and replayed after it is
  already outside the contract. It should still not *crash*: a large `didx` field is simply a
  table index that exists, so the scan resumes at some table — degraded position, defined
  behavior. Verify this explicitly (§7.6) rather than assuming it.
- **Cursor values become large immediately.** A first-page cursor in standalone today is a small
  integer; after this it carries 14 low bits of table index. Clients storing cursors as
  something narrower than 64 bits, or logging/diffing them, will see the change. This is worth a
  release-note line, and it is the one user-visible artifact of the phase.
- **`SCAN` cost with `COUNT` on a sparse standalone db.** With 16384 mostly-empty tables, a scan
  step must skip empty tables. `kvstoreScan` handles this by jumping to the next **non-empty**
  table index (`kvstoreGetNextNonEmptyHashtableIndex`, used at `src/kvstore.c:513`), so the walk
  is proportional to non-empty tables, not to 16384. Confirm with a benchmark on a 10-key db
  (§8.5) — a full `SCAN` of a nearly-empty standalone database is the pathological case and the
  one most likely to show up as "SCAN got slower after upgrade".

#### 4.4 `RANDOMKEY` and sampling fairness

`dbRandomKey` (`src/db.c:442`) must now pick a table before picking an entry, and picking
uniformly among *tables* would badly bias toward keys in sparse tables. The correct primitive
already exists and is already used by eviction: `kvstoreGetFairRandomHashtableIndex`
(`src/evict.c:116`), which weights by key count via the kvstore's cumulative-count binary
indexed tree (`hashtable_size_index`, `src/kvstore.c:60`). If `dbRandomKey` does not already
route through it in cluster mode, this phase makes it do so for both modes; either way, add a
statistical test (§7.5) rather than trusting inspection — sampling bias is invisible until it
distorts eviction quality.

The same argument covers `evictionPoolPopulate` (`src/evict.c:113`) and the `expires` scan
(`src/expire.c:341`): both already operate on a multi-table kvstore in cluster mode, so they are
correct by construction here. They get *slower per sample* in standalone (a table lookup before
the entry lookup) — measure it (§8.5), because eviction quality under `maxmemory` is the most
sensitive consumer of sampling cost.

#### 4.5 Memory overhead — the real decision in this phase

Per kvstore, independent of key count:

| Allocation | Size at 16384 tables |
|---|---|
| `hashtables` pointer array (`src/kvstore.c` in `kvstoreCreate`) | 16384 × 8 B = **128 KB** |
| `hashtable_size_index` BIT (`num_hashtables + 1` entries) | 16385 × 8 B = **~128 KB** |
| **per kvstore** | **~256 KB** |
| **per database** (`keys` + `expires` + `keys_with_volatile_items`) | **~768 KB** |

Only databases actually created pay it (`createDatabaseIfNeeded`, `src/server.c:2917`; only db 0
at boot, `src/server.c:3012`). So:

- A default standalone server touching db 0 only: **~0.75 MB**. Irrelevant.
- A server using all 16 databases: **~12 MB**. Irrelevant on a normal box; *not* irrelevant to
  someone packing hundreds of tiny Valkey instances onto one host, which is a real deployment
  pattern.

**Recommendation: take the full 14 bits and the 768 KB/db, and measure empty-instance RSS as a
gate (§8.4).** Cluster mode already pays exactly this and nobody has filed a bug about it.
Adding a knob to a data-structure decision this central invites a matrix of configurations that
every later phase must then support.

**The fallback if RSS is judged unacceptable** — write it down now so it is not re-invented
later: decouple **routing granularity** from **table granularity**. Keep 16384 virtual slots for
routing (`keyHashSlot & 16383`, free), but give the kvstore `2^b` tables with `b < 14` and map
`table = slot >> (14 - b)`. Then a shard's slot range must be **table-aligned**, or two shards
would share a hashtable and exclusive ownership — the entire premise of the design — would
break. Phase 2's partition (`slot * N / 16384`, contiguous) makes alignment achievable by
rounding shard boundaries to multiples of `16384 >> b`, but it is a constraint every later phase
inherits. That is the cost of the knob, and it is why the recommendation is to avoid it.

#### 4.6 `KVSTORE_FREE_EMPTY_HASHTABLES` in standalone

Cluster mode sets this flag (`src/server.c:2897`) so emptied slots release their table
(`freeHashtableIfNeeded`, guarded at `src/kvstore.c:216`). Standalone should set it too:
without it, a workload that fills and empties keys leaves 16384 allocated tables behind
permanently. The cost is alloc/free churn on a table that repeatedly empties — a real but
already-shipped tradeoff, since cluster mode lives with it. Keep the flags identical between
modes so there is one behavior to reason about.

### 5. Interactions to check, and why each is probably fine

| Area | Verdict |
|---|---|
| **RDB save/load** | No format change. Save iterates the kvstore; load recomputes the index from the key via `getKVStoreIndexForKey`. `RDB_OPCODE_SLOT_INFO` is load-side only (`src/rdb.h:146`, `src/rdb.c:3274`). An RDB written before the change loads fine after it. |
| **AOF load** | Same path as any client write; index recomputed per key. |
| **Replication** | Byte stream is commands, not tables. Unaffected. |
| **`DBSIZE`** | `kvstoreSize` is an O(1) maintained counter (`key_count`, `src/kvstore.c:60`). Unaffected. |
| **`KEYS`** | Full iteration; now walks non-empty tables. Already how cluster mode behaves. |
| **`FLUSHALL`/`FLUSHDB`** | `kvstoreEmpty` already loops all tables (`src/kvstore.c:319`). |
| **Defrag** | Operates over the kvstore's tables; cluster mode already exercises the multi-table path. Re-run the defrag tests (§8.3) — this is the least-covered consumer. |
| **`db->blocking_keys` / `watched_keys` / `ready_keys`** | Plain dicts (`src/server.c:2907-2910`), not slot-partitioned. Untouched by this phase; they become per-shard in Phase 4/5. |
| **`OBJECT FREQ` / LFU / LRU** | Per-object; unaffected. |
| **Modules keyspace iteration** | Uses the same kvstore APIs cluster mode already feeds. |

### 6. Files touched

- `src/server.c` — `createDatabase` (`:2893-2899`): always 14 slot bits, identical flags in both
  modes.
- `src/db.c` — `getKVStoreIndexForKey` (`:233`) always returns the slot; `getKeySlot` (`:240`)
  drops the cluster-mode assert and keeps its caching/backfill logic.
- `src/db.c` — `dbRandomKey` (`:442`): route through the fair random table index.
- `src/server.h` — delete the dead `calculateKeySlot` declaration (`:3553`).
- `tests/unit/standalone-virtual-slots.tcl` — **new**: the §7 integration tests.
- `src/unit/test_kvstore.cpp` — extend with the cursor-encoding and fair-sampling cases
  (§7.5, §7.6) if not already covered.
- `valkey.conf` / release notes — one line noting that standalone `SCAN` cursor **values** change
  (the guarantee does not).

**Not touched:** `cluster.c` (no new caller of `clusterSlotByCommand`), `replication.c`,
`rdb.c`, the command table, dispatch.

### 7. Test plan

**Behavior preservation — the point of the phase (`tests/unit/standalone-virtual-slots.tcl`):**

1. `MGET a b c` / `DEL a b c` / `MSET` across keys hashing to different slots all succeed in
   standalone. **No `-CROSSSLOT` may ever appear.**
2. `CLUSTER KEYSLOT` and the rest of `CLUSTER` behave exactly as before in standalone.
3. `SCAN` full-iteration completeness: insert 100k keys, iterate to cursor 0, assert every key
   seen exactly once; repeat with concurrent writes asserting only the weak guarantee (present
   throughout ⇒ returned).
4. `SCAN` with `MATCH`, `COUNT`, and `TYPE` unchanged; `HSCAN`/`SSCAN`/`ZSCAN` (which scan an
   object, not the kvstore) untouched.
5. `RANDOMKEY` distribution: with keys skewed heavily into few slots, a large sample must follow
   the key distribution, not the table distribution. (Chi-squared or a coarse bucket assertion —
   this is the fairness test §4.4 demands.)
6. Cursor robustness: feed `SCAN` a hand-crafted large cursor; assert no crash, no error, and
   eventual termination.
7. `DBSIZE`, `KEYS *`, `FLUSHDB`, `SWAPDB`, `MOVE`, `COPY` across databases all unchanged.
8. RDB round trip: save with the old build, load with the new one, and the reverse; keyspace
   identical both ways.
9. Expiry still works end to end (`expires` kvstore is now partitioned too): set TTLs across many
   slots, confirm active and lazy expiry both fire.

**Rehash win — the independent value (§2.2):**

10. Load a large keyspace in standalone and record max single-command latency during growth,
    before vs after. The spike from the whole-table rehash should disappear. This is the number
    worth publishing even if the threading program stops.

### 8. Verification

1. `make -C src` and a CMake build.
2. `./runtest --single unit/standalone-virtual-slots`.
3. Full suite in **both** modes: `./runtest` (standalone) and `./runtest-cluster`, plus the
   defrag and expire suites explicitly — they are the heaviest kvstore consumers.
4. **Empty-instance RSS**: start a default server, `INFO memory`; compare `used_memory` and RSS
   before vs after, and again with all 16 databases touched. Publish both numbers; §4.5's
   recommendation stands or falls on them.
5. **Benchmarks** on standalone, before vs after: `SET`/`GET` throughput (the extra `keyHashSlot`
   per key access is the cost), `SCAN` over a 10-key database (the sparse-table pathological
   case), `RANDOMKEY` throughput, and eviction rate under `maxmemory` with `allkeys-lru`.
6. Latency-during-growth run from §7.10.

### 9. Honest risks

- **A per-key hash appears on the standalone hot path.** `keyHashSlot` (CRC16 over the key, or
  the hashtag substring) now runs for standalone key access where it never did. The
  `current_client->slot` cache (`src/db.c:252-257`) absorbs most of it for single-key commands,
  but multi-key commands hash every key. Measure it (§8.5); if it shows, the mitigation is
  caching per argv entry, not abandoning the phase.
- **Sparse-database `SCAN` regression.** 16384 tables holding 10 keys is a worse shape than 1
  table holding 10 keys. The non-empty-table skip makes it survivable; measure it anyway.
- **RSS on small instances.** §4.5. The fallback exists but adds an alignment constraint to every
  later phase.
- **This phase is not reversible in place.** Once cursors and table layout change, rolling back
  means another cursor-encoding change. Land it early in a release cycle, not late.

### 10. References

- Core design: §4 (ownership model,
  the virtual-slots paragraph), §10.3 (phasing), §11 (standalone's weaker case).
- Prerequisite: [Phase 2](#16-phase-2-the-ownership-map-and-config) — `slot_to_shard[]` +
  `shard-threads`.
- Consumer: [Phase 4](#19-phase-4-part-2-dispatch-and-the-remote-continuation) — dispatch reads `c->slot`, which this
  phase makes meaningful in standalone.
- Background: [04-keyspace-and-data-model.md](04-keyspace-and-data-model.md) (kvstore and
  hashtable), [proposal-dashtable-adoption.md](proposal-dashtable-adoption.md) (the other,
  orthogonal answer to the rehash-spike problem).
- Code: `src/server.c:2893` (`createDatabase`), `src/db.c:233,240,442`, `src/kvstore.c:56,141,
  294,476`, `src/evict.c:113,116`, `src/cluster.h:10`, `src/rdb.h:146`.

## 18. Phase 4 part 1: per-thread event loops

This is the concrete, line-anchored design of the **prerequisite
half of Phase 4**: turning Valkey's single global event loop into one loop per execution shard,
so that a shard thread owns *both* a set of client sockets and a slice of slots. It is the code
form of §5b, which calls this "the part with no Valkey analog" and "arguably a bigger lift
than the slot-ownership and journal machinery combined."

[Phase 4 (part 2)](#19-phase-4-part-2-dispatch-and-the-remote-continuation) is the LOCAL/REMOTE/BARRIER dispatch
branch and the REMOTE continuation. It explicitly assumes this note's work "exists or lands
alongside." **This note is that work.** Read them as one phase split by subsystem: part 1 owns
the loop and the connection, part 2 owns the command.

Prerequisites: [Phase 2](#16-phase-2-the-ownership-map-and-config) (`slot_to_shard[]`,
`shard-threads`) and [Phase 3](#17-phase-3-virtual-slots-for-standalone) (virtual slots, so standalone
has a slot to route on).

### 1. Scope

> "Per-thread event loop + multi-threaded single-key reads, no migration. Stand up Model B
> (§5b): de-globalize `server.el` into `conn->el`, per-shard `aeEventLoop`s, `SO_REUSEPORT`
> accept, round-robin placement." (§10.4)

**In scope:** the shard thread and its `aeEventLoop`; `conn->el`; connection placement at accept;
splitting `beforeSleep` into a global part and a per-shard part; making the client registries and
the hot-path globals safe for N loops; the cross-thread wake fd.

**Out of scope:** the dispatch branch and the REMOTE round trip (part 2); connection migration
([Phase 4a](#20-phase-4a-connection-migration)); writes, journal, propagation (Phase 5);
per-shard expiry/eviction (Phase 6). At the end of this phase a shard thread runs its own loop
and serves its own clients — but every command still executes exactly as today, because part 2
supplies the branch that makes ownership matter.

That split is deliberate: **this phase should be landable and testable with `shard-threads > 1`
and still produce byte-identical behavior**, because until part 2 lands, every command that
reaches a shard thread whose slot it does not own simply takes the barrier. Slow, correct,
and a very small correctness surface to debug the threading against.

### 2. The starting reality

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
  in `server.io_poll_state`. One loop, one connection owner. This is §5b's **Model A**,
  and it is precisely why Model A cannot produce a LOCAL fast path: the socket owner and the data
  owner are different threads by construction, so every command is a REMOTE hop.

### 3. Verified anchors (this checkout)

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

### 4. Design

#### 4.1 The decision that makes everything else tractable: shard 0 *is* the main thread

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

#### 4.2 Change 1 — de-globalize the loop into `conn->el`

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

#### 4.3 Change 2 — accept and placement

**Recommendation: keep a single acceptor on shard 0 and hand the new connection to its shard.
Do not open per-shard `SO_REUSEPORT` listeners in this phase.** This revises §5b Change 2,
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
command forever** — which is why §10 gates the two separately and why this phase's
benchmarks must use deliberately placed clients (§7.6) and say so when publishing numbers.

Mechanically: `acceptCommonHandler` (`src/socket.c:333`) runs on shard 0, creates the
`connection` and the `client` (`createClient`, `src/networking.c:285`) but **does not register
the fd**; it picks a target shard, sets `conn->el`, and posts a `SHARD_REQ_ADOPT` control
message to that shard's inbox. The target shard, in its own loop, arms the read handler and
adds the client to its local registries. Until the adopt message is processed the fd is
registered nowhere, which is safe (data waits in the socket buffer) as long as adoption is
never dropped — assert on inbox-full and fall back to keeping the client on shard 0.

#### 4.4 Change 3 — splitting `beforeSleep`

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

#### 4.5 Client registries: per-shard lists, global index under barrier

`server.clients` and `server.clients_index` (`src/networking.c:212-218`) are read by `CLIENT
LIST`, `CLIENT KILL`, `CLIENT NO-EVICT`, `clientsCron` (`src/server.c:1227`), `INFO clients`, and
the maxclients check (`src/networking.c:1878`). N threads creating and freeing clients cannot
share them unlocked.

The split that avoids a lock on the hot path:

- **Per-shard**: `shard->clients` (list + the client's `client_list_node`),
  `shard->clients_pending_write`, `shard->unblocked_clients`, `shard->clients_to_close`.
  All four are touched only by the owning thread, on the hot path, with no synchronization.
- **Global**: nothing on the hot path. `CLIENT LIST`/`CLIENT KILL`/`CLIENT INFO`/`clientsCron`
  become **barrier operations** (§6): quiesce every shard, walk every shard's list, act,
  release. They are administrative and already escalate under §6's rule, so this costs
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

#### 4.6 The thread-safety audit — the enumeration this phase actually rests on

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
| Module GIL (`moduleReleaseGIL`) | Stays on shard 0. Every module command takes the barrier (§8), so no module code runs on a shard thread in this phase. |

The audit is not optional garnish — it *is* the phase. Budget for it explicitly: TSan on the
full test suite at `shard-threads 4` is the acceptance criterion (§8.4), and a clean TSan run is
worth more than any number of hand-reviewed diffs.

#### 4.7 The cross-thread wake

Each shard's loop needs an `eventfd` (or self-pipe) registered on its own `el` with a no-op read
handler, so another thread can force `aeApiPoll` to return. Part 2 §3.5 specifies it for delivering
REMOTE results; it is introduced here because **adoption messages need it first** — a shard
sitting in `aeApiPoll` with no clients yet would otherwise not notice its first connection.
Coalesce writes (wake only on an empty→non-empty transition) so a burst of messages costs one
syscall.

### 5. Configuration

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

### 6. Files touched

- `src/shard.{c,h}` — **extend** (created in part 2 / §14.2): `shardThreadMain`,
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

### 7. Test plan

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

### 8. Verification

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

### 9. Honest risks

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
  a REMOTE hop forever (§5a). Any benchmark published from this phase without saying
  whether clients were placed is misleading.
- **Blast radius on the connection layer.** Three transports (socket, TLS, RDMA) each carry their
  own `server.el` uses. RDMA in particular has connection-manager fds with their own lifecycle;
  keeping them on shard 0 is the conservative call, and RDMA should be tested explicitly
  (`runtest-rdma`) rather than assumed.

### 10. References

- Core design: §5b (Model A vs
  Model B — this note is its implementation), §5a (Dragonfly's connection layer), §10.4.
- Part 2 of this phase: [Phase 4: dispatch + REMOTE continuation](#19-phase-4-part-2-dispatch-and-the-remote-continuation).
- Next: [Phase 4a: connection migration](#20-phase-4a-connection-migration).
- The alternative that keeps one loop: [proposal-readonly-io-execution.md](proposal-readonly-io-execution.md).
- Background: [01-server-lifecycle-and-event-loop.md](01-server-lifecycle-and-event-loop.md),
  [03-clients-and-networking.md](03-clients-and-networking.md),
  [09-threading-and-io-model.md](09-threading-and-io-model.md), `design-docs/io-threads.md`.
- Code: `src/ae.c:76,540,551`, `src/server.c:1854,2040,2727,2731,3004,3087,3106,3216,7841`,
  `src/connection.h:159,463`, `src/socket.c:97,122,333`, `src/networking.c:212,285,406,3318`,
  `src/io_threads.c:235,282,793`, `src/anet.c:383`, `src/zmalloc.c:98`, `src/object.c:161`.

## 19. Phase 4 part 2: dispatch and the REMOTE continuation

This is the concrete, line-anchored design of the one hot
integration point of slot-per-thread: the **LOCAL / REMOTE / BARRIER branch** at command
dispatch, and the **continuation mechanism** that lets a command started on one thread finish
on another and reply correctly. It is the code half of §5 / §5a and the concrete form of
§14.3, refined against the real return-value and blocking contracts in this checkout.

It assumes [Phase 2](#16-phase-2-the-ownership-map-and-config) (`slot_to_shard[]` +
`shard-threads`) has landed, and that Model B's per-thread event loops
(§5b — `conn->el`, per-shard `aeEventLoop`, `SO_REUSEPORT` accept) exist or land alongside.
The event-loop de-globalization is **not** re-derived here; this note is strictly the dispatch
decision and the cross-thread result path. Per §10 step 4, the executable scope is
**single-key reads**: LOCAL and REMOTE carry reads; everything else takes the barrier. Writes
+ journal + sequencer are Phase 5.

### 1. The one claim this note rests on

**A REMOTE hop is not a new control-flow mechanism. It is a new blocking type.** Valkey
already has, and relies on every day, the exact primitive a cross-thread hop needs: *start
executing a command, discover it cannot complete synchronously, suspend it without resetting
the client or propagating anything, and resume it later when a result arrives.* That is
`BLPOP`. The blocking framework already guarantees the three properties the continuation
needs:

- **No premature reset / propagation.** `commandProcessed` returns early when
  `c->flag.blocked` is set (`src/networking.c:3887`) — argv/argc are preserved, the client is
  not reset, replication offset is not advanced. Exactly what a suspended REMOTE command
  needs.
- **Input is held, not processed.** While `CLIENT_BLOCKED` is set the query buffer is
  accumulated but not parsed into new commands (`blockClient`, `src/blocked.c:104`). This is
  what preserves **per-client ordering for free**: command *k+1* is not dispatched until *k*
  resumes and unblocks — the same reason `BLPOP` then `GET` on one connection can't reorder.
- **A resume path exists.** `unblockClient(c, queue_for_reprocessing=1)` (`src/blocked.c:217`)
  queues the client on `server.unblocked_clients`; `processUnblockedClients`
  (`src/blocked.c:158`) drains it in `beforeSleep` and re-drives the held command via
  `processPendingCommandAndInputBuffer` (`src/networking.c:3962`, gated on
  `c->flag.pending_command`).

So Phase 4 adds one enum value — `BLOCKED_SHARD` — and reuses the machinery, rather than
inventing the ad-hoc `awaiting_shard` flag sketched in §14.3. The genuinely new work is
narrow and nameable: (a) the owner must run the command **without a socket** and hand back
**reply bytes**, and (b) the unblock signal arrives **cross-thread**, so it must wake the
coordinator's event loop instead of running inline like `handleClientsBlockedOnKeys`.

### 2. Verified anchors (this checkout)

- **Dispatch tail.** `processCommand` (`src/server.c:4315`) returns `C_OK`/`C_ERR` and today
  ends by calling `call(c, ...)` (`src/server.c:3875`). Its caller
  `processCommandAndResetClient` (`src/networking.c:3932`) runs `commandProcessed(c)` only when
  `processCommand` returned `C_OK` (`:3936-3937`).
- **The blocked-client early-out.** `commandProcessed` (`src/networking.c:3879`) bails at
  `:3887` (`if (c->flag.blocked) return;`) — no reset, no reploff advance, no propagation.
- **Block / unblock / resume.** `blockClient(c, btype)` (`src/blocked.c:106`);
  `unblockClient(c, queue_for_reprocessing)` (`src/blocked.c:217`); `processUnblockedClients`
  (`src/blocked.c:158`) in `beforeSleep`; re-drive via `processPendingCommandAndInputBuffer`
  (`src/networking.c:3962`) which consumes `c->flag.pending_command` (`:3967-3968`).
- **Blocking enum.** `blocking_type` (`src/server.h:340-351`): `BLOCKED_POSTPONE` at `:347`,
  `BLOCKED_SHUTDOWN` at `:348`, terminated by `BLOCKED_NUM` at `:349`. A new `BLOCKED_SHARD`
  goes before `BLOCKED_NUM`. `server.blocked_clients_by_type[btype]` is sized by `BLOCKED_NUM`.
- **Client flags bitfield.** `struct ClientFlags` (`src/server.h:1112`): `blocked : 1`
  (`:1117`), `unblocked : 1` (`:1120`), `pending_command : 1` (`:1146`). No new flag needed —
  `BLOCKED_SHARD` rides `blocked`/`pending_command`.
- **Block state.** `c->bstate` (`src/server.h:1329`), `blockingState.btype`
  (`src/server.h:960`), lazily inited by `initClientBlockingState` (called from `blockClient`).
- **Reply buffers (single-owner, home thread only).** `c->buf[]`/`c->bufpos`
  (`src/server.h:1289-1290,1340`), `c->reply` list (`:1334`), `c->reply_bytes` (`:1338`).
  Append helpers: `_addReplyToBufferOrList` (`src/networking.c:719`), `_addReplyProtoToList`
  (`src/networking.c:700`), `addReplyProto` (`src/networking.c:824`).
- **Fake-client precedent (execute with no socket).** `createClient(NULL)` (`src/aof.c:1491`);
  the AOF loader runs full commands through a `fakeClient` with `argv`/`argc` set directly and
  `call()`-equivalent execution (`src/aof.c:1560-1640`). This is the template for a per-shard
  executor client.
- **Queue transport.** `src/queues.h`: `spscQueue` (`:112`), `spscEnqueue(q, data, commit)`
  (`:136`), `spscDequeueBatch` (`:140`), `spscIsFull`/`spscIsEmpty` (`:132,142`); `mpscQueue`
  (`:45`) for the many-coordinators→one-owner direction if needed. Tagged-pointer job trick:
  `src/io_threads.c:38`.
- **Structures already defined.** `shard`, `slot_to_shard[]`, `shardExecJob`
  (§14.2); `shardDispatch`/`shardBarrierRun` sketch (§14.3/§14.5). This note
  refines the dispatch/continuation half; it does not redefine the shard table.

### 3. Design

The REMOTE round trip at a glance — a command whose slot is owned by another thread. Nothing
executes across the boundary: the coordinator hands over *data* (argv) and gets back *data*
(reply bytes); neither thread touches the other's client, socket, or reply buffer.

```text
  COORDINATOR thread (owns the socket)              OWNER thread (owns the slot's data)
  ────────────────────────────────────              ───────────────────────────────────
  1. shardDispatch: owner != me            (§3.1)
       blockClient(c, BLOCKED_SHARD)        (§3.2)   ── client suspended, thread keeps working
       build shardExecJob{argv,handle,coord}
       spscEnqueue(owner->inbox, job) ───────────┐
       return to event loop, serve other clients │
                                                  ▼
                                          2. shardBeforeSleep drains inbox     (§3.3)
                                               call() on socket-less executor client
                                               shardDetachReply → flat RESP `sds` bytes
                                          ┌── spscEnqueue(coord->results, res)
                                          │    shardWakeLoop(coord_shard) ── 1 byte to eventfd
                                          │        (§3.5: wakes coord's aeApiPoll)
                                          ▼
  4. shardBeforeSleep drains results       (§3.4)
       shardLookupLiveClient(handle)   ◀── §4: drop if client died mid-hop
       _addReplyProtoToList(c, bytes)
       unblockClient(c, 1) ── resume: finalize, parse NEXT pipelined command (no re-exec)
```

The LOCAL case skips all of this (`call()` inline, §3.1); the BARRIER case parks every shard
and runs on the coordinator (§14.5). Only REMOTE pays the two-hop cost above.

#### 3.1 The branch point — `shardDispatch(client *c)`

The branch replaces the direct `call()` at the tail of `processCommand` (`src/server.c:4315`).
`c->slot` is already computed (`clusterSlotByCommand` inside `prepareCommandGeneric`, §2), and
all gate checks (auth, cluster redirect, OOM, etc.) have already passed, so dispatch
sees a validated, slot-tagged command.

```c
/* src/shard.c — tail of processCommand, after all gate checks. Returns like
 * processCommand: C_OK if handled (incl. suspended), C_ERR if the client died. */
int shardDispatch(client *c) {
    if (server.shard_threads == 1)                 /* Phase-2 identity: provable no-op */
        return call(c, CMD_CALL_FULL), C_OK;

    int slot = c->slot;
    if (slot < 0 || commandNeedsBarrier(c))        /* keyless/global/multi-slot/MULTI/Lua/module */
        return shardBarrierRun(c);                  /* §14.5 */

    int owner = slot_to_shard[slot];
    if (owner == myShardId())                       /* LOCAL: the fast path, zero hops */
        return call(c, CMD_CALL_FULL), C_OK;

    return shardRemoteBegin(c, owner);              /* REMOTE: suspend + hop (§3.2) */
}
```

`commandNeedsBarrier(c)` is a predicate over `c->cmd->flags` + argv: true unless the command
is flagged `CMD_SHARD_SAFE` **and** resolves to a single slot. Default the flag **off**; in
Phase 4 only trivially-safe single-key **reads** (`GET`, `STRLEN`, `HGET`, `LLEN`, …) get it.
This keeps the correctness surface tiny — anything not explicitly proven safe takes the barrier
and runs exactly as today.

#### 3.2 REMOTE begin — suspend as `BLOCKED_SHARD`, then hop

```c
int shardRemoteBegin(client *c, int owner) {
    shardExecJob *job = zmalloc(sizeof(*job));
    job->coordinator = c;
    job->handle      = c->id;              /* uint64 client id, for disconnect validation (§4) */
    job->coord_shard = myShardId();
    job->argv = c->argv; job->argc = c->argc;   /* borrowed; lifetime rule in §4 */
    job->slot = c->slot;

    /* Suspend using the existing blocking framework — NOT a bespoke flag. */
    blockClient(c, BLOCKED_SHARD);         /* src/blocked.c:106; querybuf now held, not parsed */
    c->flag.pending_command = 1;           /* so processPendingCommandAndInputBuffer re-drives on resume */

    if (spscIsFull(&server_shards[owner].inbox)) {
        /* Backpressure: owner inbox full. Do NOT spin the coordinator. Park the job on a
         * per-owner overflow list drained by shardBeforeSleep; or, simplest for Phase 4,
         * unblock with a transient -TRYAGAIN. Measure before choosing (§6). */
        return shardRemoteAbort(c, owner, job);
    }
    spscEnqueue(&server_shards[owner].inbox, tagJob(job, SHARD_REQ_EXEC), /*commit=*/true);
    return C_OK;                            /* no reply yet; commandProcessed() sees blocked, bails */
}
```

Because `blockClient` set `c->flag.blocked`, the return path is already correct with **zero new
plumbing**: `processCommandAndResetClient` calls `commandProcessed`, which returns at
`src/networking.c:3887` without resetting the client or advancing offsets. The client sits
blocked; its socket stays registered on the coordinator's loop; its half-built reply buffer is
untouched.

#### 3.3 Owner side — execute with no socket, capture reply bytes

The owner thread drains its inbox in `shardBeforeSleep` (§5b). For a `SHARD_REQ_EXEC`
job it runs the command against a **per-shard executor client** — one long-lived
`createClient(NULL)` per shard, the AOF-loader pattern (`src/aof.c:1491,1560`) — never against
the coordinator's client (which lives on another thread and owns a socket this thread must not
touch):

```c
void shardExecOne(shard *self, shardExecJob *job) {
    client *x = self->executor;            /* per-shard, socket-less, reply goes to c->buf/c->reply */
    x->argv = job->argv; x->argc = job->argc; x->slot = job->slot;
    x->db   = server.db + job->dbid;

    call(x, CMD_CALL_FULL & ~CMD_CALL_PROPAGATE);   /* Phase 4 = reads; no journal/propagation yet */

    /* Detach the reply as a flat byte blob. x->buf (bufpos) + x->reply list -> one sds. */
    sds bytes = shardDetachReply(x);       /* concat x->buf[0..bufpos] + each reply node */
    resetClient(x);                        /* frees argv-independent per-command state on the executor */

    shardResult *res = zmalloc(sizeof(*res));
    res->handle = job->handle; res->coordinator = job->coordinator; res->bytes = bytes;
    spscEnqueue(&server_shards[job->coord_shard].results, tagRes(res, SHARD_RES_DONE), true);
    shardWakeLoop(job->coord_shard);       /* cross-thread wake of the coordinator's el (§3.5) */
}
```

The owner produces **RESP bytes**, not a formatted-for-this-socket reply — the coordinator owns
formatting/encoding decisions (RESP2/3, push frames). In practice `call()` on the executor
already emits RESP into `x->buf`/`x->reply`; `shardDetachReply` just concatenates those into one
`sds` and clears them. The executor's protocol version must be pinned to the coordinator
client's (`x->resp = c->resp`) so map/set/double encodings match what the real client
negotiated — set it per job.

#### 3.4 Coordinator side — deliver the continuation and resume

The coordinator's `shardBeforeSleep` drains its `results` queue:

```c
void shardDrainResults(shard *self) {
    void *items[64]; size_t n;
    while ((n = spscDequeueBatch(&self->results, items, 64))) {
        for (size_t i = 0; i < n; i++) {
            shardResult *res = untagRes(items[i]);
            client *c = shardLookupLiveClient(res->coordinator, res->handle);  /* §4 disconnect guard */
            if (c) {
                _addReplyProtoToList(c, c->reply, res->bytes, sdslen(res->bytes)); /* src/networking.c:700 */
                c->reply_bytes += sdslen(res->bytes);
                putClientInPendingWriteQueue(c);      /* flush on this shard's beforeSleep */
                unblockClient(c, /*queue_for_reprocessing=*/1);  /* src/blocked.c:217 */
            }
            sdsfree(res->bytes); zfree(res);
        }
    }
}
```

`unblockClient(..., 1)` puts `c` on `server.unblocked_clients` (per-shard under Model B);
`processUnblockedClients` (`src/blocked.c:158`) then calls
`processPendingCommandAndInputBuffer` (`src/networking.c:3962`). Here is the subtle part worth
stating explicitly: **the resume must NOT re-execute the command.** The reply is already
appended and this was a read (nothing to redo). So `BLOCKED_SHARD` clears `pending_command`
and lets the normal post-unblock flow finalize stats (`updateStatsOnUnblock`,
`src/blocked.c:129`) and reset the client — it re-enters `processPendingCommandAndInputBuffer`
only to *finish* the command's bookkeeping and then continue parsing the **next** pipelined
command from the held querybuf. Concretely: set `pending_command = 0` in the `BLOCKED_SHARD`
arm of `unblockClient` (mirroring how `BLOCKED_WAIT` finalizes without redo), append reply
before unblocking, and the re-drive resumes at the *next* command. This is the one place the
"reuse BLPOP" analogy needs care: BLPOP re-runs its command on wake; REMOTE does not.

#### 3.5 The cross-thread wake — the only piece with no in-tree analog

`handleClientsBlockedOnKeys` wakes blocked clients **inline on the same thread**. A REMOTE
result is produced on the owner thread while the coordinator sits in `aeApiPoll`. So each shard
loop needs a **wake fd** (an `eventfd`/self-pipe registered on its own `el` with a no-op read
handler); `shardWakeLoop(coord_shard)` writes one byte to force `aeApiPoll` to return, after
which `shardBeforeSleep` → `shardDrainResults` runs. This is the standard "async loop wake"
pattern; it is new to Valkey's client path but trivial and well-trodden. Coalesce writes (only
wake if the results queue transitioned empty→non-empty) to avoid a syscall per result.

### 4. Correctness details the sketch must pin down

- **argv lifetime across the hop.** `job->argv` borrows the coordinator client's argv. The
  coordinator is `BLOCKED_SHARD`, so `commandProcessed` did **not** reset it
  (`src/networking.c:3887`) — argv stays valid for the whole hop. Rule: the owner **reads**
  argv, never frees it; the coordinator frees argv only in `resetClient` after the result is
  delivered. The `robj`s are shared read-only across threads for the hop's duration; this is
  safe only because they are not mutated (reads) — a write path (Phase 5) must either deep-copy
  argv into the job or guarantee the coordinator won't touch them, and that is a Phase-5
  decision, not smuggled in here.
- **Client disconnect mid-hop.** The coordinator may free the client (connection reset,
  `CLIENT KILL`, timeout) while a job is in flight. The job carries `handle = c->id` (a
  monotonic id, `src/server.h`), not just the pointer. On result delivery,
  `shardLookupLiveClient` validates the pointer is still live **and** the id matches; a freed
  or recycled client drops the result. Symmetrically, freeing a `BLOCKED_SHARD` client must
  mark it so a late result is discarded — reuse the blocked-client teardown path
  (`unblockClient` is already called from client-free for other block types).
- **A REMOTE command that itself blocks on the owner (Phase 5+).** `BLPOP` on a remote slot
  would block *on the owner shard*. In Phase 4 (reads only) this cannot arise. When it does, the
  owner blocks its executor on keys as usual and posts the result only when the owner-side
  block resolves; the coordinator client stays `BLOCKED_SHARD` throughout. Flagged here so the
  Phase-4 `CMD_SHARD_SAFE` set explicitly excludes blocking commands.
- **Per-client causal order (read-your-writes).** Writes take the barrier (§3.1), which
  quiesces all shards and runs on the coordinator synchronously before the client's next
  command is parsed; a subsequent REMOTE read therefore observes the write. Within a client,
  `BLOCKED_SHARD` holds the querybuf so a read is never dispatched ahead of an earlier command.
- **No reply reordering under pipelining.** Same mechanism: the coordinator processes one
  client's commands strictly in order, and each REMOTE command fully resumes (reply appended)
  before the next is parsed. Reply bytes are appended to `c->reply` in dispatch order.

### 5. Files touched

- `src/shard.c` / `src/shard.h` — **extend** (§14.2 defines the module): `shardDispatch`
  branch, `shardRemoteBegin`, `shardExecOne`, `shardDetachReply`, `shardDrainResults`,
  `shardWakeLoop`, the per-shard executor client + wake fd, `commandNeedsBarrier`, `myShardId`.
- `src/server.h` — add `BLOCKED_SHARD` to `blocking_type` before `BLOCKED_NUM`
  (`:349`); add `CMD_SHARD_SAFE` command flag. No new `ClientFlags` bit (reuse
  `blocked`/`pending_command`).
- `src/blocked.c` — `unblockClient` (`:217`) gains a `BLOCKED_SHARD` arm that finalizes
  **without re-execution** (clear `pending_command`, run `updateStatsOnUnblock`); teardown on
  client-free discards in-flight results.
- `src/server.c` — `processCommand` (`:4315`) tail calls `shardDispatch(c)` instead of `call()`.
- `src/networking.c` — no change to `commandProcessed`/`processCommandAndResetClient`; they
  already do the right thing for blocked clients (the point of §1). Reply append reuses
  `_addReplyProtoToList` (`:700`).
- `src/commands/*.json` — add `SHARD_SAFE` to the Phase-4 read set; regenerate `commands.def`.
- `src/unit/test_shard_dispatch.cpp` — **new**: branch-selection + detach/reattach unit tests.
- `tests/unit/shard-remote.tcl` — **new**: end-to-end LOCAL/REMOTE read correctness.

**Not touched in Phase 4:** the journal/sequencer (Phase 5), propagation, `call()` internals,
connection migration (§5b Change 4 / Phase 4a).

### 6. Test plan

**Unit (`src/unit/test_shard_dispatch.cpp`)** — no threads; drive the pure pieces:

1. `BranchSelectsLocalWhenOwnerIsSelf` — stub `slot_to_shard`/`myShardId`; a single-slot
   `SHARD_SAFE` read on an owned slot picks LOCAL (no job enqueued).
2. `BranchSelectsRemoteForForeignSlot` — foreign slot enqueues exactly one `SHARD_REQ_EXEC`
   and blocks the client as `BLOCKED_SHARD`.
3. `BranchSelectsBarrierForNonSafeOrMultiSlot` — non-`SHARD_SAFE` cmd, keyless cmd, and
   multi-slot argv all route to `shardBarrierRun`.
4. `DetachReattachRoundTrips` — a reply built into an executor client's `buf`+`reply` list,
   detached to `sds`, reattached to a second client, yields byte-identical output for RESP2
   and RESP3 (verifies the encoding pin in §3.3).

**Integration (`tests/unit/shard-remote.tcl`)** — real server, `shard-threads > 1`:

5. LOCAL/REMOTE transparency — with keys spread so some land on foreign shards, `GET`/`MGET`
   (single-slot in cluster) return identical results to `shard-threads 1`.
6. Pipeline ordering — a pipeline mixing LOCAL and REMOTE reads returns replies in request
   order.
7. Disconnect mid-hop — kill a client with an in-flight REMOTE read (inject latency on the
   owner); server does not crash, no leaked block, `blocked_clients` returns to 0.
8. Read-your-writes — `SET` (barrier) then `GET` (REMOTE) on the same connection observes the
   write.
9. No-op at default — `shard-threads 1`: the whole existing suite passes unchanged (dispatch
   is the identity branch).

**Backpressure/perf (`src/valkey-benchmark`):** saturate one owner from many coordinators;
confirm the §3.2 full-inbox path degrades gracefully (no coordinator spin), and publish the
REMOTE per-command tax vs LOCAL (the break-even the §11 demands).

### 7. Verification

1. `make -C src && make -C src test-unit UNIT_TEST_PATTERN='ShardDispatch*'`.
2. `./runtest --single unit/shard-remote`.
3. Regression: `./runtest` with default config — zero behavior change (identity branch).
4. Manually: `shard-threads 4`, cluster mode; `DEBUG SLOT-SHARD` (Phase 2) to find a foreign
   slot; `GET` a key on it; confirm the reply is correct and `INFO clients` shows the client
   transiently `BLOCKED_SHARD` under load.

### 8. References

- Core design: §5 (execution
  paths), §5a (connection mechanics / continuation), §5b (per-thread loop — prerequisite),
  §14.3 (the dispatch sketch this note refines), §14.5 (barrier).
- Phase 2 (prerequisite): [Phase 2](#16-phase-2-the-ownership-map-and-config).
- Code: `src/server.c:4315` (`processCommand`), `src/networking.c:3879,3932,3962`
  (blocked-client return contract), `src/blocked.c:106,158,217` (block/resume framework),
  `src/server.h:340` (`blocking_type`), `src/aof.c:1491` (socket-less executor precedent),
  `src/queues.h` (transport).

## 20. Phase 4a: connection migration

This is the concrete, line-anchored design of **Phase 4a** of §10 — moving a live client
connection
from one shard thread to another, so that a client whose traffic targets a foreign slot stops
paying a REMOTE hop on every command.

§5b Change 4 calls this "the genuinely fiddly part" and insists it is "scope, not
polish." §5a explains why: **without migration, a badly-placed client pays a REMOTE hop on
every command, forever**, and static placement cannot fix a client whose hot slot lives on
another thread.

Prerequisites: [Phase 4 part 1](#18-phase-4-part-1-per-thread-event-loops) (per-thread event
loops, `conn->el`, the adopt path) and [Phase 4 part 2](#19-phase-4-part-2-dispatch-and-the-remote-continuation)
(LOCAL/REMOTE dispatch — without it there is no REMOTE ratio to measure and nothing to optimize).

### 1. Scope

> "Connection migration — move a live connection to the shard its traffic targets, so arbitrary
> clients go LOCAL. Independently gated because live migration (fd re-arm across loops, partial
> buffers, blocking state) is the fiddliest single piece." (§10.4a)

**In scope:** the transfer protocol for a live `client` + `connection` between shard loops; the
safety predicate that decides when a client is transferable; the policy that decides when and
where to move it; the observability to tell whether it is working.

**Out of scope:** placement at accept (part 1 §4.3 — round-robin); slot rebalancing between
shards (§9 — that moves *data*, this moves *clients*, and the two should not be
conflated); writes and the journal (Phase 5).

**The honest framing.** Migration is an optimization whose absence is a correctness-neutral
performance cliff. It should therefore be built so that **failing to migrate is always safe**:
every decision point can decline, and the client simply keeps paying REMOTE hops. A migration
system that must succeed is far harder than one that may give up, and there is no reason to
build the harder one.

### 2. Verified anchors (this checkout)

- **What a client owns, and therefore what must move.** `struct client` (`src/server.h`):
  `connection *conn` (`:1300`), `sds querybuf` (`:1302`) — partially-parsed input,
  `char *buf` + `list *reply` (`:1332`, `:1334`) — unflushed output,
  `listNode clients_pending_write_node` (`:1339`) — embedded node, linked into a *list owned by a
  shard*, `blockingState *bstate` (`:1329`, lazily created), `multiState *mstate` (`:1328`),
  `hashtable *pubsub_channels` (`:1203`), `rax *client_tracking_prefixes` (`:1210`),
  `int slot` (`:1378`), `volatile uint8_t io_read_state` / `io_write_state` (`:1356-1357`),
  `_Atomic(size_t) io_tracked_reply_len` (`:1373`).
- **The embedded pending-write node is the sharp edge.** `clients_pending_write_node` is a
  `listNode` *inside* the client struct (`src/server.h:1339`), linked with
  `listLinkNodeHead(server.clients_pending_write, &c->clients_pending_write_node)`
  (`src/networking.c:422`) and unlinked at `src/networking.c:2045-2046`. It is not a pointer that
  can be reassigned — the node lives in the client and is threaded into whichever shard's list.
  Migration must unlink it on the source thread and relink on the destination thread, never
  leaving it linked into a list another thread is walking.
- **Registries the client is a member of.** `server.clients` + `c->client_list_node`
  (`src/networking.c:212,216`), `server.clients_index` rax keyed by client id (`:218`, looked up
  at `:2426`), plus the per-shard lists part 1 §4.5 introduces
  (`clients_pending_write`, `unblocked_clients`, `clients_to_close` — `src/networking.c:422`,
  `:2055-2057`, `:2250-2251`).
- **The precedent for "this client is busy, skip it."** `clientsCron` already refuses to touch a
  client with I/O in flight: `if (c->io_read_state != CLIENT_IDLE || c->io_write_state !=
  CLIENT_IDLE) continue;` (`src/server.c:1236`). The migration safety predicate is the same idea,
  with more conditions.
- **Blocking framework** (used to define "quiescent"): `blockClient` (`src/blocked.c:106`),
  `unblockClient` (`src/blocked.c:217`), `processUnblockedClients` (`src/blocked.c:158`),
  `blocking_type` enum (`src/server.h:340-351`), and part 2's `BLOCKED_SHARD`.
- **The fd registration calls that must run on the owning thread.** `aeCreateFileEvent` /
  `aeDeleteFileEvent` via `connSetReadHandler` (`src/connection.h:282`), `connSetWriteHandler`
  (`:275`), `connSetWriteHandlerWithBarrier` (`:291`), `connHasReadHandler` (`:389`) — all of
  which reach `src/socket.c:239-254` (and the TLS mirror `src/tls.c:1201-1208,1250-1260`), where
  part 1 §4.2 replaced `server.el` with `conn->el`.
- **Transport for the control messages.** `spscQueue` (`src/queues.h:112`), `spscEnqueue`
  (`:136`), `spscDequeueBatch` (`:140`).

### 3. Design

#### 3.1 The protocol

The invariant that makes this safe is one sentence: **at every instant, exactly one thread may
touch a given `client`, and that thread is the one whose loop the fd is registered on.** The
protocol is therefore a hand-off with a gap in the middle where the fd is registered *nowhere*
and the client belongs to the message in flight.

```text
   SOURCE shard A (owns the client now)            DEST shard B
   ────────────────────────────────────            ───────────────────────────────
   1. policy proposes move (§3.3)
   2. safety predicate says yes (§3.2)
   3. DETACH — all on A's thread:
        connSetReadHandler(conn, NULL)   ─ fd unregistered from A->el
        connSetWriteHandler(conn, NULL)
        unlink clients_pending_write_node from A's list
        remove from A's clients / unblocked / close lists
        conn->el = NULL          ← "registered nowhere" window opens
   4. spscEnqueue(B->inbox, SHARD_REQ_ADOPT_LIVE{client*}) ──────┐
      shardWakeLoop(B)                                            │
      (A never touches this client again)                        ▼
                                                    5. ADOPT — all on B's thread:
                                                         conn->el = B->el
                                                         link into B's clients list
                                                         relink pending-write node if
                                                           the client had unflushed output
                                                         connSetReadHandler(conn, readQueryFromClient)
                                                         re-arm write handler if needed
                                                       ← window closes; B owns it
```

**During the window (step 4) the socket is not polled by anyone.** That is safe: incoming bytes
sit in the kernel receive buffer, and outgoing bytes sit in `c->buf`/`c->reply`. It is *not* safe
for the window to be unbounded, so:

- The adopt message must never be dropped. If B's inbox is full, **abort the migration**: A
  re-arms the handlers and keeps the client. Aborting is always allowed (§1), and this is the
  first place to use it.
- B must process adoptions with priority over ordinary work in `shardBeforeSleep`, so the window
  is one loop iteration.
- A watchdog counts clients in the window; a non-zero count that persists across cron cycles is a
  bug, and should log loudly rather than silently stall a connection.

**Nothing is copied.** The `client` and `connection` structs move by pointer; only *ownership*
changes. This is what makes the operation cheap and, more importantly, what keeps `c->querybuf`,
`c->buf`, `c->reply`, `mstate`, `bstate`, and the tracking rax trivially intact — they are the
same memory, reachable from exactly one thread at a time. The only things that genuinely need
work are the **list memberships** (§2: the embedded `clients_pending_write_node`) and the **fd
registration**.

#### 3.2 The safety predicate — when a client may move

Refuse unless *all* of these hold. Each one is a condition under which some other thread or some
in-flight operation still has a claim on the client:

| Condition | Why |
|---|---|
| `c->io_read_state == CLIENT_IDLE && c->io_write_state == CLIENT_IDLE` | An I/O worker may be mid-read/write on this client's buffers. Same guard `clientsCron` uses (`src/server.c:1236`). |
| Not `c->flag.blocked` — in particular not `BLOCKED_SHARD` | A REMOTE result is in flight carrying this client's pointer (part 2 §4). Moving it would deliver the reply on the wrong thread. Also excludes `BLPOP`/`WAIT`/`XREAD` waiters, whose wake-up state is registered with the *source* shard. |
| No in-flight command: not `c->flag.pending_command`, `c->argc == 0` | Migrating mid-command means two threads reason about one execution. |
| Not inside `MULTI` (`c->flag.multi` / `c->mstate` non-empty) | The queued commands and the `WATCH` set are consistent with the source shard's view. Deferring until `EXEC` completes costs nothing. |
| Not a replica, not the primary link, not a monitor, not a module-blocked client | These connections are shard 0's by construction (part 1 §4.2 rule 1) and must never move. |
| No pending TLS handshake / `connTypeHasPendingData()` for this conn | Transport-level state that assumes a loop. |
| Not currently being freed (`c->flag.close_asap` / on a close list) | Racing a teardown. |
| Cooldown since its last migration has elapsed | §3.3. |

The predicate is evaluated **on the source thread**, which owns the client, so it reads only
thread-local state — no locking, and no TOCTOU window, because nothing else can change these
fields.

**Clients that subscribe.** Pub/Sub is global and routed through the coordinator (§8), so
a subscriber may migrate — but its `pubsub_channels` (`src/server.h:1203`) participates in the
global channel registry (`server.pubsub_channels`, `src/server.h:2252`). Until that registry is
made shard-aware, treat "has any subscription" as a refusal condition. It is a small population
and a large correctness surface.

#### 3.3 The policy — when and where to move

The mechanism is the hard part; the policy should be as dumb as it can be while still working.

**Measurement.** Each client keeps a tiny per-shard histogram of the shards its recent commands
targeted: `uint16_t shard_hits[N]` decayed periodically (or an exponentially-weighted top-1
estimate if N is large). Updated in dispatch (part 2 §3.1), which already computes `owner`, so it
costs one increment on a cache line the thread already owns.

**Trigger.** In `shardBeforeSleep` (or on a slower per-shard cron tick, which is cheaper and
plenty responsive), for each client: if `shard_hits[best] / total > threshold` (start at 0.8),
`best != myShardId()`, and the sample count exceeds a floor (say 100 commands, so a handful of
early commands cannot move a connection), propose migration to `best`.

**Damping — the part that will actually bite.** Without it, a client whose traffic alternates
between two shards will ping-pong, and migration is not free (a loop-iteration stall plus two
`epoll` mutations plus cache-cold buffers on the destination).

- Per-client **cooldown**: no migration within N seconds of the last one; back off multiplicatively
  on repeated moves of the same client.
- Per-shard **rate cap**: at most K migrations per shard per second, so a topology shift cannot
  turn into a migration storm.
- **Hysteresis**: require the dominant shard to stay dominant across two consecutive evaluation
  windows.
- **Global kill switch**: `shard-connection-migration no` disables the whole mechanism, which is
  how an operator recovers from pathological behavior without a restart, and how a benchmark
  isolates its effect.

**A client with no dominant shard should not move at all.** A client spreading uniformly over
all slots cannot be made LOCAL; migrating it just adds cost. The threshold handles this, but say
it explicitly, because the tempting "move it to the least-loaded shard" heuristic is load
balancing, not locality, and this mechanism should do exactly one thing.

#### 3.4 Interaction with slot rebalancing

§9 allows `slot_to_shard[]` to be re-partitioned under a barrier — moving *data* ownership
between threads. That instantly invalidates every client's affinity measurement, and could
trigger a stampede of migrations.

Rule: **on any `slot_to_shard[]` change, reset all affinity histograms and start the cooldown
clock**. Rebalancing is rare and already takes a barrier, so this is a cheap addition at exactly
the right place. Without it, the two mechanisms will fight each other, and that failure mode is
very hard to diagnose from the outside.

#### 3.5 Observability

Migration is a background behavior that silently determines whether the whole design delivers.
It must be visible or it cannot be tuned:

- `INFO`: `shard_migrations_total`, `shard_migrations_refused` broken down by predicate reason,
  `shard_migrations_aborted` (inbox full), current in-window count.
- Per-shard: client count, and the aggregate **LOCAL hit ratio** — the single number that says
  whether placement is working. Publish it per shard and overall.
- `CLIENT INFO` / `CLIENT LIST`: the client's home shard, its dominant target shard, and its
  migration count. This is what turns "my p99 is bad" into "this client is pinned to the wrong
  shard and keeps getting refused because it holds a subscription."
- A `DEBUG SHARD-MIGRATE <client-id> <shard>` for tests to force a migration deterministically —
  without it, §5's tests depend on policy timing, which makes them flaky.

### 4. Files touched

- `src/shard.{c,h}` — `shardMigrateConnection(client *c, int dst)`, the `SHARD_REQ_ADOPT_LIVE`
  message and its handler, the safety predicate, the policy evaluator, per-shard rate limiting,
  counters.
- `src/server.h` — per-client affinity state (`shard_hits[]`, last-migration timestamp, migration
  count) on `struct client`; keep it small and cold-packed, since it is per-connection memory.
- `src/networking.c` — factor client registry link/unlink into `clientAttachToShard(c, shard)` /
  `clientDetachFromShard(c)` covering `client_list_node` (`:212-216`),
  `clients_pending_write_node` (`:422`, `:2045-2046`), `unblocked_clients` (`:2055-2057`), and
  `clients_to_close` (`:2250-2251`). Part 1 already needs this factoring for accept-time adoption;
  4a reuses it for both sides of the hand-off.
- `src/blocked.c` — the predicate consults `c->flag.blocked` / `bstate->btype`; no behavior
  change.
- `src/config.c` — `shard-connection-migration` (bool, default **yes** once proven; ship it
  **no** and flip after the benchmark), `shard-migration-threshold`,
  `shard-migration-cooldown-ms`, `shard-migration-max-per-sec`.
- `src/debug.c` — `DEBUG SHARD-MIGRATE <client-id> <shard>`.
- `tests/unit/shard-migration.tcl` — **new**: §5.
- `src/unit/test_shard_policy.cpp` — **new**: the policy as a pure function over a histogram.

### 5. Test plan

**Unit (`src/unit/test_shard_policy.cpp`)** — no threads:

1. `NoDominantShardDoesNotMigrate` — uniform histogram proposes nothing.
2. `DominantForeignShardProposesMove` — 90% on shard 3 proposes shard 3.
3. `CooldownAndHysteresisSuppressPingPong` — alternating dominance across windows produces at
   most one move, then nothing until cooldown expires.
4. `RateCapHolds` — 10k eligible clients on one shard yield at most K proposals per second.

**Integration (`tests/unit/shard-migration.tcl`)**, using `DEBUG SHARD-MIGRATE` for determinism:

5. **Correctness across a move.** A client with a deep pipeline in flight and a partially-received
   command in `querybuf` is migrated; every reply arrives, in order, with no duplication and no
   loss. Run it with the migration forced at many different points via a fuzzing loop, because
   the interesting bugs live at boundaries the test author did not think of.
6. **Unflushed replies survive.** Force a migration while `c->buf`/`c->reply` hold unflushed data
   (client not reading); after the move, the destination flushes it correctly and in order.
7. **The predicate is honored.** A blocked (`BLPOP`) client, a `BLOCKED_SHARD` client, a client
   inside `MULTI`, a subscriber, and a replica each refuse migration; the refusal reason is
   counted in `INFO`.
8. **Abort path.** Fill the destination inbox; the migration aborts, the client keeps working on
   the source shard, `shard_migrations_aborted` increments, and nothing is left half-detached.
9. **Disconnect during the window.** Kill the client between detach and adopt; no crash, no leak,
   no double free, `connected_clients` returns to baseline.
10. **Rebalance interaction.** Change `slot_to_shard[]` under load; histograms reset, no
    migration storm, LOCAL ratio recovers.
11. **Policy end to end.** Start 100 clients with round-robin placement and slot-affine traffic;
    within a bounded time the LOCAL hit ratio exceeds 90% and migrations stop. **This is the
    phase's headline test** — it is the claim §5a makes about Dragonfly, tested.
12. **Kill switch.** With migration disabled, behavior is identical to Phase 4 and the LOCAL ratio
    stays at its round-robin baseline.

### 6. Verification

1. `make -C src && make -C src test-unit UNIT_TEST_PATTERN='ShardPolicy*'`.
2. `./runtest --single unit/shard-migration`.
3. Full suite at `shard-threads 4` with migration **on**, and again with it **off** — identical
   results.
4. **TSan** with migration on and an aggressive policy (threshold 0.5, no cooldown) under load.
   This configuration exists to break things; it is the strongest signal available on whether the
   hand-off is really exclusive.
5. ASan run of §5.9 (disconnect-in-window) in a loop.
6. Benchmark: round-robin placement, migration off vs on. Report LOCAL hit ratio, throughput, and
   p99 latency, plus the transient cost of a migration burst at the start of the run. **The gap
   between off and on is the entire justification for this phase** — if it is small, say so, and
   consider whether smarter accept-time placement plus client-side hints would have been enough.

### 7. Honest risks

- **The window is a real stall.** A migrating connection is unpolled for a loop iteration. Under a
  latency SLO this shows up as a p99 blip correlated with migration bursts. The rate cap bounds
  it; the benchmark must report it rather than average it away.
- **The predicate will be wrong before it is right.** Every refusal condition in §3.2 is a claim
  that no other thread holds a reference. New features add new references — a future
  cross-shard blocking type, a new module API, a tracking mechanism — and each is a chance to
  migrate a client someone else is holding. Mitigation: make the predicate one function with a
  comment naming *why* each condition exists, and add an assertion on the destination that the
  client's every list membership is empty on arrival.
- **Policy is where this gets tuned forever.** Thresholds, decay, and cooldown are workload
  dependent. Ship conservative defaults and the kill switch; resist adding adaptivity until real
  traffic asks for it.
- **It may not be worth it.** If accept-time placement plus cluster-aware clients (which already
  know which node owns a slot, §5) get most of the LOCAL ratio, this phase's complexity
  buys little. §6.6 is designed to answer that honestly, and "we measured it and skipped 4a" is a
  legitimate outcome — it is why §10 gates it separately.
- **Subscribers and blocked clients are permanently excluded** under §3.2 as written. For a
  workload dominated by blocking commands or pub/sub, migration does approximately nothing.
  Name that population before promising the feature.

### 8. References

- Core design: §5a (why migration is
  scope, not polish), §5b Change 4, §9 (slot rebalancing — the other thing that moves), §10.4a.
- Prerequisites: [Phase 4 part 1](#18-phase-4-part-1-per-thread-event-loops) (the loops and the
  adopt path this reuses), [Phase 4 part 2](#19-phase-4-part-2-dispatch-and-the-remote-continuation) (`BLOCKED_SHARD`,
  the REMOTE hop this exists to eliminate).
- Background: [03-clients-and-networking.md](03-clients-and-networking.md) (client struct and
  reply buffers).
- Code: `src/server.h:1203,1210,1300,1302,1328,1329,1332,1334,1339,1356,1378`,
  `src/networking.c:212,422,2045,2055,2250,2426`, `src/server.c:1236` (the busy-client guard
  precedent), `src/connection.h:275,282,291`, `src/blocked.c:106,158,217`, `src/queues.h:112`.

## 21. Phase 5: writes, journals, and the sequencer

This is the concrete, line-anchored design of **Phase 5** of §10 — letting **write** commands
execute on shard threads, and preserving today's replication and AOF guarantees while they do.

This is where Part I's §7 — "the part that decides whether the design is viable" — meets
the actual propagation code. [Phase 1](#15-phase-1-the-ordering-model-harness) proved the ordering
model in isolation and built `shard_journal.{c,h}`; this phase wires that module into
`propagateNow` and makes the shard threads its producers.

Prerequisites: [Phase 1](#15-phase-1-the-ordering-model-harness) (the module and its gate),
[Phase 4 part 1](#18-phase-4-part-1-per-thread-event-loops) (per-thread loops), and
[Phase 4 part 2](#19-phase-4-part-2-dispatch-and-the-remote-continuation) (LOCAL/REMOTE dispatch for reads — writes
extend the same branch).

### 1. Scope

> "Single-key writes + per-shard journals + the sequencer." (§10.5)

**In scope:** extending `CMD_SHARD_SAFE` to single-key **writes**; per-shard propagation
accumulation; the commit-id stamp and journal append; the sequencer that merges shard journals
into the existing replication backlog and AOF in commit-id order; the argv-lifetime change that
writes force; `WAIT`/`WAITAOF` correctness; keyspace-notification ordering.

**Out of scope:** multi-key and multi-slot writes, `MULTI`/`EXEC`, Lua, functions, modules — all
continue to take the barrier (§6), and the barrier is *already* correct for them because
it runs today's code on a quiesced keyspace. Per-shard expiry and eviction, which also produce
writes, are [Phase 6](#22-phase-6-per-shard-expiry-and-eviction). Replacing the barrier with VLL is
[proposal-vll-transactions.md](proposal-vll-transactions.md).

### 2. Verified anchors (this checkout)

The propagation pipeline this phase re-plumbs, end to end:

- `call()` (`src/server.c:3875`) snapshots `server.dirty` at `:3907`, recomputes the delta at
  `:3983`, and at `:4060-4083` decides `propagate_flags` from the dirty count and the client's
  `force_repl`/`prevent_prop` flags, ending in
  `alsoPropagate(c->db->id, c->argv, c->argc, propagate_flags, c->slot)` (`:4083`).
- `alsoPropagate` (`src/server.c:3680`) deep-copies argv and appends to the global
  `server.also_propagate` op array (`serverOpArrayAppend`, `src/server.c:3450`; freed by
  `serverOpArrayFree`, `:3471`; the append call is at `:3702`).
- `postExecutionUnitOperations` (`src/server.c:3799`) returns early when
  `server.execution_nesting` is non-zero (`:3800`), then runs `firePostExecutionUnitJobs()`,
  `propagatePendingCommands()` (`:3805`), and `modulePostExecutionUnitOperations()`.
- `propagatePendingCommands` (`src/server.c:3746`) wraps a multi-op unit in `MULTI`/`EXEC`
  (`:3768`, `:3779`) unless the command is `CMD_TOUCHES_ARBITRARY_KEYS` (`:3759-3763`), and calls
  `propagateNow` per op (`:3775`).
- `propagateNow` (`src/server.c:3626`) gates on `shouldPropagate(target)`, asserts the
  replica-pause invariant, and fans out to `replicationFeedReplicas` (`src/replication.c:579`)
  and the AOF feed (`feedAppendOnlyFile`, `src/aof.c:1446`).
- **The replication offset is assigned at emit time, not execution time.**
  `feedReplicationBuffer` (`src/replication.c:449`) advances `server.primary_repl_offset` as it
  copies (`:476`, `:507`); the no-replica/no-backlog shortcut still bumps it by one
  (`src/replication.c:597`) precisely so AOF fsync tracking keeps working.
- `WAIT` reads that counter: `waitCommand` (`src/replication.c:5086`),
  `replicationCountAcksByOffset` (`src/replication.c:5052`), unblock at `:5164`.
- AOF fsync bookkeeping crosses threads already: `_Atomic(long long)
  server.fsynced_reploff_pending` (`src/server.h:2105`) is published by a bio thread and consumed
  in `beforeSleep`.
- Other write-path side effects that must keep their order: `signalModifiedKey` (`src/db.c:755`) →
  `touchWatchedKey` (`src/multi.c:464`), `signalFlushedDb` (`src/db.c:760`), and
  `notifyKeyspaceEvent` (`src/notify.c:105`).
- `mustObeyClient` (`src/server.c:3589`) marks primary/AOF-sourced clients — the apply path
  of §4.7 — and is consulted by `getKeySlot` (`src/db.c:252-262`).
- Journal module from Phase 1: `journalRec`, `shardJournalCommit`, `shardJournalFlush`,
  `shardJournalEmittedUpTo` in `src/shard_journal.{c,h}`.

### 3. Design

#### 3.1 The shape of the change: keep `propagateNow`, change only who calls it

```text
   TODAY (one thread)
     call() ─► alsoPropagate ─► server.also_propagate ─► propagatePendingCommands
                                                            └─► propagateNow ─► repl backlog + AOF

   PHASE 5 (N shards)
     shard i: call() ─► alsoPropagate ─► shard[i].also_propagate
                                            └─► at unit end: build ONE journalRec
                                                 seq = fetch_add(commit_id)
                                                 append to shard[i] ring          (no lock)
                                                                │
     shard 0 beforeSleep: shardJournalFlush() ──────────────────┘
                            └─ in seq order ─► propagateNow ─► repl backlog + AOF
```

**`propagateNow` and everything below it stay exactly as they are, single-threaded, on shard 0.**
That is the design's central economy: the replication buffer, the backlog, the offset arithmetic,
the AOF buffer, the replica fan-out, the `WAIT` contract — none of it is touched, because the
sequencer becomes its only caller and the sequencer is one thread. The multi-threading stops at
the journal boundary.

Concretely the sequencer's emit callback is:

```c
static void shardSequencerEmit(const journalRec *rec, void *privdata) {
    UNUSED(privdata);
    if (rec->is_unit) {                       /* multi-op unit: preserve the MULTI/EXEC wrapping */
        propagateNow(-1, &shared.multi, 1, PROPAGATE_AOF | PROPAGATE_REPL, -1);
        for (int j = 0; j < rec->numops; j++)
            propagateNow(rec->ops[j].dbid, rec->ops[j].argv, rec->ops[j].argc,
                         rec->ops[j].target, rec->ops[j].slot);
        propagateNow(-1, &shared.exec, 1, PROPAGATE_AOF | PROPAGATE_REPL, -1);
    } else {
        propagateNow(rec->dbid, rec->argv, rec->argc, rec->target, rec->slot);
    }
}
```

which is `propagatePendingCommands` (`src/server.c:3746`) with its op array coming from a journal
record instead of a global. Keeping the `MULTI`/`EXEC` wrapping *inside the record* rather than
around it is what preserves execution-unit atomicity on the replica: a unit must not be split by
another shard's records interleaving.

#### 3.2 Per-shard propagation state

Three globals move onto the shard:

| Global | Anchor | Becomes |
|---|---|---|
| `server.also_propagate` | `src/server.c:3450,3471,3702` | `shard->also_propagate` — accumulated and drained entirely on the owning thread. |
| `server.dirty` | read `src/server.c:3907`, `:3983` | `shard->dirty`; `INFO`'s `rdb_changes_since_last_save` sums across shards under barrier. |
| `server.execution_nesting` | `src/server.c:3800` | Per-shard (it is a per-execution-stack property, so this is a correctness fix, not just a partition). |

`alsoPropagate` and `propagatePendingCommands` become shard-relative: same code, `server.` →
`thisShard()->`. Nothing about *when* they run changes — `postExecutionUnitOperations`
(`src/server.c:3799`) still fires at the top of the call stack, on whichever shard executed.

#### 3.3 The commit stamp — where exactly

At the end of the execution unit, on the owning shard, `propagatePendingCommands`' replacement
does:

```c
if (shard->also_propagate.numops == 0) return;      /* read-only command: no record, no seq */
journalRec *rec = journalRecFromOpArray(&shard->also_propagate);
shardJournalCommit(shard->journal, rec);            /* seq = fetch_add; append to ring */
serverOpArrayFree(&shard->also_propagate);
```

Three properties this placement buys, all of them load-bearing for §7:

- **The id is taken after execution completes.** Per-key order follows, because one shard owns a
  key and its ring is FIFO.
- **Per-client causality follows from the dispatch contract**, not from anything here: the
  coordinator does not dispatch command *k+1* until *k* has resumed
  ([Phase 4 part 2](#19-phase-4-part-2-dispatch-and-the-remote-continuation) §1 — `BLOCKED_SHARD` holds the querybuf),
  so *k* took the lower id. This is the property Phase 1 tested; Phase 5 must not weaken it,
  which is why **the REMOTE result must be posted after the commit, never before** (§3.4).
- **Read-only commands take no id at all**, so the shared atomic sees write traffic only.

#### 3.4 REMOTE writes: the argv-lifetime change

[Phase 4 part 2](#19-phase-4-part-2-dispatch-and-the-remote-continuation) §4 flags this explicitly and defers it here:

> "The `robj`s are shared read-only across threads for the hop's duration; this is safe only
> because they are not mutated (reads) — a write path (Phase 5) must either deep-copy argv into
> the job or guarantee the coordinator won't touch them, and that is a Phase-5 decision, not
> smuggled in here."

**Decision: keep the borrow, and rely on `BLOCKED_SHARD`.** The coordinator's client is blocked
for the whole hop, so `commandProcessed` returns early (`src/networking.c:3887`) and never
resets argv; the coordinator therefore does not touch those `robj`s, and the owner only reads
them. Deep-copying argv on every REMOTE write would add an allocation and a copy to the hop,
which is the exact tax §11 already worries about.

Two consequences that must be handled rather than assumed:

- **Command rewriting.** Commands that rewrite themselves for propagation (`SPOP` → `SREM`,
  `EXPIRE` → `PEXPIREAT`, `INCRBYFLOAT` → `SET`) call `rewriteClientCommandVector`-style helpers
  that *replace* `c->argv`. On a REMOTE write the executing client is the owner's executor client
  (part 2 §3.3), not the coordinator's — so the rewrite happens to the executor's argv, and the
  journal must capture *that* form. `alsoPropagate` already deep-copies argv into the op array
  (`src/server.c:3680-3702`), which means **the journal record owns its own copy and the borrow
  ends at unit end.** The borrow only has to survive the hop, which it does.
- **Ordering of the result vs the commit.** The owner must `shardJournalCommit` **before**
  posting `SHARD_RES_DONE`. Otherwise the coordinator could resume the client, dispatch command
  *k+1* to another shard, and let *k+1* take a lower id than *k* — the exact P2 violation Phase 1
  encodes as a failing test. Write this ordering as a comment at the enqueue site and as a test
  (§6.4); it is one line and it is the whole guarantee.

#### 3.5 `WAIT`, `WAITAOF`, and read-your-own-offset

A subtle gap opens between execution and emit: the client's write has committed (it has a `seq`)
but the sequencer may not have emitted it yet, so `server.primary_repl_offset`
(`src/replication.c:476`) does not yet cover it. A client that writes and immediately calls
`WAIT` would wait on an offset that excludes its own write — silently returning success too
early. That is a correctness regression, not a latency one.

Fix, in two parts:

1. The client records the `seq` of its last write (`c->last_write_seq`).
2. `WAIT`/`WAITAOF` are barrier commands (§6), and the barrier already flushes the
   sequencer before running the command (§14.5 step 2). Add the assertion that
   `shardJournalEmittedUpTo() >= c->last_write_seq` after that flush, and only then read the
   offset. Because the barrier quiesces every shard, the flush cannot stall on a gap: every id
   below the highest has been committed by a parked shard.

The same argument covers anything else that reads `primary_repl_offset` as a proxy for "my writes
are durable": `INFO replication`'s offsets, `FAILOVER`, and the replica-pause paths. Rule of
thumb to write into the code: **`primary_repl_offset` is meaningful only for records the
sequencer has emitted**; if a caller needs it to cover a specific write, it must flush first.

#### 3.6 Keyspace notifications and `WATCH`

Two side effects of a write are visible outside the shard:

- **`notifyKeyspaceEvent`** (`src/notify.c:105`) publishes to pub/sub. If shards publish inline,
  two clients watching different key patterns can observe events in an order inconsistent with
  the replication stream — a new anomaly. **Route notification publishes through the journal**
  as part of the record, so they are emitted in commit-id order by the sequencer, matching the
  replica's view. It costs nothing extra (they are already sequenced) and removes a whole class
  of "the notification arrived before the write" reports.
- **`signalModifiedKey`** (`src/db.c:755`) → `touchWatchedKey` (`src/multi.c:464`) marks watching
  clients dirty for `MULTI`/`EXEC` CAS. The watchers may be homed on other shards, and
  `db->watched_keys` is a per-db dict (`src/server.c:2910`), not partitioned. Since `EXEC` takes
  the barrier, correctness only requires the dirty mark to be *visible* by the time `EXEC` runs.
  Simplest correct approach: the owning shard records the touched key in its journal record and
  the **sequencer** applies `touchWatchedKey` on shard 0 during emit — one thread, before any
  `EXEC` barrier can observe it, because the barrier flushes the sequencer first. This trades a
  little latency in CAS invalidation for not sharding `watched_keys`, which is the right trade
  at this phase.

#### 3.7 Which commands become `SHARD_SAFE` writes

Extend part 2's flag conservatively — the flag defaults **off** and a command is barrier-free
only if flagged *and* single-slot:

- **In:** single-key writes with no side effect beyond the key and its TTL — `SET` (without
  `GET`? no: with `GET` too, it is still one key), `SETNX`, `SETEX`, `GETSET`, `APPEND`,
  `INCR`/`DECR`/`INCRBY`/`INCRBYFLOAT`, `SETRANGE`, `HSET`/`HDEL`/`HINCRBY`, `LPUSH`/`RPUSH`/
  `LPOP`/`RPOP` (non-blocking), `SADD`/`SREM`, `ZADD`/`ZREM`/`ZINCRBY`, `DEL`/`UNLINK` on one
  key, `EXPIRE`-family, `PERSIST`.
- **Out, and stay out for now:** anything blocking (`BLPOP` — part 2 §4 excludes them and Phase 5
  keeps that), anything multi-key or cross-slot, anything that can move keys between databases
  or slots (`MOVE`, `COPY` with `DB`, `RENAME`), anything `CMD_TOUCHES_ARBITRARY_KEYS`
  (`src/server.c:3759`), `SPOP` with count (random, and rewrites to a multi-key `SREM`),
  `GETEX`-style commands whose propagation depends on server state, everything module-related,
  and everything that can trigger eviction (see §4.6).

Add commands to this list **one at a time, each with a replica-equivalence test**. A wrongly
flagged command is a silent divergence between primary and replica, which is the most expensive
bug class this project can produce.

### 4. The interactions that decide whether this is safe

#### 4.1 Nested execution units

`postExecutionUnitOperations` early-returns on `server.execution_nesting` (`src/server.c:3800`)
so that an inner `call()` does not propagate before the outer one finishes. Making the counter
per-shard (§3.2) is necessary, but also verify the nesting sources that can appear on a shard
thread in this phase: key-miss notifications, expired-key deletions during lookup, and
`firePostExecutionUnitJobs`. Anything that can nest *across* shards must not exist — and does
not, because cross-shard work is either a REMOTE hop (whose owner runs its own unit) or a
barrier.

#### 4.2 Lazy expiration on the write path

Looking up a key can delete it and propagate a `DEL`/`UNLINK` (the lazy-expire path). Under
sharding this happens on the owner of the key — the same shard executing the command — so the
`DEL` lands in the same shard's op array, in the same execution unit, in the right order. This is
the case that is correct for free *because* ownership is per-slot, and it is worth an explicit
test (§6.7) since it is the most common way a "read-only" `GET` produces a write.

#### 4.3 The barrier must flush before it runs

§14.5 step 2 already says so; Phase 5 makes it load-bearing. A barrier command runs on a
quiesced keyspace and must see a fully-ordered prefix of the stream. It must also **not**
interleave its own propagation with journal records — so the barrier command propagates through
`propagateNow` directly, on shard 0, after the flush, and takes a commit id so that its position
in the stream is defined relative to later shard records.

#### 4.4 AOF

`flushAppendOnlyFile(0)` runs in `beforeSleep` on shard 0. The sequencer must run **before** it,
so records emitted this iteration reach `aof_buf` before the flush — the same ordering
`propagatePendingCommands` has today relative to `beforeSleep`'s AOF write
(`src/server.c:1854` region). With `appendfsync always`, a write's fsync is therefore deferred to
the first loop iteration after its record is emitted; that is already true today for any command
completing mid-iteration.

`server.fsynced_reploff_pending` (`src/server.h:2105`) is already atomic and already
cross-thread; nothing changes for it, because it tracks emitted offsets, which remain
single-threaded.

#### 4.5 The replica apply path

A replica applying from its primary runs commands through a client for which `mustObeyClient`
(`src/server.c:3589`) is true, and those commands may touch **any** slot — `getKeySlot` even
backfills `c->slot` for them (`src/db.c:260-262`) precisely because `getNodeByQuery()` never
ran. §8 lists this as needing its own design.

**Phase 5 decision: the apply path takes the barrier, unconditionally.** A replica therefore gets
no execution scaling from this phase. That is an acceptable and honest limitation — a replica's
apply stream is single-threaded today anyway, so nothing regresses — and it avoids designing
concurrent apply (which needs the primary to communicate its own commit order) in the same phase
that introduces concurrent writes on the primary. Note it in the release notes: **`shard-threads`
scales primaries, not replica apply.**

#### 4.6 Eviction during a write

`performEvictions` (`src/evict.c:404`) runs before command execution when `maxmemory` is
exceeded, evicts keys from *any* database and *any* slot, and propagates `DEL`s. A shard thread
cannot do that safely. **Phase 5 decision: if the server is over `maxmemory`, writes take the
barrier**, i.e. the eviction check escalates. That makes `maxmemory`-constrained workloads
unscaled by this phase — the honest cost — and is why per-shard eviction gets its own phase
([Phase 6](#22-phase-6-per-shard-expiry-and-eviction) §5) with its own measurement gate.

#### 4.7 Cluster slot migration

§8 notes slot ownership now implies thread ownership. Any `slot_to_shard[]` change or
cluster slot migration must take a barrier, and — new in this phase — the barrier must flush the
sequencer before reassigning, so that no journal record exists for a slot whose owner is about to
change. Coordinate with `design-docs/atomic-slot-migration.md` rather than inventing a second
mechanism.

### 5. Files touched

- `src/shard_journal.{c,h}` — extend Phase 1's module: multi-op unit records (`is_unit`,
  `ops[]`), attached keyspace notifications and watched-key touches (§3.6),
  `shardJournalEmittedUpTo` consumers.
- `src/shard.{c,h}` — per-shard `also_propagate`, `dirty`, `execution_nesting`; commit-before-post
  ordering in `shardExecOne`; barrier flush ordering.
- `src/server.c` — `alsoPropagate` (`:3680`) / `serverOpArrayAppend` (`:3450`) /
  `propagatePendingCommands` (`:3746`) become shard-relative; `postExecutionUnitOperations`
  (`:3799`) commits to the journal instead of propagating directly; `beforeSleep` runs
  `shardJournalFlush` before `flushAppendOnlyFile`; `call()` (`:3875`) uses `shard->dirty`.
  **`propagateNow` (`:3626`) is not modified** — only its call site moves.
- `src/server.h` — per-client `last_write_seq`; move `dirty` / `also_propagate` /
  `execution_nesting` out of `struct valkeyServer` (or keep them as the shard-0 instance).
- `src/replication.c` — `waitCommand` (`:5086`) flushes and checks `last_write_seq` (§3.5). No
  change to the feed or the offset arithmetic.
- `src/notify.c` — `notifyKeyspaceEvent` (`:105`) records into the journal when running on a
  shard thread; publishes directly on shard 0 / under barrier.
- `src/multi.c` — `touchWatchedKey` (`:464`) applied by the sequencer (§3.6).
- `src/evict.c` — escalate to barrier when over `maxmemory` (§4.6).
- `src/commands/*.json` — `SHARD_SAFE` on the §3.7 write set; regenerate `commands.def`.
- `tests/unit/shard-writes.tcl`, `tests/integration/shard-replication.tcl` — **new**.
- `src/unit/test_shard_journal.cpp` — extend with unit-record and notification-ordering cases.

### 6. Test plan

The bar is **byte-identical replication**, so most tests are differential.

1. **Replica equivalence under concurrent writers.** N clients writing disjoint keys across
   shards; after quiescing, primary and replica keyspaces are identical (`DEBUG DIGEST`), and the
   replica's applied command stream matches a single-threaded run's stream modulo interleaving of
   independent records.
2. **Per-client causality on the wire.** One connection issues `SET a 1` then `SET b 1` with `a`
   and `b` on different shards; a replica must never observe `b` without `a`. Run it in a loop
   under load — this is Phase 1's P2, now end to end.
3. **`shard-threads 1` is byte-identical.** The replication stream from a `shard-threads 1`
   server is bit-for-bit what `main` produces for the same workload.
4. **Commit-before-post ordering (§3.4).** Inject a delay between commit and result-post on the
   owner; assert per-client causality still holds. Then deliberately invert the order and assert
   the test *fails* — the same negative-test discipline Phase 1 uses.
5. **`WAIT` correctness.** Write then `WAIT 1 0` on the same connection with the sequencer
   artificially stalled; `WAIT` must not return before the write is emitted and acked. Same for
   `WAITAOF` with `appendfsync always`.
6. **Execution-unit atomicity.** A command propagating multiple ops (e.g. a write plus a lazy
   expire `DEL`) appears on the replica wrapped in `MULTI`/`EXEC` with no other shard's record
   interleaved.
7. **Lazy expire on a shard** (§4.2): a `GET` of an expired key on a non-zero shard propagates
   the `DEL` in the right position.
8. **Notification ordering** (§3.6): a subscriber to keyspace events sees them in an order
   consistent with the replication stream.
9. **AOF equivalence.** Same workload with AOF on; load the AOF into a fresh server; digest
   matches.
10. **Barrier interleaving.** Mix `SHARD_SAFE` writes with `MULTI`/`EXEC`, Lua, and `FLUSHALL`;
    replica digest matches and no record is lost across a barrier.
11. **Crash consistency.** Kill -9 mid-workload with AOF on; recovery yields a prefix of the
    committed stream, never a gap.
12. **Over-`maxmemory` escalation** (§4.6): writes still succeed and evictions propagate
    correctly, just via the barrier.

### 7. Verification

1. `make -C src && make -C src test-unit UNIT_TEST_PATTERN='ShardJournal*'`.
2. `./runtest --single unit/shard-writes`, `./runtest --single integration/shard-replication`.
3. Full suite plus `./runtest-cluster` at `shard-threads 1` (byte-identical) and `4`.
4. **TSan** at `shard-threads 4` under a write-heavy workload. Zero races.
5. Long-running differential soak: random command mix, periodic `DEBUG DIGEST` comparison between
   primary and replica, ≥1 hour, with a recorded seed.
6. Benchmark and publish: write throughput vs `shard-threads` (1, 2, 4, 8), the sequencer's
   per-record cost in production shape, reorder-buffer depth and stall p99 (Phase 1 §5's metrics,
   now on the real server), and the REMOTE-write tax vs LOCAL.

### 8. Honest risks

- **Silent divergence is the failure mode.** A wrongly-flagged `SHARD_SAFE` command, or a
  side effect that escapes the journal, produces a primary and a replica that disagree — and
  nothing errors. `DEBUG DIGEST` comparison in the soak (§7.5) is the only real defense, and it
  needs to run for hours, not minutes.
- **The sequencer is a new single-threaded bottleneck.** Every write in the system passes through
  it. Phase 1 measured it in isolation; if the real-server number is worse (cache-cold records,
  larger argv), the design's write ceiling is the sequencer, not the shards.
- **Gap stalls become replication latency.** A shard that commits an id and then is descheduled
  stalls emission of every later record. Bounded, but it is a new tail-latency source that does
  not exist today, and `WAIT` users will see it.
- **Over-`maxmemory` and replica-apply get no scaling** (§4.5, §4.6). Two large, common
  deployments are excluded by this phase. Say so plainly rather than letting a benchmark imply
  otherwise.
- **The `WATCH` path is deferred, not solved** (§3.6). Applying `touchWatchedKey` in the
  sequencer is correct given `EXEC` barriers, but it couples CAS invalidation latency to
  sequencer progress. If `MULTI`-heavy workloads matter, this needs revisiting.

### 9. References

- Core design: §7 (the ordering
  crux), §6 (the barrier), §8 (the rest of the system), §10.5, §14.4 (the journal sketch).
- Prerequisites: [Phase 1](#15-phase-1-the-ordering-model-harness) (the module and its gate),
  [Phase 4 part 1](#18-phase-4-part-1-per-thread-event-loops),
  [Phase 4 part 2](#19-phase-4-part-2-dispatch-and-the-remote-continuation) (argv lifetime, `BLOCKED_SHARD`).
- Next: [Phase 6](#22-phase-6-per-shard-expiry-and-eviction) (per-shard expiry and eviction — the other
  producers of writes).
- Related: [10-dragonfly-snapshot-model.md](10-dragonfly-snapshot-model.md) (the same journal
  makes a per-shard snapshot cut coherent), [proposal-forkless-rdb.md](proposal-forkless-rdb.md),
  `design-docs/atomic-slot-migration.md`.
- Background: [02-command-execution-path.md](02-command-execution-path.md),
  [06-persistence-rdb-aof.md](06-persistence-rdb-aof.md), [07-replication.md](07-replication.md).
- Code: `src/server.c:3450,3471,3589,3626,3680,3702,3746,3799,3875,3907,3983,4060,4083`,
  `src/replication.c:449,476,579,597,5052,5086`, `src/aof.c:1446`, `src/notify.c:105`,
  `src/multi.c:464`, `src/db.c:755,760`, `src/evict.c:404`.

## 22. Phase 6: per-shard expiry and eviction

This is the concrete, line-anchored design of **Phase 6** of §10 — moving the two background
keyspace sweepers, active expiry and eviction, off the single thread and onto the shards that own
the data.

Prerequisites: [Phase 4 part 1](#18-phase-4-part-1-per-thread-event-loops) (per-shard loops
and timers), [Phase 4 part 2](#19-phase-4-part-2-dispatch-and-the-remote-continuation) (dispatch), and
[Phase 5](#21-phase-5-writes-journals-and-the-sequencer) (the journal — both sweepers propagate writes, so
they cannot run on a shard before there is a journal to put those writes in).

**This phase is really two phases with very different risk profiles**, and the recommendation is
to land them separately:

- **6a — per-shard expiry.** A clean win. The scan primitive it needs (`kvstoreScan` over a
  hashtable *range*) already exists and is already used by cluster `SCAN`. Slot ownership makes
  it embarrassingly parallel with no shared state.
- **6b — per-shard eviction.** Genuinely hard, and gated on measurement. Eviction is a *global*
  decision (a memory limit for the whole process) implemented by *local* actions, and every
  simple partition of it either overshoots the limit or evicts the wrong keys. The honest first
  answer is to leave eviction on the barrier and only build 6b if the barrier is measured to hurt.

### 1. Scope

> "Per-shard expiry and eviction." (§10.6)
>
> "Expiry: per-shard cycles over owned slots. Naturally parallel; the lazy-expire path is already
> local to the key's shard. Eviction: per-shard eviction against a global `maxmemory` atomic with
> per-shard slack, to avoid a shared counter on the hot path." (§8)

**In scope:** the active-expire cycle scoped to owned slots; per-shard cron scheduling and time
budgets; the eviction analysis, its per-shard design, and the decision of when to build it.

**Out of scope:** lazy expiry (already correct — §3.1); `evictClients` (output-buffer eviction of
*clients*, not keys — that stays on shard 0, part 1 §4.4); the field-expiry (`FIELDS`/hash-TTL)
job is in scope structurally but gets the same treatment as `KEYS`, so it is not discussed
separately.

### 2. Verified anchors (this checkout)

**Expiry:**

- `activeExpireCycle(int type)` (`src/expire.c:459`) is the entry point, called from
  `databasesCron` (`src/server.c:1304`, slow cycle at `:1312`) and from `beforeSleep` for the fast
  cycle (`src/server.c:1916`).
- The work happens in `activeExpireCycleJob(jobType, cycleType, timelimit_us)`
  (`src/expire.c:199`). It iterates `CRON_DBS_PER_CALL` databases per call, keeping **file-static
  cross-call state**: `static expireState _expire_state[ACTIVE_EXPIRY_TYPE_COUNT]`
  (`src/expire.c:210`) holding `current_db` and `timelimit_exit`.
- Per-database scan position is `db->expiry[jobType].cursor` (field at `src/server.h:909`, reset
  by `resetDbExpiryState`, declared `src/server.h:3792`), advanced by
  `kvstoreScan(kvs, cursor, -1, -1, scan_cb, expireShouldSkipTableForSamplingCb, &data)`
  (`src/expire.c:341`) — note the **`-1, -1`**: scan the whole store.
- The kvstore being scanned is `db->expires` for `KEYS` and `db->keys_with_volatile_items` for
  `FIELDS` (`src/expire.c:273-289`).
- Budget constants: `ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP` 20, `ACTIVE_EXPIRE_CYCLE_FAST_DURATION`
  1000 µs, `ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC` 25%, `ACTIVE_EXPIRE_CYCLE_ACCEPTABLE_STALE` 10%
  (`src/expire.c:122-125`), scaled by `activeExpireEffort()` (`src/expire.c:195`).
- Stats consulted to decide whether to run a fast cycle at all:
  `server.stat_expired_keys_stale_perc` and `server.stat_expired_keys_with_vola_stale_perc`
  (`src/expire.c:215-218`).
- Per-key expiry action: `activeExpireCycleTryExpire(db, val, now, didx)` (`src/expire.c:66`),
  called from the scan callback at `src/expire.c:150`.
- **The enabling primitive:** `kvstoreScan` already supports a bounded table range —
  `first_idx`/`last_idx`, documented at `src/kvstore.c:472-475` and implemented at `:476` with
  fast-forward (`didx < first_idx`), early return past `last_idx`, and range-exhausted checks at
  `:511-520`. Cluster `SCAN` already uses it (`src/db.c:1355`).
- **Lazy expiry is already slot-local:** `expireIfNeededWithDictIndex(db, key, val, flags,
  dict_index)` (`src/db.c:2203`) takes the table index the lookup already computed;
  `expireIfNeeded` (`src/db.c:2263`) derives it at `:2265-2266`.

**Eviction:**

- `performEvictions()` (`src/evict.c:404`) — the whole mechanism, called before command execution.
- `getMaxmemoryState(...)` (`src/evict.c:265`) reads `zmalloc_used_memory()` (`:270`, `:310`) —
  a **process-global** figure.
- The eviction loop `while (mem_freed < (long long)mem_tofree)` (`src/evict.c:437`) samples
  **every database** (`for (i = 0; i < server.dbnum; i++)`, `:452`) via
  `evictionPoolPopulate(db, kvs, pool)` (`:473`; defined `:113`), which picks a table with
  `kvstoreGetFairRandomHashtableIndex(samplekvs)` (`:116`) and samples it with
  `kvstoreHashtableSampleEntries` (`:120`).
- The pool itself is a file-static single instance: `static struct evictionPoolEntry
  *EvictionPoolLRU` (`src/evict.c:64`, allocated at `:102`, used at `:449`).
- Freed memory is measured by `zmalloc_used_memory()` deltas around the delete
  (`src/evict.c:561`, `:567`), and progress is re-checked with `getMaxmemoryState(NULL, NULL,
  NULL, NULL)` (`:593`, `:622`).
- Time budget: `evictionTimeLimitUs()` (`src/evict.c:363`, used at `:427`).
- Random-key fallback for the non-LRU policies: `kvstoreHashtableRandomEntry(kvs, slot, &entry)`
  (`src/evict.c:537`).
- `zmalloc_used_memory()` sums **per-thread** counters (`src/zmalloc.c:98-116`), so it is already
  safe to call from any thread — it is the *decision*, not the read, that is unsafe to parallelize.

### 3. Phase 6a — per-shard expiry

#### 3.1 What is already correct

**Lazy expiry needs no work.** `expireIfNeededWithDictIndex` (`src/db.c:2203`) runs on whichever
thread looked the key up, and after Phase 4 that thread is the slot's owner by construction. The
`DEL`/`UNLINK` it propagates lands in that shard's op array and journal (Phase 5 §4.2). This is
the payoff of slot ownership showing up as an absence of work.

#### 3.2 The change: scope the scan to owned slots

Because Phase 2 partitions slots into **contiguous** ranges (`slot * N / CLUSTER_SLOTS`), each
shard owns exactly one interval `[first_slot, last_slot]` — which is precisely the shape
`kvstoreScan`'s range mode takes.

```c
/* Per shard, per expiry job type. Replaces db->expiry[jobType].cursor. */
typedef struct shardExpiryState {
    unsigned long cursor;        /* kvstore cursor, confined to this shard's range */
    unsigned int  current_db;    /* was the file-static _expire_state[].current_db */
    bool          timelimit_exit;
    double        stale_perc;    /* was server.stat_expired_keys_*_stale_perc */
} shardExpiryState;

/* The scan, per iteration — the only substantive line that changes. */
cursor = kvstoreScan(kvs, cursor, shard->first_slot, shard->last_slot,
                     scan_cb, expireShouldSkipTableForSamplingCb, &data);
```

`kvstoreScan` handles the boundaries itself: it fast-forwards a cursor below `first_idx`, returns
0 when the range is exhausted (`src/kvstore.c:511-520`), and the shard then restarts at
`first_slot`. **No new kvstore API is needed for expiry** — which is the single biggest reason
6a is cheap and 6b is not.

Three pieces of state move from globals/db onto the shard:

| Today | Anchor | Becomes |
|---|---|---|
| `static expireState _expire_state[]` | `src/expire.c:210` | `shard->expiry[jobType]` |
| `db->expiry[jobType].cursor` | `src/server.h:909` | `shard->expiry[jobType].cursor` — one cursor per (shard, db, jobType). Keep it indexed by db, since the cycle still round-robins databases. |
| `server.stat_expired_keys_stale_perc`, `…_with_vola_stale_perc` | `src/expire.c:215-218` | Per-shard; summed (weighted by sampled keys) for `INFO`. |

#### 3.3 Scheduling and the time budget

Each shard runs its own cycles on its own loop:

- **Fast cycle** in `shardBeforeSleep` (mirroring `src/server.c:1916`), bounded by
  `ACTIVE_EXPIRE_CYCLE_FAST_DURATION` (`src/expire.c:123`).
- **Slow cycle** on a per-shard timer, mirroring `databasesCron` (`src/server.c:1304-1312`).

**The budget subtlety that must not be got wrong.** `ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC` is 25%
(`src/expire.c:124`) — of *one thread's* time. If each of N shards spends 25% of its own loop
expiring, total expiry CPU is 0.25 × N cores, which is the correct generalization: each shard
owns 1/N of the keys and spends the same *fraction* of its own capacity on them. **Do not** divide
the percentage by N — that would leave total expiry throughput flat while the keyspace and the
insert rate scale, and stale keys would accumulate. State this explicitly in the code comment,
because "keep the total the same" is the intuitive and wrong instinct.

The adaptive parts (`ACTIVE_EXPIRE_CYCLE_ACCEPTABLE_STALE`, the fast-cycle skip when the previous
cycle did not hit its time limit, `src/expire.c:225-229`) become per-shard decisions on per-shard
statistics — which is strictly better than today, since a hot shard can expire aggressively while
a cold one stays idle. Today one global decision serves the whole keyspace.

#### 3.4 Propagation

Expired-key deletions propagate `DEL`/`UNLINK`. They go through the owning shard's op array and
journal exactly like a client write (Phase 5 §3.2-3.3), taking a commit id and appearing in the
sequenced stream. Two details:

- The unit is one expire action (or the batch the current code treats as a unit); it must not be
  merged with an unrelated client command's unit.
- `propagatePendingCommands` avoids the `MULTI`/`EXEC` wrapper for
  `CMD_TOUCHES_ARBITRARY_KEYS` commands (`src/server.c:3759-3763`) and active expire is
  explicitly cited in that comment as the analogous case. Preserve that: expire records are
  standalone, not transactions.
- Replicas must not expire keys themselves; that gate is unchanged and lives above this layer.

#### 3.5 Why 6a is worth doing on its own

On a large keyspace with many volatile keys, active expiry is a measurable slice of the single
thread's time, and it competes directly with command execution. Sharding it removes that
competition *and* scales the sweep rate with the keyspace. It has no cross-shard coordination, no
new API, no global counter, and its correctness surface is one scan-range argument and some state
relocation. If Phase 6 ships only 6a, that is a good outcome.

### 4. Phase 6b — per-shard eviction, and why it is hard

#### 4.1 The structural problem

Eviction is a **global** decision — "the process is over `maxmemory`" — carried out by **local**
actions. Every part of `performEvictions` (`src/evict.c:404`) assumes one actor:

- **The target is global.** `getMaxmemoryState` (`src/evict.c:265`) reads
  `zmalloc_used_memory()` for the whole process. N shards each independently concluding "we are
  4 GB over" and each freeing 4 GB overshoots by 4×(N-1) GB — evicting most of the keyspace to
  fix a small overage. This is a *data-loss-shaped* bug, not a performance bug.
- **The sampling is global.** `evictionPoolPopulate` (`src/evict.c:113`) picks a random table
  across the entire kvstore (`kvstoreGetFairRandomHashtableIndex`, `:116`). A shard must sample
  only its own slots, or it will try to evict a key it does not own. **There is no range-limited
  fair-random API today** — `kvstoreGetFairRandomHashtableIndex` takes no bounds — so 6b needs a
  new kvstore primitive, e.g. `kvstoreGetFairRandomHashtableIndexInRange(kvs, first, last)`
  weighted by the cumulative-count BIT over that sub-range. That is a real but contained addition.
- **The pool is a singleton.** `static struct evictionPoolEntry *EvictionPoolLRU`
  (`src/evict.c:64`) must become per-shard. Easy.
- **Progress measurement is a global delta.** `mem_freed` accumulates
  `zmalloc_used_memory()` differences around each delete (`src/evict.c:561`, `:567`). With N
  shards allocating and freeing concurrently, those deltas measure *the process*, not *this
  eviction* — the loop's termination condition becomes noise-driven. This is the part that most
  resists a clean fix.
- **Eviction quality degrades under partition.** Approximated LRU/LFU works by sampling the
  *whole* keyspace for the globally-coldest keys. Per-shard sampling finds each shard's coldest
  keys, which is a different and worse answer when shards are unevenly cold. The user-visible
  effect is a lower cache hit rate at the same `maxmemory` — a silent quality regression that no
  functional test catches.

#### 4.2 The recommendation: barrier first, measure, then decide

**Ship Phase 6 with eviction still on the barrier** (the Phase 5 §4.6 decision: over `maxmemory`
⇒ escalate). Then measure, on a `maxmemory`-constrained workload at `shard-threads 4/8`:

1. What fraction of wall time is spent inside eviction barriers?
2. What is the p99 latency impact of a barrier during steady-state eviction?
3. How often does the workload actually cross the limit — continuously (a cache at capacity, the
   common case) or rarely (a database with headroom)?

If eviction barriers are a small fraction, **stop** — build nothing, and document that
`maxmemory`-bound workloads scale less. If they dominate (likely for a cache running permanently
at its limit, which is a very common deployment), build 6b with the design below.

#### 4.3 If 6b is built: credit-based eviction

The design that avoids both overshoot and a shared counter on the hot path:

```text
   shard 0 cron (the only thread that decides "how much"):
     getMaxmemoryState() ─► mem_tofree                       (one global read, once per round)
     split into per-shard credits, weighted by each shard's
     tracked memory, not equally  ────────────────┐
                                                  ▼
   each shard, in its own loop:  evict from its own slots until its credit is consumed
                                 (per-shard pool, range-limited sampling, per-shard deltas)
                                 report actual freed bytes back ──┐
                                                                  ▼
   shard 0 next round: recompute from the real global figure, re-issue credits
```

- **One decider.** Only shard 0 reads the global state and hands out budgets, so overshoot is
  bounded by one round's credits rather than N× the deficit.
- **Weighted credits, not equal shares.** An equal split evicts from shards that hold little,
  which is both ineffective and destructive. Weight by per-shard memory — which needs per-shard
  memory tracking, and that is exactly what
  [proposal-memory-aware-rebalance.md](proposal-memory-aware-rebalance.md) designs for slots.
  **6b should depend on that work rather than inventing a second accounting.**
- **Local progress measurement.** A shard measures its own freed bytes from its own per-thread
  `zmalloc` counter (`src/zmalloc.c:98-116` — the counters are already per-thread, so a shard can
  read *its own* slot exactly), sidestepping §4.1's noisy global delta. This is a genuinely nice
  fit and the strongest argument that 6b is buildable.
- **Quality mitigation.** Keep a small cross-shard "coldest candidates" summary that shard 0
  refreshes each round from per-shard pools, so credits skew toward shards holding globally-cold
  keys. Measure hit-rate against the single-threaded baseline at equal `maxmemory` (§6.7); if the
  regression is material, credits alone are not enough and the honest answer is to keep the
  barrier.
- **Writes still need a synchronous check.** A command that would exceed `maxmemory` must not
  proceed on the strength of a stale per-shard view. Keep a cheap fast-path check against a
  relaxed global atomic with per-shard slack (§8's phrasing), and escalate to the barrier
  when the slack is exhausted — so the *hard* limit is always enforced by one thread, and the
  concurrent path only handles the steady-state.

### 5. Files touched

**6a:**

- `src/expire.c` — `activeExpireCycleJob` (`:199`) takes a shard; `_expire_state` (`:210`) moves
  to the shard; the scan (`:341`) passes `shard->first_slot, shard->last_slot`; stale-percentage
  stats become per-shard (`:215-218`).
- `src/server.h` — `db->expiry[].cursor` (`:909`) relocates to per-shard state; `INFO`
  aggregation for the expiry stats.
- `src/server.c` — `databasesCron` (`:1304`) and the `beforeSleep` fast cycle (`:1916`) become
  per-shard invocations on shard timers/`shardBeforeSleep`.
- `src/shard.{c,h}` — `shardExpiryState`, the per-shard cron tick.
- `tests/unit/shard-expire.tcl` — **new**.

**6b (only if §4.2 says so):**

- `src/kvstore.{c,h}` — `kvstoreGetFairRandomHashtableIndexInRange(kvs, first, last)`, weighted
  by the BIT over the sub-range.
- `src/evict.c` — per-shard `EvictionPoolLRU` (`:64`); `evictionPoolPopulate` (`:113`) takes a
  range; `performEvictions` (`:404`) splits into a shard-0 "decide" half and a per-shard "evict my
  credit" half; local `zmalloc` accounting replaces the global deltas (`:561`, `:567`).
- `src/shard.{c,h}` — credit issue/consume/report.
- `src/server.h` / `src/config.c` — per-shard memory tracking hooks (shared with
  [proposal-memory-aware-rebalance.md](proposal-memory-aware-rebalance.md)).
- `tests/unit/shard-eviction.tcl` — **new**.

### 6. Test plan

**6a:**

1. **Coverage.** Every volatile key eventually expires, whatever shard owns it, at
   `shard-threads` 1/2/4/8. No slot is ever skipped — the range-boundary bug this phase most
   risks. Seed keys deliberately at slot 0, at each shard boundary, and at slot 16383.
2. **No double expiry.** Two shards must never both expire the same key (they cannot, by
   ownership — assert it with a counter and a test rather than trusting it).
3. **Replication.** Expired-key `DEL`s reach the replica in sequenced order; primary and replica
   digests match after a mass-expiry event (Phase 5 §6).
4. **Stale-key ratio under load.** With a high insert rate of volatile keys, the ratio of expired
   but not-yet-collected keys does not degrade as `shard-threads` grows — the property §3.3's
   budget rule exists to protect.
5. **`shard-threads 1` is unchanged.** Existing expire suite passes bit-identically.
6. **Cursor persistence across a rebalance.** Change `slot_to_shard[]` under barrier mid-cycle;
   no key is skipped and no shard scans a slot it no longer owns.

**6b:**

7. **`maxmemory` is respected without overshoot.** Drive the server past the limit at
   `shard-threads 8`; peak `used_memory` overshoot stays within a stated bound (the one-round
   credit), and the server converges back under the limit.
8. **Eviction quality.** Same workload, same `maxmemory`, measure hit rate at `shard-threads` 1
   vs 8. Publish the delta. **This is 6b's gate** — a large regression means the barrier was the
   better answer.
9. **No cross-shard eviction.** A shard never deletes a key outside its slot range (assert in a
   debug build).
10. **Policy coverage.** All `maxmemory-policy` values, including `volatile-ttl` and the random
    policies (which use a different code path, `src/evict.c:537`).
11. **`maxmemory 0`** (unlimited) never engages any of this.

### 7. Verification

1. `make -C src`; `./runtest --single unit/shard-expire` (and `unit/shard-eviction` for 6b).
2. Full suite plus `runtest-cluster` at `shard-threads 1` and `4`; the existing `unit/expire` and
   `unit/maxmemory` suites are the ones that matter most here.
3. **TSan** at `shard-threads 4` with a heavy volatile-key workload — expiry runs concurrently
   with commands on every shard, which is the most concurrent thing in the server after this
   phase.
4. Long soak with primary/replica `DEBUG DIGEST` comparison across mass-expiry events.
5. Benchmarks: expiry sweep rate vs `shard-threads`; stale-key ratio at a fixed insert rate;
   command-latency impact of the slow cycle at each shard count. For 6b: §6.7 and §6.8.

### 8. Honest risks

- **Range-boundary bugs skip keys silently.** An off-by-one in `first_slot`/`last_slot` leaves a
  slot never actively expired; its keys still expire lazily on access, so nothing fails — memory
  just grows and nobody knows why. §6.1's boundary seeding is the defense, and it deserves to be
  a permanent test, not a one-off check.
- **Expiry CPU multiplies by N.** §3.3's budget rule is correct, but it means an 8-shard server
  can spend 2 cores' worth of time expiring. That is the right behavior for a keyspace 8× larger;
  it will still surprise someone reading `top`. Make it visible in `INFO`.
- **Eviction quality is a silent regression.** Nothing errors when per-shard sampling picks worse
  victims; the hit rate just drops. Only §6.8 catches it, and only if it is actually run.
- **Overshoot is data loss.** The single most dangerous outcome in this phase is N shards each
  evicting the full deficit. The credit design exists specifically to make that impossible;
  if 6b is built any other way, this is the failure to fear.
- **6b's dependency on per-shard memory tracking** means it should not start before
  [proposal-memory-aware-rebalance.md](proposal-memory-aware-rebalance.md) lands, or it will grow
  a second, incompatible accounting.

### 9. References

- Core design: §8 (the table this
  phase implements two rows of), §10.6.
- Prerequisites: [Phase 4 part 1](#18-phase-4-part-1-per-thread-event-loops) (per-shard cron),
  [Phase 5](#21-phase-5-writes-journals-and-the-sequencer) (the journal both sweepers write into; §4.6 is the
  barrier decision this phase revisits).
- Dependency for 6b: [proposal-memory-aware-rebalance.md](proposal-memory-aware-rebalance.md)
  (per-slot memory tracking).
- Background: [05-expiration-and-eviction.md](05-expiration-and-eviction.md).
- Code: `src/expire.c:66,122-125,199,210,341,459`, `src/server.c:1304,1312,1916`,
  `src/server.h:909`, `src/db.c:2203,2263`, `src/kvstore.c:472-476,511-520`,
  `src/evict.c:64,102,113,116,120,265,363,404,437,473,537,561,567`, `src/zmalloc.c:98-116`.
