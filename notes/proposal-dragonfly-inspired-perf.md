# Proposal — Dragonfly-inspired performance directions for Valkey

**Status: pre-issue draft. Not a `design-docs/` document.**
Per `design-docs/README.md`, a design doc starts with an issue for alignment and is
committed alongside the code it describes. This is the thing you write *before* that:
a survey of what Dragonfly does, an honest mapping onto Valkey's actual architecture,
and a ranked plan. Individual stages below should graduate into issues, then into
`design-docs/`, one at a time.

---

## 1. Why Dragonfly is fast

Dragonfly's throughput advantage is not one trick. It is five decisions that reinforce
each other:

| # | Decision | What it buys |
|---|---|---|
| D1 | **Shared-nothing, shard-per-thread.** The keyspace is partitioned across threads; each thread exclusively owns its slice. No locks on data. | Near-linear scaling with cores. This is the whole ballgame. |
| D2 | **Dashtable** (segmented extendible hashing) instead of a chained dict. | Cache-line buckets, high fill factor, and — critically — *incremental resize one segment at a time*, plus per-bucket version stamps. |
| D3 | **Fork-less snapshotting**, built on D2's version stamps. | No `fork()`, no 2× RAM headroom, no copy-on-write page storms during writes. |
| D4 | **A deterministic transaction framework** (VLL-inspired) for multi-shard commands. | Multi-key atomicity without deadlock, without a global lock. |
| D5 | **io_uring + fibers** for all I/O. | Fewer syscalls per operation; async code that reads like blocking code. |

The headline "25× Redis" numbers come almost entirely from **D1** — running 64 cores
instead of 1. D2–D5 are what make D1 *possible* without giving back the win elsewhere.

## 2. What Valkey already has

This is the part that changes the plan, so it goes before the proposals.

**Valkey is much closer to Dragonfly than the Redis it forked from.**

- **`src/hashtable.c` is already ~a dashtable for lookup purposes.** 64-byte
  (one cache line) buckets, 7 entries per bucket, hash tags in the bucket metadata to
  kill false positives, SIMD tag matching, and a ~91% fill factor
  (`ENTRIES_PER_BUCKET`/`BUCKET_FACTOR`, `src/hashtable.c:164-195`). The lookup-cost
  and memory-overhead gap versus Dragonfly's dashtable is *mostly closed already*.
  What remains is the resize story (Valkey still incrementally rehashes between two
  full tables) and the absence of version stamps.
- **`src/kvstore.c` is already the shard substrate.** It is an array of hash tables,
  one per hash slot in cluster mode. The partition Dragonfly builds by hand *already
  exists in the data model*.
- **Cluster mode already forbids cross-shard commands.** A multi-key command whose
  keys hash to different slots is rejected with `-CROSSSLOT`
  (`src/cluster.c:1316`). So in cluster mode, essentially every command is
  single-shard *by construction*.
- **`src/io_threads.c` already has a real worker pool** with SPMC/MPSC/SPSC queues,
  offloading socket reads, writes, accepts, polls, and deferred frees
  (`design-docs/io-threads.md`).

The consequence is worth stating plainly:

> Valkey does not need to import Dragonfly's architecture. It needs to finish the one
> it already started. The keyspace is partitioned, the partitions are already
> disjoint-by-protocol in cluster mode, and there is already a thread pool. What is
> missing is letting those threads *execute commands* instead of only doing I/O.

And what Valkey is missing that Dragonfly has, concretely: fork-less snapshots,
per-shard command execution, and a syscall-efficient I/O backend.

## 3. Non-goals

Things Dragonfly does that Valkey should **not** copy:

- **Fibers / a green-thread runtime.** Dragonfly needed one because it was greenfield
  C++. Retrofitting a fiber scheduler onto 300k lines of callback-structured C buys
  ergonomics, not throughput, and would touch every subsystem. Skip it.
- **Replacing `hashtable.c` with a dashtable.** See §2. The remaining delta does not
  justify swapping out the most performance-critical, most recently rewritten data
  structure in the tree. Extend it instead (§4.1) — the concrete design for *how* is
  [proposal-dashtable-adoption.md](proposal-dashtable-adoption.md).
- **Dragonfly's cluster model.** Valkey's cluster is a real, gossip-based,
  failover-capable system; that is a feature, not overhead to be optimized away.

## 4. The proposals, ranked

Ranked by (value ÷ blast radius). Stages 1–3 are independent of each other and of the
threading model — they are worth doing *even if Stage 4 never happens*.

---

### 4.1 Stage 1 — Fork-less snapshotting (highest value per unit of risk)

> The exact version-stamp rules Dragonfly uses (the `<=` cut comparison,
> conservative vs. relaxed pre-image, and why cross-shard ordering is *not* enforced by
> the snapshot) are written up in
> [10-dragonfly-snapshot-model.md](10-dragonfly-snapshot-model.md). Read that before
> implementing this stage.

**Problem.** Every RDB save, every AOF rewrite, and every full replica sync goes
through `serverFork()` (`src/server.c:7135`, called from `src/rdb.c:1682`,
`src/rdb.c:3836`, `src/aof.c:2626`). This costs:

- a page-table copy at fork time — tens to hundreds of ms of hard stall on a large
  instance, in the middle of the event loop;
- copy-on-write amplification — a write-heavy instance can approach **2× RSS** during
  a save, which is why operators over-provision memory by 2× and why
  `maxmemory` is set so conservatively;
- an OOM-kill risk that is a genuine production hazard.

**Dragonfly's answer.** Don't fork. Give each bucket a version stamp. A snapshot fiber
walks the table serializing buckets, holding a snapshot version. When a *mutating*
command touches a bucket whose version is older than the snapshot version, it
serializes that bucket's pre-image into the snapshot stream **before** applying the
mutation, then bumps the version. This is copy-on-write moved from the OS page level
(4KB granularity, whole-process) to the data-structure level (one bucket, one cache
line).

**The Valkey shape of it.**

```text
kvstore (per slot)                     snapshot producer
+---------------------------+          +---------------------------------+
| hashtable                 |          | cursor: slot 3, bucket 128      |
|  bucket[0]  ver=snap      |          | snap_version = V                |
|  bucket[1]  ver=snap  <---+----------+ walks buckets, emits < V ones   |
|  bucket[2]  ver=old       |          +---------------------------------+
|  bucket[3]  ver=old       |                        ^
+---------------------------+                        |
              ^                                      |
   write to bucket[2] ---> pre-image serialized here-+  then mutate, ver := V
```

**The honest cost.** Valkey's bucket is *exactly* one cache line: 7 × 8-byte entry
pointers + 8 bytes of metadata = 64 bytes. **There is no spare room for a version
stamp.** Options:

1. **Side array** — a parallel `uint32_t` (or `uint16_t`, with wraparound handling)
   per bucket, allocated only while a snapshot is in flight. Cost: ~4 bytes per 7
   entries ≈ **0.6 bytes/key**, and *only during a snapshot*. This is the right
   trade and should be the default proposal.
2. Grow the bucket to 2 cache lines — rejected, it taxes every lookup forever to
   benefit an occasional snapshot.

**Interaction with the rest of the system.** The snapshot stream must interleave with
the replication backlog correctly — this is exactly the problem `rdb.c` already solves
for the fork case (the child's snapshot plus the accumulated backlog since fork).
Fork-less changes *who* produces the snapshot, not the RDB-plus-backlog contract, so
`replication.c` should be largely untouched. The dangerous edge is that the snapshot
now runs **incrementally on the main thread**, so it must be bounded per event-loop
iteration like `activeExpireCycle` is, and it must be correct across rehash (a bucket
can move between the two tables mid-snapshot — this is the single hardest detail and
should be prototyped first).

**Why this is first.** It is the only proposal here that is a *pure win with no
threading-model change*, it removes the biggest operational pain of running large
Valkey instances, and it is a prerequisite for Stage 4 anyway (a forking snapshot in a
multi-threaded server is much worse).

---

### 4.2 Stage 2 — Bucket-local cache eviction

**Problem.** `performEvictions()` (`src/evict.c:404`) samples random keys into an
eviction pool (`evictionPoolPopulate()`, `src/evict.c:113`) to approximate LRU. Under
memory pressure this is a random-access scan of the keyspace — cache-hostile, and it
runs on the hot path.

**Dragonfly's answer.** In cache mode, evict from *the bucket you are already inserting
into*. The bucket is in L1 because you just touched it. No sampling, no pool, no global
structure, O(1) and cache-local.

**The Valkey shape of it.** A new `maxmemory-policy` (e.g. `allkeys-bucket-lru`) that,
on insert into a full bucket, evicts the coldest of the 7 entries already there rather
than growing the table. Approximation quality is worse than sampled-LRU in theory;
Dragonfly's bet is that for a *cache* workload nobody can tell, and the hit-rate data
supports that.

Self-contained, opt-in, no compatibility surface. Good first contribution for someone
learning the codebase.

---

### 4.3 Stage 3 — Pipeline squashing

**Problem.** Valkey pays full per-command dispatch overhead (`processCommand` → `call`,
ACL checks, propagation bookkeeping) for every command in a deep pipeline, even when
100 pipelined commands all hit the same slot.

**Dragonfly's answer.** Squash a pipeline into one batched hop per shard.

**The Valkey shape of it.** Group a client's parsed-but-unexecuted pipeline by slot,
then execute per-slot runs back to back, amortizing lookup and propagation setup. In
single-threaded Valkey the win is modest (cache locality on the kvstore + fewer
propagation flushes). In a Stage-4 world it becomes large, because a squashed batch is
one cross-thread hop instead of N. **Defer this until Stage 4 is real** — on its own
the payoff probably doesn't justify the churn in `networking.c`.

---

### 4.4 Stage 4 — Shard-per-thread command execution (the actual prize)

This is where the throughput is. Everything above is worth doing; none of it changes
the fact that **one core executes every command.**

#### The core insight that makes it tractable

Two writes to **disjoint keys commute.** Any interleaving of them is a valid
serialization. This is what lets shard threads run without a global order — and, more
importantly, it is what lets the **replication stream** stay correct, which is the
thing that usually kills this idea.

#### Design: owned shards + escalation barrier

```text
  Client connections (on I/O threads)
        │
        ▼   route by slot
  ┌────────────────────────────┬────────────────────────────────────────┐
  │ single slot                │ multi-slot / global                     │
  ▼                            ▼                            ▼            ▼
 Shard thread 0    Shard thread 1    Shard thread N    Escalation: quiesce
 owns slots        owns slots        ...               all shards, run on
 0–4095            4096–8191                           coordinator as today
  │                 │                 │                            │
  ▼                 ▼                 ▼                            ▼
 per-shard         per-shard         per-shard                barrier marker
 journal           journal           journal                       │
  │                 │                 │                            │
  └─────────────────┴────────┬────────┴────────────────────────────┘
                             ▼
             Sequencer: merge to repl backlog / AOF
```

- **Slots are statically assigned to shard threads.** `kvstore` already indexes by
  slot; add a `slot → shard` map. A shard thread owns its hashtables outright — no
  locks, exactly Dragonfly's model.
- **Single-slot commands** (the overwhelming majority, and in cluster mode
  *structurally* almost all of them per §2) execute **entirely on the owning shard
  thread**. This is the fast path and where the scaling comes from.
- **Anything else escalates**: multi-slot commands in non-cluster mode, `MULTI`/`EXEC`,
  Lua/functions, `FLUSHALL`, `SWAPDB`, `DEBUG`, module calls. The coordinator raises a
  barrier, shard threads quiesce at a safe point, and the command runs exactly as it
  does today, single-threaded, with the whole keyspace visible.

**The escalation barrier is the key risk-reduction move.** It is strictly slower than
Dragonfly's VLL transaction framework (D4) for multi-shard work — but it is *obviously
correct*, it reuses all existing code for every hard command, and it means the project
does not have to solve distributed transactions before it can ship anything. If
profiling later shows escalation is a real bottleneck for real workloads, VLL-style
per-shard transaction queues can replace the barrier **later, behind the same
interface**. Do not start there.

#### Replication and AOF: the thing that actually breaks

Valkey's replication stream is a single totally-ordered command stream, and replicas
apply it single-threaded. N shard threads producing writes concurrently have no such
order.

The commutativity insight resolves this: each shard appends to its **own journal
buffer**; a sequencer merges them into the replication backlog / AOF. Because
concurrently-executing commands are on **disjoint keys by construction**, *any* merge
order is a valid serialization, and a replica applying the merged stream single-threaded
reaches the same state. Escalated commands emit a **barrier marker** — all shard
journals are flushed before it and none after until it completes, which gives multi-key
and global commands their sequencing point for free.

This is the part to prototype and stress-test *first*, before any shard-threading code
lands. If it doesn't hold up, the whole stage is dead and it is much cheaper to learn
that early.

#### The rest of the honest breakage list

| Area | Problem | Disposition |
|---|---|---|
| **Modules** | The module API promises single-threaded execution and a GIL. Modules call into the keyspace from anywhere. | **Hardest compat surface.** Almost certainly: modules force escalation, at least initially. |
| **Blocking commands** | `BLPOP` waiter on shard A, `LPUSH` on shard B. | Cross-shard wakeup via the existing job queues; ready-key handling moves to the owning shard. |
| **Pub/Sub, keyspace notifications** | Global fan-out from shard threads. | Route through the coordinator; ordering guarantees need a written spec. |
| **`SCAN`, `RANDOMKEY`, `DBSIZE`** | Global keyspace views. | Fan-out and merge; `SCAN`'s existing weak guarantees help a lot here. |
| **Expiry / eviction** | `activeExpireCycle` and `performEvictions` walk all slots. | Become per-shard; `maxmemory` accounting becomes a shared atomic with per-shard slack. |
| **Cluster & atomic slot migration** | Slot ownership now *also* means thread ownership. | Slot migration must coordinate with shard reassignment (`design-docs/atomic-slot-migration.md`). |
| **`INFO` / stats** | Global counters written from N threads. | Per-shard counters, summed on read. |

#### Phasing

1. **Prove the journal-merge model.** A standalone test that N concurrent disjoint-key
   writers, merged by a sequencer, produce a stream that replays to an identical
   keyspace. No server changes. Cheap. Do this first.
2. **Introduce `slot → shard` ownership** with `shards = 1`. Pure refactor; every
   command "escalates". Should be a no-op at runtime and fully testable.
3. **Move single-key reads only** (`GET`, `HGET`, …) to shard threads. Read-only is a
   much smaller correctness surface — no journal, no propagation.
4. **Add single-key writes** + per-shard journals + the sequencer.
5. **Move expiry/eviction per-shard.**
6. *(Optional, much later)* Replace the escalation barrier with VLL-style transaction
   queues if and only if profiling justifies it.

Steps 1–3 are individually shippable and individually valuable. If the project stalls
after step 3, Valkey still has multi-threaded reads, which is most of the read-heavy
win.

---

### 4.5 Stage 5 — io_uring event-loop backend

Add `src/ae_iouring.c` alongside `ae_epoll.c` / `ae_kqueue.c`. Batched submission and
completion cuts syscalls per operation, which matters exactly when you are already at
saturation — i.e. **after** Stage 4, not before. Ranked last on purpose: on a
single-threaded execution core the CPU is not spent in syscalls, so this optimizes the
wrong thing until the execution bottleneck is gone. It is also the one item here that
Valkey's `ae` abstraction is genuinely well-shaped to absorb.

---

## 5. Stage 0 — the measurement that gates all of this

**Do not write any of the above code before this exists.**

> The actionable version — environment, workload matrix, exact instrumentation, and a
> decision table mapping results to the next feature — is
> [proposal-stage0-measurement.md](proposal-stage0-measurement.md).

A baseline that answers: at saturation on a modern many-core box, where does the main
thread's time actually go? Command dispatch? Hashtable lookup? Reply construction?
Propagation? Eviction sampling?

Two specific numbers decide the whole plan:

1. **What fraction of main-thread time is keyspace work** (the part Stage 4 can
   parallelize) versus overhead that stays serialized? This is Amdahl's law and it
   sets the ceiling on the entire Stage 4 investment.
2. **What is the p99 stall from `fork()`** on a large write-heavy instance, and what is
   peak RSS amplification during a save? This sizes Stage 1.

If (1) comes back low, Stage 4 is not worth the years it will cost, and the honest
answer is that Valkey's existing I/O-threads bet was the right one. That is a real
possible outcome and this document should not pretend otherwise.

## 6. Recommendation

- **Do Stage 1 (fork-less snapshot) now.** It stands alone, it is bounded, and it fixes
  the worst operational property of running Valkey at scale. It would be worth doing if
  Dragonfly had never existed.
- **Do Stage 2 (bucket-local eviction)** as a small, independent follow-on.
- **Do Stage 0 before committing to Stage 4**, and be prepared for it to say no.
- **Treat Stage 4 as a multi-year architectural program, not a feature.** Its
  prerequisite is not code — it is a written spec of what Valkey's ordering guarantees
  actually are, because today they are defined implicitly by "there is one thread."

## 7. Code anchors

| Thing | Where |
|---|---|
| Bucket layout, fill factor | `src/hashtable.c:164-195` |
| Bucket design comment | `src/hashtable.c:222+` |
| Per-slot hashtable array | `src/kvstore.c:1-12` |
| `-CROSSSLOT` rejection | `src/cluster.c:1316` |
| Fork sites | `src/server.c:7135`, `src/rdb.c:1682`, `src/rdb.c:3836`, `src/aof.c:2626` |
| Eviction pool sampling | `src/evict.c:113`, `src/evict.c:404` |
| I/O worker pool | `src/io_threads.c`, `design-docs/io-threads.md` |
| Slot migration | `design-docs/atomic-slot-migration.md` |
