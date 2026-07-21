# Proposal — Slot-per-thread Phase 1: the ordering-model harness

**Status: pre-issue draft, ready to implement.** This is the concrete design of **Phase 1** of
[proposal-slot-per-thread.md](proposal-slot-per-thread.md) §10 — the standalone experiment that
proves (or kills) the commit-id ordering model of parent §7 **before any server code is
written**.

It is the only phase whose deliverable is an *answer*, not a feature. Parent §12 names two of
its three abandonment criteria in terms of this harness; this note makes those criteria
executable.

All `file:line` anchors were checked against this checkout.

---

## 1. Scope

> "Prove the ordering model. The §7 harness. No server changes. Do this first; it is the only
> step that can kill the project." (parent §10.1)

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

**Not throwaway.** The parent doc calls Phase 1 a validation experiment, and parent §14.7 calls
it "`shard_journal.c` + fake shards only." That framing is right about the *gate* but wastes the
code. Build the **real** `src/shard_journal.{c,h}` now — it has no dependency on shards,
threads, or the command table, only on `queues.h` and a record type — and drive it from tests.
Phase 5 then wires the same module into `propagateNow` instead of reimplementing it. The only
throwaway parts are the fake shard threads and the fake keyspace, which belong in the test file.

## 2. Verified anchors (this checkout)

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

## 3. What "correct" means — the three assertions

Let each fake client issue a sequence of commands; each command executes on the shard owning its
key. Define `seq(r)` as the record's commit id and `emit(r)` as its position in the sequencer's
output.

**P1 — State equivalence.** Replaying the emitted stream single-threaded onto an empty keyspace
yields a keyspace byte-identical to the one the concurrent execution produced. This is the
property replicas depend on.

**P2 — Per-client causality.** For any client `k` and its consecutive commands `c_i`, `c_{i+1}`:
`seq(c_i) < seq(c_{i+1})`, and therefore `emit(c_i) < emit(c_{i+1})`. **This is the property
parent §7 says an arrival-order merge silently destroys**, and the whole reason the commit-id
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

## 4. Design

### 4.1 The module under test — `src/shard_journal.{c,h}`

```c
/* shard_journal.h — no dependency on shards, threads, or the command table. */

typedef struct journalRec {
    uint64_t  seq;            /* global commit id */
    int       dbid;           /* -1 = do not emit SELECT (propagateNow's convention) */
    int       slot;           /* owning slot, or -1 */
    int       target;         /* PROPAGATE_AOF | PROPAGATE_REPL */
    int       argc;
    robj    **argv;           /* the deterministic, post-rewrite form (parent §14.4) */
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

### 4.2 The fake shards and the dependency the harness must not cheat on

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

### 4.3 Randomized differential harness

The gtest cases (§5) pin specific behaviors; the randomized harness is what actually finds the
bug. Parameters swept: shard count (1, 2, 4, 8, `nproc`), writer count (1 … 4×shards), key space
size (tiny — to force same-key contention within a shard — through large), command mix
(`SET`/`INCR`/`DEL`/`APPEND`, all deterministic on replay), and artificial per-command latency
skew (one shard made 10× slower, to maximize reorder-buffer depth).

Every run asserts P1–P5 and, on failure, dumps the seed plus the emitted stream. Run it under
**TSan** and **ASan** in CI-length sweeps; a data race here is a fatal finding, not a warning.

### 4.4 The negative test that must exist

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

## 5. What to measure (this is half the deliverable)

The gate is not just "the tests pass" — parent §11 demands a published break-even, and parent
§12 says a cost that "eats the scaling" is an abandonment criterion. Report:

| Metric | Why it decides something |
|---|---|
| `atomic_fetch_add` throughput at 1/2/4/8/16 threads (ops/s, and ns/op at each level) | The hard ceiling on total write rate. If it saturates below the single-thread command rate today, the design is dead as written. |
| Sequencer cost per record (ns), linear scan vs min-heap, at N = 2…32 | Sequencer is single-threaded; if it costs more per record than `propagateNow` does today, it has just replaced one bottleneck with another. |
| Reorder-buffer depth: mean / p99 / max, under balanced and 10×-skewed shards | The memory and latency cost of the gap-wait. |
| Stall time: wall-clock a record waits for a lower id, p99 / max | This is added replication latency. It is the number `WAIT` users will feel. |
| End-to-end merged throughput vs. a single-threaded baseline producing the same stream | The actual scaling answer at the journal layer, independent of the keyspace work. |

Publish these as a table in the phase report, with the machine and core count named — parent §11
says the break-even must be published, and this is where that number is born.

## 6. Files touched

- `src/shard_journal.h`, `src/shard_journal.c` — **new**: `journalRec`, per-shard SPSC rings,
  `server_commit_id`, `shardJournalCommit`, `shardJournalFlush`, `shardJournalEmittedUpTo`.
  Depends only on `queues.h`, `zmalloc.h`, and the `robj` type.
- `src/Makefile` (object list, near `:514`) and `cmake/Modules/SourceFiles.cmake` (near `:14`) —
  build wiring, **both** required.
- `src/unit/test_shard_journal.cpp` — **new**: P1–P5 gtest cases, the fake-shard harness, and the
  negative batching test. Auto-discovered (`src/unit/Makefile:14`); no registration.
- `src/unit/test_shard_journal_bench.cpp` *(or a `--benchmark` flag on the above)* — **new**: the
  §5 measurements.

**Not touched:** `server.c`, `replication.c`, `aof.c`, `networking.c`, `config.c`. Phase 1 adds a
module and tests and changes no runtime behavior — it is safe to land in `main` on its own,
which is what makes the measurement reproducible by others later.

## 7. Test plan

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
8. `ArrivalOrderMergeViolatesCausality` — the same for the "just merge in arrival order" shortcut
   parent §7 rejects. Makes the rejected alternative a test, not a paragraph.

**Randomized (§4.3):** seeded sweep, asserted against P1–P5, run under TSan and ASan.

## 8. Verification

1. `make -C src && make -C src test-unit UNIT_TEST_PATTERN='ShardJournal*'`.
2. CMake build to confirm the second source list is wired: configure + build + run the gtest
   target.
3. TSan: rebuild with `SANITIZER=thread` and run the randomized sweep; **zero** reported races.
4. ASan/UBSan run of the same sweep.
5. Long sweep (≥10 min, ≥8 shards) with a recorded seed list; any failure must be replayable from
   its seed alone.
6. Produce the §5 table.

## 9. Gate — and what "no" looks like

**Proceed to Phase 2 only if:** P1–P5 hold across the randomized sweep under TSan, *and* the §5
numbers show the shared atomic plus sequencer costing meaningfully less per write than the
keyspace work they are ordering (Stage 0, [proposal-stage0-measurement.md](proposal-stage0-measurement.md),
says what that work costs today).

**Stop if** (parent §12, made concrete here):

- P2 cannot be preserved without a cost that erases the scaling — i.e. the only schemes that
  preserve per-client causality cost more than they save. Parent §7 already refuses to make
  arrival-order the default, so "we could just relax it" is not an escape hatch at this gate.
- `atomic_fetch_add` on one cache line saturates below the write rate a single thread achieves
  today. Then N shards cannot out-produce one thread no matter how fast the keyspace work gets.
- p99 stall time from gap-waiting is large enough to be a visible replication-latency
  regression, and no bounded-reorder variant fixes it.

Writing the "no" down before running the experiment is the point of doing the experiment first.

## 10. References

- Parent design: [proposal-slot-per-thread.md](proposal-slot-per-thread.md) §7 (the ordering
  crux), §10.1 (phasing), §12 (abandonment criteria), §14.4 (the journal sketch this note
  refines).
- Consumer of this module: [Phase 5](proposal-slot-per-thread-phase5.md) (writes + propagation).
- Snapshot dependency: [10-dragonfly-snapshot-model.md](10-dragonfly-snapshot-model.md) — the
  same journal is what makes a per-shard snapshot cut globally coherent.
- Measurement context: [proposal-stage0-measurement.md](proposal-stage0-measurement.md).
- Code: `src/server.c:3626,3680,3746,3799,3875` (propagation today), `src/replication.c:449,579`
  (feed + offset), `src/aof.c:1446`, `src/queues.h` (transport), `src/unit/Makefile:14` (test
  discovery).
