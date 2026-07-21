# Proposal — Slot-per-thread Phase 5: single-key writes, per-shard journals, the sequencer

**Status: pre-issue draft.** This is the concrete, line-anchored design of **Phase 5** of
[proposal-slot-per-thread.md](proposal-slot-per-thread.md) §10 — letting **write** commands
execute on shard threads, and preserving today's replication and AOF guarantees while they do.

This is where the parent doc's §7 — "the part that decides whether the design is viable" — meets
the actual propagation code. [Phase 1](proposal-slot-per-thread-phase1.md) proved the ordering
model in isolation and built `shard_journal.{c,h}`; this phase wires that module into
`propagateNow` and makes the shard threads its producers.

Prerequisites: [Phase 1](proposal-slot-per-thread-phase1.md) (the module and its gate),
[Phase 4 part 1](proposal-slot-per-thread-phase4-eventloop.md) (per-thread loops), and
[Phase 4 part 2](proposal-slot-per-thread-phase4.md) (LOCAL/REMOTE dispatch for reads — writes
extend the same branch).

All `file:line` anchors were checked against this checkout.

---

## 1. Scope

> "Single-key writes + per-shard journals + the sequencer." (parent §10.5)

**In scope:** extending `CMD_SHARD_SAFE` to single-key **writes**; per-shard propagation
accumulation; the commit-id stamp and journal append; the sequencer that merges shard journals
into the existing replication backlog and AOF in commit-id order; the argv-lifetime change that
writes force; `WAIT`/`WAITAOF` correctness; keyspace-notification ordering.

**Out of scope:** multi-key and multi-slot writes, `MULTI`/`EXEC`, Lua, functions, modules — all
continue to take the barrier (parent §6), and the barrier is *already* correct for them because
it runs today's code on a quiesced keyspace. Per-shard expiry and eviction, which also produce
writes, are [Phase 6](proposal-slot-per-thread-phase6.md). Replacing the barrier with VLL is
[proposal-vll-transactions.md](proposal-vll-transactions.md).

## 2. Verified anchors (this checkout)

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
- `mustObeyClient` (`src/server.c:3589`) marks primary/AOF-sourced clients — the apply path of
  §4.7 — and is consulted by `getKeySlot` (`src/db.c:252-262`).
- Journal module from Phase 1: `journalRec`, `shardJournalCommit`, `shardJournalFlush`,
  `shardJournalEmittedUpTo` in `src/shard_journal.{c,h}`.

## 3. Design

### 3.1 The shape of the change: keep `propagateNow`, change only who calls it

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

### 3.2 Per-shard propagation state

Three globals move onto the shard:

| Global | Anchor | Becomes |
|---|---|---|
| `server.also_propagate` | `src/server.c:3450,3471,3702` | `shard->also_propagate` — accumulated and drained entirely on the owning thread. |
| `server.dirty` | read `src/server.c:3907`, `:3983` | `shard->dirty`; `INFO`'s `rdb_changes_since_last_save` sums across shards under barrier. |
| `server.execution_nesting` | `src/server.c:3800` | Per-shard (it is a per-execution-stack property, so this is a correctness fix, not just a partition). |

`alsoPropagate` and `propagatePendingCommands` become shard-relative: same code, `server.` →
`thisShard()->`. Nothing about *when* they run changes — `postExecutionUnitOperations`
(`src/server.c:3799`) still fires at the top of the call stack, on whichever shard executed.

### 3.3 The commit stamp — where exactly

At the end of the execution unit, on the owning shard, `propagatePendingCommands`' replacement
does:

```c
if (shard->also_propagate.numops == 0) return;      /* read-only command: no record, no seq */
journalRec *rec = journalRecFromOpArray(&shard->also_propagate);
shardJournalCommit(shard->journal, rec);            /* seq = fetch_add; append to ring */
serverOpArrayFree(&shard->also_propagate);
```

Three properties this placement buys, all of them load-bearing for parent §7:

- **The id is taken after execution completes.** Per-key order follows, because one shard owns a
  key and its ring is FIFO.
- **Per-client causality follows from the dispatch contract**, not from anything here: the
  coordinator does not dispatch command *k+1* until *k* has resumed
  ([Phase 4 part 2](proposal-slot-per-thread-phase4.md) §1 — `BLOCKED_SHARD` holds the querybuf),
  so *k* took the lower id. This is the property Phase 1 tested; Phase 5 must not weaken it,
  which is why **the REMOTE result must be posted after the commit, never before** (§3.4).
- **Read-only commands take no id at all**, so the shared atomic sees write traffic only.

### 3.4 REMOTE writes: the argv-lifetime change

[Phase 4 part 2](proposal-slot-per-thread-phase4.md) §4 flags this explicitly and defers it here:

> "The `robj`s are shared read-only across threads for the hop's duration; this is safe only
> because they are not mutated (reads) — a write path (Phase 5) must either deep-copy argv into
> the job or guarantee the coordinator won't touch them, and that is a Phase-5 decision, not
> smuggled in here."

**Decision: keep the borrow, and rely on `BLOCKED_SHARD`.** The coordinator's client is blocked
for the whole hop, so `commandProcessed` returns early (`src/networking.c:3887`) and never
resets argv; the coordinator therefore does not touch those `robj`s, and the owner only reads
them. Deep-copying argv on every REMOTE write would add an allocation and a copy to the hop,
which is the exact tax parent §11 already worries about.

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

### 3.5 `WAIT`, `WAITAOF`, and read-your-own-offset

A subtle gap opens between execution and emit: the client's write has committed (it has a `seq`)
but the sequencer may not have emitted it yet, so `server.primary_repl_offset`
(`src/replication.c:476`) does not yet cover it. A client that writes and immediately calls
`WAIT` would wait on an offset that excludes its own write — silently returning success too
early. That is a correctness regression, not a latency one.

Fix, in two parts:

1. The client records the `seq` of its last write (`c->last_write_seq`).
2. `WAIT`/`WAITAOF` are barrier commands (parent §6), and the barrier already flushes the
   sequencer before running the command (parent §14.5 step 2). Add the assertion that
   `shardJournalEmittedUpTo() >= c->last_write_seq` after that flush, and only then read the
   offset. Because the barrier quiesces every shard, the flush cannot stall on a gap: every id
   below the highest has been committed by a parked shard.

The same argument covers anything else that reads `primary_repl_offset` as a proxy for "my writes
are durable": `INFO replication`'s offsets, `FAILOVER`, and the replica-pause paths. Rule of
thumb to write into the code: **`primary_repl_offset` is meaningful only for records the
sequencer has emitted**; if a caller needs it to cover a specific write, it must flush first.

### 3.6 Keyspace notifications and `WATCH`

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

### 3.7 Which commands become `SHARD_SAFE` writes

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

## 4. The interactions that decide whether this is safe

### 4.1 Nested execution units

`postExecutionUnitOperations` early-returns on `server.execution_nesting` (`src/server.c:3800`)
so that an inner `call()` does not propagate before the outer one finishes. Making the counter
per-shard (§3.2) is necessary, but also verify the nesting sources that can appear on a shard
thread in this phase: key-miss notifications, expired-key deletions during lookup, and
`firePostExecutionUnitJobs`. Anything that can nest *across* shards must not exist — and does
not, because cross-shard work is either a REMOTE hop (whose owner runs its own unit) or a
barrier.

### 4.2 Lazy expiration on the write path

Looking up a key can delete it and propagate a `DEL`/`UNLINK` (the lazy-expire path). Under
sharding this happens on the owner of the key — the same shard executing the command — so the
`DEL` lands in the same shard's op array, in the same execution unit, in the right order. This is
the case that is correct for free *because* ownership is per-slot, and it is worth an explicit
test (§6.7) since it is the most common way a "read-only" `GET` produces a write.

### 4.3 The barrier must flush before it runs

Parent §14.5 step 2 already says so; Phase 5 makes it load-bearing. A barrier command runs on a
quiesced keyspace and must see a fully-ordered prefix of the stream. It must also **not**
interleave its own propagation with journal records — so the barrier command propagates through
`propagateNow` directly, on shard 0, after the flush, and takes a commit id so that its position
in the stream is defined relative to later shard records.

### 4.4 AOF

`flushAppendOnlyFile(0)` runs in `beforeSleep` on shard 0. The sequencer must run **before** it,
so records emitted this iteration reach `aof_buf` before the flush — the same ordering
`propagatePendingCommands` has today relative to `beforeSleep`'s AOF write
(`src/server.c:1854` region). With `appendfsync always`, a write's fsync is therefore deferred to
the first loop iteration after its record is emitted; that is already true today for any command
completing mid-iteration.

`server.fsynced_reploff_pending` (`src/server.h:2105`) is already atomic and already
cross-thread; nothing changes for it, because it tracks emitted offsets, which remain
single-threaded.

### 4.5 The replica apply path

A replica applying from its primary runs commands through a client for which `mustObeyClient`
(`src/server.c:3589`) is true, and those commands may touch **any** slot — `getKeySlot` even
backfills `c->slot` for them (`src/db.c:260-262`) precisely because `getNodeByQuery()` never ran.
Parent §8 lists this as needing its own design.

**Phase 5 decision: the apply path takes the barrier, unconditionally.** A replica therefore gets
no execution scaling from this phase. That is an acceptable and honest limitation — a replica's
apply stream is single-threaded today anyway, so nothing regresses — and it avoids designing
concurrent apply (which needs the primary to communicate its own commit order) in the same phase
that introduces concurrent writes on the primary. Note it in the release notes: **`shard-threads`
scales primaries, not replica apply.**

### 4.6 Eviction during a write

`performEvictions` (`src/evict.c:404`) runs before command execution when `maxmemory` is
exceeded, evicts keys from *any* database and *any* slot, and propagates `DEL`s. A shard thread
cannot do that safely. **Phase 5 decision: if the server is over `maxmemory`, writes take the
barrier**, i.e. the eviction check escalates. That makes `maxmemory`-constrained workloads
unscaled by this phase — the honest cost — and is why per-shard eviction gets its own phase
([Phase 6](proposal-slot-per-thread-phase6.md) §5) with its own measurement gate.

### 4.7 Cluster slot migration

Parent §8 notes slot ownership now implies thread ownership. Any `slot_to_shard[]` change or
cluster slot migration must take a barrier, and — new in this phase — the barrier must flush the
sequencer before reassigning, so that no journal record exists for a slot whose owner is about to
change. Coordinate with `design-docs/atomic-slot-migration.md` rather than inventing a second
mechanism.

## 5. Files touched

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

## 6. Test plan

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

## 7. Verification

1. `make -C src && make -C src test-unit UNIT_TEST_PATTERN='ShardJournal*'`.
2. `./runtest --single unit/shard-writes`, `./runtest --single integration/shard-replication`.
3. Full suite plus `./runtest-cluster` at `shard-threads 1` (byte-identical) and `4`.
4. **TSan** at `shard-threads 4` under a write-heavy workload. Zero races.
5. Long-running differential soak: random command mix, periodic `DEBUG DIGEST` comparison between
   primary and replica, ≥1 hour, with a recorded seed.
6. Benchmark and publish: write throughput vs `shard-threads` (1, 2, 4, 8), the sequencer's
   per-record cost in production shape, reorder-buffer depth and stall p99 (Phase 1 §5's metrics,
   now on the real server), and the REMOTE-write tax vs LOCAL.

## 8. Honest risks

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

## 9. References

- Parent design: [proposal-slot-per-thread.md](proposal-slot-per-thread.md) §7 (the ordering
  crux), §6 (the barrier), §8 (the rest of the system), §10.5, §14.4 (the journal sketch).
- Prerequisites: [Phase 1](proposal-slot-per-thread-phase1.md) (the module and its gate),
  [Phase 4 part 1](proposal-slot-per-thread-phase4-eventloop.md),
  [Phase 4 part 2](proposal-slot-per-thread-phase4.md) (argv lifetime, `BLOCKED_SHARD`).
- Next: [Phase 6](proposal-slot-per-thread-phase6.md) (per-shard expiry and eviction — the other
  producers of writes).
- Related: [10-dragonfly-snapshot-model.md](10-dragonfly-snapshot-model.md) (the same journal
  makes a per-shard snapshot cut coherent), [proposal-forkless-rdb.md](proposal-forkless-rdb.md),
  `design-docs/atomic-slot-migration.md`.
- Background: [02-command-execution-path.md](02-command-execution-path.md),
  [06-persistence-rdb-aof.md](06-persistence-rdb-aof.md), [07-replication.md](07-replication.md).
- Code: `src/server.c:3450,3471,3589,3626,3680,3702,3746,3799,3875,3907,3983,4060,4083`,
  `src/replication.c:449,476,579,597,5052,5086`, `src/aof.c:1446`, `src/notify.c:105`,
  `src/multi.c:464`, `src/db.c:755,760`, `src/evict.c:404`.
