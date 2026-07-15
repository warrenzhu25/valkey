# Proposal — Fork-less RDB in Valkey

**Status: pre-issue draft.** This is the end-to-end design for producing an RDB — for
`BGSAVE`, automatic saves, and replication full sync — **without `fork()`**. It consumes
the snapshot primitive designed in
[proposal-dashtable-adoption.md](proposal-dashtable-adoption.md) (**P1**: per-bucket
version stamps + a serialize-before-mutate hook) and focuses on everything *around* byte
production that today lives in the fork/child/reap machinery.

Companions: mechanics in [09-dragonfly-snapshot-model.md](09-dragonfly-snapshot-model.md),
the fork baseline in [06-persistence-rdb-aof.md](06-persistence-rdb-aof.md), ranking in
[proposal-dragonfly-inspired-perf.md](proposal-dragonfly-inspired-perf.md) §4.1, and the
threaded endgame in [proposal-slot-per-thread.md](proposal-slot-per-thread.md).

All `file:line` anchors were checked against this checkout.

---

## 1. Goal and scope

**Produce a byte-identical RDB stream without forking**, for three consumers that all
funnel through the same serializer today:

- **`BGSAVE` / automatic saves** → disk (`rdbSaveBackground`, `src/rdb.c:1673`).
- **Diskless replication full sync** → replica sockets (`rdbSaveToReplicasSockets`,
  `src/rdb.c:3756`).
- (Unchanged) **`SAVE`** — already blocking in the main process (`rdbSave`,
  `src/rdb.c:1634`); not a fork case, left as-is.

**In scope:** the producer, its cooperative scheduling, the save lifecycle that replaces
the child process, diskless fan-out, and the config/observability surface.

**Out of scope (provided by P1):** the point-in-time mechanism itself — version stamps,
the mutation hook, conservative vs. relaxed, and the rehash/defrag-during-snapshot pause.
See the Dashtable-adoption doc §4. **Also out of scope:** the RDB *byte format*, which is
deliberately unchanged (§4.2).

## 2. What fork buys today, and the machinery to replace

The fork trick (note 06): the child gets a COW snapshot of memory for free — a consistent
point-in-time view, no locks, no pause — then walks the keyspace via a safe `kvstore`
iterator (`src/rdb.c:1417-1421`) and writes it out while the parent serves traffic.

The costs that motivate this proposal:

- **Fork stall** — a page-table copy, tens to hundreds of ms of hard stall mid-event-loop
  on a large instance.
- **COW amplification** — a write-heavy instance approaches **2× RSS** during a save,
  driving 2× memory over-provisioning and conservative `maxmemory`. Tracked as
  `current_cow_size` / `rdb_last_cow_size` (`src/server.c:6413,6425`).
- **OOM-kill risk** at fork time — a genuine production hazard.

The machinery that implements the fork model — **all of which fork-less must replace or
retire** — is:

| Piece | Location | Fork-less replacement |
|---|---|---|
| Fork the child | `serverFork(CHILD_TYPE_RDB)` at `src/rdb.c:1682`, `:3836` | No fork; start a **producer job** (§3) |
| Child does the walk | `rdbSaveRio` / `rdbSaveRioWithEOFMark` (`src/rdb.c:1481,1534`) | Same functions, driven **cooperatively** (§3.2) |
| Reap child, dispatch | `checkChildrenDone` → `waitpid(WNOHANG)` (`src/server.c:1426,1430`), `backgroundSaveDoneHandler` (`src/rdb.c:3719`) | **In-process completion** callback (§5) |
| Abort a save | `killRDBChild` (`src/rdb.c:3740`) | Cancel the producer job (§5) |
| "Is a save running" | `hasActiveChildProcess()` / `child_type == CHILD_TYPE_RDB` (`src/server.c:877,897`) | A `save_in_progress` flag not tied to a PID (§5, §7) |
| COW/progress reporting | child info pipe → `current_cow_size` etc. | New progress + pre-image metrics (§7) |

## 3. Architecture

### 3.1 The producer replaces the child

Instead of a child process, a **snapshot producer** runs **in the serving process**:

- It holds a P1 **cut** on the target table(s) and walks buckets in index order, feeding
  each live entry to `rdbSaveRio`'s existing serialization over a `rio` (§4.1).
- Concurrent writes on the serving path hit the P1 **mutation hook**, which flushes a
  bucket's pre-image into the same stream before the write applies (conservative variant).
- Structural change (rehash/resize/defrag) on the table is **paused for the save's
  duration** (Dashtable-adoption §4.4), so bucket indices stay valid.

In single-threaded mode there is **one producer at a time** (matching today's single
child). In the slot-per-thread world, each owning thread runs a producer over its own
slots, and cross-shard consistency comes from the replication journal, not the walk
([09 §5](09-dragonfly-snapshot-model.md); slot-per-thread §replication).

### 3.2 Cooperative, time-sliced execution — the core challenge

The fork child had its own CPU; the producer shares the serving thread. It therefore
**must not run to completion in one shot** — it yields.

- Drive the producer from the event loop, analogous to **incremental rehash's microsecond
  budget** (`hashtableRehashMicroseconds`, `src/hashtable.h:135`) and the cron save-check
  (`src/server.c:1732`). Each `beforeSleep`/cron tick, serialize for a bounded time budget
  (e.g. a few hundred µs), persist a **resume cursor** (bucket index within table within
  `kvstore` — the same coordinates SCAN already encodes, `src/kvstore.c:141-148`), and
  return to serving.
- The mutation hook is **not** budgeted — it must complete a pre-image flush inline before
  a write proceeds, because correctness depends on capturing the at-cut value. Pre-images
  go to an in-memory staging buffer drained by the producer (§6), so the hook's inline
  cost is a memcpy, not an I/O.

This is the central latency trade: fork-less removes the fork stall but spreads
serialization CPU across many event-loop ticks on the serving thread. The budget bounds
per-tick impact; total wall-clock save time rises, which is acceptable and tunable.

## 4. Byte production

### 4.1 Reuse the existing serializer and `rio`

`rdbSaveRio` (`src/rdb.c:1481`) already writes through the `rio` abstraction that feeds a
file, a socket, or a buffer identically (note 06) — this is why the same code serves disk
saves *and* replica streaming (`rdbSaveRioWithEOFMark`, `src/rdb.c:1534`). Fork-less
changes **who calls it and how it yields**, not the function:

- Disk save → `rio` over the temp RDB file, `rename(2)` on completion (same as today's
  `backgroundSaveDoneHandlerDisk`, `src/rdb.c:3667`).
- Diskless → `rio` fanning out to N replica sockets (§6).

The producer replaces the *iterator driver*, not the *record writer*. Instead of one
uninterrupted `kvstoreIterator` pass (`src/rdb.c:1417-1421`), it walks buckets under the
P1 cut with the resume cursor of §3.2.

### 4.2 The RDB format is unchanged

Version tags (`RDB_VERSION`), opcodes, and per-type encodings are untouched. A fork-less
RDB is **byte-compatible** with a forked one — same loaders, same tooling, same replicas.
This is a hard requirement and the basis of the equivalence gate (§9).

### 4.3 Consistency

P1 conservative mode gives a true point-in-time image **as of the cut**. For replication,
writes after the cut accumulate in the replica output buffer / backlog and are sent after
the RDB — **exactly today's RDB-plus-backlog contract** (`src/replication.c` full-sync
path). Nothing about the offset/PSYNC contract changes.

## 5. Save lifecycle without a child

Today: fork → child exits → `checkChildrenDone` reaps → `backgroundSaveDoneHandler`
dispatches on `rdb_child_type` (DISK/SOCKET, `src/rdb.c:3719-3729`). Fork-less replaces
this with an in-process state machine:

```
IDLE → STARTING → RUNNING → { FINALIZING → DONE | FAILED | ABORTED } → IDLE
```

- **STARTING:** take the P1 cut, allocate the version array, pause structural change,
  open the target (temp file or replica sockets), init the resume cursor.
- **RUNNING:** each tick, serialize within the time budget (§3.2); drain the pre-image
  staging buffer; advance the cursor.
- **FINALIZING (disk):** flush/fsync temp file, `rename(2)`, update
  `stat_rdb_saves`/`lastbgsave_status`, run the same post-save bookkeeping as
  `backgroundSaveDoneHandlerDisk`.
- **FINALIZING (socket):** write the EOF mark, transition replicas `WAIT_BGSAVE_END` →
  online (mirrors `backgroundSaveDoneHandlerSocket`, `src/rdb.c:3694`).
- **FAILED / ABORTED:** free the version array, resume structural change, discard the temp
  file / drop the affected replicas. `ABORTED` replaces `killRDBChild`
  (`src/rdb.c:3740`) — cancellation is just tearing down the job, no signal.

**`hasActiveChildProcess()` decoupling.** Many call sites gate on
`child_type == CHILD_TYPE_RDB` (`src/server.c:897`) — AOF-rewrite scheduling, dict
resizing, etc. Fork-less introduces a distinct `rdb_save_in_progress` predicate. Crucially,
some of those gates exist **because of fork** (e.g. "don't rehash while a child holds COW
pages") and are **no longer needed** fork-less; each gate must be reviewed and either kept
(genuine mutual exclusion, e.g. one producer at a time) or dropped (fork-COW artifact).

## 6. Diskless replication fan-out — the sharp edge

Today one child streams to all attaching replicas at once; multiple `SYNC`/`PSYNC` waiters
attach to a single in-flight BGSAVE (`startBgsaveForReplication`, `src/replication.c:1019`;
attach CASES at `:1246-1288`). A **slow replica just back-pressures the child process** —
harmless to serving.

Fork-less breaks that isolation: the producer *is* the serving thread, so **a slow replica
must never block serving.** Design:

- Replica sockets are **non-blocking**; the producer writes what it can and, on `EAGAIN`,
  **buffers into that replica's output buffer** and yields — it does not spin or block.
- The producer's forward progress is gated by the **slowest replica's buffer high-water
  mark**, not by a blocking write. If a replica's buffer exceeds
  `client-output-buffer-limit`, that replica is **dropped** (same policy as today), and the
  producer continues for the rest.
- Multiple attaching replicas still share **one cut and one walk**; each gets the same
  byte stream via the fan-out `rio`. The "attach to in-flight save" logic
  (`src/replication.c:1246`, CASE 1) carries over: late attachers within the same cut can
  join; those arriving after the walk passed their coverage wait for the next save (CASE 3,
  `:1288`).
- Disk-vs-socket target selection (`src/replication.c:1033`) is unchanged; only the
  producer behind each target changes.

This is the single biggest new risk fork-less introduces that fork did not have, and §10
treats it as such.

## 7. Config and observability

**Config:**
- `rdb-forkless {no|yes|replication-only}` — opt-in, default `no` initially (§9). Lets
  operators enable it for saves, for replication, or both.
- `rdb-forkless-slice-us <n>` — the per-tick serialization budget (§3.2).

**INFO remapping** (`src/server.c:6413-6425`):
- `rdb_bgsave_in_progress` continues to mean "a background save is running" — now true when
  the fork-less producer is RUNNING, so existing tooling keeps working.
- `current_cow_size` / `rdb_last_cow_size` → **~0** fork-less (no COW). Keep the fields for
  compatibility; add new ones:
  - `rdb_save_progress_pct` — buckets walked / total.
  - `rdb_forkless_preimage_bytes` — bytes flushed by the mutation hook this save (the
    fork-less analog of COW pressure; the number operators should watch).
- `rdb_last_bgsave_status` / `stat_rdb_saves` semantics unchanged.

## 8. Interactions

- **maxmemory / eviction during a save.** Fork-less, evictions are ordinary deletes on the
  serving thread and hit the mutation hook, so evicted keys are captured at their at-cut
  value before removal — correct by construction. (Under fork, the child was oblivious to
  parent evictions; fork-less is actually cleaner here.)
- **AOF rewrite** also forks today (`src/aof.c`, gated at `src/server.c:1631,1661`). The
  same P1 primitive applies; the AOF base-file production is a follow-up with its own
  equivalence gate. Not designed here.
- **One heavy op at a time.** A fork-less RDB and a fork-less AOF rewrite walking the same
  tables simultaneously is possible in principle but should be **serialized** initially
  (one producer), matching today's single-child reality.
- **Shutdown during a save.** `SHUTDOWN` waits for or aborts the producer instead of the
  child; a `SHUTDOWN SAVE` can finalize synchronously via the blocking `rdbSave` path.

## 9. Rollout

Fork-less must earn `default on`. Staged, coexisting with fork throughout:

| Stage | Deliverable | Gate |
|---|---|---|
| **0** | Land P1 (Dashtable-adoption Stages 1–2): version array + hook + structural-change pause. | P1 gates. |
| **1** | Fork-less **disk** `BGSAVE` behind `rdb-forkless=yes`; fork path retained as default. | Fork-less RDB **byte-identical** to forked RDB across fuzz corpora and real datasets. |
| **2** | Cooperative scheduling + budget; latency validation under write load. | p99 serving latency during save within target; save completes in bounded wall-clock. |
| **3** | Fork-less **diskless replication** full sync with fan-out + slow-replica handling (§6). | Replica converges bit-for-bit vs fork-based full sync; a stalled replica does not raise serving latency. |
| **4** | Flip default to fork-less where supported; keep fork as fallback for a release. | Soak in production-like load; no regression in save success rate. |

## 10. Risks

1. **Serving latency from in-process serialization (§3.2).** Primary trade. Mitigation:
   strict per-tick budget; per-shard producers in the threaded world; measure p99, not mean.
2. **Slow-replica back-pressure onto serving (§6).** New failure mode fork did not have.
   Mitigation: strictly non-blocking fan-out, COB-limit drops, never block the producer.
3. **Structural-change pause stalls a needed resize (P1 §4.4).** A long save defers table
   growth. Mitigation: bounded save duration; both-tables coverage as a follow-up.
4. **Pre-image write amplification under write bursts.** Conservative mode stages many
   pre-images during a churny save. Bounded by churn during the window, and still avoids
   fork's page-level 2× ceiling — but must be surfaced (`rdb_forkless_preimage_bytes`, §7)
   and buffer-capped.
5. **Two save paths during transition (§9).** Larger test surface. Mitigation: byte-equality
   gates before any default flip.
6. **Silent removal of fork-motivated gates (§5).** Dropping a `hasActiveChildProcess`
   gate that turns out to guard more than COW would be a subtle bug. Mitigation: review each
   call site explicitly; keep genuine mutual-exclusion gates.

## 11. Open questions

- **Who runs the producer in single-threaded mode** — a `beforeSleep` job, or a dedicated
  I/O-thread task (`src/io_threads.c`) to isolate serialization CPU from command execution?
- **Pre-image staging buffer cap and policy** when a write burst outruns the producer's
  drain rate — block the writer briefly, or spill?
- **Interaction with `WAIT`/`WAITAOF`** and the exact offset assigned at the cut, so
  post-cut acknowledgements remain correct.
- Should `rdb-forkless-slice-us` be **adaptive** (widen the budget when idle, shrink under
  load) rather than a fixed value?

## 12. References

- Snapshot primitive this consumes: [proposal-dashtable-adoption.md](proposal-dashtable-adoption.md) §4 (P1)
- Exact version/cut rules: [09-dragonfly-snapshot-model.md](09-dragonfly-snapshot-model.md)
- Fork baseline being replaced: [06-persistence-rdb-aof.md](06-persistence-rdb-aof.md)
- Code: `src/rdb.c` (save + done handlers), `src/replication.c` (full sync), `src/server.c`
  (child lifecycle, INFO), `src/hashtable.c`/`src/kvstore.c` (walk + cursor)
