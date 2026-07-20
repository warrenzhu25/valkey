# Proposal — Fork-less RDB in Valkey

**Status: pre-issue draft.** This is the end-to-end design for producing an RDB — for
`BGSAVE`, automatic saves, and replication full sync — **without `fork()`**. It consumes
the snapshot primitive designed in
[proposal-dashtable-adoption.md](proposal-dashtable-adoption.md) (**P1**: per-bucket
version stamps + a serialize-before-mutate hook) and focuses on everything *around* byte
production that today lives in the fork/child/reap machinery.

Companions: mechanics in [10-dragonfly-snapshot-model.md](10-dragonfly-snapshot-model.md),
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
([10 §5](10-dragonfly-snapshot-model.md); slot-per-thread §replication).

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

### 8.1 Relation to AOF — snapshot-frequency as a durability knob

Dragonfly is **snapshot-only**: it has historically had no append-only log, relying on
frequent point-in-time snapshots for persistence (and replication for HA). It's tempting to
read that as "cheap snapshots *replace* AOF." They don't — and being precise about why is what
tells you which half Valkey should actually follow. (Note the terminology trap: "relaxed" in
[10-dragonfly-snapshot-model.md](10-dragonfly-snapshot-model.md) §4 is a *within-snapshot*
consistency mode — serialize the new value in place, no pre-image — not a durability scheme.
For persistence you want the **conservative** variant; relaxed's "as-of-finish" isn't a clean
crash-recovery instant.)

**The two have different cost curves, and neither dominates:**

| | Cost per unit of durability | Data-loss window on crash |
|---|---|---|
| **AOF** | **O(writes)** — each write logged once | fsync interval: **≤1 s** (`appendfsync everysec`) or **0** (`always`) |
| **Snapshot-only** | **O(dataset)** — re-serialize everything, each time | the **snapshot interval** |

AOF writes bytes proportional to the *write rate*; a snapshot writes bytes proportional to the
*dataset size*. To match `everysec`'s ≤1 s window with snapshots you'd re-serialize the whole
dataset every second — fine for a small instance, absurd for a large one. So snapshot-only is a
**simplification with a durability downgrade**, defensible for a replication-HA + backup-DR
posture (Dragonfly's bet), not a strict win.

**Valkey already owns the stronger half of this.** The modern multi-part AOF (chapter 06,
`06-persistence-rdb-aof.md`) is *literally a snapshot base plus an incremental tail*:
`aof-use-rdb-preamble` defaults to yes (`src/config.c:3364`), so the AOF **base file is an
RDB snapshot**, and the incremental files are the command log since that base, tied by a
manifest. That is strictly more capable than snapshot-only — cheap base *plus* a bounded-loss
tail. A user who wants Dragonfly's model today already can: run RDB-only with `save` rules and
no AOF.

**So what fork-less snapshotting actually changes here is not "drop AOF" — it's making the
snapshot cheap enough to move the durability knob:**

- **Snapshot-only becomes a real *option*.** Once a full snapshot costs no fork stall and no
  2× COW (§2), snapshotting every few seconds is affordable, and users who tolerate an
  interval-sized loss window can skip AOF entirely — no rewrite machinery, no AOF write
  amplification. This should be offered as an explicit mode, **never** as a replacement,
  because AOF's O(writes) / ≤1 s contract still dominates for large-dataset, low-loss-tolerance
  workloads.
- **AOF's replay tail can shrink toward zero.** Because the AOF base *is* a snapshot and
  rewrites are triggered by incremental growth (chapter 06), a cheap fork-less base lets you
  rewrite the base far more often, keeping the command tail (and thus replay time and loss
  surface) tiny. Valkey's manifest architecture already supports sliding along this
  continuum between "classic AOF" and "snapshot-only" — you simply rewrite the base
  aggressively instead of letting the incremental grow. Fork-less RDB is the enabler; no new
  format is needed.

**Bottom line for this proposal:** the AOF interaction (§8, "AOF rewrite also forks") is not
just "make rewrite fork-less too" — it's that a cheap fork-less snapshot turns *snapshot
frequency* into a first-class durability knob spanning from AOF-`everysec` down to periodic
backups. Follow Dragonfly's cheap-snapshot half wholeheartedly; follow its drop-AOF half only
as an opt-in mode.

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
- Exact version/cut rules: [10-dragonfly-snapshot-model.md](10-dragonfly-snapshot-model.md)
- Fork baseline being replaced: [06-persistence-rdb-aof.md](06-persistence-rdb-aof.md)
- Code: `src/rdb.c` (save + done handlers), `src/replication.c` (full sync), `src/server.c`
  (child lifecycle, INFO), `src/hashtable.c`/`src/kvstore.c` (walk + cursor)

---

## 13. Implementation guide (ready-to-code)

This section turns §3–§6 into concrete artifacts. The single hardest problem — and the one
that shapes everything — is that today's walk (`rdbSaveRio` → `rdbSaveDb` →
`kvstoreIteratorNext`, `src/rdb.c:1417-1421`) runs to completion in **one** synchronous call
against a live `kvstoreIterator`. Fork-less has to make that walk **resumable across
event-loop ticks** while the same tables keep serving writes. Two viable structures follow;
pick one before writing anything else.

### 13.1 The resumable-walk decision (do this first)

| Option | How | Cost |
|---|---|---|
| **A. Long-lived iterator** | Keep one `kvstoreIterator` alive for the whole save; call `kvstoreIteratorNext` for a bounded time each tick, then return. | The iterator holds a *safe-iterator* pause on rehashing for the entire save (`src/hashtable.c`, safe-iter list). Fine because P1 already pauses structural change (§3.1) — but it means the iterator object, not a plain cursor, is the resume state. |
| **B. Cursor walk** | Re-derive position each tick from a saved `(dbid, slot, bucket_idx)` cursor — the same coordinates the kvstore cursor already encodes (`hashtableCursorToKvstoreCursor`, `src/kvstore.c:141`). | No long-lived iterator, but you re-implement bucket iteration and must handle a bucket that gained/lost entries via the mutation hook since last tick. |

**Recommend A.** With structural change paused (P1 §4.4), a long-lived `kvstoreIterator` is
stable for the save's duration, and "resume" is simply "don't free the iterator between
ticks." Option B only becomes necessary if we later want to *allow* rehashing mid-save.

### 13.2 Core data structures (`src/rdb_forkless.h`)

```c
typedef enum {
    SNAP_IDLE, SNAP_STARTING, SNAP_RUNNING,
    SNAP_FINALIZING, SNAP_DONE, SNAP_FAILED, SNAP_ABORTED
} snapState;

typedef enum { SNAP_TARGET_DISK, SNAP_TARGET_SOCKETS } snapTarget;

typedef struct snapshotProducer {
    snapState        state;
    snapTarget       target;
    rio              rdb;              /* the SAME rio abstraction (§4.1): file or fan-out */
    int              req, rdbver;
    rdbSaveInfo     *rsi;

    /* Resume state (Option A): where the walk is paused. */
    int              cur_db;          /* which db we're on */
    kvstoreIterator *it;              /* live iterator into db->keys; NULL between dbs */
    int              phase;           /* header / functions / dbs / footer — mirror rdbSaveRio's stages */

    /* Pre-image staging (§6): the mutation hook fills this; the producer drains it. */
    unsigned char   *preimage_buf; size_t preimage_len, preimage_cap;

    /* Disk target */
    char            *tmpfile;         /* temp-*.rdb; rename on FINALIZE */
    int              fd;

    /* Socket target */
    list            *replicas;        /* clients in WAIT_BGSAVE_END sharing this cut */

    /* Metrics (§7) */
    unsigned long long buckets_total, buckets_done, preimage_bytes;
    monotime         started;
} snapshotProducer;

extern snapshotProducer *server_snapshot;   /* NULL when SNAP_IDLE; one at a time */
```

### 13.3 New functions (`src/rdb_forkless.c`)

```c
/* Replaces rdbSaveBackground's fork branch. Sets up the cut and the producer,
 * returns immediately; the walk happens over subsequent ticks. */
int snapshotStart(int req, snapTarget target, char *filename_or_null,
                  rdbSaveInfo *rsi, list *replicas_or_null);

/* Driven from beforeSleep (§13.5). Serializes for up to `slice_us`, drains the
 * pre-image buffer, advances the cursor. Transitions RUNNING->FINALIZING when the
 * walk is complete. Returns C_OK unless a write error forces FAILED. */
int snapshotStep(snapshotProducer *p, int slice_us);

/* The FINALIZING half of the state machine (§5). Disk: fsync+rename+bookkeeping,
 * mirroring backgroundSaveDoneHandlerDisk (src/rdb.c:3667). Socket: EOF mark +
 * replica online transition, mirroring backgroundSaveDoneHandlerSocket (:3694). */
void snapshotFinalize(snapshotProducer *p);

/* Replaces killRDBChild (src/rdb.c:3740). Tears down the job: free version array,
 * resume structural change, discard temp file / drop replicas. No signal. */
void snapshotAbort(snapshotProducer *p);

/* The P1 mutation hook, installed on every owned table for the save's duration.
 * Called INLINE before a write applies (NOT time-budgeted, §3.2): if the bucket's
 * version is <= the cut, memcpy its pre-image into p->preimage_buf. */
void snapshotPreimageHook(hashtable *ht, bucket *b);   /* signature per P1 */

/* Predicate replacing "child_type == CHILD_TYPE_RDB" for RDB purposes (§5). */
static inline int rdbSaveInProgress(void) {
    return server_snapshot && server_snapshot->state == SNAP_RUNNING;
}
```

### 13.4 Making `rdbSaveRio` resumable without forking it

`rdbSaveRio` (`src/rdb.c:1481`) is a linear sequence: magic → aux → functions → per-db walk →
EOF → checksum. Rather than rewrite it, **split it into stepped stages** driven by
`p->phase`, reusing the existing record writers verbatim:

```c
int snapshotStep(snapshotProducer *p, int slice_us) {
    monotime deadline = getMonotonicUs() + slice_us;

    /* Always drain pending pre-images first — they are the at-cut truth (§6). */
    if (p->preimage_len && rioWrite(&p->rdb, p->preimage_buf, p->preimage_len) == 0) goto werr;
    p->preimage_len = 0;

    switch (p->phase) {
    case PHASE_HEADER:    /* magic + aux + modules-aux + functions (rdb.c:1488-1496) */
        if (writeHeader(p) < 0) goto werr;
        p->phase = PHASE_DBS; p->cur_db = 0; p->it = NULL;
        /* fallthrough */
    case PHASE_DBS:
        while (p->cur_db < server.dbnum) {
            if (!p->it) { if (openDbWalk(p) < 0) goto werr; }   /* SELECT/RESIZE opcodes + kvstoreIteratorInit */
            void *next;
            while (getMonotonicUs() < deadline && kvstoreIteratorNext(p->it, &next)) {
                if (rdbSaveKeyValuePairFromEntry(&p->rdb, next, p->cur_db) < 0) goto werr;  /* existing writer */
                p->buckets_done++;
            }
            if (getMonotonicUs() >= deadline) return C_OK;      /* YIELD: resume here next tick */
            kvstoreIteratorRelease(p->it); p->it = NULL; p->cur_db++;
        }
        p->phase = PHASE_FOOTER;
        /* fallthrough */
    case PHASE_FOOTER:    /* modules-aux-after + EOF opcode + CRC64 (rdb.c:1508-1517) */
        if (writeFooter(p) < 0) goto werr;
        p->state = SNAP_FINALIZING;
    }
    return C_OK;
werr:
    p->state = SNAP_FAILED;
    return C_ERR;
}
```

The bodies of `writeHeader` / `openDbWalk` / `writeFooter` are lifted **line for line** from
`rdbSaveRio`/`rdbSaveDb` — the only change is that the per-db `while` loop now checks a
deadline and can return mid-database with `p->it` and `p->cur_db` preserved. This is the
concrete meaning of "changes who calls it and how it yields, not the function" (§4.1).

### 13.5 Integration points (file-by-file)

| File | Change |
|---|---|
| `src/rdb_forkless.{c,h}` | **New.** §13.2–§13.4. |
| `src/rdb.c` | `rdbSaveBackground` (`:1673`): when `server.rdb_forkless` is on and target eligible, call `snapshotStart()` instead of `serverFork(CHILD_TYPE_RDB)` at `:1682`. Refactor `writeHeader`/`openDbWalk`/`writeFooter` out of `rdbSaveRio`/`rdbSaveDb` so both fork and fork-less share them. `rdbSaveToReplicasSockets` (`:3756`) grows a fork-less branch. |
| `src/server.c` | `beforeSleep` (`:1854`): add `if (server_snapshot && server_snapshot->state == SNAP_RUNNING) snapshotStep(server_snapshot, server.rdb_forkless_slice_us);` and a FINALIZING check calling `snapshotFinalize`. `checkChildrenDone` (`:1426`) unchanged for AOF/module children; RDB completion no longer flows through it. Audit every `hasActiveChildProcess()` / `child_type == CHILD_TYPE_RDB` (`:877`, `:897`) — see §13.6. |
| `src/replication.c` | `startBgsaveForReplication` (`:1019`) routes to `snapshotStart(..., SNAP_TARGET_SOCKETS, replicas)` when fork-less; attach-to-in-flight (`:1246`) shares `p->replicas` and one cut. |
| `src/config.c` | `createEnumConfig("rdb-forkless", ...)` (`no`/`yes`/`replication-only`, default `no`) with a `rdb_forkless_enum[]` table, mirroring `repl-diskless-load` at `:3435`; plus `createIntConfig("rdb-forkless-slice-us", NULL, MODIFIABLE_CONFIG, 50, 5000, server.rdb_forkless_slice_us, 300, INTEGER_CONFIG, NULL, NULL)`. |
| `src/server.c` INFO (`:6413-6425`) | `rdb_bgsave_in_progress` also true when `rdbSaveInProgress()`; add `rdb_save_progress_pct` (`buckets_done*100/buckets_total`) and `rdb_forkless_preimage_bytes` (`p->preimage_bytes`); `current_cow_size` reports ~0 fork-less. |
| `src/hashtable.c` / P1 | Install/uninstall `snapshotPreimageHook` on owned tables in `snapshotStart`/`snapshotFinalize`/`snapshotAbort`. |

### 13.6 The `hasActiveChildProcess()` audit (§5) — the subtle one

Each call site that today means "an RDB child is running" must be classified. Grep
`hasActiveChildProcess\|CHILD_TYPE_RDB` and for each decide:

- **Keep, retarget to `rdbSaveInProgress()`** — genuine mutual exclusion: "don't start a
  second heavy op," "don't start AOF rewrite while a save runs" (§8), shutdown coordination.
- **Drop entirely** — the gate exists *only* because fork COW made it unsafe, e.g. the dict
  resize/rehash guard at `src/server.c:855` (`server.dict_resizing` / `in_fork_child`).
  Fork-less has no COW pages to protect, and P1 pauses structural change explicitly for the
  save anyway, so this guard is a fork artifact. **Dropping the wrong one is risk #6 in §10** —
  do it one site at a time with a test.

### 13.7 Diskless fan-out rio (§6)

The socket target is a `rio` whose write callback fans one buffer to N non-blocking replica
sockets:

```c
static size_t fanoutRioWrite(rio *r, const void *buf, size_t len) {
    snapshotProducer *p = r->io.fanout.producer;
    listIter li; listNode *ln; listRewind(p->replicas, &li);
    while ((ln = listNext(&li))) {
        client *replica = ln->value;
        /* Append to the replica's output buffer; the event loop drains it.
         * NEVER block the producer on a slow replica (§6). */
        addReplyProtoToReplica(replica, buf, len);
        if (replicaOutputBufferExceedsLimit(replica))   /* client-output-buffer-limit */
            dropReplica(replica);                        /* same policy as today */
    }
    return len;   /* producer keeps walking; back-pressure is via the COB limit, not blocking */
}
```

Forward progress is gated by the slowest surviving replica's buffer high-water mark, not by a
blocking `write` — the core of §6. Late attachers within the same cut join `p->replicas`
(CASE 1, `src/replication.c:1246`); those arriving after the walk passed their coverage wait
for the next save (CASE 3, `:1288`).

### 13.8 Equivalence & latency harness (the gates in §9)

- **Byte-equality (Stage 1 gate).** New test `tests/integration/rdb-forkless.tcl`: for a
  corpus (fuzz-generated + real dumps), produce an RDB with `rdb-forkless no` and with
  `rdb-forkless yes` **under identical, quiesced state**, and assert the files are
  byte-identical (or, if aux timestamps differ, load-equal via `DEBUG RELOAD` digest
  `DEBUG DIGEST`). This is the §4.2 requirement mechanized.
- **Consistency under writes (Stage 2 gate).** Drive continuous writes during a fork-less
  save; assert the loaded RDB equals a point-in-time `DEBUG DIGEST` captured at the cut. This
  validates the P1 hook end-to-end.
- **Latency (Stage 2 gate).** `valkey-benchmark` write load during a save; assert p99 stays
  within target and that no single tick exceeds `rdb-forkless-slice-us` by more than the
  cost of one `rdbSaveKeyValuePair` (the deadline is checked between keys, so a single huge
  value is the worst case — note that as a known bound).
- **Slow-replica isolation (Stage 3 gate).** A replica that reads at 1 MB/s must not raise
  primary p99; assert the producer keeps serving other replicas and drops the slow one at the
  COB limit rather than stalling.

### 13.9 Build order

1. Land P1 (Dashtable-adoption Stages 1–2): version array + `snapshotPreimageHook` + the
   structural-change pause. **No fork-less code yet.**
2. Refactor `writeHeader`/`openDbWalk`/`writeFooter` out of `rdbSaveRio`/`rdbSaveDb` so fork
   and fork-less share the record writers — a pure, separately-testable refactor that leaves
   the forked path byte-identical.
3. `snapshotStart`/`Step`/`Finalize`/`Abort` for the **disk** target behind
   `rdb-forkless=yes`; §13.8 byte-equality + consistency gates.
4. Cooperative budget tuning + latency gate.
5. Diskless fan-out (§13.7) + slow-replica gate.
6. Flip default where supported; keep fork as fallback one release (§9 Stage 4).
