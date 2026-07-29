# Design Document: Fork-less RDB

## Overview

`BGSAVE` and automatic saves normally `fork()` a child that inherits a
copy-on-write (COW) snapshot of memory and serializes it while the parent keeps
serving. Fork has two well-known costs: a page-table copy that stalls the event
loop (tens to hundreds of ms on a large instance), and COW amplification that
can push memory toward 2× RSS during a write-heavy save.

Fork-less RDB produces the same RDB **without forking**. The serving thread
itself produces the file cooperatively across event-loop iterations, using a
per-hashtable snapshot primitive to obtain a consistent point-in-time image:
writes that arrive during the save capture their at-cut value before mutating,
and a bounded walk serializes the rest.

It is opt-in (`rdb-forkless`, default off) and, when a save is not eligible,
falls back to fork. When disabled, the fork path is unchanged.

## Architecture

Two layers: a generic snapshot primitive in `hashtable.c`, and a save producer
in `rdb.c` that consumes it.

```text
   command path                          beforeSleep (each tick)
   (a write)                             rdbForklessSaveStep()
       |                                     |
       v  serialize-before-mutate hook       v  bounded cooperative walk
   +---------------------------------------------------------------+
   |  hashtable snapshot primitive (per table)                     |
   |  - per-bucket version array vs. a "cut"                       |
   |  - whoever reaches an uncaptured bucket first serializes it   |
   +---------------------------------------------------------------+
       |  hashtableSnapshotCB (in rdb.c)
       v
   rdbSaveKeyValuePair  ->  rio  ->  temp file  ->  rename()
```

Both the hook (on the write path) and the walk (from `beforeSleep`) call the
same callback, which serializes a bucket's live entries with the existing
`rdbSaveKeyValuePair`. The RDB byte format is unchanged.

## Snapshot primitive (`src/hashtable.c`)

While a snapshot is active on a table, structural change (rehash/resize) is
frozen and every keyspace mutation is routed through a hook, so a cooperative
walk plus the hook together emit each entry present at the cut exactly once.

### State (in `struct hashtable`)

- `snapshot_versions`: a per-top-level-bucket `uint32` array, allocated only
  while a snapshot runs (steady-state cost: one predictable branch per
  mutation).
- `snapshot_cut`: buckets whose version is `<= cut` are not yet captured.
- `snapshot_active`, `snapshot_relaxed`, and a serialize callback + privdata.

### Cut protocol (conservative variant)

```text
start:   cut = ++epoch; version array = 0; snapshot_active = 1
walk:    if (ver[b] <= cut) { serialize bucket b; ver[b] = cut + 1; }
hook:    before a write touches bucket b, same check runs first (pre-image)
```

Whoever reaches an uncaptured bucket first serializes its full live entry set
and lifts it above the cut; the other side's `<= cut` test then skips it.
Serialization is **whole-bucket-chain** granularity, keyed on the top-level
bucket index, so a concurrent mutation can never interleave a half-serialized
bucket. In relaxed mode the hook does nothing (the walk serializes whatever is
present when it arrives); conservative is the default and the only mode used for
RDB.

### Structural-change freeze

`resize()` returns early while `snapshot_active`, and `hashtableSnapshotStart`
drains any in-progress rehash so the version array (indexed into `tables[0]`)
covers all live data. New inserts still land (as chained entries) and take
versions `> cut`.

### Mutation surfaces covered

- `insert` / `hashtableFindPositionForInsert` (add, two-phase insert)
- `hashtablePop` / `hashtableTwoPhasePopFindRef` (delete, two-phase delete)
- `hashtableSnapshotCaptureKey` — for value mutations that bypass the table
  mutators: an in-place value swap (`dbSetValue`) and in-place value-content
  changes reached via a value ref (`APPEND`/`HSET`/... through `lookupKeyWrite`).
  Wired at those `db.c` write choke points.
- `hashtableReplaceReallocatedEntry` and defrag scans capture-before-relocate,
  so active-defrag relocating an entry mid-snapshot cannot lose its at-cut value.

### API

```c
void   hashtableSnapshotStart(hashtable *ht, hashtableSnapshotCB cb, void *pd, int relaxed);
size_t hashtableSnapshotWalkFrom(hashtable *ht, size_t start, size_t max_buckets); /* resumable */
size_t hashtableSnapshotBuckets(hashtable *ht);
void   hashtableSnapshotEnd(hashtable *ht);
bool   hashtableSnapshotActive(hashtable *ht);
void   hashtableSnapshotCaptureKey(hashtable *ht, const void *key);
```

## Save producer (`src/rdb.c`)

`rdbSaveBackground` routes an eligible save to the producer instead of forking;
otherwise it forks as before.

### Eligibility (`forklessSaveEligible`)

`rdb-forkless` on, non-cluster, and only DB 0 holds data. Anything else falls
back to fork. (Multi-DB and cluster need per-bucket staging to keep the
sequential, SELECT-delimited stream correct across DB boundaries — see
*Consistency*.)

### Lifecycle

```text
rdbSaveForklessStart   open temp file; write header; SELECT 0 + RESIZE;
                       hashtableSnapshotStart on DB 0's table
      |
      v  (each beforeSleep tick)
rdbForklessSaveStep    serialize 64-bucket chunks until the per-tick time budget
                       is spent (rdb-forkless-slice-us), then yield; repeat
      |
      v  (walk cursor reaches the end)
rdbForklessFinalize    hashtableSnapshotEnd; write footer; fsync; rename;
                       update lastsave / status; updateReplicasWaitingBgsave
```

`rdbForklessAbort` tears the job down on any I/O error. It reuses the Stage-2
record helpers `rdbSaveRioWriteHeader`, `rdbSaveDbSelectAndResize`, and
`rdbSaveRioWriteFooter`, which are shared with the forked path so both emit
identical records.

`serverCron` treats a fork-less save like an in-flight child for scheduling
(skips starting another save/rewrite) without reaping a nonexistent child.
`rdbForklessInProgress()` is the predicate.

## Consistency

Conservative mode gives a true point-in-time image as of the cut. A write during
the save either finds its bucket already captured (its at-cut value is already
in the file) or triggers the hook to serialize the at-cut value before mutating.
New keys added after the cut land in captured buckets and are excluded. For
replication, writes after the cut accumulate in the backlog and are sent after
the RDB — the existing RDB-plus-backlog contract.

The output is **load-equal** to a forked RDB (identical keyspace and digest) but
**not byte-identical**: a bucket captured by the hook is emitted when the write
happens rather than in walk order, so key order within the DB differs. RDB
loading is order-insensitive, so this is correct. Byte-identity, and multi-DB /
cluster support, require per-bucket staging (emit captured buckets in index
order, drain per DB under one SELECT) and are not implemented.

## Interactions

- **Replication**: fork-less handles the disk BGSAVE path. Diskless replication
  (RDB to replica sockets) is unchanged and always forks.
- **AOF**: unchanged. AOF rewrite still forks.
- **Cluster**: ineligible; falls back to fork.
- **maxmemory / eviction**: evictions during the save are ordinary deletes that
  hit the hook, so an evicted key is captured at its at-cut value first.

## Configuration and observability

- `rdb-forkless` (bool, default `no`): route eligible disk saves to the producer.
- `rdb-forkless-slice-us` (int, default 500): per-tick serialization budget.
- `INFO persistence`:
  - `rdb_bgsave_in_progress` is also true during a fork-less save.
  - `rdb_forkless_preimage_bytes`: bytes the mutation hook serialized inline for
    the last save — the un-budgeted per-write cost to watch under write load
    (the fork-less analog of COW pressure). A larger slice finishes the walk
    sooner, so fewer concurrent writes are captured.

## Relevant code

- `src/hashtable.c` / `src/hashtable.h` — snapshot primitive.
- `src/rdb.c` — producer (`rdbSaveForklessStart`, `rdbForklessSaveStep`,
  `rdbForklessFinalize`) and the Stage-2 record helpers.
- `src/db.c` — `hashtableSnapshotCaptureKey` call sites (`lookupKey` under
  `LOOKUP_WRITE`, `dbSetValue`).
- `src/server.c` — `beforeSleep` drives the step; `INFO` fields.
- `src/config.c` — `rdb-forkless`, `rdb-forkless-slice-us`.
- `tests/integration/forkless-rdb.tcl` — quiesced load-equality, point-in-time
  under concurrent overwrites, post-cut adds excluded, multi-DB fork fallback.
