# Fork-less RDB — Complete Change Summary

This document summarizes every change that makes up the fork-less RDB feature on
the `forkless-rdb-stage1` branch: the base implementation (the snapshot primitive
and the cooperative save producer) and the performance/observability series
layered on top. For the architectural deep-dive of the snapshot primitive and
cut protocol, see [`forkless-rdb.md`](./forkless-rdb.md); this file is the
end-to-end map of *what changed and why*, with measured results.

## 1. What and why

`BGSAVE` and scheduled saves normally `fork()` a child that serializes a
copy-on-write (COW) snapshot of memory while the parent keeps serving. Fork has
two well-known costs:

- **Fork stall** — duplicating the process page tables freezes the event loop
  once, for tens to hundreds of ms on a large instance.
- **COW amplification** — pages dirtied during the save are copied, pushing RSS
  toward 2× under write-heavy saves.

Fork-less RDB produces the same RDB **without forking**. The serving thread
itself produces the file cooperatively across event-loop iterations, using a
per-hashtable snapshot primitive for a consistent point-in-time image: writes
arriving during the save capture their at-cut value before mutating, and a
bounded, paced walk serializes the rest. Enabled with `rdb-forkless yes`.

## 2. Architecture at a glance

```text
   command path                       beforeSleep (each tick, paced)
   (a write)                          rdbForklessSaveStep()
       |                                   |
       v  serialize-before-mutate hook     v  duty-budgeted adaptive walk
   +---------------------------------------------------------------+
   |  hashtable snapshot primitive (per table)                     |
   |  - per-top-level-bucket version array vs. a "cut"             |
   |  - whoever reaches an uncaptured bucket first serializes it   |
   +---------------------------------------------------------------+
       |  forklessSnapshotCB serializes records into an in-memory buffer
       v
   sds chunks --(bounded queue)--> writer thread --> temp file --> rename()
                                   (all write()/fsync()/reclaim here)
```

Two layers: the generic snapshot primitive in `hashtable.c`, and the save
producer in `rdb.c` that consumes it. Both the write-path hook and the
`beforeSleep` walk call the same callback; the RDB byte format is unchanged and
the output is load-equal to a forked RDB (identical keyspace and digest).

## 3. Change set

### 3a. Base implementation

| Commit | Change |
|---|---|
| `0f53b39d1` | `hashtable`: fork-less snapshot primitive (per-bucket version/cut, serialize-before-mutate hook) |
| `d43aa119a` | `hashtable`/`db`: close snapshot seam gaps (in-place value mutations, defrag capture-before-relocate) |
| `532a0b048` | `rdb`: extract shared stage helpers from `rdbSaveRio`/`rdbSaveDb` (header/select/footer records) |
| `b491b9123` | `rdb`: fork-less disk BGSAVE producer (single-DB) |
| `a886a5b8a` | tests: `integration/forkless-rdb.tcl` |
| `a615b7d21` | `rdb`: time-based budget + observability |
| `39d2585f3` | `hashtable`: snapshot active rehash tables |
| `ab75ce7d7` | `kvstore`: retain tables during snapshots |
| `bc257e62d` | `rdb`: multi-DB forkless saves |
| `38782c560` | `rdb`: emit cluster slot info in forkless saves |
| `460da7cc0` | `rdb`: enable forkless disk saves by default |

Result of this phase: a correct, cooperative, single-threaded save covering
single-DB, multi-DB, and cluster. But it forced a blunt tradeoff — progress was
gated by event-loop traffic, and the per-tick serialize slice both stalled the
loop and pushed fsync onto it.

### 3b. Performance & observability series

Each item below was measured before/after; see §5 for numbers.

| # | Commit | Change | Problem it fixes |
|---|---|---|---|
| 1 | `79db9a0de` | Pace progress with a 1 ms timer event | Idle saves crawled (~14 min) because progress only advanced on event-loop wakeups (~10 Hz idle). |
| 2 | `6dbe287e9` | Budget each tick by a **duty-cycle** target (`rdb-forkless-duty-pct`) | A fixed slice over-spent under load (fat p99) and under-spent when idle. Duty bounds latency as a *rate*. |
| 2b | `6377acd72` | Size the walk batch **adaptively** to the remaining budget | A fixed 64-bucket batch (~1 ms) was coarser than the budget, so the duty knob barely bit. |
| 3 | `ed50b65fe` | Offload incremental fsync to a **bio** thread | The ~12 ms busy-load p99 was entirely inline fsync + page-cache reclaim on the serving thread. |
| 3b | `8ac3b242f` | Run **all** save I/O on a dedicated **writer thread** | fsync-on-bio still left `write()`/`fflush` on the serving thread contending with the background fsync on the same inode. |
| 5 | `2e0fcf189` | Report progress % and elapsed time in `INFO` | A paced save can run for tens of seconds with no signal it was progressing vs stuck. |

(#4, deferring the pre-image hook into `beforeSleep`, was implemented, measured,
and **reverted** — see §6.)

## 4. Configuration and observability

### Config (`config.c`)

| Option | Type | Default | Meaning |
|---|---|---|---|
| `rdb-forkless` | bool | `yes` | Route eligible disk saves to the fork-less producer. |
| `rdb-forkless-duty-pct` | int 1–100 | `25` | Target share of wall time the save may consume on the serving thread. Bounds added latency as a rate; also sets how fast an idle save completes. |
| `rdb-forkless-slice-us` | int 50–100000 | `500` | Hard per-tick cap on serialize time, so no single tick stalls the loop. |

### `INFO persistence`

| Field | Meaning |
|---|---|
| `rdb_bgsave_in_progress` | `1` during a fork-less save too (not just a forked child). |
| `rdb_current_bgsave_time_sec` | Elapsed seconds of the in-flight save (fork-less included), else `-1`. |
| `rdb_forkless_progress_percent` | Bucket-based walk completion `0..100`, or `-1` when no fork-less save runs. |
| `rdb_forkless_preimage_bytes` | Bytes the mutation hook serialized inline for the last save — the write-amplification cost to watch under load (the fork-less analog of COW pressure). |

## 5. Performance results

Test bed: 10M keys × 200 B (2.38 GB dataset, ~388 MB RDB), 10-core box.
"Busy load" = 50 connections issuing random `SET`s during the save; latency from
a 4 ms `PING` probe on a separate connection.

### Fork vs. fork-less (final) under write load

| Metric | Fork (baseline) | Fork-less (final) |
|---|---|---|
| Event-loop stall | **68–73 ms** (the fork) | **0** |
| p50 latency | 0.17 ms | 0.21 ms |
| p99 latency | 0.71 ms | **0.6–0.9 ms** |
| Max latency | **47 ms** | **1.9–2.9 ms** |
| Peak extra memory | **+311 MB** (COW) | +108 MB (pre-image) |
| Save wall time | 6 s | 8–9 s |
| RDB output | — | load-equal, byte-size identical |

Fork-less now **matches fork's typical latency and beats its worst case ~16×**,
with no fork stall and ~3× less peak memory.

### Busy-load p99 across the series (duty = 25%)

| Stage | p99 | max | save |
|---|---|---|---|
| #2b — fsync inline | 12.5 ms | 14.8 ms | 11 s |
| #3 — fsync on bio thread | 5.2–6.9 ms | 8–25 ms | 12 s |
| #3b — full I/O offload | **0.6–0.9 ms** | **1.9–2.9 ms** | 8–9 s |
| *diagnostic: fsync fully off* | *1.0 ms* | *7.9 ms* | *10 s* |

The diagnostic isolated the cost: with incremental fsync disabled, p99 collapsed
from ~13 ms to ~1 ms — proving the stall was I/O, not CPU serialization, and
motivating the writer thread.

### Idle-save duration (388 MB, no client traffic)

| Stage | Idle save |
|---|---|
| Original (beforeSleep-only) | **~14 min** (~0.5% duty at 10 Hz) |
| #1 — timer pacer | **13 s** |
| #3b — default duty = 25% | ~57 s (gentle by design; raise duty for faster) |

Duty knob scaling (idle, after #2b): 50% → 13 s, 25% → 36–74 s, 10% → 239 s.

## 6. Key design decisions and lessons

- **Progress must not depend on traffic.** The pacer (#1) decouples save
  progress from client I/O by capping the event loop's poll timeout; without it
  an idle loop wakes only ~`hz` times/sec and a save crawls.
- **Budget as a rate, not a fixed slice (#2).** A fixed per-tick slice means
  different things under different traffic. A duty-cycle target bounds latency
  impact proportionally and adapts to tick frequency.
- **Granularity must be finer than the budget (#2b).** A budget can only bite if
  the unit of work (walk batch) is smaller than it; the batch is sized from the
  remaining budget and a running per-bucket cost estimate.
- **In a single-threaded loop, relocating work does not reduce latency (#4,
  reverted).** Deferring the pre-image hook's serialization from the write path
  into `beforeSleep` changed nothing — both occupy the same thread and stall all
  clients equally. The measured p99 was unchanged (and the tail slightly worse),
  so the change was reverted. This sharpened the real fix:
- **The only way to remove I/O cost from the serving thread is another thread
  (#3 → #3b).** Fork wins p99 because the child does I/O on its own CPU. The
  writer thread reproduces that: the serving thread serializes into memory and
  issues zero file syscalls; a dedicated thread owns the fd and does all
  `write()`/`fsync()`/reclaim, fed through a bounded (32 MB), back-pressured
  queue. The checksum stays on the serving thread, so the RDB is byte-identical.

## 7. Correctness

Validated across the series:

- `integration/forkless-rdb.tcl` (6 tests): quiesced load-equality, point-in-time
  under concurrent overwrites, post-cut adds excluded, multi-DB.
- Stress: 5 rounds of `BGSAVE` under 50-connection write load — no crash,
  deadlock, or abort; RDB valid and growing.
- Fresh-load digest equality: a server loading the fork-less RDB is
  digest-identical to the source (single-DB and multi-DB across DB 0/1/5).
- Writer-thread teardown/error paths join before the fd is closed, so no bio/
  writer operation ever touches a closed or reused fd.

## 8. Known limitations and future work

- **Idle saves stay conservative** at default duty (25%) — ~57 s for 388 MB,
  because the controller is gentle even when there are no clients to protect.
  A refinement would ramp duty toward 100% when the loop is genuinely idle.
- **Snapshot bookkeeping is 4 bytes/bucket** (`uint32_t snapshot_versions`),
  though within one snapshot it is effectively boolean; a bitmap would cut it
  ~32× (≈8 MB → 256 KB per 10M-key table).
- **The finalize join can briefly block the event loop** while the writer drains
  the backlog + final fsync; draining cooperatively before joining would remove
  the blip.
- **Ineligible saves fail rather than fork.** When a slot import is in progress
  the snapshot cannot cover the imported data, and the save currently returns an
  error instead of falling back to fork (a deliberate choice to surface, not
  mask, that fork-less did not run).
- **Diskless replication and AOF rewrite still fork** — out of scope here; the
  snapshot primitive could in principle drive both.

## 9. Relevant code

- `src/hashtable.c` / `src/hashtable.h` — snapshot primitive (`hashtableSnapshotStart`,
  `hashtableSnapshotWalkFrom`, `hashtableSnapshotCaptureKey`, `hashtableSnapshotEnd`).
- `src/rdb.c` — producer: `rdbSaveForklessStart`, `rdbForklessSaveStep` (pacer +
  duty budget + adaptive batch), `forklessSnapshotCB`, the writer thread
  (`forklessWriter`, `forklessEnqueueChunk`, `forklessFlushChunk`, `forklessIOJoin`),
  `rdbForklessFinalize`, `rdbForklessCleanup`, `rdbForklessProgressPercent`.
- `src/server.c` — `beforeSleep` drives the step; `INFO persistence` fields.
- `src/config.c` — `rdb-forkless`, `rdb-forkless-duty-pct`, `rdb-forkless-slice-us`.
- `tests/integration/forkless-rdb.tcl` — correctness coverage.
