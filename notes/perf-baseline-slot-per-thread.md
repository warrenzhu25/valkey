# Slot-per-thread performance baseline (jemalloc)

*Measured 2026-07-31 on macOS (Apple silicon, 10 cores), server and load generator on
the same box. **Indicative, not decision-grade** — `tools/profiling/stage0.sh` requires
Linux + performance governor + pinning. Values are steady-state rps (mid-run "overall"),
`valkey-benchmark -d 16 -r 1000000 -c 50 --threads 4`.*

**Build the allocator that matters.** Valkey defaults to jemalloc on Linux but **libc on
macOS**. All numbers below are with jemalloc (`make USE_JEMALLOC=yes`; delete
`src/.make-settings` when switching allocators). The bundled jemalloc builds fine on this
Mac. See "Process lessons" for why this is not optional.

## Current baseline — HEAD `c9ed66e6`, jemalloc-5.3.0

| workload | st=1 | st=4 | st4/st1 |
|---|---:|---:|---:|
| GET P1  | 241,120   | 201,265   | 0.83x |
| SET P1  | 249,329   | 201,870   | 0.81x |
| GET P16 | 3,180,353 | 2,008,713 | 0.63x |
| SET P16 | 2,115,635 | 1,625,145 | 0.77x |

Slot-per-thread (`shard-threads 4`) still trails a single thread on uniform-random keys:
~0.82x non-pipelined, ~0.63-0.77x pipelined. The remaining gap is the per-command
cross-shard hand-off (round-trip wake latency at P1; dispatch + continuation at P16), not
the allocator. See [[analysis-dragonfly-cross-thread]] for how Dragonfly makes that cheap
(fibers, inline-local, pipeline squashing).

## Net effect of this session's work (st=4, jemalloc)

Baseline `7a2e6f53b` (before the session) vs HEAD `c9ed66e6`:

| workload | before | after | delta |
|---|---:|---:|---:|
| GET P1  | 201,873 | 201,892   | +0.0% |
| SET P1  | 199,969 | 200,885   | +0.5% |
| GET P16 | 586,474 | 2,022,575 | **+245%** |
| SET P16 | 570,404 | 1,621,819 | **+184%** |

On pipelined workloads the session took st=4 from **0.18x** single-thread to **0.63x** — a
~3.4x throughput gain. It did nothing for non-pipelined P1, which is purely round-trip
latency bound.

Commits (all on `conn-el`): wake coalescing (`fceb79f0`), argv move (`52f065a5`),
clientsCron UAF + barrier back-to-back (`691d5a48`), mpsc acquire / TSAN de-noise
(`6472311d`), keyspace barrier gaps + exclusion-race (`fb776dd9`), LRU/time-cache atomics
(`da9ccf78`), reply-by-reference (`60883176`), **pinned-pointer completion — drops the
per-completion `clients_index_mutex`** (`32a485fd`), **gate the FAST-expire barrier so it
isn't taken every iteration** (`91f31653`).

**Why the same commits looked like +82% on libc but +245% on jemalloc:** on libc the
cross-thread allocator lock was the dominant ceiling, masking the mutex/barrier/hand-off
costs. Remove the allocator lock (jemalloc's per-thread tcache) and the session's fixes
compound instead of being hidden.

## Process lessons (do not relearn these)

1. **Always benchmark slot-per-thread with jemalloc.** The macOS libc default silently
   ~halved pipelined multi-thread throughput via `_os_unfair_lock` on libmalloc's zone.
   Profiling on libc showed ~40% of worker time in malloc/free + that lock and nearly sent
   us building a per-command object pool -- which Dragonfly doesn't even do (it relies on
   mimalloc's per-thread heaps). The "allocation churn bottleneck" was a test-environment
   artifact; jemalloc (Linux default) makes those allocs cheap and lock-free. Config, not
   code.

2. **Profile before optimizing.** A `sample` of a saturating st=4 P16 run caught two
   things a design-first approach would have missed: (a) a self-inflicted regression -- the
   FAST-expire barrier from `fb776dd9` was being taken *every* event-loop iteration for a
   cycle that does nothing (main ~23% in `shardBarrierBeginExcluding`, workers ~30% parked);
   (b) the allocator artifact above. Fixing (a) alone was +22% P16. The Dragonfly analysis
   had pointed at squashing; the profile said the real wins were elsewhere first.

## What's left (allocator-independent)

- **Pipeline squashing** (P16): batch a connection's same-owner REMOTE commands into one
  cross-shard hop, à la Dragonfly's `MultiCommandSquasher`. Attacks dispatch overhead
  directly.
- **Lighter continuation / lower round-trip latency** (P1): `blockClient`/`unblockClient`
  per REMOTE command is the retrofit cost Dragonfly's fiber `run_barrier_.Wait()` avoids.
  P1 is where slot-per-thread is furthest from paying off and where nothing so far has moved
  the needle.
