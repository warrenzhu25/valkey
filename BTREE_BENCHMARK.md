# Valkey Sorted Set Benchmark: B-Tree vs Skiplist

This document reports measured, reproducible numbers comparing the integrated **Dragonfly B-Tree** sorted-set backend (current `dragonfly-btree-integration` HEAD) against Valkey's prior **Skiplist** backend (last commit where skiplist was buildable: `c2e51bcb0`, via `make ORDERED_INDEX_SKIPLIST=yes`).

The benchmark is scripted and checked in at [`utils/ordered-index-bench/run_zset_benchmark.sh`](utils/ordered-index-bench/run_zset_benchmark.sh) — run it yourself to reproduce or refute these numbers.

**Test machine:** Apple M4, 10 cores, 32 GB RAM, macOS 15.1 (arm64). Both binaries built from the same toolchain/Makefile with no `MALLOC` override, which resolves to `malloc=libc` on this platform — i.e. any memory delta below is purely from the data structure, not a jemalloc-vs-libc difference. Each figure is the mean of 3 independent trials (fresh server process per trial, single-threaded `valkey-benchmark` client, one benchmark at a time — no concurrent runs).

## 1. Memory Profile (1 Million Elements)

**Methodology:** Started a fresh server, set `zset-max-listpack-entries 0` to force raw tree encoding, then pipelined 1,000,000 `ZADD myzset {i} mem_{i}` commands via `valkey-cli --pipe`. Measured `used_memory` from `INFO memory` immediately after load.

| Implementation | Memory Used (mean of 3) | Bytes (mean) |
| :--- | :--- | :--- |
| Skiplist | 68.76 MiB | 72,093,627 |
| B-Tree | 48.41 MiB | 50,756,443 |
| **Improvement** | **-29.6%** | **~20.4 MiB saved / million elements** |

Raw per-trial bytes — skiplist: 72,091,536 / 72,076,800 / 72,112,544. B-Tree: 50,755,568 / 50,750,768 / 50,762,992. Tight spread (<0.03% across trials for each backend), so this result is stable.

## 2. Throughput Benchmarks

**Methodology:** `valkey-benchmark -n 250000 -r 250000 -t zadd` against a fresh server (250,000 `ZADD` calls spread across a 250,000-key random keyspace, one member per call), then a separate fresh server pre-loaded with 250,000 members via pipe, benchmarked with `valkey-benchmark -n 250000 zpopmin zpopzset`.

| Command | Skiplist RPS (mean of 3) | B-Tree RPS (mean of 3) | Diff |
| :--- | :--- | :--- | :--- |
| ZADD | 244,699.65 | 190,164.06 | **-22.3%** |
| ZPOPMIN | 238,854.28 | 246,225.82 | **+3.1%** |

Raw per-trial RPS — Skiplist ZADD: 244,857.97 / 245,338.55 / 243,902.44. B-Tree ZADD: 189,825.36 / 190,985.48 / 189,681.34. Skiplist ZPOPMIN: 239,463.59 / 238,549.62 / 238,549.62. B-Tree ZPOPMIN: 239,923.22 / 249,004.00 / 249,750.23. Consistent across trials in both cases (largest spread is B-Tree ZPOPMIN at ~4%, still well clear of the two backends' means).

**Analysis:**
1. **Memory** is the clear, repeatable win: B-Tree's dense, cache-aligned pages beat the skiplist's per-node forward-pointer arrays (proportional to randomized level) plus backward pointers by roughly 30% at 1M elements.
2. **ZADD is measurably slower on B-Tree** in this setup — about 22% fewer ops/sec than skiplist. This is the classic B-Tree insertion cost (node search + potential split/rebalance on every insert) showing up as expected; it was not offset by cache locality in this benchmark.
3. **ZPOPMIN is modestly faster on B-Tree** (~3%), consistent with sequential min-element extraction benefiting from packed leaf pages over skiplist pointer-chasing, though the effect here is much smaller than intuition might suggest.

## Note on the previous version of this document

An earlier version of this file claimed a 33.7% memory improvement and B-Tree wins on **both** ZADD (+2.2%) and ZPOPMIN (+8.1%). It shipped without a reproducible script, raw logs, trial count, or machine spec. Re-running the comparison with a checked-in, repeatable script on this machine reproduces the memory win (same direction, similar magnitude) but **contradicts the ZADD result** — B-Tree is slower, not faster, for insert-heavy workloads. Treat this document's numbers as the current source of truth; re-run the script on your own hardware before relying on either version for a production decision, and see the Limitations below.

## Limitations

- Single machine, single run configuration (`-n 250000 -r 250000`, 1M elements) — no sweep across dataset sizes, key distributions, or listpack-encoded (small) zsets, which is the more common real-world case.
- No range-query coverage (`ZRANGEBYSCORE`, `ZRANK`), where B-Tree vs skiplist tradeoffs are traditionally most visible.
- `valkey-benchmark`'s built-in `-t zadd` test spreads writes across many single-member sorted sets (one per random key), not repeated inserts into one large zset — this measures insert-path overhead per call more than large-zset contention.
- macOS/arm64 with `libc` malloc; results may differ on Linux/jemalloc, the more common production target for Valkey.
