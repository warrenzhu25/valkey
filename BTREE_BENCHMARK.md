# Valkey Sorted Set Benchmark: B-Tree vs Skiplist

This document summarizes the performance and memory profile comparing the newly integrated **Dragonfly B-Tree** sorted set implementation against Valkey’s native **Skiplist** baseline.

### 1. Memory Profile (1 Million Elements)
**Methodology:** Flushed the database and pipelined 1,000,000 elements (`ZADD myzset {i} mem_{i}`) into a single sorted set, forcing the raw tree encoding (`zset-max-listpack-entries 0`). We measured the `used_memory` metrics from `INFO memory`.

| Implementation | Memory Used | Bytes |
| :--- | :--- | :--- |
| **Valkey Skiplist** | 68.50 MB | 71,853,928 |
| **Dragonfly B-Tree**| 45.36 MB | 47,592,888 |
| **Improvement** | **-33.7%** | **~24.2 MB saved / million** |

**Why?** Skiplists in Valkey use forward-pointer arrays attached to each node (proportional to randomized levels) plus backward pointers. The Dragonfly B-Tree nodes are strictly bounded, cache-aligned, bit-packed structures caching elements directly within contiguous pages, eliminating overwhelming pointer-chasing overhead.

### 2. Throughput Benchmarks 
**Methodology:** Ran `valkey-benchmark -n 250000 -r 250000` via localhost for heavy randomized updates (`ZADD`) and iterative pops (`ZPOPMIN`).

| Command | Skiplist RPS | B-Tree RPS | Diff | 
| :--- | :--- | :--- | :--- |
| **ZADD** | 45,471 req/sec | 46,485 req/sec | **+2.2%** |
| **ZPOPMIN** | 43,378 req/sec | 46,895 req/sec | **+8.1%** |

**Analysis:**
1. **ZADD**: B-Trees traditionally suffer from split-penalties on insertion. However, the density of Dragonfly's B-Tree node layout guarantees that search pathways fit cleanly into L1/L2 CPU caches before arriving at an insertion point, offsetting the computational penalty of binary-searching nodes and rendering `ZADD` effectively equal (or mildly faster).
2. **ZPOPMIN**: Fetching limits on a B-Tree executes predictably down the left-most leaf edge and clears packed contiguous elements, making sequential iterators natively faster than traversing fragmented heap pointers in a skiplist.
