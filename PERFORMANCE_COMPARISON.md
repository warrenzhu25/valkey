# Bare-Metal Performance Comparison: Valkey vs Dragonfly

This document summarizes the performance scaling characteristics between Valkey (using standard I/O Threads vs the experimental Shard Threads) and [Dragonfly](https://dragonflydb.io/) using native Linux x86_64 binaries.

## Testing Methodology
- **Environment**: Bare-metal Linux (no Docker/container networking overhead)
- **Tool**: `valkey-benchmark -c 100 -n 100000 --threads <N>`
- **Workload**: Standard `SET` (writes) and `GET` (reads)
- **Measurement**: Live peak Requests Per Second (RPS) prior to benchmark termination to accurately measure connection-saturated throughput.

## Benchmark Results

### 1 Thread
At 1 thread, standard execution falls back to the main thread.
| Engine Configuration | SET (RPS) | GET (RPS) |
|----------------------|-----------|-----------|
| **Dragonfly** | 44,695 | 44,063 |
| **Valkey** | 39,823 | 43,404 |

### 4 Threads
| Engine Configuration | SET (RPS) | GET (RPS) |
|----------------------|-----------|-----------|
| **Dragonfly** | 144,689 | 149,027 |
| **Valkey (Shard Threads)** | 139,666 | 139,135 |
| **Valkey (IO Threads)** | 137,621 | 152,665 |

### 8 Threads
| Engine Configuration | SET (RPS) | GET (RPS) |
|----------------------|-----------|-----------|
| **Dragonfly** | 207,315 | 225,242 |
| **Valkey (Shard Threads)** | 209,693 | 195,074 |
| **Valkey (IO Threads)** | 138,192 | 274,761 |

### 16 Threads
| Engine Configuration | SET (RPS) | GET (RPS) |
|----------------------|-----------|-----------|
| **Dragonfly** | 279,230 | 321,150 |
| **Valkey (Shard Threads)** | 198,597 | 246,386 |
| **Valkey (IO Threads)** | 126,474 | 258,019 |

---

## Architectural Analysis

### 1-8 Thread Footprints
Valkey's experimental **Shard Threads** dramatically improve write scalability. Standard Valkey `io-threads` bottlenecks on writes (plateauing around ~138k RPS at 8 threads) because execution relies exclusively on a single main thread. By parallelizing write execution across Shard Threads, Valkey competes closely with Dragonfly, even slightly edging out Dragonfly loops on 8-thread writes (209k vs 207k). 

Valkey's standard **IO Threads** excel effortlessly at read-heavy workloads up to 8 threads, parsing and returning network reads gracefully without execution mutation.

### The 16-Thread Ceiling
At extreme scaling dimensions (16+ cores), the architectures fundamentally diverge:
1. **Dragonfly Shared-Nothing Engine**: By isolating memory state entirely between threads (`proactor_threads`), Dragonfly scales linearly up to ~321k reads and ~279k writes. It avoids contention penalties dynamically.
2. **Valkey Contention**: Standard `io-threads` negatively scale backward at 16 threads because 15 network workers overwhelm the single execution spinlock. While our parallel `shard-threads` initially alleviate this, they drop slightly in performance at 16 threads (plateauing at ~198k writes) as shared resource locks and NUMA access times bottleneck state mutations. Future optimization for 16+ core arrays requires deeper lockless thread isolation.
