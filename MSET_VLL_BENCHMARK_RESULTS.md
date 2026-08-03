# Two-Phase Commit (VLL) Scaling Results

Throughput evaluated against 8 core instance running 8-Key `MSET` randomization.

| MSET Implementation | RPS | P50 Latency | P99 Latency |
| :--- | :--- | :--- | :--- |
| Valkey Native Barrier (pthreadcond) | 13,800 op/s | ~ | ~ |
| Valkey Native Barrier (schedyield) | 26,500 op/s | ~ | ~ |
| **Valkey Distributed Intent (VLL)** | **42,680 op/s** | **0.615 ms** | **0.999 ms** |

