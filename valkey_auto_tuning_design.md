# Valkey Auto CPU & IO Thread Affinity Design

## 1. Problem Statement
Currently, Valkey (and historically Redis) users must manually configure `io-threads` and CPU affinity settings (`server_cpulist`, `bio_cpulist`, etc.) to fully utilize multi-core machines. For managed, containerized, or dynamically scaled deployments where the VM size may change, maintaining static `valkey.conf` values is suboptimal and error-prone, resulting in either unutilized cores or heavy CPU contention.

## 2. Goal
Enable Valkey to automatically determine the optimal number of IO threads and configure CPU affinity for the main thread, IO threads, and background threads based on the total hardware cores available to the OS on startup, while being fully aware of **container cgroup quotas** and **hardware NUMA topology**.

**Assumption**: There is only 1 Valkey instance running on the VM/container context, meaning the process can safely monopolize all available CPU resources allocated to it.

## 3. Configuration Subsystem Changes
We propose extending the existing configuration parameters to accept a new `auto` value:

```conf
# Valkey.conf
io-threads auto
server_cpulist auto
bio_cpulist auto
aof_rewrite_cpulist auto
bgsave_cpulist auto
```
When `auto` is specified, Valkey will dynamically inspect the runtime environment to determine `N` (the effective core count) and allocate CPU topology accordingly.

## 4. Environment Discovery (Containers & NUMA)

### Determining N (Container Awareness)
Relying solely on `sysconf(_SC_NPROCESSORS_ONLN)` is dangerous in containerized environments (Kubernetes, Docker), as it returns the host machine's physical core count rather than the container's quota.
If `auto` is used, Valkey will use a fallback chain to determine effective **N**:
1.  **Cgroups v2**: Read `/sys/fs/cgroup/cpu.max`. If restricted, calculate `N = quota / period`.
2.  **Cgroups v1**: Read `/sys/fs/cgroup/cpu/cpu.cfs_quota_us` and `cpu.cfs_period_us`. Calculate `N = quota / period`.
3.  **Fallback**: If unrestricted or not in a container, use `sched_getaffinity` or `sysconf(_SC_NPROCESSORS_ONLN)`.

### Determining Processor Affinity (NUMA Awareness)
Allocating the Main thread on CPU 0 (Node 0) and an IO thread on CPU 16 (Node 1) would cause devastating cross-socket memory latency.
If `auto` is used, Valkey will parse NUMA topology:
1.  Read `/sys/devices/system/node/node*/cpulist`.
2.  Group available CPUs into their respective NUMA nodes.
3.  **Strict Rule**: The Main thread and *all* IO threads must be pinned to CPUs residing on the **same NUMA node**. If `N` spans multiple nodes, Valkey will assign the networking fast-path to Node 0, and push all background threads (`bio`, `bgsave`) to Node 1+.

## 5. Resource Allocation Heuristics
Let **N** be the effective number of CPUs calculated above. We divide the allocation strategy into three tiers based on size.

### Case 1: N <= 2 (Small VMs)
With only 1 or 2 vCPUs, lock contention and context-switching overhead for IO threads typically outweigh their benefits.
*   **`io-threads`**: `1` (IO threads disabled; main thread handles all I/O).
*   **`server_cpulist`**: Pinned to all cores `0-(N-1)`.
*   **`bio/bgsave_cpulist`**: Unpinned.
*   *Reasoning*: Background tasks (forks, lazy freeing) need CPU time. Strict isolation would artificially starve the main process or background jobs.

### Case 2: 2 < N <= 8 (Medium VMs)
This is the sweet spot for IO threads. We split the machine 50/50 between the fast-path (main thread + network) and the background-path (forks + lazy freeing).
*   **`io-threads`**: `N / 2` (e.g., 2 threads for 4-core, 4 threads for 8-core).
*   **`server_cpulist`**:
    *   Main Thread: CPU `0`
    *   IO Threads: CPUs `1` to `(N/2)` (Ensuring all belong to the same NUMA node).
*   **`bio/bgsave_cpulist`**: Remaining CPUs `(N/2 + 1)` up to `N-1`.
*   *Reasoning*: BGSAVE and lazy freeing can cause latency spikes if they contend with the main thread. Pinning them to the upper half of the CPU topology guarantees stable P99 latency for the data plane.

### Case 3: N > 8 (Large VMs)
Valkey's single main thread processing loop usually becomes the bottleneck beyond 8-12 IO threads. Assigning too many IO threads leads to diminishing returns and excessive locking.
*   **`io-threads`**: `(N * 3/4) - 1`, capped at a hard limit of `14`.
*   **`server_cpulist`**:
    *   Main Thread: 1 Dedicated CPU on Node 0.
    *   IO Threads: Remaining `14` CPUs on Node 0.
*   **`bio/bgsave_cpulist`**: CPUs assigned to Node 1 (or remaining unused CPUs on Node 0).
*   *Reasoning*: Cap the IO pool to prevent lock contention. Offloading BGSAVE to an entirely separate NUMA socket prevents LLC (Last Level Cache) thrashing on the socket where the Main thread lives.

## 6. Architectural Implementation

### Current Valkey Internals
Currently, Valkey manages threads and affinity in a few distinct phases:
1.  **Parsing (`config.c`)**: Directives like `io-threads` are parsed strictly as integers into `server.io_threads_num`. Affinity lists (`server_cpulist`, `bio_cpulist`, etc.) are parsed as strings and stored on the `redisServer` struct (e.g., `server.server_cpulist = "0-2"`).
2.  **Affinity Enforcement (`setcpuaffinity.c`)**: Functions like `setcpuaffinity()` take a string like `"0-2,4"`, parse it into a bitmask, and invoke `sched_setaffinity()` (on Linux) or `pthread_setaffinity_np()`.
3.  **Thread Instantiation**: 
    - The Main thread calls `setcpuaffinity(server.server_cpulist)` in `InitServerLast()` (`server.c`).
    - I/O threads spawn in `networking.c` under `initThreadedIO()` and immediately pin themselves.
    - Background threads spawn in `bio.c` under `bioInit()` and pin using `server.bio_cpulist`.

### New Implementation Architecture
To cleanly bolt `auto` onto the existing architecture without requiring a massive rewrite of `setcpuaffinity.c` or the threading framework, we will implement an "early config mutation" phase.

1.  **Config Extension (`config.c`)**:
    Modify the standard configuration validators to accept the sentinel keyword `"auto"`.
    - If `io-threads auto` is parsed, store `server.io_threads_num = -1` (as a signal flag).
    - If `server_cpulist auto` is parsed, strictly store `server.server_cpulist = "auto"`.

2.  **Dynamic Discovery Phase (`server.c`)**:
    Introduce a new function `evaluateAutoCpuSettings()`. We will inject a call to this function in `main()` *immediately after `loadServerConfig()`* but *before* any networking or BIO threads are initialized.
    
    Inside this function:
    - Invoke cgroup parsers (e.g. read `/sys/fs/cgroup/cpu.max`) and dynamically fall back to `sysconf(_SC_NPROCESSORS_ONLN)` to lock in the effective `N` limit.
    - Determine thread pinning masks based on the logic described in Section 5 (Resource Heuristics & NUMA mapping).

3.  **Config Mutation (Trick the downstream routines)**:
    Rather than changing how threads bind themselves, we will translate the evaluated hardware topology maps directly back into Valkey's standard string representations. We replace the `"auto"` placeholders in the struct:
    - `if (server.io_threads_num == -1) server.io_threads_num = computed_io_count;`
    - `if (server.server_cpulist && !strcmp(server.server_cpulist, "auto")) { sdsfree(server.server_cpulist); server.server_cpulist = sdsnew("0-3"); }`
    - `if (server.bio_cpulist && !strcmp(server.bio_cpulist, "auto")) { sdsfree(server.bio_cpulist); server.bio_cpulist = sdsnew("4-7"); }`
    
    By mutating the `redisServer` struct directly, we "trick" the downstream `InitServerLast()`, `initThreadedIO()`, and `bioInit()` logic. The existing code will run exactly as if the user had manually typed `"0-3"` and `"4-7"` into the config file.

4.  **Logging**:
    Since `auto` abstracts the exact topography away from the user, it is critical we log the result of the mutation right before the threads are launched to aid in debugging:
    ```
    * Auto CPU tuning enabled: Detected 16 effective cores (NUMA Nodes: 2).
    * Automatically configured io-threads=11.
    * Affinity - Main thread: CPU0(Node0), IO threads: CPU1-CPU11(Node0), Background: CPU12-CPU15(Node1).
    ```
