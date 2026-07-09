# Valkey Global Auto-Tuning Design

## 1. Problem Statement & Vision
The success of dynamic CPU & IO threading tuning proves that requiring users to manually tune `valkey.conf` for their exact deployment restricts performance and scalability. In serverless, containerized, or dynamically shifting workloads, the environment and data access patterns fluctuate constantly. 

The vision is to expand the `"auto"` configuration sentinel to a broad array of Valkey settings. By allowing Valkey to introspect the operating system and its own runtime telemetry, it can self-optimize to extract maximum performance without human intervention.

---

## 2. Config Identification & Categorization
Configs that are ripe for auto-tuning fall into two distinct phases with different data sources:

**Phase A: Environment/Startup Tuning (Introspecting the OS)**
These configs depend on hardware and OS limits. They should be evaluated at boot or when configuration reloads occur.
*   `maxmemory`: Dependent on physical RAM or container quotas.
*   `maxclients`: Dependent on system file descriptor limits.
*   `tcp-backlog`: Dependent on OS socket limits.

**Phase B: Runtime/Telemetry Tuning (Introspecting Data & Metrics)**
These configs depend on data distribution, workload patterns, and live metrics. They should be evaluated periodically during the server cron cycle (similar to how `dynamic-hz` operates).
*   `active-expire-effort`: Dependent on eviction pressure and memory headroom.
*   `client-output-buffer-limit`: Dependent on total memory headroom and client count.
*   `aof-rewrite-percentage`: Dependent on write throughput and disk IO capability.
*   `hash-max-listpack-entries` / `zset-max-listpack-entries`: Dependent on average value size.

---

## 3. Auto-Tuning Strategies per Config

### 3.1. Memory Bounds (`maxmemory`)
*   **Current State:** Defaults to `0` (unlimited), which causes the OS OOM killer to terminate Valkey in containers if it exceeds its quota. Users must manually set exact byte limits.
*   **Auto Strategy:** 
    *   If set to `auto`, Valkey will check cgroup memory limits (e.g., `/sys/fs/cgroup/memory.max` in v2 or `memory.limit_in_bytes` in v1).
    *   If no cgroup limit exists, fall back to physical RAM (`sysconf(_SC_PHYS_PAGES) * sysconf(_SC_PAGE_SIZE)`).
    *   **Rule:** Set `maxmemory` to `80%` of the detected limit. The remaining 20% serves as a safety buffer for allocator fragmentation, the OS page cache, and child processes (BGSAVE).

### 3.2. Network & Connections (`maxclients` & `tcp-backlog`)
*   **Current State:** `maxclients` defaults to 10,000. `tcp-backlog` defaults to 511. If the OS `ulimit -n` or `somaxconn` is lower, Valkey downgrades it quietly.
*   **Auto Strategy:** 
    *   `maxclients auto`: Query `getrlimit(RLIMIT_NOFILE)`. Reserve 128 FDs for internal use (listening sockets, AOF, modules), and set `maxclients = Limit - 128`. (If the limit is infinite, default to 65,000).
    *   `tcp-backlog auto`: Read `/proc/sys/net/core/somaxconn`. Set `tcp-backlog` equal to the OS maximum to maximize connection burst tolerance.

### 3.3. Expiration Pacing (`active-expire-effort`)
*   **Current State:** An integer 1-10 dictating how aggressively Valkey spends CPU cycles evicting expired keys. High values waste CPU; low values cause memory bloat.
*   **Auto Strategy:**
    *   Introduce `active-expire-effort auto`.
    *   During `serverCron`, calculate:
        1. **Expiration Density**: Ratio of keys with a TTL vs keys without a TTL.
        2. **Memory Headroom**: Distance from current memory usage to `maxmemory`.
    *   **Rule:** If memory usage surpasses 90% of `maxmemory` *and* Expiration Density is high, smoothly scale effort towards `8`. If memory usage is below 50%, relax effort to `1` to prioritize raw throughput for the main thread.

### 3.4. Buffer Scaling (`client-output-buffer-limit`)
*   **Current State:** Static byte hard/soft limits per client type (normal, pubsub, replica). If a spike occurs, clients get disconnected ruthlessly even if the server has gigabytes of free RAM.
*   **Auto Strategy:**
    *   Use a dynamic algorithm. If `client-output-buffer-limit pubsub auto` is set:
    *   Look at total free RAM. Allow total pubsub buffers to collectively grow up to 10% of `maxmemory`.
    *   Individual client limits dynamically adjust based on `(10% maxmemory) / active_pubsub_clients`. This prevents OOMs during mass-broadcasts while allowing massive buffers when only a single client is lagging.

### 3.5. Persistence (`aof-rewrite-percentage`)
*   **Current State:** Defaults to 100% (rewrite when AOF doubles in size). Can lead to massive disk IO spikes.
*   **Auto Strategy:**
    *   Evaluate the write capability of the underlying disk (by timing fsync latency in the background thread).
    *   If `aof-rewrite-percentage auto` is enabled, and background `fsync` latency is detected to be extremely low (indicating NVMe/SSD), reduce the percentage to ~50% to trigger smaller, faster rewrites more frequently. If `fsync` implies rotational rust latency, raise to 150% to batch writes further apart.

---

## 4. Implementation Approach

Expanding the auto-tuning engine requires standardizing the parsing of `"auto"` and separating OS-level evaluation from runtime evaluation.

### Step 1: Sentinel Standardization
Modify `config.c` so that standard integer, memory, and string parsers can universally accept `"auto"`. If `"auto"` is provided, flip a boolean flag on the config definition (e.g., `server.auto_maxmemory = 1`) and initialize the actual value to a fallback.

### Step 2: Boot-Time (OS) Evaluator 
Create a centralized `evaluateOSConfigurations()` function in `server.c`. 
This is called in `main()` during server startup. It assesses cgroups, NUMA, rlimits, and sysfs. It then mutates the `server.` configurations for CPU (`io-threads`), Memory (`maxmemory`), and Network (`maxclients`, `tcp-backlog`).

### Step 3: Runtime Config Governor
Create `evaluateRuntimeConfigurations(struct redisServer *server)` and hook it into `serverCron()` (e.g., executing every 1000ms / 1hz).
This function looks at:
- `server.stat_expiredkeys` (velocity)
- `server.stat_fsync_latency` (new metric)
- Memory usage (`zmalloc_used_memory()`)
It uses these metrics to mutate `server.active_expire_effort` and buffer limits dynamically without requiring a restart, continuously sailing the ship based on varying weather conditions.

### Step 4: Configuration Observability
Because configs can morph dynamically at runtime, the `CONFIG GET` command must be modified.
If a user runs `CONFIG GET maxmemory`, Valkey should return the *currently evaluated* limit, but perhaps add a new command `CONFIG GET --raw` to show if it is fundamentally set to `"auto"`. Alternatively, log all dynamic shifts (e.g., "Auto-tuner shifted active-expire-effort from 1 to 4") to the server log at `LL_NOTICE` so observability platforms can track the engine's decisions.
