# Valkey Auto CPU & IO Thread Affinity Design

## 1. Problem Statement
Valkey users must manually configure `io-threads` and CPU affinity settings (`server-cpulist`, `bio-cpulist`, etc.) to fully utilize multi-core machines. For managed, containerized, or dynamically scaled deployments where the VM size may change, maintaining static `valkey.conf` values is suboptimal and error-prone, resulting in either unutilized cores or heavy CPU contention.

## 2. Goal
Enable Valkey to automatically determine the number of IO threads and configure CPU affinity for the main thread, IO threads, and background threads based on the CPU resources actually available to the process, while being aware of **container cgroup quotas** and **hardware NUMA topology**.

**Assumption**: There is only one Valkey instance in the VM/container context, so the process can safely monopolize the CPU resources allocated to it. Auto-tuning must be opt-in, because this assumption is false for co-located deployments.

**Non-goal**: Affinity is not available on every platform. On macOS and Windows the affinity portion of `auto` degrades to a no-op and only the `io-threads` count is derived (see §11).

---

## 3. Current Valkey Internals (verified against the tree)

This section records what the code actually does today. The original draft of this document contained several inaccuracies; they are corrected here and called out in §4.

### 3.1 Configuration Surface

Four CPU-affinity configs are registered in [`src/config.c:3412-3415`](src/config.c). The canonical names are hyphenated; the underscore spellings are aliases:

| Canonical | Alias | Flags | Struct field |
|---|---|---|---|
| `server-cpulist` | `server_cpulist` | `IMMUTABLE_CONFIG`, `EMPTY_STRING_IS_NULL` | `server.server_cpulist` |
| `bio-cpulist` | `bio_cpulist` | `IMMUTABLE_CONFIG`, `EMPTY_STRING_IS_NULL` | `server.bio_cpulist` |
| `aof-rewrite-cpulist` | `aof_rewrite_cpulist` | `IMMUTABLE_CONFIG`, `EMPTY_STRING_IS_NULL` | `server.aof_rewrite_cpulist` |
| `bgsave-cpulist` | `bgsave_cpulist` | `IMMUTABLE_CONFIG`, `EMPTY_STRING_IS_NULL` | `server.bgsave_cpulist` |

`io-threads` is registered at [`src/config.c:3457`](src/config.c):

```c
createIntConfig("io-threads", NULL, DEBUG_CONFIG | MODIFIABLE_CONFIG,
                1, IO_THREADS_MAX_NUM, server.io_threads_num, 1,
                INTEGER_CONFIG, NULL, updateIOThreads),
```

Note the bounds: **minimum 1**, maximum `IO_THREADS_MAX_NUM` (256, [`src/config.h:369`](src/config.h)). It is `MODIFIABLE_CONFIG`, not immutable — `CONFIG SET io-threads` works at runtime.

There is also a fifth field, `server.slot_migration_cpulist` ([`src/server.h:2366`](src/server.h)), which is **dead code**: it is never registered as a config and never read. The slot-migration child actually pins itself with `bgsave_cpulist` ([`src/cluster_migrateslots.c:1635`](src/cluster_migrateslots.c)). Any `auto` work should either wire this field up or delete it.

### 3.2 Affinity Mechanism

`serverSetCpuAffinity()` ([`src/server.c:7427`](src/server.c)) is a thin wrapper that compiles to a no-op unless `USE_SETCPUAFFINITY` is defined, which happens only on Linux, NetBSD, FreeBSD and DragonFly ([`src/config.h:359-362`](src/config.h)).

`setcpuaffinity()` ([`src/setcpuaffinity.c:73`](src/setcpuaffinity.c)) parses a `taskset`-style list (`"0,2,3"`, `"0,2-3"`, `"0-20:2"`) into a `cpu_set_t` and calls `sched_setaffinity()`. Two properties matter for this design:

- It **silently returns** on every parse failure (lines 100-101, 111-112, 124-125, 137-138). No error, no log.
- It always pins **the calling thread** (`sched_setaffinity(0, ...)`), so each thread must call it for itself.

The silent-failure property means `server-cpulist auto` **already parses and is already ignored today**. A typo like `server-cpulist atuo` behaves identically. Introducing the `auto` sentinel therefore requires adding a validation callback, or users will get a silent no-op instead of an error.

Call sites, and which thread/process each one pins:

| Site | Pinned entity | List used |
|---|---|---|
| [`src/server.c:7838`](src/server.c) | main thread | `server_cpulist` |
| [`src/io_threads.c:289`](src/io_threads.c) (`IOThreadMain`) | each IO thread | `server_cpulist` |
| [`src/bio.c:254`](src/bio.c) | each bio thread | `bio_cpulist` |
| [`src/rdb.c:1691`](src/rdb.c), [`src/rdb.c:3854`](src/rdb.c) | BGSAVE child (post-fork) | `bgsave_cpulist` |
| [`src/aof.c:2635`](src/aof.c) | AOF rewrite child (post-fork) | `aof_rewrite_cpulist` |
| [`src/cluster_migrateslots.c:1635`](src/cluster_migrateslots.c) | slot-migration child | `bgsave_cpulist` |

**The main thread and every IO thread pin to the same `server_cpulist` string.** There is no separate list for the main thread, and `IOThreadMain()` does not use its thread id to select a CPU. This is the single most important constraint on the design (see §4.1).

### 3.3 Startup Ordering

Relevant order inside `main()` ([`src/server.c`](src/server.c)):

```
7693  loadServerConfig()
7743  daemonize()
7760  initServer()               -> adjustOpenFilesLimit()  (server.c:3001)
7764  checkTcpBacklogSettings()  -> reads /proc/sys/net/core/somaxconn
7773  initListeners()            -> listen(fd, server.tcp_backlog)
7793  InitServerLast()           -> bioInit(); initIOThreads(1)
7838  serverSetCpuAffinity(server.server_cpulist)
7839  aeMain()
```

Two corrections to the original draft: the main thread pins itself in `main()` immediately before `aeMain()`, **not** in `InitServerLast()`; and IO threads are created by `initIOThreads()` in [`src/io_threads.c`](src/io_threads.c), **not** by `initThreadedIO()` in `networking.c` (that function no longer exists).

### 3.4 Valkey Already Auto-Tunes Active IO Threads

This is the most consequential omission in the original design. `io-threads` is **not** the number of threads doing work; it is a **ceiling**. Valkey already scales the active count between 1 and `io-threads` at runtime, in `IOThreadsAfterSleep()` ([`src/io_threads.c:149`](src/io_threads.c)).

The policy, with constants from [`src/io_threads.c:139-146`](src/io_threads.c):

- **Ignition.** While `active_io_threads_num == 1`, the main thread's active-time fraction is sampled every 50ms (`IOThreadsBeforeSleep()`, [`src/io_threads.c:131-136`](src/io_threads.c)). When it exceeds `IO_IGNITION_MAIN_THREAD_ACTIVE_PERCENT` (30%), one IO thread is unparked.
- **Scale up.** Every `IO_SAMPLE_RATE_MS` (10ms) the depth of the shared SPMC inbox is sampled. Once `STATS_METRIC_SAMPLES` samples have accumulated, an average queue depth `> 1` unparks one more thread.
- **Scale down.** An average depth of `0`, at least `IO_COOLDOWN_MS` (1000ms) after the last scale-up, parks one thread — but only if that thread's private queue is empty, and, when dropping to a single thread, only if the shared inbox is also empty.
- Threads are parked on `io_threads_mutex[i]`, so an inactive IO thread consumes no CPU. Its cost is a stack, a private queue, and a shared query buffer.
- The hidden `io-threads-always-active` bool ([`src/config.c:3393`](src/config.c)) bypasses all of the above for tests.

`updateIOThreads()` ([`src/io_threads.c:429`](src/io_threads.c)) resizes the pool on `CONFIG SET`. It **can fail**: if pending IO responses exceed the outbox capacity it returns `"Can't update IO threads under load, try again later"` rather than risk a deadlock in `drainIOThreadsQueue()`.

---

## 4. Design Review: Gaps in the Original Draft

### 4.1 The per-thread affinity plan is not implementable as config mutation

Every tier in the original §5 assigns the main thread one CPU and IO threads a disjoint set (`Main Thread: CPU 0`, `IO Threads: CPUs 1..N/2`). Both read the same `server.server_cpulist` string. Writing `"0-4"` into it pins the main thread **and** all IO threads to the union `{0,1,2,3,4}` — the kernel is free to migrate all of them onto CPU 0.

The "trick the downstream routines" approach cannot express the intended topology. Options, cheapest first:

1. **Union mask, no isolation** (zero code change beyond discovery). Pin main+IO to one NUMA node's CPUs, background to another. Loses the dedicated-main-thread property, keeps NUMA locality — which is where most of the win is.
2. **Add `main-cpulist`** and have `IOThreadMain()` keep using `server-cpulist`. One new config, minimal code, expresses "main on 0, IO on 1-11".
3. **Index-based pinning.** Pass the thread id into the affinity decision in `IOThreadMain()` and pin IO thread `i` to the `i`-th CPU of the computed set. Strongest isolation, largest change, and it interacts badly with §3.4: a parked thread's CPU sits idle, so a 1:1 pin wastes cores at low load.

Recommendation: **(1) for v1, (2) as a follow-up.** Do not do (3) until the runtime scaler is affinity-aware.

### 4.2 The `-1` sentinel cannot be stored in `io_threads_num`

The draft proposes `server.io_threads_num = -1` as the "auto" flag. `createIntConfig("io-threads", ..., 1, IO_THREADS_MAX_NUM, ...)` declares a **minimum of 1**; the numeric parser enforces bounds before the value ever reaches the struct, so `-1` is rejected.

There is a precedent for encoding a sentinel in the value: `PERCENT_CONFIG` stores percentages as negative numbers, and `maxmemory-clients` declares `min = -100` to make room ([`src/config.c:3555`](src/config.c), parser at [`src/config.c:2242`](src/config.c)). Two viable routes:

- Lower the `io-threads` minimum to `-1` and treat negatives as sentinels, mirroring `PERCENT_CONFIG`. Cheap, but `CONFIG GET io-threads` would transiently show `-1` before resolution.
- Add an `AUTO_CONFIG` flag alongside `PERCENT_CONFIG` that sets a side-band bit (`server.io_threads_auto`) and leaves the numeric field at a resolved fallback. Cleaner, and it generalizes to the configs in the companion global design.

Recommendation: **the `AUTO_CONFIG` flag.** It is the only approach that works for `maxmemory` (whose range is `0..ULLONG_MAX`, with no negative space at all).

### 4.3 `auto` on a string config fails open, not closed

Per §3.2, `server-cpulist auto` currently reaches `setcpuaffinity()`, fails to parse `'a'` as a digit, and returns without pinning anything. Until an `auto` handler exists, this is a silent misconfiguration. Any implementation must land the validation callback in the **same commit** as the parser change, and `setcpuaffinity()` should be given a return value so genuine parse failures can be logged.

### 4.4 The heuristics treat `io-threads` as a steady-state count

Given §3.4, `io-threads` should be chosen as a **ceiling** that the runtime scaler is free not to reach. The tiers in the original §5 are calibrated as if every configured thread runs hot, which makes them far too conservative. Specifically:

- `io-threads 1` for `N <= 2` is still right, but for a different reason: below two cores the pool cannot ignite without stealing the main thread's core.
- The `N/2` split for medium VMs leaves half the machine idle under load, since parked threads cost no CPU.
- The hard cap of `14` for large VMs duplicates a job the scaler already does, and `IO_THREADS_MAX_NUM` is 256, not 16.

Revised heuristics are in §7.

### 4.5 Unaddressed: cgroup CPU quota is not an integer, and is not affinity

`cpu.max` expresses a *bandwidth* quota (e.g. `150000 100000` = 1.5 CPUs), not a *cpuset*. Three consequences the draft misses:

- `N = quota / period` can be **fractional**. `1.5` cores must round somewhere; rounding up oversubscribes and invites CFS throttling, which shows up as latency spikes, not as reduced throughput.
- A bandwidth quota does not constrain *which* CPUs the process runs on. The cpuset is `/sys/fs/cgroup/cpuset.cpus.effective` (v2), and it is what `sched_getaffinity()` reflects. Affinity decisions must use the cpuset; thread-count decisions must use `min(quota, |cpuset|)`.
- Under a bandwidth quota, spawning more threads than the quota allows is actively harmful: all threads are throttled together at the end of each period.

### 4.6 Unaddressed: cgroup namespaces and the v2 unified path

`/sys/fs/cgroup/cpu.max` is only correct when the container's cgroup is the root of its namespace. Outside that case the process's own cgroup must be resolved via `/proc/self/cgroup` and joined to the mount point from `/proc/self/mountinfo`. A hybrid v1/v2 host adds a third layout. This is real work and should be scoped as such.

### 4.7 Unaddressed: interaction with `CONFIG SET`

If `io-threads auto` resolves to 8 at boot and an operator later runs `CONFIG SET io-threads 4`, does the config revert to manual? What does `CONFIG GET io-threads` return while `auto` is active? See §9.

---

## 5. Configuration Surface

```conf
io-threads          auto
server-cpulist      auto
bio-cpulist         auto
aof-rewrite-cpulist auto
bgsave-cpulist      auto
```

`auto` is a per-config opt-in. Setting `io-threads auto` alone derives only the thread count and leaves affinity untouched, which is the correct default for co-located deployments. A convenience meta-config (`auto-tuning yes`) that expands to all five is possible but should be a separate change.

The four cpulist configs are `IMMUTABLE_CONFIG`, so their `auto` resolution happens exactly once, at boot. `io-threads` is `MODIFIABLE_CONFIG` and therefore needs a runtime story (§9).

---

## 6. Environment Discovery

### 6.1 Effective CPU budget

No cgroup, NUMA, or CPU-count detection exists anywhere in `src/` today; all of this is new code. It should live in a new `src/cpuinfo.c` behind a small interface, so it can be unit-tested with a fake sysfs root rather than only on a real container.

Two distinct quantities must be computed, and the draft conflated them:

- **`cpuset`** — *which* CPUs we may run on. Source of truth is `sched_getaffinity(0, ...)`, which already accounts for `cpuset.cpus.effective` and any inherited `taskset`. Read it directly; do not parse sysfs for this.
- **`quota`** — *how much* CPU time we may consume, as a possibly-fractional core count.
  1. cgroup v2: `<cgroup>/cpu.max`, format `"<quota|max> <period>"`. `max` means unrestricted.
  2. cgroup v1: `<cgroup>/cpu.cfs_quota_us` and `cpu.cfs_period_us`. `-1` quota means unrestricted.
  3. Unrestricted: `quota = |cpuset|`.

  Resolving `<cgroup>` requires `/proc/self/cgroup` + `/proc/self/mountinfo` in the general case (§4.6).

Then:

```
N_threads  = floor(min(quota, |cpuset|))     # never round a quota up
N_affinity = cpuset                          # the actual CPU ids
```

`floor()` of a `1.5`-core quota yields `1`, which correctly selects the single-threaded tier.

### 6.2 NUMA topology

Parse `/sys/devices/system/node/node*/cpulist` and intersect each node's set with `N_affinity`. Nodes with an empty intersection are ignored. If `/sys/devices/system/node/` is absent (common in containers), assume a single node.

Rule: **the main thread and all IO threads share one NUMA node.** Pick the node with the largest intersection, breaking ties by lowest node id. Background threads and fork children go to the remaining nodes if any exist, otherwise to the leftover CPUs of the chosen node.

This is a real effect, not a theoretical one: the main thread and IO threads share the client structs, query buffers, and reply blocks, so a cross-socket split turns every reply into remote-node traffic.

---

## 7. Resource Allocation Heuristics (revised)

Let `N = N_threads` from §6.1, and read `io-threads` as a **ceiling** (§3.4, §4.4).

### Case 1: N <= 2

- `io-threads`: `1` — the pool cannot ignite without contending with the main thread for its only core.
- `server-cpulist`: unset (inherit the cpuset).
- `bio-cpulist`, `bgsave-cpulist`, `aof-rewrite-cpulist`: unset. Pinning background work on a 1-2 core box starves either the data plane or the background jobs; the scheduler does better.

### Case 2: 2 < N <= 8

- `io-threads`: `N - 1` — one core's worth of headroom for bio, fork children, and the kernel. The scaler parks what it does not need.
- `server-cpulist`: all CPUs of the chosen NUMA node that fall in `N_affinity`.
- `bio-cpulist` / `bgsave-cpulist` / `aof-rewrite-cpulist`: the same set. Isolating background work on a machine this size costs more in idle cores than it saves in P99.

Rationale for the change from the draft's `N/2`: since parked IO threads cost no CPU, a ceiling of `N/2` is a hard limit on burst throughput with no idle-state benefit.

### Case 3: N > 8

- `io-threads`: `min(N - 2, 16)`. The `16` is a soft cap on *contention on the shared SPMC inbox*, not on core count; it should be validated by benchmark before being fixed (§10).
- `server-cpulist`: the chosen NUMA node's CPUs, minus the CPUs handed to background work.
- `bio-cpulist` / `bgsave-cpulist` / `aof-rewrite-cpulist`: a second NUMA node if one exists; otherwise the top `2` CPUs of the chosen node.

Offloading BGSAVE to a separate socket prevents LLC thrashing on the socket where the main thread lives. When there is only one node, the top-2-CPUs carve-out is a compromise: it protects the data plane from fork-child CPU spikes at the cost of two cores.

### Under a bandwidth quota

When `quota < |cpuset|` the process is bandwidth-limited, not cpuset-limited. Do **not** pin — the cpuset is wide and pinning to `floor(quota)` CPUs discards scheduling flexibility for no gain. Derive `io-threads` from `floor(quota)` and leave every cpulist unset. This is the common Kubernetes case (`resources.limits.cpu: "1500m"` with no `cpuset` cgroup) and the draft's rules would pin such a pod to CPU 0 on a 96-core host.

---

## 8. Implementation

### Step 1 — `AUTO_CONFIG` flag (`src/config.c`)

Add an `AUTO_CONFIG` flag to the numeric and string config vtables, mirroring how `PERCENT_CONFIG` is threaded through `numericParseString()` ([`src/config.c:2242`](src/config.c)).

- On parse, if the literal is `"auto"` and the config declares `AUTO_CONFIG`: set `server.<name>_auto = 1`, store the existing default, return success.
- If the literal is `"auto"` and the config does **not** declare `AUTO_CONFIG`: return `"argument couldn't be parsed into an integer"` — i.e. reject, rather than the current silent no-op (§4.3).

This same flag is what the companion global-tuning design needs.

### Step 2 — Discovery (`src/cpuinfo.c`, new)

```c
typedef struct {
    double  quota;          /* fractional cores; HUGE_VAL if unrestricted */
    cpu_set_t cpuset;       /* from sched_getaffinity() */
    int     n_effective;    /* floor(min(quota, CPU_COUNT(&cpuset))) */
    int     numa_nodes;
    cpu_set_t node_cpus[MAX_NUMA_NODES];
} cpuTopology;

int cpuTopologyDetect(cpuTopology *out, const char *sysfs_root);
```

Taking `sysfs_root` as a parameter is what makes this unit-testable; production passes `"/"`.

### Step 3 — Resolution (`src/server.c`)

Add `evaluateAutoCpuSettings()` and call it in `main()` **between `loadServerConfig()` (7693) and `initServer()` (7760)**. `initServer()` is the deadline because `adjustOpenFilesLimit()` runs there, and `initListeners()`/`InitServerLast()` consume the resolved values downstream.

The function resolves the sentinels into the ordinary struct fields, exactly as if the operator had typed the values:

```c
if (server.io_threads_auto) server.io_threads_num = computed_io_threads;
if (server.server_cpulist_auto) server.server_cpulist = sdsnew(fastpath_mask);
/* ... */
```

Downstream (`InitServerLast()` → `bioInit()` → `initIOThreads()`, and the `serverSetCpuAffinity()` at 7838) runs unmodified. This part of the original design is sound — with the caveat from §4.1 that a single `server_cpulist` cannot separate main from IO.

### Step 4 — Make affinity failures visible

Change `setcpuaffinity()` to return `int`, and have `serverSetCpuAffinity()` log at `LL_WARNING` on parse failure. Six call sites; two of them are in post-fork children where `serverLog()` is already used, so this is safe.

---

## 9. Observability and Runtime Semantics

### Logging

Log the resolution once, at `LL_NOTICE`, before threads launch:

```
* Auto CPU tuning: cpuset={0-15} quota=unrestricted -> N=16, NUMA nodes=2
* Auto CPU tuning: io-threads=14 (ceiling; runtime scaler parks idle threads)
* Auto CPU tuning: server-cpulist=0-7 (node 0), bio/bgsave/aof-rewrite-cpulist=8-15 (node 1)
```

Log the *derivation*, not just the result. An operator debugging a throttled pod needs to see `quota=1.5` to understand why they got one IO thread on a 96-core host.

### `CONFIG GET`

`CONFIG GET io-threads` must return the **resolved** value (`8`), because tooling parses it as an integer. The fact that it was derived belongs in `CONFIG INFO io-threads`, which this branch's parent commit (`0e52ec8`, "Add support for `CONFIG INFO <config>`") introduced at [`src/config.c:3841`](src/config.c). Adding an `auto: yes|no` row there is a much better fit than the draft's proposed `CONFIG GET --raw`, which does not match `CONFIG GET`'s glob-pattern signature.

### `CONFIG SET io-threads`

`io-threads` is `MODIFIABLE_CONFIG`. Proposed semantics:

- `CONFIG SET io-threads 4` clears `io_threads_auto` and pins the ceiling at 4.
- `CONFIG SET io-threads auto` re-runs discovery and re-applies. This is genuinely useful: a container can be resized in place (`cpu.max` rewritten) without a restart.
- Both go through `updateIOThreads()`, which may fail under load ([`src/io_threads.c:448`](src/io_threads.c)). `auto` re-resolution must propagate that error rather than leaving `io_threads_num` desynchronized from the live pool.

The four cpulist configs stay `IMMUTABLE_CONFIG`; re-pinning live threads is out of scope.

---

## 10. Testing

- **Unit** (`cpuTopologyDetect` against a fake sysfs root): cgroup v2 `max 100000`; v2 `150000 100000` (fractional); v1 quota `-1`; v1 `200000 / 100000`; missing `/sys/devices/system/node`; single node; two nodes with a partial cpuset intersection; cgroup-namespace-relative paths.
- **Unit** (heuristics, pure function from `cpuTopology` → resolved config): each tier boundary at `N ∈ {1,2,3,8,9,16}`, plus the bandwidth-quota case where pinning must be suppressed.
- **Integration** (`tests/unit/introspection.tcl`): `server-cpulist auto` starts and reports a resolved list in `CONFIG GET`; a bad literal is now rejected at startup instead of silently ignored (this is the regression guard for §4.3).
- **Integration**: `CONFIG SET io-threads auto` resizes the live pool; and, under sustained load, surfaces `"Can't update IO threads under load"` rather than corrupting state.
- **Benchmark, gating the `16` cap in §7**: `valkey-benchmark -P 16 -c 200` on a 32-core and a 64-core box, sweeping `io-threads` over `{4,8,16,24,32}`, measuring P99 and throughput. The cap should be set where P99 stops improving, not guessed.
- **Non-Linux CI**: macOS build must compile and start with `server-cpulist auto` (no-op path, §11).

## 11. Portability

| Platform | `io-threads auto` | cpulist `auto` | cgroup quota |
|---|---|---|---|
| Linux | yes | yes | v1 + v2 |
| FreeBSD / DragonFly / NetBSD | yes | yes (`cpuset_setaffinity`/`pthread_setaffinity_np`) | n/a — use `sysconf` |
| macOS | yes (via `sysconf(_SC_NPROCESSORS_ONLN)`) | **no-op**, log once at `LL_NOTICE` | n/a |

On platforms without `USE_SETCPUAFFINITY`, `server-cpulist auto` must not be an error — it must resolve to "unset" and log that affinity is unsupported. Silently accepting it (today's behavior) is acceptable only because nothing else is possible; logging is the improvement.

## 12. Risks and Open Questions

- **The single-instance assumption.** Two Valkey processes with `auto` on one box will both claim node 0 and fight. Mitigation: `auto` is opt-in, and the log line makes the claim visible. A stronger mitigation — detecting a peer via the cpuset — is out of scope.
- **The `16` cap in Case 3 is a guess.** It must not be merged as a constant without the §10 benchmark behind it.
- **Interaction with the runtime scaler is untested at high ceilings.** With `io-threads 32`, the scale-up path unparks one thread per `STATS_METRIC_SAMPLES` samples at 10ms each; reaching 32 active threads takes a while. Whether the ceiling is ever reached under a burst is an open empirical question, and it may argue for a faster ramp rather than a lower cap.
- **Fork children inherit the parent's affinity** before they call `serverSetCpuAffinity()`. Between `fork()` and the pin, a BGSAVE child runs on the main thread's CPUs. At `N <= 2` this is why Case 1 leaves everything unpinned.
- **`slot_migration_cpulist` is dead** (§3.1). Decide: wire it up, or delete it.
