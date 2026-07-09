# Valkey Global Auto-Tuning Design

## 1. Problem Statement & Vision
Requiring users to manually tune `valkey.conf` for their exact deployment restricts performance and scalability. In serverless, containerized, or dynamically shifting workloads, the environment and data access patterns fluctuate constantly.

The vision is to extend the `auto` configuration sentinel introduced by the [CPU/IO auto-tuning design](valkey_auto_tuning_design.md) to a broader set of Valkey settings, so the server can introspect the OS and its own runtime telemetry and self-optimize.

This document assumes the `AUTO_CONFIG` flag mechanism from §8 Step 1 of that design. Everything here builds on it.

---

## 2. Config Identification & Categorization

Configs fall into two phases with different data sources, different evaluation points, and — importantly — different reversibility.

**Phase A: Environment/Startup tuning (introspecting the OS).** Evaluated once, in `main()`, between `loadServerConfig()` ([`src/server.c:7693`](src/server.c)) and `initServer()` ([`src/server.c:7760`](src/server.c)).

| Config | Mutability | Derived from |
|---|---|---|
| `maxmemory` | `MODIFIABLE_CONFIG` | cgroup memory limit or physical RAM |
| `maxclients` | `MODIFIABLE_CONFIG` | `getrlimit(RLIMIT_NOFILE)` |
| `tcp-backlog` | `IMMUTABLE_CONFIG` | `somaxconn` |

**Phase B: Runtime/telemetry tuning (introspecting metrics).** Evaluated periodically from `serverCron()`.

| Config | Mutability | Derived from |
|---|---|---|
| `active-expire-effort` | `MODIFIABLE_CONFIG` (1..10) | memory pressure, expire velocity |
| `client-output-buffer-limit` | `MODIFIABLE_CONFIG`, `MULTI_ARG_CONFIG` | memory headroom, client count |
| `auto-aof-rewrite-percentage` | `MODIFIABLE_CONFIG` | fsync latency |
| `hash-max-listpack-entries` etc. | `MODIFIABLE_CONFIG` | average value size |

### 2.1 A correction: `dynamic-hz` is not a precedent

The earlier draft cited `dynamic-hz` as the model for runtime tuning. **`dynamic-hz` is a deprecated config in Valkey** — it appears only in the `deprecated_configs[]` table at [`src/config.c:477`](src/config.c) and does nothing. There is currently *no* precedent in the tree for a config that mutates itself at runtime, which raises the bar for Phase B: it is a genuinely new behavior class, not an extension of an existing one.

The nearest real precedent is the IO-thread scaler ([`src/io_threads.c:149`](src/io_threads.c)), which adapts an *internal* variable (`active_io_threads_num`) while leaving the user-visible `io-threads` config untouched. That distinction is the right one to copy (§6).

---

## 3. Auto-Tuning Strategies per Config

### 3.1 Memory Bounds (`maxmemory`) — Phase A

**Current state.** [`src/config.c:3539`](src/config.c): `createULongLongConfig("maxmemory", NULL, MODIFIABLE_CONFIG, 0, ULLONG_MAX, ..., 0, MEMORY_CONFIG, NULL, updateMaxmemory)`. Defaults to `0` (unlimited), so in a container the kernel OOM-killer terminates Valkey when it exceeds its quota.

**Strategy.**
- Read the cgroup memory limit: v2 `memory.max`, v1 `memory.limit_in_bytes`. Both may read `max` / a sentinel meaning unlimited.
- Fall back to physical RAM via `zmalloc_get_memory_size()` ([`src/zmalloc.h:141`](src/zmalloc.h)) — this already exists; don't re-derive it from `sysconf`.
- Set `maxmemory` to **80%** of the detected limit.

**Why the range has no negative space.** `maxmemory` is declared `0..ULLONG_MAX`. Unlike `maxmemory-clients` ([`src/config.c:3555`](src/config.c)), which encodes percentages as negative values under `PERCENT_CONFIG` (min `-100`), there is no room to smuggle a sentinel into the value. This is the concrete reason the `AUTO_CONFIG` side-band flag is required rather than a magic number.

**Open problems the 80% rule does not solve.**
- `maxmemory` bounds the dataset, not RSS. Allocator fragmentation, replication buffers, and the AOF buffer all live outside it. 80% is a guess that fails for fragmentation ratios above ~1.25.
- A `fork()` for BGSAVE can double RSS under a write-heavy workload. In a container with a hard `memory.max`, the child is what gets OOM-killed. If `maxmemory auto` is set *and* persistence is enabled, the headroom must account for the fork, and 20% is not obviously enough.
- Reading `memory.max` has the same cgroup-namespace resolution problem described in §4.6 of the CPU design.

Recommendation: ship `maxmemory auto` with an explicit `maxmemory-auto-percent` (default 80) rather than hardcoding, and log the derivation.

### 3.2 Connections (`maxclients`) — Phase A

**Current state.** [`src/config.c:3506`](src/config.c), default 10,000. `adjustOpenFilesLimit()` ([`src/server.c:2585`](src/server.c)) already runs at startup and on every `CONFIG SET maxclients` (via `updateMaxclients` → [`src/config.c:2728`](src/config.c)). It tries to raise `RLIMIT_NOFILE` to `maxclients + CONFIG_MIN_RESERVED_FDS` and, failing that, **lowers `maxclients` to fit**, logging a warning.

**So the clamp already exists.** `maxclients auto` is the *inverse* operation: raise `maxclients` to consume whatever the rlimit allows.

**Strategy.** Query `getrlimit(RLIMIT_NOFILE)`; set `maxclients = rlim_cur - CONFIG_FDSET_INCR`. Use `CONFIG_FDSET_INCR` (128, [`src/server.h:226`](src/server.h)), not a fresh literal — it is already defined as `CONFIG_MIN_RESERVED_FDS + 96` and is the number the rest of the code reserves. If `rlim_cur` is `RLIM_INFINITY`, clamp to 65,000.

Because `adjustOpenFilesLimit()` runs inside `initServer()`, `maxclients auto` must resolve **before** it, and then `adjustOpenFilesLimit()` becomes a no-op safety net. Ordering is not optional here.

### 3.3 Backlog (`tcp-backlog`) — Phase A

**Current state.** [`src/config.c:3480`](src/config.c), `IMMUTABLE_CONFIG`, default 511. `checkTcpBacklogSettings()` ([`src/server.c:2669`](src/server.c)) already reads `/proc/sys/net/core/somaxconn` (guarded by `HAVE_PROC_SOMAXCONN`, with a `HAVE_SYSCTL_KIPC_SOMAXCONN` path for BSD) and warns when the configured backlog exceeds it.

**Strategy.** `tcp-backlog auto` reads the same source and sets the backlog equal to `somaxconn`. Reuse of `checkTcpBacklogSettings()`'s reader is the whole implementation — factor its file/sysctl read into `getSomaxconn()` and call it from both places.

Ordering: `checkTcpBacklogSettings()` runs at [`src/server.c:7764`](src/server.c), before `initListeners()` at 7773 passes `server.tcp_backlog` to `listen()`. Resolving in `evaluateOSConfigurations()` (before `initServer()`) is safely ahead of both.

Caveat: a larger backlog is not free. It trades connection-burst tolerance for a longer queue of half-open connections during a SYN flood. Setting it to `somaxconn` unconditionally is defensible only because `somaxconn` is itself an administrator's choice.

### 3.4 Expiration Pacing (`active-expire-effort`) — Phase B

**Current state.** [`src/config.c:3493`](src/config.c), an int in `1..10`, default 1. High values spend CPU on the active expire cycle; low values let expired keys accumulate.

**Strategy.** The inputs are already available — do not invent new metrics:
- Memory pressure: `getMaxmemoryState(&total, &logical, &tofree, &level)` ([`src/evict.c:265`](src/evict.c)) returns `level` as the fraction of `maxmemory` in use.
- Expire velocity: `server.stat_expiredkeys` ([`src/server.h:1868`](src/server.h)), sampled as a delta per cron tick.

**Rule.** When `level > 0.9` and expire velocity is nonzero, ramp effort toward `8`. When `level < 0.5`, decay toward `1`. Ramp one step per tick with a cooldown, so a memory spike does not slam effort to 8 and back.

**Caveat.** If `maxmemory` is `0` (unlimited), `getMaxmemoryState()` reports no pressure and `level` is meaningless. `active-expire-effort auto` therefore only functions when `maxmemory` is set — which is an argument for pairing it with `maxmemory auto`, and for logging a warning when it is enabled alone.

### 3.5 Buffer Scaling (`client-output-buffer-limit`) — Phase B

**Current state.** This is **not** an ordinary config. [`src/config.c:3590`](src/config.c) registers it via `createSpecialConfig(..., MULTI_ARG_CONFIG, setConfigClientOutputBufferLimitOption, getConfigClientOutputBufferLimitOption, rewriteConfigClientOutputBufferLimitOption, NULL)`. It has bespoke parse, get, and rewrite callbacks, and its value is a per-class `{hard, soft, soft_seconds}` triple.

**Consequence for this design.** The generic `AUTO_CONFIG` flag from the CPU design lives on the numeric/string config vtables and **does not reach special configs**. `client-output-buffer-limit pubsub auto` requires editing `setConfigClientOutputBufferLimitOption()` directly. This is strictly more work than the draft implied, and it should be scoped as its own change rather than bundled.

**Strategy.** Let the total pubsub output-buffer budget be 10% of `maxmemory`, divided across active pubsub clients: `per_client_hard = (0.10 * maxmemory) / max(1, active_pubsub_clients)`.

**Caveat.** This makes a client's disconnection threshold depend on *other clients'* arrival and departure. A client that is comfortably under its limit can be disconnected because ten new subscribers appeared. That is a surprising failure mode and needs a floor (never shrink an individual limit below the static default) before it is safe.

Also note that `maxmemory-clients` ([`src/config.c:3555`](src/config.c)) already provides a global cap on client memory expressed as a percentage of `maxmemory`. It may be that the right answer is to point users at the existing mechanism rather than add a second, interacting one.

### 3.6 Persistence (`auto-aof-rewrite-percentage`) — Phase B

**Current state.** [`src/config.c:3462`](src/config.c), default 100 (rewrite when the AOF doubles).

Note the correct config name is `auto-aof-rewrite-percentage`, not `aof-rewrite-percentage`.

**Strategy.** Classify the disk by fsync latency and adjust: low latency (NVMe/SSD) → ~50%, triggering smaller, more frequent rewrites; high latency (rotational, or a throttled network volume) → ~150%, batching writes further apart.

**The metric does not exist.** The draft refers to `server.stat_fsync_latency`. There is no such field. What exists is a latency-monitor sample, `latencyAddSampleIfNeeded("aof-fsync-always", latency)` ([`src/aof.c:1379`](src/aof.c)), and it is:
- only recorded on the `appendfsync always` path, and
- gated on `latency-monitor-threshold`, which defaults to `0` = **disabled** ([`src/config.c:3532`](src/config.c)).

So this strategy needs a new, always-on, low-overhead fsync-latency accumulator in the bio thread. That is a real prerequisite, not a detail.

**Caveat.** Disk classification from fsync latency is unreliable on cloud block storage, where latency varies with burst-credit balance and neighboring tenants. A rewrite triggered by a transient latency dip can itself saturate the volume. Consider gating this behind a long observation window (minutes, not cron ticks), or dropping it from v1.

### 3.7 Encoding Thresholds (`hash-max-listpack-entries`, etc.) — Phase B, not recommended

The draft lists these as tunable from average value size. They should be dropped, for a structural reason:

**Encoding conversion is one-way.** [`src/t_hash.c:178`](src/t_hash.c) and [`src/t_hash.c:405`](src/t_hash.c) convert listpack → hashtable when the threshold is exceeded. Nothing converts back. Lowering `hash-max-listpack-entries` at runtime affects only *future* writes; raising it does not re-encode existing hashtable-encoded keys. An auto-tuner that lowers the threshold under memory pressure and raises it again when pressure clears will produce a keyspace whose encodings depend on the order keys were written in — non-deterministic memory usage, and no way to converge.

---

## 4. Implementation Approach

### Step 1 — Reuse `AUTO_CONFIG`
Defined in §8 Step 1 of the CPU design. Applies cleanly to `maxmemory`, `maxclients`, `tcp-backlog`, `active-expire-effort`. Does **not** apply to `client-output-buffer-limit` (§3.5).

### Step 2 — Boot-time evaluator
`evaluateOSConfigurations()` in [`src/server.c`](src/server.c), called from `main()` between `loadServerConfig()` (7693) and `initServer()` (7760). It resolves `maxmemory`, `maxclients`, `tcp-backlog`, and — via `evaluateAutoCpuSettings()` — the CPU configs. The ordering constraint is hard: `initServer()` calls `adjustOpenFilesLimit()`, which consumes `maxclients`.

### Step 3 — Runtime governor
`evaluateRuntimeConfigurations()` hooked into `serverCron()` under `run_with_period(1000)`.

Design constraint borrowed from the IO-thread scaler: **the governor must not write the user's config value.** Keep the operator's intent (`auto`) and the derived value in separate fields, exactly as `io_threads_num` (intent) and `active_io_threads_num` (derived) are separate. Otherwise `CONFIG REWRITE` will bake a transient derived value into `valkey.conf`, and the next restart will start from a value that was correct for a workload that no longer exists.

Every governor decision needs: a cooldown, a step limit (one increment per tick), and a hysteresis band, so it cannot oscillate. The IO scaler's `IO_COOLDOWN_MS` (1000ms) is the model.

### Step 4 — Observability

- `CONFIG GET <name>` returns the **resolved** value, always. Tooling parses these as integers.
- `CONFIG INFO <name>` gains an `auto: yes|no` row. `configInfoCommand()` was added at [`src/config.c:3841`](src/config.c) by this branch's parent commit (`0e52ec8`) and is the natural home. The draft's `CONFIG GET --raw` does not fit — `CONFIG GET` takes glob patterns, and `--raw` would be ambiguous with a pattern.
- `CONFIG REWRITE` must write `auto`, not the resolved value. This needs a change in each affected config's rewrite path.
- Every governor transition logs at `LL_NOTICE`: `Auto-tuner: active-expire-effort 1 -> 4 (memory level 0.93, expired 12k/s)`. Log the inputs, not just the transition.

---

## 5. Sequencing

The strategies are not equally ready. Suggested order:

1. `AUTO_CONFIG` flag + `CONFIG INFO`/`CONFIG REWRITE` plumbing. No user-visible tuning; pure mechanism.
2. `maxclients auto`, `tcp-backlog auto`. Both read a single OS value, both have an existing reader to reuse, both are trivially testable.
3. `maxmemory auto`. Needs the cgroup reader and the fork-headroom question answered.
4. `active-expire-effort auto`. First Phase B config; establishes the governor, cooldown, and no-write-back discipline.
5. `client-output-buffer-limit ... auto`. Only after the special-config plumbing exists and the cross-client-interference floor is designed.
6. `auto-aof-rewrite-percentage auto`. Blocked on an always-on fsync latency metric; may not be worth it.
7. Encoding thresholds: **do not implement** (§3.7).

## 6. Risks

- **A self-modifying config is a new behavior class.** With `dynamic-hz` deprecated (§2.1), nothing in Valkey does this today. Operators, monitoring, and config-management tooling all assume `CONFIG GET` is stable unless someone calls `CONFIG SET`. Phase B breaks that assumption. The `CONFIG REWRITE` hazard in §4 Step 3 is the sharpest edge.
- **Auto-tuned configs interact.** `active-expire-effort auto` is meaningless without `maxmemory` (§3.4); `client-output-buffer-limit auto` is denominated in `maxmemory` (§3.5). A user who sets one but not the other gets silent no-ops. Enabling any Phase B config should validate its dependencies at startup and refuse, not warn.
- **Every threshold in this document is a guess.** 80% of RAM, 10% for pubsub, effort 8 at level 0.9, 50%/150% for AOF. None is benchmarked. Each should land with the benchmark that justifies it, or as a config with a documented default.
- **Testability.** All OS readers must take a root path parameter so they can be unit-tested against a fixture tree rather than requiring a real container. All governors must be pure functions from a metrics snapshot to a decision, tested without a running server.
