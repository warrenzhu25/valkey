# Proposal — Stage 0: the measurement that gates the roadmap

**Status: pre-issue draft.** This is the actionable plan behind
[proposal-dragonfly-inspired-perf.md](proposal-dragonfly-inspired-perf.md) §5 ("Stage 0 —
the measurement that gates all of this"). That section names the two numbers that decide
the plan; this note says exactly how to get them and what each result means for what to
build next.

> **Do not write feature code before this exists.** Its most valuable possible outcome is
> a "no" — evidence that the expensive feature (slot-per-thread) is not worth its years.

Companions: [proposal-slot-per-thread.md](proposal-slot-per-thread.md) (the thing this
gates), [proposal-forkless-rdb.md](proposal-forkless-rdb.md) and
[proposal-dashtable-adoption.md](proposal-dashtable-adoption.md) (the thing this only
*sizes*, because it ships regardless).
[proposal-mainthread-cpu-distribution.md](proposal-mainthread-cpu-distribution.md) proposes
turning **Q1** below into an always-on `INFO` metric — answering the gating question from
production traffic rather than only a `perf` lab run.

---

## 1. The decisions this gates

| Question | Result | Feature it selects |
|---|---|---|
| **Q1 — Is the serving thread execution-bound or I/O-bound at saturation?** | Execution-bound (keyspace work dominates a pinned 100% core) | **Slot-per-thread** (Stage 4) has a high ceiling — justified |
| | I/O-bound (network syscalls dominate, or the core isn't saturated) | **I/O backend / io_uring** (Stage 5, D5) — Stage 4 is the wrong bet |
| **Q2 — How bad is `fork()` in practice?** | Large fork stall and/or high COW RSS under write load | **Fork-less RDB** — sizes the win and sets its targets |

**Q2 does not branch the roadmap** — fork-less RDB is the recommended first feature
regardless, because it stands alone and fixes a universal operational pain. Stage 0's job
for Q2 is only to *quantify* the win and set acceptance targets (max tolerable p99 during
save, etc.). **Q1 is the real fork in the road**, and it is the one that must not be
guessed.

## 2. Environment — get this right or the numbers lie

- **Linux, many-core, bare metal or a dedicated instance.** The user's dev box is macOS;
  macOS is fine for functional work but **not** for these numbers (no `perf`, different
  syscall/scheduler behavior, no realistic fork/COW). Measure on Linux.
- **Pin and isolate.** Pin the Valkey main thread and I/O threads to dedicated cores
  (`taskset`/`CPU affinity`); keep `valkey-benchmark`/`memtier` on *different* cores, ideally
  a different NUMA node's client or a second box, so the load generator doesn't steal the
  cycles you're trying to measure.
- **Disable noise.** Turn off `appendonly` and automatic `save` for the pure-execution
  runs (measure those separately in §5.D). Fix CPU frequency governor to `performance`;
  disable turbo variance if you need repeatability.
- **Warm up, then measure a steady window.** Discard the first N seconds; report a stable
  interval. Repeat each cell ≥3× and report median + spread, not a single run.
- **Record the config that produced each number** (`io-threads`, dataset size, value size,
  pipeline depth, connection count, kernel, CPU). A number without its config is noise.

### 2.1 Running on a VM (when bare metal isn't available)

A VM is acceptable for decision-grade numbers **only if** you control for virtualization
noise. Ranked best to worst: bare metal > dedicated-host / metal instance type >
dedicated-vCPU VM > shared/burstable VM (**not usable** — see below).

**Pick the right instance.**
- Use a **dedicated-CPU** shape, never a **burstable / shared-vCPU** one (AWS `t*`, GCP
  `e2`/shared-core, Azure `B`-series). Burstable instances throttle via CPU credits, so the
  io-threads scaling sweep (§5.C — the decisive Q1 experiment) measures the credit
  scheduler, not Valkey.
- Prefer a **bare-metal instance type** (AWS `*.metal`, GCP sole-tenant/bare-metal) when the
  budget allows — it removes the hypervisor from CPU and (mostly) network entirely and makes
  fork/COW and `perf` behave like real hardware.
- Provision **≥ (Valkey cores + I/O-thread cores + headroom)** dedicated vCPUs so pinning
  has room and the hypervisor isn't oversubscribed.

**Control for steal time.** On a VM the hypervisor can preempt your vCPUs. Watch the `st`
column in `top`/`vmstat`, or `%steal` in `mpstat`/`pidstat`. **Any non-trivial steal
invalidates the run** — the CPU was doing someone else's work mid-measurement. Re-run, or
move to a dedicated host.

**Pin inside the guest, and know it's a soft pin.** `taskset`/cpuset still pins to *vCPUs*,
but vCPU→physical-core mapping is the hypervisor's call and can move. Mitigate: dedicated
vCPUs (above), disable the balloon driver, and if the platform allows, enable **CPU pinning
/ NUMA passthrough** at the hypervisor (KVM `<vcpupin>`, VMware latency-sensitivity = high).

**Host-level knobs you may not own.** THP and the CPU frequency governor live on the
**hypervisor host**. On a self-managed KVM host, set them there (governor `performance`,
THP per Valkey's guidance). On a managed cloud VM you usually **can't** — so treat
`latest_fork_usec` as valid *within* one instance type but **not comparable across**
different host hardware. Record the exact instance type with every fork number.

**Network.** Keep the load generator on a **separate VM in the same placement group /
availability zone** (low-latency, same rack where possible), not co-resident. Co-locating
the client steals vCPUs *and* routes traffic through the virtual switch back to the same
host — distorting the very network-cost signal Q1 depends on.

**Two clean setups that work:**
1. *Best VM option:* one bare-metal instance, Valkey pinned, `valkey-benchmark` on a second
   instance in the same AZ.
2. *Acceptable:* two dedicated-vCPU VMs (server + client) in the same placement group, steal
   time verified ~0, instance type recorded.

**Containers on a VM** inherit all of the above **plus** the Docker caveats: use
`--network=host` (skip the bridge/NAT hop), `--cpuset-cpus` for pinning (never the `--cpus`
quota), profile with `perf` **from the host/guest against the container PID** rather than
inside the container, and bind-mount a real volume for RDB writes instead of the overlay
layer. A host-network, cpuset-pinned container on a dedicated VM is fine; Docker Desktop on
macOS is not (Linux-in-a-VM with virtualized CPU/network and no real `perf`).

## 3. Workload matrix

Run the matrix; the *shape* of how metrics move across it is more informative than any
single cell.

| Axis | Values | Why it matters |
|---|---|---|
| **Command mix** | 100% GET; 100% SET; 80/20 GET/SET | Read vs write stresses different paths (lookup vs insert+propagation) |
| **Value size** | 16 B; 512 B; 8 KB | Small = per-op overhead dominates (exec-bound signal); large = bytes/copies dominate (I/O signal) |
| **Pipeline depth** | 1; 16 | Depth 1 maximizes syscall-per-op (I/O signal); deep pipelining amortizes syscalls and exposes execution cost |
| **Keyspace / dataset** | fits-in-cache; ≫ L3 (multi-GB) | Cache-resident vs memory-latency-bound lookup |
| **Connections** | 50; 500 | Event-loop and I/O-thread scaling |
| **io-threads** | 1; 4; 8 (sweep) | The **most important axis for Q1** (see §5.C) |

Tools: `valkey-benchmark -t get,set -c <conns> -P <pipe> -d <size> -r <keyspace> -n <N>
--threads <t>`, and/or `memtier_benchmark` for realistic ratios and Gaussian key
distributions. Use both if possible; agreement raises confidence.

## 4. The two numbers (restated concretely)

1. **Keyspace-work fraction of main-thread cycles** — the share of a saturated main
   thread spent in command dispatch + hashtable lookup/insert + reply construction +
   propagation, *excluding* network syscalls and event-loop overhead. This is the
   **Amdahl ceiling** on slot-per-thread: if it's small, parallelizing it can't help much.
2. **Fork p99 stall + peak COW RSS amplification** on a large write-heavy instance — sizes
   fork-less RDB.

## 5. Instrumentation

### 5.A Main-thread saturation and time split (Q1 core)

- **Is the main thread the bottleneck?** `top -H -p <pid>` / `pidstat -t -p <pid> 1` —
  watch per-thread CPU. If the **main thread sits at ~100%** while throughput plateaus →
  execution- or single-thread-bound. If it has **idle headroom** at plateau → the limit is
  elsewhere (I/O, locks, client).
- **Where do the cycles go?** `perf record -g -p <pid> -- sleep 20` then `perf report`, or a
  flamegraph (`perf script | stackcollapse-perf.pl | flamegraph.pl`). Bucket the main
  thread's stacks into:
  - **keyspace/execution:** `lookupKey*`, `hashtable*`, command procs, `call()`,
    `createObject`/reply building, `propagate*`
  - **networking/I/O:** `read`/`write`/`send`/`recv`, `epoll_wait`, `connSocket*`,
    `writeToClient`, buffer copies
  - **other:** expiry/eviction sampling, cron, allocator
  The keyspace/execution share is number (1).

### 5.B Syscall profile (Q1 corroboration)

`perf stat -e syscalls:sys_enter_read,syscalls:sys_enter_write,syscalls:sys_enter_epoll_wait
-p <pid> sleep 10`, or `strace -c -f -p <pid>` over a short window. High time in
`read`/`write`/`epoll_wait` relative to useful work = I/O-bound. Cross-check: **syscalls per
operation** — if it's ~O(1) per request at pipeline depth 1 and drops sharply with deep
pipelining, the syscall overhead (not execution) was the wall.

### 5.C The io-threads sweep (the decisive experiment for Q1)

Valkey already offloads socket I/O to a worker pool (`src/io_threads.c`,
`design-docs/io-threads.md`). Sweep `io-threads` 1 → 4 → 8 on a read-heavy, small-value,
low-pipeline workload and plot throughput:

- **Throughput keeps scaling with io-threads, then flattens when the main thread hits
  100%** → the residual bottleneck is the **single execution thread** → **Q1 = execution-bound**
  → slot-per-thread is the justified next big bet.
- **Throughput flattens early while the main thread still has idle time**, or scales almost
  linearly with io-threads with no main-thread wall → the limit is **I/O plumbing** →
  **Q1 = I/O-bound** → prioritize the io_uring backend (D5), not slot-per-thread.

This sweep, more than any single profile, separates the two roadmap branches.

### 5.D Fork cost (Q2)

- **Fork stall:** `INFO stats` → **`latest_fork_usec`** (and `total_forks`); trigger
  `BGSAVE` at several dataset sizes (1 GB, 8 GB, 32 GB) and record the fork duration curve.
  This is the hard event-loop stall.
- **COW amplification:** during a `BGSAVE` run under a sustained write load, sample
  `INFO` → **`current_cow_size`** (live) and **`rdb_last_cow_size`** (final)
  (`src/server.c:6413,6425`) against total RSS. Peak COW / RSS is the memory-amplification
  figure that drives 2× over-provisioning.
- **Latency during save:** run `valkey-cli --latency-history` (or the `LATENCY` monitor,
  `latency-monitor-threshold`) through a `BGSAVE` and compare p99/p100 during the save vs
  steady state. The delta is what fork-less RDB must beat, and its acceptance target.

## 6. Decision table

Starting heuristics — calibrate to the actual hardware, but decide *before* looking so the
thresholds aren't rationalized after the fact:

| Observation | Read as | Next feature |
|---|---|---|
| Main thread ~100%, keyspace/exec ≳ 50% of its cycles, io-threads sweep flattens at the main-thread wall | Execution-bound | **Slot-per-thread** (Stage 4) — high ceiling |
| Main thread < ~100% at plateau, or network syscalls dominate the profile, or throughput scales ~linearly with io-threads | I/O-bound | **io_uring backend** (Stage 5 / D5); shelve Stage 4 |
| Keyspace/exec share is middling (~30–50%) | Ambiguous ceiling | Re-run with deeper pipelining + larger dataset before committing; lean D5-first |
| `latest_fork_usec` in tens–hundreds of ms and/or peak COW a large fraction of RSS | Fork is a real tax | **Fork-less RDB** — build it regardless; these numbers set its targets |

**Independent of Q1, fork-less RDB is the recommended first thing to ship** (it's standalone
and low-risk); Q1 decides which *big* bet — Stage 4 or D5 — comes after it.

## 7. Deliverable

A short report, committed alongside these notes, containing:

- The environment block (§2) and the workload matrix results (§3) as a table.
- One flamegraph per representative workload, with the three-way cycle split (§5.A).
- The io-threads scaling plot (§5.C) — the single most important artifact.
- The fork-cost curves (§5.D): `latest_fork_usec` vs dataset size, peak COW/RSS, and the
  p99-during-save delta.
- A one-paragraph verdict naming the next feature per §6, written to be falsifiable.

## 8. Pitfalls

- **Client-bound instead of server-bound.** If `valkey-benchmark` can't push enough load,
  you'll measure the client. Verify the server core is actually saturated; add benchmark
  threads / a second load box until it is.
- **Measuring a cache-resident toy dataset** and concluding "execution-bound" when
  production is memory-latency-bound. Include the ≫ L3 dataset cell.
- **Leaving AOF/save on during pure-exec runs**, contaminating the profile with I/O and
  fork. Isolate (§2).
- **Single-run conclusions.** Repeat; report spread. Frequency scaling and noisy neighbors
  move these numbers more than people expect.
- **Deciding thresholds after seeing results.** Write §6's cut lines down first.

## 9. References

- What this gates: [proposal-dragonfly-inspired-perf.md](proposal-dragonfly-inspired-perf.md) §5, [proposal-slot-per-thread.md](proposal-slot-per-thread.md)
- What it sizes: [proposal-forkless-rdb.md](proposal-forkless-rdb.md), [proposal-dashtable-adoption.md](proposal-dashtable-adoption.md)
- I/O threads baseline: `design-docs/io-threads.md`, `src/io_threads.c`
- INFO fields: `src/server.c:6413-6425` (`current_cow_size`, `rdb_last_cow_size`); `latest_fork_usec` in `INFO stats`
