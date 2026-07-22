# Valkey profiling tools

Helper scripts for CPU-profiling Valkey on a self-hosted cloud VM — the kind of
engine-internals analysis you *can't* do on a managed service (Memorystore,
ElastiCache), since those give you no host access, no `perf`, no shell.

| Script | Runs on | Purpose |
|--------|---------|---------|
| `valkey-profile.sh` | any Linux VM (apt/dnf/yum, x86 or arm64) | install toolchain, build Valkey with frame pointers, tune host, capture on-CPU flame graphs and off-CPU stalls |
| `provision-gcp.sh` | your workstation (needs `gcloud`) | create a dedicated-vCPU GCE VM (`c3`, non-burstable), copy the profiler over, run setup |
| `valkey-compare.sh` | the profiling VM | automated A/B comparison of two builds/refs: metrics + `perf stat` + `perf diff` + differential flame graph |
| `stage0.sh` | the profiling VM | the Stage 0 roadmap gate: io-threads sweep, workload matrix, fork/COW cost, and a report that applies the decision thresholds mechanically |

## Quick start

```bash
# 1. provision a dedicated-vCPU box (paid; prints teardown cmd) — or bring your own VM
./provision-gcp.sh

# 2. on the VM: build + tune (if you didn't use provision-gcp.sh)
sudo ./valkey-profile.sh setup

# 3. single-run flame graph
./valkey-profile.sh start
./valkey-profile.sh load 120 &
sudo ./valkey-profile.sh flame 30      # -> ~/valkey-profiles/flame-*.svg

# 4. A/B two commits/branches
./valkey-compare.sh ab unstable my-feature-branch
#   -> ~/valkey-ab/diff-base-vs-cand/{summary.txt,perf-diff.txt,flame-diff.svg}

# 5. Stage 0 — the measurement that gates the performance roadmap
SERVER_CPUS=0-1 CLIENT_CPUS=2-7 ./stage0.sh all
#   -> ./stage0-results/report.md
```

## Stage 0

`stage0.sh` implements [`notes/proposal-stage0-measurement.md`](../../notes/proposal-stage0-measurement.md):
the io-threads sweep (§5.C) that separates the two roadmap branches, the workload
matrix (§3), the execution-heavy cell where slot-per-thread could win (§3.1), and
fork stall + COW amplification (§5.D).

Two things it does deliberately:

- **It gates itself.** `stage0.sh check` verifies the §2 environment requirements —
  Linux, `perf` present, `performance` governor, both server and load generator
  pinned. If any fail, every report carries a `NOT DECISION-GRADE` banner. Running it
  on a laptop produces numbers, and the report says plainly that they cannot choose a
  roadmap branch.
- **It applies the §6 thresholds itself**, so the cut lines cannot be moved after
  seeing the results. It refuses to give a verdict at all when main-thread CPU% is
  unavailable, because throughput alone cannot distinguish execution-bound from
  I/O-bound — the sharpest pitfall in the whole exercise.

The sweep is necessary but **not sufficient**: Q1 needs the §5.A cycle split from a
flame graph (`valkey-profile.sh flame`) to say where the cycles actually went. The
report lists what is still missing.

## Notes

- **Use a dedicated (non-burstable) vCPU** — burstable instances (AWS `t*`, GCP
  `e2-micro`, Azure `B*`) throttle mid-capture and corrupt measurements. Good
  free option: OCI Ampere A1 (4 dedicated cores). x86 match for Memorystore:
  GCP `c3`/`c2`.
- **Pin the server** with `taskset` (the scripts do this) — Valkey's data path is
  effectively single-threaded, so consistent core placement matters.
- **Comparisons are noisy** — bump `REPEATS`, trust `summary.txt` (metrics/IPC)
  first, and treat the stack-level diffs as hypotheses to confirm by re-running.
- Each script prints usage with no arguments; config is via env vars documented
  in the headers.
