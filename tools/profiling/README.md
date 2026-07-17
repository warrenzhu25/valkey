# Valkey profiling tools

Helper scripts for CPU-profiling Valkey on a self-hosted cloud VM — the kind of
engine-internals analysis you *can't* do on a managed service (Memorystore,
ElastiCache), since those give you no host access, no `perf`, no shell.

| Script | Runs on | Purpose |
|--------|---------|---------|
| `valkey-profile.sh` | any Linux VM (apt/dnf/yum, x86 or arm64) | install toolchain, build Valkey with frame pointers, tune host, capture on-CPU flame graphs and off-CPU stalls |
| `provision-gcp.sh` | your workstation (needs `gcloud`) | create a dedicated-vCPU GCE VM (`c3`, non-burstable), copy the profiler over, run setup |
| `valkey-compare.sh` | the profiling VM | automated A/B comparison of two builds/refs: metrics + `perf stat` + `perf diff` + differential flame graph |

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
```

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
