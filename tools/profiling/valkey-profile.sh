#!/usr/bin/env bash
#
# valkey-profile.sh — build + tune + profile Valkey on any Linux cloud VM.
#
# Cloud-agnostic: works on GCP, OCI, AWS, Azure, or bare metal. Detects the
# package manager (apt/dnf/yum) and the CPU arch (x86_64 or arm64) itself.
#
# Subcommands:
#   setup    Install toolchain + build Valkey (frame pointers) + tune the host.
#   start    Launch valkey-server pinned to one core (dedicated-vCPU friendly).
#   stop     Stop the pinned server.
#   load     Drive load with valkey-benchmark (so there's something to profile).
#   flame    Capture an on-CPU flame graph (perf) into an .svg.
#   offcpu   Capture an off-CPU stall profile (bpftrace) — fork/fsync/lock waits.
#   info     Dump the engine-side counters (commandstats/latencystats/slowlog).
#
# Usage:
#   sudo ./valkey-profile.sh setup
#   ./valkey-profile.sh start
#   ./valkey-profile.sh load &        # generate traffic
#   sudo ./valkey-profile.sh flame 30 # 30s flame graph
#
set -euo pipefail

# ---- config (override via env) ---------------------------------------------
VALKEY_REPO="${VALKEY_REPO:-https://github.com/valkey-io/valkey.git}"
VALKEY_REF="${VALKEY_REF:-unstable}"
PREFIX="${PREFIX:-$HOME/valkey}"           # where we clone/build
FLAMEGRAPH_DIR="${FLAMEGRAPH_DIR:-/opt/FlameGraph}"
PORT="${PORT:-6379}"
PIN_CORE="${PIN_CORE:-1}"                  # physical core to pin the main thread to
OUTDIR="${OUTDIR:-$HOME/valkey-profiles}"
FREQ="${FREQ:-99}"                         # perf sampling Hz (99 avoids lockstep)

SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo"
log() { printf '\033[1;32m==>\033[0m %s\n' "$*"; }
die() { printf '\033[1;31mERR\033[0m %s\n' "$*" >&2; exit 1; }

pkg_install() {
  if command -v apt-get >/dev/null; then
    $SUDO apt-get update -y
    $SUDO DEBIAN_FRONTEND=noninteractive apt-get install -y "$@"
  elif command -v dnf >/dev/null; then
    $SUDO dnf install -y "$@"
  elif command -v yum >/dev/null; then
    $SUDO yum install -y "$@"
  else
    die "no supported package manager (apt/dnf/yum) found"
  fi
}

# ---- setup -----------------------------------------------------------------
install_build_deps() {
  log "Installing build + profiling toolchain"
  if command -v apt-get >/dev/null; then
    pkg_install build-essential git pkg-config libssl-dev tcl \
                bpftrace linux-tools-common linux-tools-generic || true
    # cloud kernels (linux-gcp/-aws/-azure) ship perf in a version-matched pkg
    $SUDO apt-get install -y "linux-tools-$(uname -r)" 2>/dev/null || \
      log "note: linux-tools-$(uname -r) unavailable; 'perf' may need a matching pkg"
  else
    pkg_install gcc make git pkgconfig openssl-devel tcl bpftrace perf || \
      pkg_install gcc make git openssl-devel perf
  fi
}

build_valkey() {
  log "Building Valkey ($VALKEY_REF) with frame pointers for clean stacks"
  [ -d "$PREFIX/.git" ] || git clone --depth 1 -b "$VALKEY_REF" "$VALKEY_REPO" "$PREFIX"
  # -fno-omit-frame-pointer: reliable stack unwinding under load without DWARF.
  # -g + funwind-tables: symbols/unwind info so perf resolves Valkey frames.
  make -C "$PREFIX" -j"$(nproc)" BUILD_TLS=yes \
    CFLAGS="-O2 -fno-omit-frame-pointer -g -funwind-tables"
  log "Built: $PREFIX/src/valkey-server"
}

install_flamegraph() {
  [ -d "$FLAMEGRAPH_DIR" ] && return
  log "Installing FlameGraph into $FLAMEGRAPH_DIR"
  $SUDO git clone --depth 1 https://github.com/brendangregg/FlameGraph "$FLAMEGRAPH_DIR"
}

tune_host() {
  log "Tuning host for profiling + Valkey"
  # Let perf see kernel symbols and full stacks.
  $SUDO sysctl -w kernel.perf_event_paranoid=-1 >/dev/null
  $SUDO sysctl -w kernel.kptr_restrict=0        >/dev/null
  # Valkey/Redis recommendations: allow fork-based BGSAVE, disable THP jitter.
  $SUDO sysctl -w vm.overcommit_memory=1        >/dev/null
  if [ -w /sys/kernel/mm/transparent_hugepage/enabled ] 2>/dev/null; then
    echo never | $SUDO tee /sys/kernel/mm/transparent_hugepage/enabled >/dev/null || true
  fi
  # Pin CPU to a fixed clock if the cloud exposes the governor (often it doesn't
  # on virtualized guests — that's fine, dedicated vCPUs are already stable).
  if command -v cpupower >/dev/null; then
    $SUDO cpupower frequency-set -g performance >/dev/null 2>&1 || \
      log "note: CPU governor not settable on this guest (normal on cloud VMs)"
  fi
}

cmd_setup() {
  install_build_deps
  build_valkey
  install_flamegraph
  tune_host
  mkdir -p "$OUTDIR"
  log "Setup complete. Next: ./valkey-profile.sh start"
}

# ---- run / load ------------------------------------------------------------
server_pid() { pgrep -x valkey-server | head -n1 || true; }

cmd_start() {
  [ -x "$PREFIX/src/valkey-server" ] || die "run 'setup' first"
  [ -n "$(server_pid)" ] && die "valkey-server already running (pid $(server_pid))"
  log "Starting valkey-server pinned to core $PIN_CORE on port $PORT"
  # taskset pins the process so the single-threaded data path stays on one core
  # — essential for stable measurement on a dedicated vCPU.
  taskset -c "$PIN_CORE" "$PREFIX/src/valkey-server" \
    --port "$PORT" --save '' --daemonize yes \
    --logfile "$OUTDIR/valkey.log"
  sleep 1
  log "Running: pid $(server_pid). Log: $OUTDIR/valkey.log"
}

cmd_stop() {
  local pid; pid="$(server_pid)"
  [ -z "$pid" ] && { log "no valkey-server running"; return; }
  kill "$pid"; log "stopped pid $pid"
}

cmd_load() {
  local secs="${1:-60}"
  log "Driving load for ${secs}s (mixed SET/GET, pipelined)"
  # Pin the benchmark to a *different* core so it doesn't steal the server's.
  timeout "$secs" taskset -c "$((PIN_CORE+1))" \
    "$PREFIX/src/valkey-benchmark" -p "$PORT" -t set,get \
    -n 100000000 -r 1000000 -P 16 -c 50 --threads 2 || true
}

# ---- profile ---------------------------------------------------------------
cmd_flame() {
  local secs="${1:-30}" pid; pid="$(server_pid)"
  [ -z "$pid" ] && die "valkey-server not running (start it + generate load first)"
  command -v perf >/dev/null || die "perf not installed (rerun setup)"
  local out="$OUTDIR/flame-$(date +%Y%m%d-%H%M%S)"
  log "Recording on-CPU stacks for ${secs}s (pid $pid @ ${FREQ}Hz)"
  $SUDO perf record -F "$FREQ" -g -p "$pid" -o "$out.data" -- sleep "$secs"
  $SUDO perf script -i "$out.data" > "$out.stacks"
  if [ -d "$FLAMEGRAPH_DIR" ]; then
    "$FLAMEGRAPH_DIR/stackcollapse-perf.pl" "$out.stacks" \
      | "$FLAMEGRAPH_DIR/flamegraph.pl" --title "Valkey on-CPU" > "$out.svg"
    log "Flame graph: $out.svg"
  else
    log "Raw stacks: $out.stacks (FlameGraph dir missing; run 'perf report -i $out.data')"
  fi
}

cmd_offcpu() {
  local secs="${1:-30}" pid; pid="$(server_pid)"
  [ -z "$pid" ] && die "valkey-server not running"
  command -v bpftrace >/dev/null || die "bpftrace not installed (rerun setup)"
  local out="$OUTDIR/offcpu-$(date +%Y%m%d-%H%M%S).txt"
  log "Recording off-CPU stalls for ${secs}s (blocked/sleeping time, pid $pid)"
  # Aggregate kernel+user stacks by time spent OFF cpu — surfaces fork stalls,
  # fsync on AOF, epoll/mutex waits that on-CPU sampling can never see.
  $SUDO timeout "$secs" bpftrace -e "
    kprobe:finish_task_switch /pid == $pid/ {
      @start[tid] = nsecs;
    }
    kprobe:finish_task_switch /@start[tid]/ {
      \$us = (nsecs - @start[tid]) / 1000;
      @off_us[kstack, ustack] = sum(\$us);
      delete(@start[tid]);
    }" | tee "$out" >/dev/null || true
  log "Off-CPU profile: $out"
}

cmd_info() {
  local cli="$PREFIX/src/valkey-cli"
  log "Engine-side counters (same commands work against Memorystore too)"
  "$cli" -p "$PORT" INFO commandstats
  "$cli" -p "$PORT" INFO latencystats 2>/dev/null || true
  "$cli" -p "$PORT" SLOWLOG GET 25
  "$cli" -p "$PORT" LATENCY DOCTOR
}

# ---- dispatch --------------------------------------------------------------
case "${1:-}" in
  setup)  cmd_setup ;;
  start)  cmd_start ;;
  stop)   cmd_stop ;;
  load)   shift; cmd_load "$@" ;;
  flame)  shift; cmd_flame "$@" ;;
  offcpu) shift; cmd_offcpu "$@" ;;
  info)   cmd_info ;;
  *) cat <<EOF
valkey-profile.sh — build + tune + profile Valkey on any cloud VM

  sudo ./valkey-profile.sh setup       install toolchain, build Valkey, tune host
       ./valkey-profile.sh start       launch valkey-server pinned to core $PIN_CORE
       ./valkey-profile.sh load [secs] drive benchmark traffic (default 60s)
  sudo ./valkey-profile.sh flame [secs] on-CPU flame graph  (default 30s)
  sudo ./valkey-profile.sh offcpu [secs] off-CPU stall profile (default 30s)
       ./valkey-profile.sh info        dump commandstats/latencystats/slowlog
       ./valkey-profile.sh stop        stop the server

Env overrides: VALKEY_REF, PORT, PIN_CORE, OUTDIR, FREQ, PREFIX
EOF
    ;;
esac
