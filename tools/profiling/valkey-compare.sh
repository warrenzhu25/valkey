#!/usr/bin/env bash
#
# valkey-compare.sh — automated A/B perf comparison of two Valkey runs.
#
# Captures, for each run, three comparable layers:
#   1. outcome metrics   — valkey-benchmark --csv (rps, p50, p99), median of N repeats
#   2. hardware counters — perf stat (cycles, instructions/IPC, cache/branch misses)
#   3. stack attribution — perf record -> collapsed stacks (for perf diff + diff flame)
#
# Then 'diff' produces:
#   - a metrics table with % deltas
#   - a perf-stat delta (IPC, miss rates)
#   - perf diff  (per-symbol Delta column)
#   - a differential flame graph .svg (red=hotter in B, blue=colder)
#
# Usage:
#   ./valkey-compare.sh run  base   unstable          # a git ref, tag, or sha ...
#   ./valkey-compare.sh run  cand   my-feature-branch # ... or an absolute binary path
#   ./valkey-compare.sh diff base cand
#   ./valkey-compare.sh ab   unstable my-feature-branch   # build+run both, then diff
#
set -euo pipefail

VALKEY_REPO="${VALKEY_REPO:-https://github.com/valkey-io/valkey.git}"
ROOT="${ROOT:-$HOME/valkey-ab}"           # per-label builds + results live here
FLAMEGRAPH_DIR="${FLAMEGRAPH_DIR:-/opt/FlameGraph}"
PORT="${PORT:-7777}"
PIN_CORE="${PIN_CORE:-1}"                  # server core (both runs use the SAME core)
BENCH_CORE="${BENCH_CORE:-2}"
DURATION="${DURATION:-30}"                 # perf capture window (secs)
REPEATS="${REPEATS:-3}"                    # benchmark repeats; median is reported
FREQ="${FREQ:-99}"
CFLAGS_PROF="-O2 -fno-omit-frame-pointer -g -funwind-tables"

SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo"
log() { printf '\033[1;32m==>\033[0m %s\n' "$*"; }
die() { printf '\033[1;31mERR\033[0m %s\n' "$*" >&2; exit 1; }
median() { sort -n | awk '{a[NR]=$1} END{print (NR%2)?a[(NR+1)/2]:(a[NR/2]+a[NR/2+1])/2}'; }

build_ref() {   # <label> <ref-or-binary> ; echoes path to valkey-server
  local label="$1" ref="$2" dir="$ROOT/$label"
  if [ -x "$ref" ]; then echo "$ref"; return; fi   # already a binary
  if [ ! -x "$dir/src/valkey-server" ]; then
    log "[$label] building Valkey @ $ref" >&2
    rm -rf "$dir"; git clone "$VALKEY_REPO" "$dir" >&2
    git -C "$dir" checkout "$ref" >&2
    make -C "$dir" -j"$(nproc)" BUILD_TLS=yes CFLAGS="$CFLAGS_PROF" >&2
  fi
  echo "$dir/src/valkey-server"
}

wait_ready() { for _ in $(seq 1 30); do "$1" -p "$PORT" ping >/dev/null 2>&1 && return; sleep 0.3; done; die "server not ready"; }

cmd_run() {   # <label> <ref-or-binary>
  local label="$1" ref="${2:?ref or binary required}"
  local out="$ROOT/$label/result"; mkdir -p "$out"
  local server; server="$(build_ref "$label" "$ref")"
  local cli="${server%server}cli" bench="${server%server}benchmark"

  log "[$label] starting server pinned to core $PIN_CORE"
  taskset -c "$PIN_CORE" "$server" --port "$PORT" --save '' --daemonize yes \
    --logfile "$out/valkey.log" --appendonly no
  wait_ready "$cli"
  local pid; pid="$(pgrep -x valkey-server | head -n1)"

  # --- metrics: median rps over REPEATS short benchmark runs ---
  log "[$label] measuring throughput/latency (${REPEATS}x)"
  : > "$out/rps.samples"
  for i in $(seq 1 "$REPEATS"); do
    taskset -c "$BENCH_CORE" "$bench" -p "$PORT" -t set,get -n 1000000 -r 1000000 \
      -P 16 -c 50 --threads 2 --csv > "$out/bench.$i.csv"
    # --csv rows: "cmd","rps","avg","min","p50","p95","p99","max"
    awk -F',' 'NR>0{gsub(/"/,"");print $2}' "$out/bench.$i.csv" | paste -sd+ - | bc >> "$out/rps.samples"
  done
  median < "$out/rps.samples" > "$out/rps.median"
  cp "$out/bench.$REPEATS.csv" "$out/bench.csv"

  # --- capture perf sampling + perf stat over one steady-state window ---
  log "[$label] capturing perf (record + stat) for ${DURATION}s under load"
  timeout $((DURATION+5)) taskset -c "$BENCH_CORE" "$bench" -p "$PORT" -t set,get \
    -n 100000000 -r 1000000 -P 16 -c 50 --threads 2 >/dev/null 2>&1 &
  local loadpid=$!
  sleep 2   # let it reach steady state
  $SUDO perf stat -x, -o "$out/stat.csv" \
    -e cycles,instructions,cache-references,cache-misses,branch-misses \
    -p "$pid" -- sleep "$DURATION" 2>/dev/null &
  $SUDO perf record -F "$FREQ" -g -p "$pid" -o "$out/perf.data" -- sleep "$DURATION"
  wait || true; kill "$loadpid" 2>/dev/null || true

  $SUDO perf script -i "$out/perf.data" 2>/dev/null \
    | "$FLAMEGRAPH_DIR/stackcollapse-perf.pl" > "$out/folded.txt"
  "$cli" -p "$PORT" INFO commandstats > "$out/commandstats.txt"
  "$cli" -p "$PORT" shutdown nosave 2>/dev/null || true
  log "[$label] done -> $out (median rps: $(cat "$out/rps.median"))"
}

stat_val() { awk -F',' -v e="$2" '$3==e{print $1}' "$1"; }  # perf stat -x, : val is col1, event col3

cmd_diff() {  # <labelA> <labelB>
  local a="$1" b="$2" A="$ROOT/$1/result" B="$ROOT/$2/result"
  [ -f "$A/perf.data" ] && [ -f "$B/perf.data" ] || die "run both labels first"
  local dst="$ROOT/diff-$a-vs-$b"; mkdir -p "$dst"

  # --- outcome metrics table ---
  local ra rb; ra="$(cat "$A/rps.median")"; rb="$(cat "$B/rps.median")"
  {
    echo "=== Throughput (median of $REPEATS runs, ops/sec) ==="
    printf "%-10s %15s\n" "$a" "$ra"
    printf "%-10s %15s\n" "$b" "$rb"
    awk -v x="$ra" -v y="$rb" 'BEGIN{printf "%-10s %14.1f%%\n","delta",(y-x)/x*100}'
    echo
    echo "=== Hardware counters (perf stat over ${DURATION}s) ==="
    for ev in instructions cycles cache-misses branch-misses; do
      printf "%-16s %s: %-14s  %s: %-14s\n" "$ev" \
        "$a" "$(stat_val "$A/stat.csv" "$ev")" "$b" "$(stat_val "$B/stat.csv" "$ev")"
    done
    awk -v ia="$(stat_val "$A/stat.csv" instructions)" -v ca="$(stat_val "$A/stat.csv" cycles)" \
        -v ib="$(stat_val "$B/stat.csv" instructions)" -v cb="$(stat_val "$B/stat.csv" cycles)" \
        'BEGIN{printf "%-16s %s: %-14.3f  %s: %-14.3f\n","IPC","'"$a"'",ia/ca,"'"$b"'",ib/cb}'
  } | tee "$dst/summary.txt"

  # --- per-symbol perf diff ---
  log "perf diff (Delta column = B relative to A) -> $dst/perf-diff.txt"
  $SUDO perf diff "$A/perf.data" "$B/perf.data" > "$dst/perf-diff.txt" 2>/dev/null || \
    log "note: perf diff needs matching build ids; symbol-level diff may be partial"
  head -30 "$dst/perf-diff.txt" || true

  # --- differential flame graph ---
  if [ -x "$FLAMEGRAPH_DIR/difffolded.pl" ]; then
    log "differential flame graph -> $dst/flame-diff.svg (red=hotter in $b)"
    "$FLAMEGRAPH_DIR/difffolded.pl" -n "$A/folded.txt" "$B/folded.txt" \
      | "$FLAMEGRAPH_DIR/flamegraph.pl" --title "$a -> $b (red=regressed)" > "$dst/flame-diff.svg"
  fi
  log "Comparison written to $dst/"
}

cmd_ab() {    # <refA> <refB>
  cmd_run base "$1"
  cmd_run cand "$2"
  cmd_diff base cand
}

case "${1:-}" in
  run)  shift; cmd_run "$@" ;;
  diff) shift; cmd_diff "$@" ;;
  ab)   shift; cmd_ab "$@" ;;
  *) cat <<EOF
valkey-compare.sh — automated A/B perf comparison

  ./valkey-compare.sh run  <label> <ref|binary>   capture one run (metrics+stat+stacks)
  ./valkey-compare.sh diff <labelA> <labelB>       metrics + perf diff + diff flame graph
  ./valkey-compare.sh ab   <refA> <refB>           build+run both, then diff (one shot)

Env: DURATION($DURATION) REPEATS($REPEATS) PORT($PORT) PIN_CORE($PIN_CORE) FREQ($FREQ)
Requires: FlameGraph in $FLAMEGRAPH_DIR (from valkey-profile.sh setup), perf, bpftrace.
EOF
    ;;
esac
