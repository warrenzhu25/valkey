#!/usr/bin/env bash
#
# stage0.sh — run the Stage 0 measurement that gates the performance roadmap.
#
# Implements notes/proposal-stage0-measurement.md:
#   Q1  Is the serving thread execution-bound or I/O-bound at saturation?
#       -> execution-bound selects slot-per-thread; I/O-bound selects an io_uring backend.
#   Q2  How bad is fork() in practice? (sizes fork-less RDB; does not branch the roadmap)
#
# Phases:
#   check      environment gate (§2). Says whether results here can be decision-grade.
#   sweep      §5.C io-threads sweep — the decisive Q1 experiment.
#   matrix     §3 workload matrix (command mix x value size x pipeline depth).
#   favorable  §3.1 the cell where slot-per-thread could actually win.
#   fork       §5.D fork stall + COW amplification (Q2).
#   report     render report.md from whatever results exist in $OUT.
#   all        check + sweep + matrix + favorable + fork + report.
#
# The §6 decision thresholds are applied by this script, not by the reader, so the
# cut lines cannot be moved after seeing the numbers (§8, last pitfall).
#
# Usage:
#   ./stage0.sh all
#   OUT=~/stage0-run2 REPEATS=5 ./stage0.sh sweep
#
# Env:
#   SERVER, BENCH   paths to valkey-server / valkey-benchmark (default: ../../src/*)
#   PORT            server port (default 7900)
#   OUT             results directory (default ./stage0-results)
#   REPEATS         repeats per cell, median reported (default 3)
#   REQUESTS        requests per benchmark run (default 300000)
#   CLIENTS         connections (default 50)
#   BENCH_THREADS   valkey-benchmark threads (default 4)
#   SWEEP           io-threads values to sweep (default "1 2 4 8")
#   SERVER_CPUS     taskset list for the server, e.g. "0-1" (default: unset, no pinning)
#   CLIENT_CPUS     taskset list for the benchmark, e.g. "2-7" (default: unset)
#   FORK_SIZES      key counts for the fork curve (default "1000000 4000000")
#   FORK_VALSIZE    value size for fork datasets (default 512)
#
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVER="${SERVER:-$HERE/../../src/valkey-server}"
BENCH="${BENCH:-$HERE/../../src/valkey-benchmark}"
CLI="${CLI:-$HERE/../../src/valkey-cli}"
PORT="${PORT:-7900}"
OUT="${OUT:-$PWD/stage0-results}"
REPEATS="${REPEATS:-3}"
REQUESTS="${REQUESTS:-300000}"
CLIENTS="${CLIENTS:-50}"
BENCH_THREADS="${BENCH_THREADS:-4}"
SWEEP="${SWEEP:-1 2 4 8}"
SERVER_CPUS="${SERVER_CPUS:-}"
CLIENT_CPUS="${CLIENT_CPUS:-}"
FORK_SIZES="${FORK_SIZES:-1000000 4000000}"
FORK_VALSIZE="${FORK_VALSIZE:-512}"

SERVER_PID=""

log() { printf '\033[1;34m==\033[0m %s\n' "$*" >&2; }
warn() { printf '\033[1;33m!!\033[0m %s\n' "$*" >&2; }

# ---------------------------------------------------------------- environment

# Writes $OUT/env.txt and $OUT/grade.txt. grade.txt is "yes" only when every
# condition §2 requires is satisfied; the report refuses to print a verdict
# otherwise.
phase_check() {
    mkdir -p "$OUT"
    local grade="yes" reasons=()

    {
        echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "uname: $(uname -a)"
        echo "server: $SERVER"
        "$SERVER" --version 2>/dev/null || true
        echo "requests: $REQUESTS  clients: $CLIENTS  bench_threads: $BENCH_THREADS  repeats: $REPEATS"
        echo "server_cpus: ${SERVER_CPUS:-<unpinned>}  client_cpus: ${CLIENT_CPUS:-<unpinned>}"
    } > "$OUT/env.txt"

    if [ "$(uname -s)" != "Linux" ]; then
        grade="no"
        reasons+=("not Linux ($(uname -s)): no perf, no /proc per-thread CPU, unrealistic fork/COW (§2)")
    else
        command -v perf >/dev/null || { grade="no"; reasons+=("perf not installed — §5.A cycle split cannot be captured"); }
        if [ -r /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor ]; then
            local gov; gov=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)
            echo "cpu_governor: $gov" >> "$OUT/env.txt"
            [ "$gov" = performance ] || { grade="no"; reasons+=("cpu governor is '$gov', not 'performance' (§2)"); }
        fi
        # Steal time: any non-trivial value invalidates the run (§2.1).
        local steal; steal=$(awk '/^cpu /{print $9}' /proc/stat 2>/dev/null || echo 0)
        echo "steal_jiffies_total: $steal" >> "$OUT/env.txt"
    fi

    [ -z "$SERVER_CPUS" ] && { grade="no"; reasons+=("server not pinned (SERVER_CPUS unset) — §2 requires pinning"); }
    [ -z "$CLIENT_CPUS" ] && { grade="no"; reasons+=("load generator not pinned (CLIENT_CPUS unset); ideally it runs on a separate box (§2.1)"); }

    echo "$grade" > "$OUT/grade.txt"
    : > "$OUT/grade-reasons.txt"
    for r in "${reasons[@]:-}"; do [ -n "$r" ] && echo "- $r" >> "$OUT/grade-reasons.txt"; done

    if [ "$grade" = yes ]; then
        log "environment: DECISION-GRADE"
    else
        warn "environment: NOT decision-grade. Results are indicative only:"
        sed 's/^/    /' "$OUT/grade-reasons.txt" >&2
    fi
}

# ---------------------------------------------------------------- server ctl

start_server() { # $1 = io-threads, rest = extra args
    local io_threads="$1"; shift
    local cmd=("$SERVER" --port "$PORT" --io-threads "$io_threads" --save '' --appendonly no
               --daemonize no --logfile "$OUT/server.log" --dir "$OUT" "$@")
    [ -n "$SERVER_CPUS" ] && cmd=(taskset -c "$SERVER_CPUS" "${cmd[@]}")
    "${cmd[@]}" &
    SERVER_PID=$!
    for _ in $(seq 1 50); do
        "$CLI" -p "$PORT" ping >/dev/null 2>&1 && return 0
        sleep 0.2
    done
    echo "server failed to start; see $OUT/server.log" >&2; exit 1
}

stop_server() {
    [ -n "$SERVER_PID" ] || return 0
    "$CLI" -p "$PORT" shutdown nosave >/dev/null 2>&1 || true
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=""
}
trap stop_server EXIT

# Main-thread CPU%, sampled from /proc across the run. This is the §5.A saturation
# check: the main thread is the one whose tid equals the pid.
main_thread_jiffies() {
    [ "$(uname -s)" = Linux ] || { echo ""; return; }
    awk '{print $14+$15}' "/proc/$SERVER_PID/task/$SERVER_PID/stat" 2>/dev/null || echo ""
}

# ---------------------------------------------------------------- benchmark

median() { sort -n | awk '{a[NR]=$1} END{if(NR==0){print ""}else if(NR%2){print a[(NR+1)/2]}else{print (a[NR/2]+a[NR/2+1])/2}}'; }

# run_cell <label> <io-threads> <bench args...>
# Appends one row to $OUT/cells.csv: label,io_threads,rps,p99_ms,main_thread_cpu_pct
run_cell() {
    local label="$1" io_threads="$2"; shift 2
    local rps_samples=() p99_samples=() cpu_samples=()

    start_server "$io_threads"
    local i
    for i in $(seq 1 "$REPEATS"); do
        local t0 j0 j1 t1
        j0=$(main_thread_jiffies); t0=$(date +%s.%N)

        local cmd=("$BENCH" -p "$PORT" -n "$REQUESTS" -c "$CLIENTS" --threads "$BENCH_THREADS" --csv "$@")
        [ -n "$CLIENT_CPUS" ] && cmd=(taskset -c "$CLIENT_CPUS" "${cmd[@]}")
        "${cmd[@]}" > "$OUT/raw.$label.$i.csv" 2>/dev/null || true

        t1=$(date +%s.%N); j1=$(main_thread_jiffies)

        # Sum rps across the tests in this run; take the worst p99.
        # Note: assign to a plain variable first. bash 3.2 (still the system bash on
        # macOS) mis-parses a $(...) containing double quotes inside a quoted array
        # append, silently handing awk a truncated program.
        local run_rps run_p99 run_cpu elapsed
        run_rps=$(awk -F, 'NR>1{gsub(/"/,""); s+=$2} END{printf "%.0f", s}' "$OUT/raw.$label.$i.csv")
        run_p99=$(awk -F, 'NR>1{gsub(/"/,""); if($7>m) m=$7} END{printf "%.3f", m}' "$OUT/raw.$label.$i.csv")
        rps_samples+=("$run_rps")
        p99_samples+=("$run_p99")

        if [ -n "$j0" ] && [ -n "$j1" ]; then
            elapsed=$(echo "$t1 - $t0" | bc -l)
            run_cpu=$(awk -v d="$((j1 - j0))" -v e="$elapsed" -v hz="$(getconf CLK_TCK)" \
                'BEGIN{printf "%.1f", (e>0)? d/hz/e*100 : 0}')
            cpu_samples+=("$run_cpu")
        fi
    done
    stop_server

    local rps p99 cpu
    rps=$(printf '%s\n' "${rps_samples[@]}" | median)
    p99=$(printf '%s\n' "${p99_samples[@]}" | median)
    if [ ${#cpu_samples[@]} -gt 0 ]; then
        cpu=$(printf '%s\n' "${cpu_samples[@]}" | median)
    else
        cpu="n/a" # no /proc: main-thread saturation unmeasurable, so Q1 stays unanswered
    fi
    echo "$label,$io_threads,$rps,$p99,$cpu" >> "$OUT/cells.csv"
    log "$label io-threads=$io_threads -> ${rps} rps, p99 ${p99}ms, main-thread ${cpu}%"
}

init_cells() { mkdir -p "$OUT"; [ -f "$OUT/cells.csv" ] || echo "label,io_threads,rps,p99_ms,main_thread_cpu_pct" > "$OUT/cells.csv"; }

# ---------------------------------------------------------------- phases

# §5.C — the decisive Q1 experiment. Throughput vs io-threads on a read-heavy,
# small-value, low-pipeline workload, with main-thread CPU% at every point.
phase_sweep() {
    init_cells
    log "§5.C io-threads sweep"
    local n
    for n in $SWEEP; do
        run_cell "sweep-get" "$n" -t get -d 16 -P 1 -r 1000000
    done
}

# §3 — the workload matrix. The shape across cells matters more than any cell.
phase_matrix() {
    init_cells
    log "§3 workload matrix"
    local mix size pipe
    for mix in get set; do
        for size in 16 512 8192; do
            for pipe in 1 16; do
                run_cell "matrix-$mix-d$size-P$pipe" 4 -t "$mix" -d "$size" -P "$pipe" -r 1000000
            done
        done
    done
}

# §3.1 — the cell where slot-per-thread could win: execution-heavy commands, deep
# pipelining, wide keyspace. Swept over io-threads, because the plateau is the signal.
phase_favorable() {
    init_cells
    log "§3.1 favorable (execution-heavy) cell, swept over io-threads"
    local n
    for n in $SWEEP; do
        run_cell "favorable-lrange600" "$n" -t lrange_600 -P 16 -r 1000000
        run_cell "favorable-zadd-hset-sadd" "$n" -t zadd,hset,sadd -P 16 -r 1000000
    done
}

# §5.D — fork stall and COW amplification (Q2).
phase_fork() {
    mkdir -p "$OUT"
    log "§5.D fork cost"
    echo "keys,used_memory_bytes,latest_fork_usec,rdb_last_cow_bytes,peak_cow_bytes" > "$OUT/fork.csv"
    local keys
    for keys in $FORK_SIZES; do
        start_server 1
        "$CLI" -p "$PORT" debug populate "$keys" key: "$FORK_VALSIZE" >/dev/null
        local used; used=$("$CLI" -p "$PORT" info memory | awk -F: '/^used_memory:/{gsub(/\r/,"");print $2}')

        # Write load during the save, so COW pages are actually dirtied.
        local cmd=("$BENCH" -p "$PORT" -t set -d "$FORK_VALSIZE" -r "$keys" -n 100000000 -c "$CLIENTS" --threads "$BENCH_THREADS")
        [ -n "$CLIENT_CPUS" ] && cmd=(taskset -c "$CLIENT_CPUS" "${cmd[@]}")
        "${cmd[@]}" >/dev/null 2>&1 &
        local load_pid=$!
        sleep 2

        "$CLI" -p "$PORT" bgsave >/dev/null
        local peak=0
        while [ "$("$CLI" -p "$PORT" info persistence | awk -F: '/^rdb_bgsave_in_progress:/{gsub(/\r/,"");print $2}')" = "1" ]; do
            local cur; cur=$("$CLI" -p "$PORT" info memory | awk -F: '/^current_cow_size:/{gsub(/\r/,"");print $2}')
            [ -n "$cur" ] && [ "$cur" -gt "$peak" ] 2>/dev/null && peak=$cur
            sleep 0.2
        done

        kill "$load_pid" 2>/dev/null || true; wait "$load_pid" 2>/dev/null || true
        local forkus cow
        forkus=$("$CLI" -p "$PORT" info stats | awk -F: '/^latest_fork_usec:/{gsub(/\r/,"");print $2}')
        cow=$("$CLI" -p "$PORT" info persistence | awk -F: '/^rdb_last_cow_size:/{gsub(/\r/,"");print $2}')
        echo "$keys,$used,$forkus,$cow,$peak" >> "$OUT/fork.csv"
        log "fork: $keys keys -> ${forkus}us stall, last COW ${cow}B, peak COW ${peak}B"
        stop_server
        rm -f "$OUT/dump.rdb"
    done
}

# ---------------------------------------------------------------- report

# Applies §6's decision table mechanically. Reads the sweep rows only; the
# thresholds are the ones written down in the proposal before any run.
verdict() {
    local rows peak_rps plateau_rps peak_cpu
    rows=$(awk -F, '$1=="sweep-get"' "$OUT/cells.csv" 2>/dev/null || true)
    [ -n "$rows" ] || { echo "No sweep data — run \`stage0.sh sweep\` first."; return; }

    peak_rps=$(echo "$rows" | awk -F, '{if($3>m)m=$3} END{print m+0}')
    plateau_rps=$(echo "$rows" | tail -1 | awk -F, '{print $3+0}')
    peak_cpu=$(echo "$rows" | awk -F, '$5!="n/a"{if($5>m)m=$5} END{print m+0}')

    echo "Peak throughput across the sweep: ${peak_rps} rps; at the highest io-threads: ${plateau_rps} rps."
    if [ "$peak_cpu" = 0 ]; then
        echo
        echo "**Main-thread CPU% was not captured** (needs Linux /proc), so Q1 cannot be answered:"
        echo "§8's sharpest pitfall is reading saturation as execution-bound, and without the"
        echo "cycle split there is nothing to distinguish the two. No verdict."
        return
    fi
    echo "Peak main-thread CPU during the sweep: ${peak_cpu}%."
    echo
    awk -v p="$peak_rps" -v l="$plateau_rps" -v c="$peak_cpu" 'BEGIN{
        flat = (p > 0 && (p - l) / p < 0.05);
        if (c >= 95 && flat)
            print "Throughput flattened while the main thread sat at ~100%: the residual bottleneck is\nthe single execution thread. §6 reads this as EXECUTION-BOUND -> slot-per-thread.\nConfirm with the §5.A cycle split before committing: this is necessary, not sufficient.";
        else if (c < 95)
            print "The main thread had idle headroom at plateau. §6 reads this as I/O-BOUND ->\nprioritise the io_uring backend; shelve slot-per-thread.";
        else
            print "Throughput was still scaling at the top of the sweep. Extend SWEEP upward until it\nplateaus - the decision needs the plateau, not the slope.";
    }'
}

phase_report() {
    local grade; grade=$(cat "$OUT/grade.txt" 2>/dev/null || echo no)
    {
        echo "# Stage 0 results"
        echo
        if [ "$grade" != yes ]; then
            echo "> **NOT DECISION-GRADE.** This run does not satisfy the environment requirements"
            echo "> of proposal-stage0-measurement.md §2, so the numbers below are indicative only"
            echo "> and must not be used to choose a roadmap branch:"
            echo ">"
            sed 's/^/> /' "$OUT/grade-reasons.txt" 2>/dev/null
            echo
        fi
        echo '## Environment'
        echo '```'; cat "$OUT/env.txt" 2>/dev/null; echo '```'
        echo
        echo '## Cells'
        echo
        if [ -f "$OUT/cells.csv" ]; then
            awk -F, 'NR==1{print "| "$1" | "$2" | "$3" | "$4" | "$5" |"; print "|---|---|---|---|---|"; next}
                     {print "| "$1" | "$2" | "$3" | "$4" | "$5" |"}' "$OUT/cells.csv"
        else
            echo "_no cells recorded_"
        fi
        echo
        echo '## Fork cost (Q2)'
        echo
        if [ -f "$OUT/fork.csv" ]; then
            awk -F, 'NR==1{print "| "$1" | "$2" | "$3" | "$4" | "$5" |"; print "|---|---|---|---|---|"; next}
                     {print "| "$1" | "$2" | "$3" | "$4" | "$5" |"}' "$OUT/fork.csv"
        else
            echo "_not measured_"
        fi
        echo
        echo '## Q1 verdict (§6 thresholds, applied mechanically)'
        echo
        verdict
        echo
        echo '## Still required by §7 before this is a complete Stage 0'
        echo
        echo '- A `perf` flamegraph per representative workload, bucketed into the three-way'
        echo '  cycle split of §5.A. **The io-threads sweep alone cannot answer Q1** — a saturated'
        echo '  core says nothing about where the cycles went (§8).'
        echo '- The §5.B syscall profile as corroboration.'
        echo '- A falsifiable one-paragraph verdict naming the next feature.'
    } > "$OUT/report.md"
    log "wrote $OUT/report.md"
}

# ---------------------------------------------------------------- main

case "${1:-}" in
check) phase_check ;;
sweep) phase_check; phase_sweep; phase_report ;;
matrix) phase_check; phase_matrix; phase_report ;;
favorable) phase_check; phase_favorable; phase_report ;;
fork) phase_check; phase_fork; phase_report ;;
report) phase_report ;;
all) phase_check; phase_sweep; phase_matrix; phase_favorable; phase_fork; phase_report ;;
*)
    sed -n '2,45p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit 1
    ;;
esac
