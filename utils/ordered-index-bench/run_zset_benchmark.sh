#!/usr/bin/env bash
#
# Compares an ordered-index backend (B-Tree or skiplist) for sorted sets:
# memory footprint for a large raw-encoded zset, plus ZADD/ZPOPMIN throughput.
#
# Usage:
#   run_zset_benchmark.sh <label> <path-to-valkey-server> <path-to-valkey-cli> <path-to-valkey-benchmark> [port]
#
# Writes <label>.json with the measured results into the current directory.
set -euo pipefail

LABEL="${1:?label required (e.g. skiplist, btree)}"
SERVER_BIN="${2:?path to valkey-server required}"
CLI_BIN="${3:?path to valkey-cli required}"
BENCH_BIN="${4:?path to valkey-benchmark required}"
PORT="${5:-7777}"
N="${N:-1000000}"
BENCH_REQUESTS="${BENCH_REQUESTS:-250000}"
BENCH_KEYSPACE="${BENCH_KEYSPACE:-250000}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKDIR="$(mktemp -d)"
LOGFILE="${WORKDIR}/server.log"

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]]; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  rm -rf "${WORKDIR}"
}
trap cleanup EXIT

CLI() { "${CLI_BIN}" -p "${PORT}" "$@"; }

start_server() {
  "${SERVER_BIN}" --port "${PORT}" --daemonize no --save "" --appendonly no \
    --logfile "${LOGFILE}" --dir "${WORKDIR}" &
  SERVER_PID=$!
  for _ in $(seq 1 100); do
    if CLI ping >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  echo "server failed to start" >&2
  cat "${LOGFILE}" >&2
  exit 1
}

start_server
CLI config set zset-max-listpack-entries 0 >/dev/null

echo "=== [$LABEL] loading ${N} elements into one zset (raw encoding) ===" >&2
python3 "${SCRIPT_DIR}/gen_zadd_protocol.py" myzset "${N}" | "${CLI_BIN}" -p "${PORT}" --pipe

CARD=$(CLI zcard myzset)
ENC=$(CLI object encoding myzset)
echo "zcard=${CARD} encoding=${ENC}" >&2

MEM_BYTES=$(CLI info memory | grep -m1 '^used_memory:' | tr -d '\r' | cut -d: -f2)
echo "used_memory bytes = ${MEM_BYTES}" >&2

echo "=== [$LABEL] valkey-benchmark ZADD (random keyspace) ===" >&2
ZADD_OUT=$("${BENCH_BIN}" -p "${PORT}" -n "${BENCH_REQUESTS}" -r "${BENCH_KEYSPACE}" -q -t zadd 2>&1)
echo "${ZADD_OUT}" >&2
ZADD_RPS=$(echo "${ZADD_OUT}" | tr '\r' '\n' | grep -m1 'requests per second' | grep -oE '[0-9]+\.[0-9]+ requests per second' | grep -oE '^[0-9]+\.[0-9]+')

echo "=== [$LABEL] restart clean + refill for ZPOPMIN ===" >&2
kill "${SERVER_PID}" 2>/dev/null || true
wait "${SERVER_PID}" 2>/dev/null || true
start_server
python3 "${SCRIPT_DIR}/gen_zadd_protocol.py" zpopzset "${BENCH_REQUESTS}" | "${CLI_BIN}" -p "${PORT}" --pipe

echo "=== [$LABEL] valkey-benchmark ZPOPMIN ===" >&2
ZPOP_OUT=$("${BENCH_BIN}" -p "${PORT}" -n "${BENCH_REQUESTS}" -q zpopmin zpopzset 2>&1)
echo "${ZPOP_OUT}" >&2
ZPOP_RPS=$(echo "${ZPOP_OUT}" | tr '\r' '\n' | grep -m1 'requests per second' | grep -oE '[0-9]+\.[0-9]+ requests per second' | grep -oE '^[0-9]+\.[0-9]+')

cat > "${LABEL}.json" <<JSON
{
  "label": "${LABEL}",
  "server_version": "$(${SERVER_BIN} --version)",
  "zcard": ${CARD},
  "encoding": "${ENC}",
  "used_memory_bytes": ${MEM_BYTES},
  "zadd_rps": ${ZADD_RPS:-null},
  "zpopmin_rps": ${ZPOP_RPS:-null}
}
JSON

echo "=== [$LABEL] summary ===" >&2
cat "${LABEL}.json"
