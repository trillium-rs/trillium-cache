#!/usr/bin/env bash
# Run the cache-tests.fyi conformance corpus against the bundled conformance_proxy.
#
# Spins up the cache-tests origin and the proxy, runs the suite, and tears both down on
# exit (including failures/interrupts). The full-corpus run saves normalized results to
# cache-tests-results.json — commit that file and diff across runs to spot regressions or
# improvements; extract summaries with jq as needed.
#
# Usage:
#   scripts/conformance.sh            # full corpus -> cache-tests-results.json
#   scripts/conformance.sh -i <id>    # single test, printed to stdout (results file untouched)
#
# Prerequisites: a cache-tests checkout with `npm install` already run. Defaults to
# ../cache-tests; override with CACHE_TESTS_DIR.

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CACHE_TESTS_DIR="${CACHE_TESTS_DIR:-$REPO_DIR/../cache-tests}"
RESULTS_FILE="$REPO_DIR/cache-tests-results.json"
LOGS_DIR="$REPO_DIR/logs"
ORIGIN_PORT="${ORIGIN_PORT:-8000}"
PROXY_PORT="${PROXY_PORT:-8080}"

TEST_ID=""
while getopts "i:h" opt; do
  case "$opt" in
    i) TEST_ID="$OPTARG" ;;
    *) echo "usage: $0 [-i test-id]" >&2; exit 1 ;;
  esac
done

if [[ ! -d "$CACHE_TESTS_DIR" ]]; then
  echo "error: cache-tests checkout not found at $CACHE_TESTS_DIR" >&2
  echo "       set CACHE_TESTS_DIR or clone it there (https://github.com/http-tests/cache-tests)." >&2
  exit 1
fi
if [[ ! -d "$CACHE_TESTS_DIR/node_modules" ]]; then
  echo "error: $CACHE_TESTS_DIR/node_modules missing — run 'npm install' there first." >&2
  exit 1
fi

ORIGIN_PID=""
PROXY_PID=""
cleanup() {
  [[ -n "$PROXY_PID" ]] && kill "$PROXY_PID" 2>/dev/null || true
  [[ -n "$ORIGIN_PID" ]] && kill "$ORIGIN_PID" 2>/dev/null || true
  # npm spawns node as a child; the kill above may only reach the npm wrapper.
  pkill -f "test-engine/server/server.mjs" 2>/dev/null || true
}
trap cleanup EXIT

wait_for_port() {
  local port="$1" name="$2"
  for _ in $(seq 1 60); do
    if curl -s -o /dev/null "http://127.0.0.1:$port/"; then return 0; fi
    sleep 0.5
  done
  echo "error: $name did not come up on :$port" >&2
  exit 1
}

mkdir -p "$LOGS_DIR"

echo "building conformance_proxy..."
cargo build --example conformance_proxy --features client --manifest-path "$REPO_DIR/Cargo.toml"

echo "starting cache-tests origin on :$ORIGIN_PORT (logs: logs/cache-tests-origin.log)..."
( cd "$CACHE_TESTS_DIR" && npm run --silent server ) >"$LOGS_DIR/cache-tests-origin.log" 2>&1 &
ORIGIN_PID=$!
wait_for_port "$ORIGIN_PORT" "origin"

echo "starting conformance proxy on :$PROXY_PORT (logs: logs/conformance-proxy.log)..."
ORIGIN="http://localhost:$ORIGIN_PORT" PORT="$PROXY_PORT" \
  "$REPO_DIR/target/debug/examples/conformance_proxy" >"$LOGS_DIR/conformance-proxy.log" 2>&1 &
PROXY_PID=$!
wait_for_port "$PROXY_PORT" "proxy"

if [[ -n "$TEST_ID" ]]; then
  echo "running single test '$TEST_ID' (results file untouched)..."
  ( cd "$CACHE_TESTS_DIR" && ./test-host.sh -i "$TEST_ID" "127.0.0.1:$PROXY_PORT" )
else
  echo "running full corpus -> $RESULTS_FILE"
  ( cd "$CACHE_TESTS_DIR" && ./test-host.sh "127.0.0.1:$PROXY_PORT" ) | jq -S . > "$RESULTS_FILE"
  pass="$(jq '[.[] | select(. == true)] | length' "$RESULTS_FILE")"
  total="$(jq 'length' "$RESULTS_FILE")"
  echo "saved $RESULTS_FILE: $pass/$total passing"
fi
