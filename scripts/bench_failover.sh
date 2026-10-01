#!/usr/bin/env bash
# Leader-failover benchmark: run cmd/bench against a live 3-node cluster,
# kill the leader mid-measurement, and report how the client sees it —
# error burst during re-election, then recovery. Ends with -sweep to prove
# no preloaded key was lost.
#
# Usage: scripts/bench_failover.sh [duration] [kill-after]
#   duration    total measurement window (default 30s)
#   kill-after  time from bench start to kill the leader (default 10s)

set -euo pipefail

DURATION="${1:-30s}"
KILL_AFTER="${2:-10s}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_DIR="$ROOT/bench-results"
JSONL="$RESULTS_DIR/failover.jsonl"
PEER_PORTS=(8094 8095 8096)
KEYSPACE=1000
WARMUP=3s

cd "$ROOT"

echo "=== building ==="
go build -o /tmp/distrikv-bench-server ./cmd/server
go build -o /tmp/distrikv-bench-cli ./cmd/client
go build -o /tmp/distrikv-bench ./cmd/bench

mkdir -p "$RESULTS_DIR"

PIDS=()
CLEANED=0
cleanup() {
    if [[ "$CLEANED" == "1" ]]; then return; fi
    CLEANED=1
    for pid in "${PIDS[@]:-}"; do
        [[ "$pid" != "0" ]] || continue # 0 would signal our own process group
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

DATA_ROOT=/tmp/distrikv-bench-failover
rm -rf "$DATA_ROOT"; mkdir -p "$DATA_ROOT"
peers="node1@127.0.0.1:${PEER_PORTS[0]},node2@127.0.0.1:${PEER_PORTS[1]},node3@127.0.0.1:${PEER_PORTS[2]}"

for i in 0 1 2; do
    /tmp/distrikv-bench-server \
        -id="node$((i+1))" \
        -addr="127.0.0.1:${PEER_PORTS[i]}" \
        -data-dir="$DATA_ROOT/node$((i+1))" \
        -peers="$peers" \
        > "$DATA_ROOT/node$((i+1)).log" 2>&1 &
    PIDS+=($!)
done
echo "started 3 nodes (pids=${PIDS[*]})"

# Find the leader (any port answering role=leader).
LEADER_IDX=-1
LEADER_PORT=""
for _ in $(seq 1 60); do
    for i in 0 1 2; do
        out=$(/tmp/distrikv-bench-cli -addr="127.0.0.1:${PEER_PORTS[i]}" -timeout=2s status 2>/dev/null || true)
        if echo "$out" | grep -q 'role=leader'; then
            LEADER_IDX=$i
            LEADER_PORT=${PEER_PORTS[i]}
            break
        fi
    done
    [[ -n "$LEADER_PORT" ]] && break
    sleep 1
done
if [[ -z "$LEADER_PORT" ]]; then
    echo "ERROR: no leader elected" >&2
    exit 1
fi
echo "leader: node$((LEADER_IDX+1)) on port $LEADER_PORT"

# Run bench; kill the leader KILL_AFTER seconds after bench starts.
ADDRS="127.0.0.1:${PEER_PORTS[0]},127.0.0.1:${PEER_PORTS[1]},127.0.0.1:${PEER_PORTS[2]}"
/tmp/distrikv-bench \
    -addrs="$ADDRS" \
    -shards=1 \
    -label="failover kill-leader@${KILL_AFTER}" \
    -read-ratio=0.5 \
    -concurrency=8 \
    -duration="$DURATION" \
    -warmup="$WARMUP" \
    -keyspace="$KEYSPACE" \
    -progress \
    -out="$JSONL" &
BENCH_PID=$!

echo "killing leader (node$((LEADER_IDX+1))) in ${KILL_AFTER}..."
sleep "$KILL_AFTER"
kill "${PIDS[$LEADER_IDX]}" 2>/dev/null || true
KILL_TS=$(date +%s)
echo "leader killed at $(date +%H:%M:%S) (pid ${PIDS[$LEADER_IDX]})"
PIDS[$LEADER_IDX]=0

BENCH_RC=0
wait "$BENCH_PID" || BENCH_RC=$?
echo "bench exited rc=$BENCH_RC"

# Recovery observation: how long until a NEW leader answers?
RECOVERED=""
for i in 0 1 2; do
    [[ "${PIDS[$i]}" == "0" ]] && continue
    for _ in $(seq 1 60); do
        out=$(/tmp/distrikv-bench-cli -addr="127.0.0.1:${PEER_PORTS[i]}" -timeout=2s status 2>/dev/null || true)
        if echo "$out" | grep -q 'role=leader'; then
            RECOVERED="node$((i+1)):$(($(date +%s)-KILL_TS))s"
            break
        fi
        sleep 1
    done
    [[ -n "$RECOVERED" ]] && break
done
echo "new leader after kill: ${RECOVERED:-NONE within 60s}"

# Data-loss check: read every preloaded key (no preload, no writes).
echo ""
echo "=== post-failover sweep (data-loss check) ==="
SWEEP_RC=0
/tmp/distrikv-bench \
    -addrs="$ADDRS" \
    -shards=1 \
    -label="failover-post-sweep" \
    -read-ratio=1.0 \
    -concurrency=4 \
    -duration=2s \
    -warmup=0s \
    -keyspace="$KEYSPACE" \
    -skip-preload \
    -sweep \
    -out="$JSONL" || SWEEP_RC=$?

echo ""
echo "=== failover run complete (bench rc=$BENCH_RC sweep rc=$SWEEP_RC) ==="
exit $(( BENCH_RC != 0 || SWEEP_RC != 0 ))
