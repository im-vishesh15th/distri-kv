#!/usr/bin/env bash
# Node-count scaling benchmark against REAL gRPC: N server processes,
# cmd/bench driving pkg/client. Single shard (one Raft group) so the only
# variable is replication degree. Matrix: nodes {1,3,5} x read-ratio
# {0,0.5,1.0} x concurrency {1,32,128}.
#
# CAVEAT (read before quoting numbers): every server and the load generator
# share ONE laptop. The 5-node runs run 6 processes on 8 cores, so their
# numbers mix replication cost with CPU contention. That is the honest
# setup this repo documents everywhere.
#
# Usage: scripts/bench_scaling.sh [duration] [warmup]
#   default duration=10s warmup=2s
#
# Output: bench-results/scaling.jsonl (one JSON object per run) and a human
# summary on stdout. Every number comes from a real run; nothing is derived.

set -euo pipefail

DURATION="${1:-10s}"
WARMUP="${2:-2s}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_DIR="$ROOT/bench-results"
JSONL="$RESULTS_DIR/scaling.jsonl"
BASE_PORT=9091
MAX_NODES=5

NODE_COUNTS=(1 3 5)
READ_RATIOS=(0 0.5 1.0)
CONCURRENCY_LIST=(1 32 128)
KEYSPACE=1000
PAYLOAD=100

cd "$ROOT"

# --- build ---
echo "=== building ==="
go build -o /tmp/distrikv-bench-server ./cmd/server
go build -o /tmp/distrikv-bench-cli ./cmd/client
go build -o /tmp/distrikv-bench ./cmd/bench
echo "go version: $(go version)"
echo "commit: $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"

mkdir -p "$RESULTS_DIR"
: > "$JSONL"

# --- cluster management ---
PIDS=()
CLEANED=0
cleanup() {
    if [[ "$CLEANED" == "1" ]]; then return; fi
    CLEANED=1
    for pid in "${PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

ports_for() {
    local n="$1" i out=""
    for ((i = 0; i < n; i++)); do
        [[ -n "$out" ]] && out+=","
        out+="127.0.0.1:$((BASE_PORT + i))"
    done
    echo "$out"
}

wait_for_leader() {
    local n="$1" i port
    for _ in $(seq 1 60); do
        for ((i = 0; i < n; i++)); do
            port=$((BASE_PORT + i))
            out=$(/tmp/distrikv-bench-cli -addr="127.0.0.1:$port" -timeout=2s status 2>/dev/null || true)
            if echo "$out" | grep -q 'role=leader'; then
                return 0
            fi
        done
        sleep 1
    done
    echo "ERROR: no leader elected" >&2
    return 1
}

start_cluster() {
    local n="$1"
    local data_root="/tmp/distrikv-scaling-data"
    rm -rf "$data_root"
    mkdir -p "$data_root"

    local peers="" i
    for ((i = 0; i < n; i++)); do
        [[ -n "$peers" ]] && peers+=","
        peers+="node$((i+1))@127.0.0.1:$((BASE_PORT + i))"
    done

    PIDS=()
    for ((i = 0; i < n; i++)); do
        /tmp/distrikv-bench-server \
            -id="node$((i+1))" \
            -addr="127.0.0.1:$((BASE_PORT + i))" \
            -data-dir="$data_root/node$((i+1))" \
            -peers="$peers" \
            > "$data_root/node$((i+1)).log" 2>&1 &
        PIDS+=($!)
    done
    echo "started $n nodes (pids=${PIDS[*]})"
    wait_for_leader "$n"
}

stop_cluster() {
    cleanup
    CLEANED=0
    PIDS=()
}

# --- matrix ---
for n in "${NODE_COUNTS[@]}"; do
    echo ""
    echo "=== cluster: nodes=$n ==="
    start_cluster "$n"
    sleep 2
    addrs="$(ports_for "$n")"

    for rr in "${READ_RATIOS[@]}"; do
        for conc in "${CONCURRENCY_LIST[@]}"; do
            label="nodes=${n} read=${rr} conc=${conc}"
            echo ""
            echo "--- run: $label (duration=$DURATION warmup=$WARMUP) ---"
            /tmp/distrikv-bench \
                -addrs="$addrs" \
                -shards=1 \
                -label="$label" \
                -read-ratio="$rr" \
                -concurrency="$conc" \
                -duration="$DURATION" \
                -warmup="$WARMUP" \
                -keyspace="$KEYSPACE" \
                -payload="$PAYLOAD" \
                -out="$JSONL"
        done
    done
    stop_cluster
done

echo ""
echo "=== scaling complete: $JSONL ($(wc -l < "$JSONL") runs) ==="
