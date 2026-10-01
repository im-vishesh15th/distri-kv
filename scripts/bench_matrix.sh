#!/usr/bin/env bash
# Full benchmark matrix against REAL gRPC: 3 server processes, cmd/bench
# driving pkg/client. Matrix: shards {1,4,16} x read-ratio {0,0.5,0.8,1.0}
# x concurrency {1,8,32,128}.
#
# Usage: scripts/bench_matrix.sh [duration] [warmup]
#   default duration=10s warmup=2s
#
# Output: bench-results/matrix.jsonl (one JSON object per run) and a human
# summary on stdout. Every number comes from a real run; nothing is derived.

set -euo pipefail

DURATION="${1:-10s}"
WARMUP="${2:-2s}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_DIR="$ROOT/bench-results"
JSONL="$RESULTS_DIR/matrix.jsonl"
BASE_PORT=8091
PEER_PORTS=(8091 8092 8093)

SHARDS_LIST=(1 4 16)
READ_RATIOS=(0 0.5 0.8 1.0)
CONCURRENCY_LIST=(1 8 32 128)
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

wait_for_leader() {
    local port
    for _ in $(seq 1 60); do
        for port in "${PEER_PORTS[@]}"; do
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
    local shards="$1"
    local data_root="/tmp/distrikv-bench-data"
    rm -rf "$data_root"
    mkdir -p "$data_root"

    local shard_cfg=""
    if [[ "$shards" -gt 1 ]]; then
        shard_cfg="$data_root/shard.json"
        printf '{"version":1,"groups":%d}\n' "$shards" > "$shard_cfg"
    fi

    local peers="node1@127.0.0.1:${PEER_PORTS[0]},node2@127.0.0.1:${PEER_PORTS[1]},node3@127.0.0.1:${PEER_PORTS[2]}"
    PIDS=()
    local i
    for i in 0 1 2; do
        local args=(
            -id="node$((i+1))"
            -addr="127.0.0.1:${PEER_PORTS[i]}"
            -data-dir="$data_root/node$((i+1))"
            -peers="$peers"
        )
        if [[ -n "$shard_cfg" ]]; then
            args+=(-shard-config="$shard_cfg")
        fi
        /tmp/distrikv-bench-server "${args[@]}" \
            > "$data_root/node$((i+1)).log" 2>&1 &
        PIDS+=($!)
    done
    echo "started 3 nodes (shards=$shards, pids=${PIDS[*]})"
    wait_for_leader
}

stop_cluster() {
    cleanup
    CLEANED=0
    PIDS=()
}

# --- matrix ---
for shards in "${SHARDS_LIST[@]}"; do
    echo ""
    echo "=== cluster: shards=$shards ==="
    start_cluster "$shards"
    sleep 2

    for rr in "${READ_RATIOS[@]}"; do
        for conc in "${CONCURRENCY_LIST[@]}"; do
            label="shards=${shards} read=${rr} conc=${conc}"
            echo ""
            echo "--- run: $label (duration=$DURATION warmup=$WARMUP) ---"
            /tmp/distrikv-bench \
                -addrs="127.0.0.1:${PEER_PORTS[0]},127.0.0.1:${PEER_PORTS[1]},127.0.0.1:${PEER_PORTS[2]}" \
                -shards="$shards" \
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
echo "=== matrix complete: $JSONL ($(wc -l < "$JSONL") runs) ==="
