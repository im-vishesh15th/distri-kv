#!/usr/bin/env bash
# Three-Node DistriKV Demo
# Starts 3 real server processes, writes a key, kills the leader,
# and confirms the key is still readable and writable.

set -euo pipefail

# Configuration
NODE_IDS=("node1" "node2" "node3")
PORTS=(8081 8082 8083)
DATA_DIRS=("/tmp/distrikv-demo/node1" "/tmp/distrikv-demo/node2" "/tmp/distrikv-demo/node3")
PEERS="node1@127.0.0.1:8081,node2@127.0.0.1:8082,node3@127.0.0.1:8083"

# Cleanup function
cleanup() {
    echo "=== Cleaning up ==="
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    rm -rf /tmp/distrikv-demo
}
trap cleanup EXIT INT TERM

# Create data directories
mkdir -p "${DATA_DIRS[@]}"

# Build the server binary
echo "=== Building server ==="
go build -o /tmp/distrikv-server ./cmd/server

# Start 3 nodes
echo "=== Starting 3 nodes ==="
PIDS=()
for i in 0 1 2; do
    /tmp/distrikv-server \
        -id="${NODE_IDS[i]}" \
        -addr=":${PORTS[i]}" \
        -data-dir="${DATA_DIRS[i]}" \
        -peers="${PEERS}" \
        -snapshot-every=100 \
        > "/tmp/distrikv-demo/${NODE_IDS[i]}.log" 2>&1 &
    PIDS+=($!)
    echo "Started ${NODE_IDS[i]} on port ${PORTS[i]} (PID: ${PIDS[i]})"
done

# Wait for nodes to start and elect leader
echo "=== Waiting for cluster to stabilize ==="
sleep 5

# Function to find current leader
find_leader() {
    for port in "${PORTS[@]}"; do
        resp=$(go run ./cmd/client -addr="127.0.0.1:${port}" status 2>/dev/null || true)
        if echo "$resp" | grep -q 'role=leader'; then
            echo "$resp" | sed -n 's/.*node=\([^ ]*\).*/\1/p'
            return 0
        fi
    done
    return 1
}

# Wait for leader election
echo "=== Waiting for leader election ==="
LEADER=""
for i in {1..30}; do
    LEADER=$(find_leader)
    if [[ -n "$LEADER" ]]; then
        echo "Leader elected: $LEADER"
        break
    fi
    sleep 1
done

if [[ -z "$LEADER" ]]; then
    echo "ERROR: No leader elected after 30 seconds"
    exit 1
fi

# Find leader's port
LEADER_PORT=""
for i in 0 1 2; do
    if [[ "${NODE_IDS[i]}" == "$LEADER" ]]; then
        LEADER_PORT="${PORTS[i]}"
        break
    fi
done

echo "Leader is ${LEADER} on port ${LEADER_PORT}"

# Write a key through the leader
echo "=== Writing key via leader ==="
KEY="demo-key-$(date +%s)"
VALUE="demo-value-$(date +%s)"
go run ./cmd/client -addr="127.0.0.1:${LEADER_PORT}" set "$KEY" "$VALUE"
echo "Wrote $KEY = $VALUE"

# Read back to confirm
echo "=== Reading key back ==="
READ_VALUE=$(go run ./cmd/client -addr="127.0.0.1:${LEADER_PORT}" get "$KEY")
echo "Read: $READ_VALUE"

if [[ "$READ_VALUE" != "$VALUE" ]]; then
    echo "ERROR: Value mismatch!"
    exit 1
fi

# Kill the leader
echo "=== Killing leader ($LEADER) ==="
for i in 0 1 2; do
    if [[ "${NODE_IDS[i]}" == "$LEADER" ]]; then
        kill "${PIDS[i]}"
        echo "Killed ${NODE_IDS[i]} (PID: ${PIDS[i]})"
        PIDS[i]=0
        break
    fi
done

# Wait for new leader election
echo "=== Waiting for new leader election ==="
sleep 5

# Find new leader
NEW_LEADER=""
for i in {1..30}; do
    NEW_LEADER=$(find_leader)
    if [[ -n "$NEW_LEADER" && "$NEW_LEADER" != "$LEADER" ]]; then
        echo "New leader elected: $NEW_LEADER"
        break
    fi
    sleep 1
done

if [[ -z "$NEW_LEADER" ]]; then
    echo "ERROR: No new leader elected after 30 seconds"
    exit 1
fi

# Find new leader's port
NEW_LEADER_PORT=""
for i in 0 1 2; do
    if [[ "${NODE_IDS[i]}" == "$NEW_LEADER" ]]; then
        NEW_LEADER_PORT="${PORTS[i]}"
        break
    fi
done

echo "New leader is ${NEW_LEADER} on port ${NEW_LEADER_PORT}"

# Read the key from the new leader
echo "=== Reading key from new leader ==="
READ_VALUE=$(go run ./cmd/client -addr="127.0.0.1:${NEW_LEADER_PORT}" get "$KEY")
echo "Read: $READ_VALUE"

if [[ "$READ_VALUE" != "$VALUE" ]]; then
    echo "ERROR: Value mismatch after failover!"
    exit 1
fi

# Write a new key through the new leader
echo "=== Writing new key via new leader ==="
KEY2="demo-key-2-$(date +%s)"
VALUE2="demo-value-2-$(date +%s)"
go run ./cmd/client -addr="127.0.0.1:${NEW_LEADER_PORT}" set "$KEY2" "$VALUE2"
echo "Wrote $KEY2 = $VALUE2"

# Read back to confirm
READ_VALUE2=$(go run ./cmd/client -addr="127.0.0.1:${NEW_LEADER_PORT}" get "$KEY2")
echo "Read: $READ_VALUE2"

if [[ "$READ_VALUE2" != "$VALUE2" ]]; then
    echo "ERROR: Value mismatch for second write!"
    exit 1
fi

# Verify data on all live nodes
echo "=== Verifying data on all live nodes ==="
for i in 0 1 2; do
    if [[ "${PIDS[i]}" != "0" ]]; then
        port="${PORTS[i]}"
        node="${NODE_IDS[i]}"
        echo "Checking $node on port $port..."
        v1=$(go run ./cmd/client -addr="127.0.0.1:${port}" get "$KEY" 2>/dev/null || echo "ERROR")
        v2=$(go run ./cmd/client -addr="127.0.0.1:${port}" get "$KEY2" 2>/dev/null || echo "ERROR")
        if [[ "$v1" != "$VALUE" || "$v2" != "$VALUE2" ]]; then
            echo "ERROR: Data mismatch on $node!"
            exit 1
        fi
    fi
done

echo ""
echo "=== SUCCESS ==="
echo "All checks passed:"
echo "  - Key written and readable before failover"
echo "  - Leader failover detected"
echo "  - Key still readable after failover"
echo "  - New writes work after failover"
echo "  - Data consistent across all live nodes"