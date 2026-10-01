#!/usr/bin/env bash
# Restart / snapshot-recovery benchmark:
#   1. start 3 nodes with -snapshot-every=100 (snapshots + log compaction)
#   2. write phase 1 (real gRPC writes, leader applies > 100 entries)
#   3. kill a follower
#   4. write phase 2 — the leader snapshots and compacts past the dead
#      node's log position, so a plain AppendEntries catch-up is impossible
#   5. restart the node on its old data dir; measure, from process start:
#        - time until its log records snapshot_restored (local snapshot)
#        - time until its log records snapshot_installed (leader shipped a
#          snapshot the compacted log can no longer cover)
#        - time until GetStatus answers with a known leader (rejoined)
#   6. kill the CURRENT leader so the restarted node must participate in a
#      new election, then sweep every preloaded key: cluster-wide data
#      integrity after snapshot recovery.
#
# Timing note: recovery times are wall-clock observed by polling the node
# log/status every 0.1s, so they carry that polling granularity.
#
# Usage: scripts/bench_recovery.sh [write-duration]
#   write-duration  per write phase (default 8s)

set -euo pipefail

WRITE_DUR="${1:-8s}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_DIR="$ROOT/bench-results"
JSONL="$RESULTS_DIR/recovery.jsonl"
PEER_PORTS=(8097 8098 8099)
KEYSPACE=1000
POLL=0.1

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

DATA_ROOT=/tmp/distrikv-bench-recovery
rm -rf "$DATA_ROOT"; mkdir -p "$DATA_ROOT"
peers="node1@127.0.0.1:${PEER_PORTS[0]},node2@127.0.0.1:${PEER_PORTS[1]},node3@127.0.0.1:${PEER_PORTS[2]}"
ADDRS="127.0.0.1:${PEER_PORTS[0]},127.0.0.1:${PEER_PORTS[1]},127.0.0.1:${PEER_PORTS[2]}"

start_node() {
    local idx=$1
    /tmp/distrikv-bench-server \
        -id="node$((idx+1))" \
        -addr="127.0.0.1:${PEER_PORTS[idx]}" \
        -data-dir="$DATA_ROOT/node$((idx+1))" \
        -peers="$peers" \
        -snapshot-every=100 \
        >> "$DATA_ROOT/node$((idx+1)).log" 2>&1 &
    PIDS[idx]=$!
}

# find_role <idx> -> prints role=... leader=... via cmd/client status
find_role() {
    local idx=$1
    /tmp/distrikv-bench-cli -addr="127.0.0.1:${PEER_PORTS[idx]}" -timeout=2s status 2>/dev/null || true
}

# wait_leader -> sets LEADER_IDX; fails after 60s
wait_leader() {
    LEADER_IDX=-1
    for _ in $(seq 1 60); do
        for i in 0 1 2; do
            if find_role "$i" | grep -q 'role=leader'; then
                LEADER_IDX=$i
                return 0
            fi
        done
        sleep 1
    done
    echo "ERROR: no leader elected" >&2
    return 1
}

echo "=== starting 3 nodes (-snapshot-every=100) ==="
for i in 0 1 2; do start_node "$i"; done
wait_leader
echo "leader: node$((LEADER_IDX+1))"
sleep 2

echo ""
echo "=== write phase 1: ${WRITE_DUR} (read-ratio=0) ==="
/tmp/distrikv-bench \
    -addrs="$ADDRS" \
    -shards=1 \
    -label="recovery-write-phase-1" \
    -read-ratio=0 \
    -concurrency=8 \
    -duration="$WRITE_DUR" \
    -warmup=1s \
    -keyspace="$KEYSPACE" \
    -out="$JSONL"

# Kill a FOLLOWER so phase 2 writes keep flowing without an election.
VICTIM=-1
for i in 0 1 2; do
    [[ "$i" != "$LEADER_IDX" ]] || continue
    if ! find_role "$i" | grep -q 'role=candidate'; then
        VICTIM=$i
        break
    fi
done
[[ "$VICTIM" != "-1" ]] || VICTIM=$(( (LEADER_IDX+1) % 3 ))
echo ""
echo "killing follower node$((VICTIM+1)) (pid ${PIDS[$VICTIM]})"
kill "${PIDS[$VICTIM]}" 2>/dev/null || true
PIDS[$VICTIM]=0
sleep 1

# Note how many log lines existed before the restart, so we only consider
# NEW lines when looking for recovery markers.
OLD_LINES=$(wc -l < "$DATA_ROOT/node$((VICTIM+1)).log" | tr -d ' ')

echo ""
echo "=== write phase 2 with node down: ${WRITE_DUR} (forces compaction past it) ==="
/tmp/distrikv-bench \
    -addrs="$ADDRS" \
    -shards=1 \
    -label="recovery-write-phase-2" \
    -read-ratio=0 \
    -concurrency=8 \
    -duration="$WRITE_DUR" \
    -warmup=1s \
    -keyspace="$KEYSPACE" \
    -out="$JSONL"

echo ""
echo "=== restarting node$((VICTIM+1)) on its old data dir ==="
T0=$(date +%s)
start_node "$VICTIM"

new_log_lines() {
    tail -n "+$((OLD_LINES+1))" "$DATA_ROOT/node$((VICTIM+1)).log"
}

# Poll for recovery markers (0.1s granularity).
RESTORED_ELAPSED=""
INSTALLED_ELAPSED=""
REJOIN_ELAPSED=""
for step in $(seq 1 600); do
    if [[ -z "$RESTORED_ELAPSED" ]]; then
        if new_log_lines | grep -q '"msg":"snapshot_restored"'; then
            RESTORED_ELAPSED="$(echo "$step $POLL" | awk '{printf "%.1f", $1*$2}')s"
        fi
    fi
    if [[ -z "$INSTALLED_ELAPSED" ]]; then
        if new_log_lines | grep -q '"msg":"snapshot_installed"'; then
            INSTALLED_ELAPSED="$(echo "$step $POLL" | awk '{printf "%.1f", $1*$2}')s"
        fi
    fi
    if [[ -z "$REJOIN_ELAPSED" ]]; then
        st=$(find_role "$VICTIM")
        # Format: node=... role=... leader=<id|-> ...
        leader_val=$(echo "$st" | sed -n 's/.* leader=\([^ ]*\).*/\1/p')
        if [[ -n "$leader_val" && "$leader_val" != "-" ]]; then
            REJOIN_ELAPSED="$(echo "$step $POLL" | awk '{printf "%.1f", $1*$2}')s"
        fi
    fi
    if [[ -n "$RESTORED_ELAPSED" && -n "$INSTALLED_ELAPSED" && -n "$REJOIN_ELAPSED" ]]; then
        break
    fi
    sleep "$POLL"
done

echo "recovery markers for node$((VICTIM+1)) after restart:"
echo "  snapshot_restored (local snapshot load): ${RESTORED_ELAPSED:-NOT OBSERVED in 60s}"
echo "  snapshot_installed (leader-shipped):     ${INSTALLED_ELAPSED:-NOT OBSERVED in 60s}"
echo "  GetStatus shows a leader (rejoined):     ${REJOIN_ELAPSED:-NOT OBSERVED in 60s}"

if [[ -z "$INSTALLED_ELAPSED" ]]; then
    echo "ERROR: restarted node never installed a leader snapshot — the write" >&2
    echo "phases did not compact past its log; recovery scenario invalid." >&2
    echo "--- new log lines ---" >&2
    new_log_lines >&2 || true
    exit 1
fi

# Failover: kill the CURRENT leader so the restarted node takes part in a
# fresh election, then verify every preloaded key end to end.
echo ""
echo "=== killing current leader to force election incl. restarted node ==="
wait_leader
CUR_LEADER=$LEADER_IDX
kill "${PIDS[$CUR_LEADER]}" 2>/dev/null || true
PIDS[$CUR_LEADER]=0
NEW_LEADER=-1
for _ in $(seq 1 60); do
    for i in 0 1 2; do
        [[ "${PIDS[$i]}" != "0" ]] || continue
        if find_role "$i" | grep -q 'role=leader'; then
            NEW_LEADER=$i
            break
        fi
    done
    [[ "$NEW_LEADER" != "-1" ]] && break
    sleep 1
done
if [[ "$NEW_LEADER" == "-1" ]]; then
    echo "ERROR: no new leader after killing node$((CUR_LEADER+1))" >&2
    exit 1
fi
echo "new leader: node$((NEW_LEADER+1)) $([[ "$NEW_LEADER" == "$VICTIM" ]] && echo '(the restarted node)' || echo '(survivor)')"

echo ""
echo "=== post-recovery sweep (all $KEYSPACE keys must be present) ==="
SWEEP_RC=0
/tmp/distrikv-bench \
    -addrs="$ADDRS" \
    -shards=1 \
    -label="recovery-post-sweep" \
    -read-ratio=1.0 \
    -concurrency=4 \
    -duration=2s \
    -warmup=0s \
    -keyspace="$KEYSPACE" \
    -skip-preload \
    -sweep \
    -out="$JSONL" || SWEEP_RC=$?

echo ""
echo "=== recovery run complete (sweep rc=$SWEEP_RC) ==="
exit "$SWEEP_RC"
