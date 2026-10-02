#!/usr/bin/env bash
# Guided leader-failure demo on the production stack. It STOPS one node container
# for a short time and starts it again. No volume is touched: the node recovers
# from its own persisted Raft log/snapshot and then catches up from the leaders.
#
#   ./scripts/failover_prod.sh            # victim = current leader of group 0
#   ./scripts/failover_prod.sh 2          # victim = distrikv-2
#
# With 3 nodes a majority is 2, so every group keeps working with one node down.
set -uo pipefail
. "$(dirname "$0")/prod_common.sh"

now() { date +%s; }
die() { echo "ABORT: $*" >&2; echo "If a node is stopped, restart it with: docker start distrikv-N" >&2; exit 1; }

echo "== 0. preconditions"
for n in $NODES; do container_running "distrikv-$n" || die "distrikv-$n is not running"; done
for g in $GROUPS_EXPECTED; do [ "$(leaders_count "$g")" -eq 1 ] || die "group $g does not have exactly one leader; run raft_status_prod.sh"; done
KEY="${DKV_KEY:-}"
[ -n "$KEY" ] || die "DKV_KEY is required for failover test"
RUN="fo-$(now)"

echo; echo "== 1. state before"
raft_table

VICTIM=${1:-$(leader_of 0)}
case "$VICTIM" in 1|2|3) ;; *) die "victim must be 1, 2 or 3 (got '$VICTIM')";; esac
echo; echo "victim: distrikv-$VICTIM"

echo; echo "== 2. write 30 keys (spread over the 3 data groups by slot hash)"
for i in $(seq 1 30); do gw_put_retry "$KEY" "$RUN-a$i" "A$i" 30 || die "write a$i failed"; done
echo "30 writes acknowledged"

echo; echo "== 3. stop distrikv-$VICTIM"
docker stop "distrikv-$VICTIM" >/dev/null || die "docker stop failed"
T0=$(now)

echo "waiting for a leader in every group among the remaining nodes..."
while :; do
  all=1
  for g in $GROUPS_EXPECTED; do [ "$(leaders_count "$g")" -eq 1 ] || all=0; done
  [ $all -eq 1 ] && break
  [ $(( $(now) - T0 )) -gt 60 ] && die "no stable leaders after 60 s"
  sleep 1
done
echo "all groups have a leader again after $(( $(now) - T0 )) s"; echo; raft_table

echo; echo "== 4. write 30 more keys with the node DOWN (majority of 2 of 3 still commits)"
for i in $(seq 1 30); do gw_put_retry "$KEY" "$RUN-b$i" "B$i" 30 || die "write b$i failed while node $VICTIM was down"; done
echo "30 writes acknowledged with distrikv-$VICTIM down"

echo; echo "== 5. read everything back (60 keys)"
miss=0
for i in $(seq 1 30); do
  [ "$(gw_value "$KEY" "$RUN-a$i")" = "A$i" ] || { miss=$((miss+1)); echo "missing/wrong: $RUN-a$i"; }
  [ "$(gw_value "$KEY" "$RUN-b$i")" = "B$i" ] || { miss=$((miss+1)); echo "missing/wrong: $RUN-b$i"; }
done
[ $miss -eq 0 ] && echo "all 60 keys correct" || die "$miss keys missing or wrong"

echo; echo "== 6. restart distrikv-$VICTIM (same volume, no data deleted)"
docker start "distrikv-$VICTIM" >/dev/null || die "docker start failed"
T1=$(now)
while [ "$(container_health "distrikv-$VICTIM")" != healthy ]; do
  [ $(( $(now) - T1 )) -gt 120 ] && die "distrikv-$VICTIM not healthy after 120 s (docker logs distrikv-$VICTIM)"
  sleep 2
done
echo "distrikv-$VICTIM healthy after $(( $(now) - T1 )) s"

echo "waiting for it to catch up (commit index >= the leader's, per group)..."
while :; do
  behind=0
  for g in $GROUPS_EXPECTED; do
    L=$(leader_of "$g"); [ -n "$L" ] || { behind=1; continue; }
    lc=$(metric_value "$(node_metrics "$L")" distrikv_raft_commit_index "$g")
    vc=$(metric_value "$(node_metrics "$VICTIM")" distrikv_raft_commit_index "$g")
    [ -n "$vc" ] && [ -n "$lc" ] && [ "$vc" -ge "$lc" ] || behind=1
  done
  [ $behind -eq 0 ] && break
  [ $(( $(now) - T1 )) -gt 120 ] && die "distrikv-$VICTIM did not catch up in 120 s"
  sleep 2
done
echo "caught up after $(( $(now) - T1 )) s"

echo; echo "== 7. state after"
raft_table
echo; echo "== 8. final read of all 60 keys"
miss=0
for i in $(seq 1 30); do
  [ "$(gw_value "$KEY" "$RUN-a$i")" = "A$i" ] || miss=$((miss+1))
  [ "$(gw_value "$KEY" "$RUN-b$i")" = "B$i" ] || miss=$((miss+1))
done
[ $miss -eq 0 ] && echo "PASS: no acknowledged write was lost" || die "$miss keys missing after recovery"
echo "(test keys $RUN-a*/$RUN-b* belong to tenant 'healthcheck'; they are harmless and can stay)"
