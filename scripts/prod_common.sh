#!/usr/bin/env bash
# Shared helpers for the *_prod.sh scripts. Source it; do not run it.
# Shared read-only helpers for the production verification scripts.
# KV/failover tests use an existing DKV_KEY supplied by the caller.
# Nothing here creates credentials, deletes volumes, or deletes data.

PROD_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROD_ENV_FILE="${PROD_ENV_FILE:-$PROD_ROOT/deployments/.env}"
PROD_COMPOSE_FILE="$PROD_ROOT/deployments/docker-compose.prod.yml"
NODES="1 2 3"
GROUPS_EXPECTED="0 1 2 metadata"

compose() { docker compose -f "$PROD_COMPOSE_FILE" --env-file "$PROD_ENV_FILE" "$@"; }

env_value() { # env_value NAME -> value from deployments/.env (empty if unset)
  [ -f "$PROD_ENV_FILE" ] || return 0
  grep -E "^$1=" "$PROD_ENV_FILE" | tail -n1 | cut -d= -f2- | sed 's/^"//;s/"$//'
}

container_running() { [ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" = true ]; }
container_health()  { docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$1" 2>/dev/null || echo missing; }

# node_metrics N -> Prometheus text of node N (empty if the node is down).
node_metrics() {
  docker exec "distrikv-$1" wget -q -O - http://127.0.0.1:9101/metrics 2>/dev/null || true
}

# metric_value <metrics-text> <metric> <group> -> integer or empty
metric_value() {
  printf '%s\n' "$1" | awk -v m="$2" -v g="$3" '
    index($0, m "{") == 1 && index($0, "group=\"" g "\"") > 0 { print $NF; exit }'
}

# leader_of GROUP -> node number (1..3) that reports itself leader, or empty.
leader_of() {
  local n m
  for n in $NODES; do
    m=$(node_metrics "$n")
    [ -n "$m" ] || continue
    [ "$(metric_value "$m" distrikv_raft_is_leader "$1")" = 1 ] && { echo "$n"; return 0; }
  done
  return 1
}

# leaders_count GROUP -> how many reachable nodes claim leadership of GROUP.
leaders_count() {
  local n m c=0
  for n in $NODES; do
    m=$(node_metrics "$n")
    [ -n "$m" ] || continue
    [ "$(metric_value "$m" distrikv_raft_is_leader "$1")" = 1 ] && c=$((c+1))
  done
  echo "$c"
}

# gw_url -> http://<gateway container IP>:8080 (reachable from the VM host).
gw_url() {
  local ip
  ip=$(docker inspect -f '{{(index .NetworkSettings.Networks "distrikv-net").IPAddress}}' distrikv-gateway 2>/dev/null)
  [ -n "$ip" ] && echo "http://$ip:8080"
}




# gw_status KEY METHOD PATH [JSON] -> prints the HTTP status code.
gw_status() {
  local key=$1 method=$2 path=$3 body=${4:-}
  if [ -n "$body" ]; then
    curl -s -o /dev/null -w '%{http_code}' --max-time 15 -X "$method" "$(gw_url)$path" -H "Authorization: Bearer $key" -d "$body"
  else
    curl -s -o /dev/null -w '%{http_code}' --max-time 15 -X "$method" "$(gw_url)$path" -H "Authorization: Bearer $key"
  fi
}

# gw_value KEY KEYNAME -> prints the stored value for KEYNAME (empty on failure).
gw_value() {
  curl -s --max-time 15 "$(gw_url)/kv/$2" -H "Authorization: Bearer $1" | sed -n 's/.*"value":"\([^"]*\)".*/\1/p'
}

# gw_put_retry KEY KEYNAME VALUE [SECONDS] -> 0 once the write returns 200.
gw_put_retry() {
  local key=$1 name=$2 val=$3 limit=${4:-30} start now code
  start=$(date +%s)
  while :; do
    code=$(gw_status "$key" PUT "/kv/$name" "{\"value\":\"$val\"}")
    [ "$code" = 200 ] && return 0
    now=$(date +%s); [ $((now-start)) -ge "$limit" ] && { echo "last status $code" >&2; return 1; }
    sleep 1
  done
}

# raft_table -> prints role per group/node and the commit index per group/node.
raft_table() {
  local g n m role term ci
  printf '%-10s' "GROUP"; for n in $NODES; do printf '%-18s' "distrikv-$n"; done; echo
  for g in $GROUPS_EXPECTED; do
    printf '%-10s' "$g"
    for n in $NODES; do
      m=$(node_metrics "$n")
      if [ -z "$m" ]; then printf '%-18s' "DOWN"; continue; fi
      role=$([ "$(metric_value "$m" distrikv_raft_is_leader "$g")" = 1 ] && echo LEADER || echo follower)
      term=$(metric_value "$m" distrikv_raft_term "$g")
      printf '%-18s' "$role (term ${term:-?})"
    done; echo
  done
  echo
  printf '%-10s' "COMMIT"; for n in $NODES; do printf '%-18s' "distrikv-$n"; done; echo
  for g in $GROUPS_EXPECTED; do
    printf '%-10s' "$g"
    for n in $NODES; do
      m=$(node_metrics "$n")
      if [ -z "$m" ]; then printf '%-18s' "-"; continue; fi
      ci=$(metric_value "$m" distrikv_raft_commit_index "$g")
      printf '%-18s' "${ci:-?}"
    done; echo
  done
}
