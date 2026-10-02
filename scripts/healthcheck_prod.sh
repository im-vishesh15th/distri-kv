#!/usr/bin/env bash
# Verify the production stack on the VM.
# The default checks are read-only. If DKV_KEY is provided, the script also
# performs a temporary PUT/GET/DELETE probe through the gateway.
#
#   DKV_KEY=... ./scripts/healthcheck_prod.sh  # everything including KV probe
#   ./scripts/healthcheck_prod.sh --no-kv    # skip the probe PUT/GET/DELETE
#   ./scripts/healthcheck_prod.sh --no-external   # skip the HTTPS check through Caddy
#
# Exit status is 0 only if every check passed.
set -uo pipefail
. "$(dirname "$0")/prod_common.sh"

DO_KV=1; DO_EXT=1
for a in "$@"; do case "$a" in --no-kv) DO_KV=0;; --no-external) DO_EXT=0;; *) echo "unknown option $a" >&2; exit 2;; esac; done

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "ok    $*"; }
bad()  { FAIL=$((FAIL+1)); echo "FAIL  $*"; }
check() { # check "description" command...
  local d=$1; shift; if "$@" >/dev/null 2>&1; then ok "$d"; else bad "$d"; fi
}

command -v docker >/dev/null || { echo "docker not found"; exit 2; }

echo "== containers (expect exactly 5, all running and healthy)"
for c in distrikv-1 distrikv-2 distrikv-3 distrikv-gateway distrikv-proxy; do
  if container_running "$c" && [ "$(container_health "$c")" = healthy ]; then ok "$c running + healthy"
  else bad "$c: running=$(container_running "$c" && echo yes || echo no) health=$(container_health "$c")"; fi
done
n=$(docker ps --filter label=com.docker.compose.project=distrikv --format '{{.Names}}' | wc -l)
[ "$n" -eq 5 ] && ok "5 containers in the compose project" || bad "compose project has $n running containers (expected 5)"

echo "== Raft: 4 groups on every node, exactly one leader per group"
for node in $NODES; do
  m=$(node_metrics "$node")
  if [ -z "$m" ]; then bad "distrikv-$node: /metrics not reachable"; continue; fi
  missing=""
  for g in $GROUPS_EXPECTED; do [ -n "$(metric_value "$m" distrikv_raft_term "$g")" ] || missing="$missing $g"; done
  [ -z "$missing" ] && ok "distrikv-$node hosts groups 0 1 2 metadata" || bad "distrikv-$node is missing groups:$missing"
done
for g in $GROUPS_EXPECTED; do
  c=$(leaders_count "$g")
  [ "$c" -eq 1 ] && ok "group $g has exactly one leader (node $(leader_of "$g"))" || bad "group $g reports $c leaders"
done

echo "== gateway"
check "data plane  /healthz (internal :8080)" docker exec distrikv-gateway wget -q -O /dev/null http://127.0.0.1:8080/healthz
check "control API /api/v1/health (internal :9091)" docker exec distrikv-gateway wget -q -O /dev/null http://127.0.0.1:9091/api/v1/health

echo "== exposure (only the proxy may publish ports)"
for c in distrikv-1 distrikv-2 distrikv-3 distrikv-gateway; do
  [ -z "$(docker port "$c" 2>/dev/null)" ] && ok "$c publishes no ports" || bad "$c publishes: $(docker port "$c" | tr '\n' ' ')"
done
pub=$(docker port distrikv-proxy 2>/dev/null | awk '{print $1}' | sort -u | tr '\n' ' ')
case "$pub" in "443/tcp 80/tcp "|"80/tcp 443/tcp ") ok "proxy publishes only 80 and 443";; *) bad "proxy publishes: $pub";; esac

if [ "$DO_EXT" -eq 1 ]; then
  echo "== reverse proxy / HTTPS (resolved to this VM, so DNS is not required)"
  DOMAIN=$(env_value DOMAIN)
  if [ -z "$DOMAIN" ]; then bad "DOMAIN missing in $PROD_ENV_FILE"; else
    R="--resolve $DOMAIN:443:127.0.0.1"
    # shellcheck disable=SC2086
    [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 $R "https://$DOMAIN/healthz")" = 200 ] && ok "https://$DOMAIN/healthz -> 200 (valid certificate)" || bad "https://$DOMAIN/healthz failed (certificate not issued yet? check: docker logs distrikv-proxy)"
    # shellcheck disable=SC2086
    [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 $R "https://$DOMAIN/api/v1/health")" = 200 ] && ok "https://$DOMAIN/api/v1/health -> 200" || bad "https://$DOMAIN/api/v1/health failed"
    # shellcheck disable=SC2086
    [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 $R "https://$DOMAIN/metrics")" = 404 ] && ok "/metrics is not routed publicly" || bad "/metrics is reachable through the proxy"
    # shellcheck disable=SC2086
    [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 $R "https://$DOMAIN/api/v1/admin/tenants")" = 401 ] && ok "admin API requires a login (401)" || bad "unexpected status for /api/v1/admin/tenants"
  fi
fi

if [ "$DO_KV" -eq 1 ]; then
  echo "== key-value round trip through the gateway"

  KEY="${DKV_KEY:-}"

  if [ -z "$KEY" ]; then
    echo "SKIP  DKV_KEY is not set; skipping authenticated KV probe"
  else
    K="hc-$(date +%s)-$$"

    gw_put_retry "$KEY" "$K" "alive" 30 \
      && ok "PUT /kv/$K" \
      || bad "PUT /kv/$K did not succeed within 30 s"

    [ "$(gw_value "$KEY" "$K")" = alive ] \
      && ok "GET /kv/$K returns the value" \
      || bad "GET /kv/$K returned the wrong value"

    [ "$(gw_status "$KEY" DELETE "/kv/$K")" = 204 ] \
      && ok "DELETE /kv/$K -> 204" \
      || bad "DELETE /kv/$K"

    [ "$(gw_status "$KEY" GET "/kv/$K")" = 404 ] \
      && ok "deleted key reads 404" \
      || bad "deleted key did not read 404"

    [ "$(gw_status "bad-key" GET "/kv/$K")" = 401 ] \
      && ok "invalid API key rejected (401)" \
      || bad "invalid API key not rejected"
  fi
fi

echo
echo "$PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
