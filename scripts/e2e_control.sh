#!/usr/bin/env bash
# End-to-end check of the web-console control API against a RUNNING stack
# (make demo-up first):
#
#   signup -> session cookie -> first API key works on the data plane ->
#   create/rotate/revoke keys -> revoked key => 401 -> tenant isolation ->
#   CSRF origin check -> logout -> operator (admin) routes
#
# Env: GATEWAY_URL (default http://localhost:8080)
#      CONTROL_URL (default http://localhost:9091)
set -euo pipefail
cd "$(dirname "$0")/.."

DATA="${GATEWAY_URL:-http://localhost:${GATEWAY_PORT:-8080}}"
CTL="${CONTROL_URL:-http://localhost:${CONTROL_PORT:-9091}}"
DB=/var/lib/gateway/gateway.db
JAR_A=$(mktemp); JAR_B=$(mktemp); JAR_ADM=$(mktemp)
trap 'rm -f "$JAR_A" "$JAR_B" "$JAR_ADM"' EXIT

PASS=0
fail() { echo "FAIL: $*" >&2; exit 1; }
ok()   { PASS=$((PASS+1)); echo "ok   $*"; }
expect() { [ "$2" = "$3" ] && ok "$1" || fail "$1: want '$2', got '$3'"; }

# api <jar> <method> <path> [json] -> HTTP_CODE, BODY
api() {
  local jar=$1 method=$2 path=$3 body=${4:-}; shift 4 || shift 3
  local out
  if [ -n "$body" ]; then
    out=$(curl -s -w '\n%{http_code}' -b "$jar" -c "$jar" -X "$method" "$CTL$path" -H 'Content-Type: application/json' "$@" -d "$body")
  else
    out=$(curl -s -w '\n%{http_code}' -b "$jar" -c "$jar" -X "$method" "$CTL$path" "$@")
  fi
  HTTP_CODE=$(printf '%s' "$out" | tail -n1)
  BODY=$(printf '%s' "$out" | sed '$d')
}
# jget <field> : extract a top-level or nested "field":"value" string from BODY
jget() { printf '%s' "$BODY" | grep -o "\"$1\":\"[^\"]*\"" | head -1 | cut -d'"' -f4; }
# data <key> <method> <path> [json] -> HTTP_CODE on the data plane
data() {
  local key=$1 method=$2 path=$3 body=${4:-}
  if [ -n "$body" ]; then
    curl -s -o /dev/null -w '%{http_code}' -X "$method" "$DATA$path" -H "Authorization: Bearer $key" -d "$body"
  else
    curl -s -o /dev/null -w '%{http_code}' -X "$method" "$DATA$path" -H "Authorization: Bearer $key"
  fi
}

SUF="$(date +%s)-$$"
PW="correct-horse-battery-$SUF"

echo "waiting for the control API..."
for i in $(seq 1 60); do
  curl -sf "$CTL/api/v1/health" >/dev/null 2>&1 && break
  [ "$i" = 60 ] && fail "control API not reachable at $CTL"
  sleep 1
done
ok "control API is up"

# --- storage accounting wiring (compose overrides replace `command:` wholesale,
#     so check what the running gateway really received) -----------------------
if docker inspect distrikv-gateway >/dev/null 2>&1; then
  docker inspect distrikv-gateway --format '{{json .Config.Cmd}}' | grep -q -- '-node-metrics-urls=' \
    && ok "gateway was started with -node-metrics-urls (StorageSource enabled)" \
    || fail "gateway has no -node-metrics-urls: a compose override replaced its command"
  for n in 1 2 3; do
    docker compose exec -T gateway wget -q -O - "http://distrikv-$n:9101/tenant-storage" 2>/dev/null | grep -q '"groups"' \
      && ok "gateway reaches distrikv-$n:9101/tenant-storage" \
      || fail "gateway cannot read http://distrikv-$n:9101/tenant-storage (node too old, or -metrics-addr missing)"
  done
else
  echo "skip  storage wiring checks (no local container named distrikv-gateway)"
fi

# --- signup (customer A) -------------------------------------------------
api "$JAR_A" POST /api/v1/auth/signup "{\"email\":\"a-$SUF@example.com\",\"password\":\"$PW\",\"tenant_id\":\"web-a-$SUF\",\"name\":\"Web A\"}"
expect "signup A -> 201" 201 "$HTTP_CODE"
KEY_A=$(jget key)
[ -n "$KEY_A" ] || fail "signup returned no api key: $BODY"
grep -q dkv_session "$JAR_A" && ok "session cookie set (HttpOnly in jar)" || fail "no session cookie"
grep -q '#HttpOnly_' "$JAR_A" && ok "cookie is HttpOnly" || fail "cookie not HttpOnly"

api "$JAR_A" GET /api/v1/me ""
expect "GET /me with session -> 200" 200 "$HTTP_CODE"

# --- the key from the console works on the data plane ---------------------
echo "waiting for the cluster to accept writes..."
for i in $(seq 1 60); do
  [ "$(data "$KEY_A" PUT /kv/probe '{"value":"1"}')" = 200 ] && break
  [ "$i" = 60 ] && fail "cluster not ready after 60s"
  sleep 1
done
ok "signup key writes through the gateway"
expect "signup key reads back" 200 "$(data "$KEY_A" GET /kv/probe)"

# --- key management --------------------------------------------------------
api "$JAR_A" POST /api/v1/tenant/keys '{"name":"prod","ttl_hours":24}'
expect "create key -> 201" 201 "$HTTP_CODE"
KEY_PROD=$(jget key); PFX_PROD=$(jget prefix)
[ -n "$KEY_PROD" ] && [ -n "$PFX_PROD" ] || fail "create key response: $BODY"
expect "new key works on the data plane" 200 "$(data "$KEY_PROD" GET /kv/probe)"

api "$JAR_A" GET /api/v1/tenant/keys ""
expect "list keys -> 200" 200 "$HTTP_CODE"
printf '%s' "$BODY" | grep -q "$KEY_PROD" && fail "key list leaks plaintext keys" || ok "key list never shows plaintext"

api "$JAR_A" POST "/api/v1/tenant/keys/$PFX_PROD/rotate" ""
expect "rotate key -> 201" 201 "$HTTP_CODE"
KEY_NEW=$(jget key)
expect "rotated-out key rejected immediately" 401 "$(data "$KEY_PROD" GET /kv/probe)"
expect "rotated-in key accepted" 200 "$(data "$KEY_NEW" GET /kv/probe)"

api "$JAR_A" DELETE "/api/v1/tenant/keys/${KEY_A:0:17}" ""
expect "revoke signup key -> 204" 204 "$HTTP_CODE"
expect "revoked key rejected immediately" 401 "$(data "$KEY_A" GET /kv/probe)"

api "$JAR_A" GET /api/v1/tenant/usage ""
expect "usage -> 200" 200 "$HTTP_CODE"

# Storage: the only write by this tenant is PUT /kv/probe = "1": 5 + 1 bytes, 1 key.
STORAGE_OK=0
for i in $(seq 1 45); do
  api "$JAR_A" GET /api/v1/tenant/storage ""
  if [ "$HTTP_CODE" = 200 ] && printf '%s' "$BODY" | grep -q '"keys":1' && printf '%s' "$BODY" | grep -q '"bytes":6'; then STORAGE_OK=1; break; fi
  sleep 1
done
[ "$STORAGE_OK" = 1 ] && ok "tenant storage = 6 bytes / 1 key via GET /tenant/storage" \
  || fail "tenant storage never reached bytes=6 keys=1 (last: $HTTP_CODE $BODY)"

# --- tenant isolation (customer B) ----------------------------------------
api "$JAR_B" POST /api/v1/auth/signup "{\"email\":\"b-$SUF@example.com\",\"password\":\"$PW\",\"tenant_id\":\"web-b-$SUF\"}"
expect "signup B -> 201" 201 "$HTTP_CODE"
api "$JAR_B" DELETE "/api/v1/tenant/keys/${KEY_NEW:0:17}" ""
expect "B cannot revoke A's key -> 404" 404 "$HTTP_CODE"
expect "A's key still works after B's attempt" 200 "$(data "$KEY_NEW" GET /kv/probe)"
api "$JAR_B" GET /api/v1/admin/tenants ""
expect "customer on admin route -> 403" 403 "$HTTP_CODE"

# --- CSRF / auth hygiene ----------------------------------------------------
api "$JAR_A" POST /api/v1/tenant/keys '{"name":"evil"}' -H 'Origin: http://evil.example'
expect "foreign Origin blocked -> 403" 403 "$HTTP_CODE"
api "$JAR_A" POST /api/v1/auth/login "{\"email\":\"a-$SUF@example.com\",\"password\":\"wrong-password-xx\"}"
expect "wrong password -> 401" 401 "$HTTP_CODE"

api "$JAR_A" POST /api/v1/auth/logout ""
expect "logout -> 204" 204 "$HTTP_CODE"
api "$JAR_A" GET /api/v1/me ""
expect "session dead after logout -> 401" 401 "$HTTP_CODE"

# --- operator (admin) routes -----------------------------------------------
ADMIN_EMAIL="ops-$SUF@example.com"
printf '%s\n' "$PW" | docker compose exec -T gateway gatewayctl -db "$DB" create-admin "$ADMIN_EMAIL" >/dev/null
api "$JAR_ADM" POST /api/v1/auth/login "{\"email\":\"$ADMIN_EMAIL\",\"password\":\"$PW\"}"
expect "operator login -> 200" 200 "$HTTP_CODE"
api "$JAR_ADM" GET /api/v1/admin/tenants ""
expect "admin list tenants -> 200" 200 "$HTTP_CODE"
api "$JAR_ADM" PUT "/api/v1/admin/tenants/web-b-$SUF/quota" '{"rps":50,"burst":100}'
expect "admin set quota -> 200" 200 "$HTTP_CODE"
api "$JAR_B" GET /api/v1/tenant ""
printf '%s' "$BODY" | grep -q '"quota_rps":50' && ok "customer sees the quota the operator set" || fail "quota not visible: $BODY"

echo
echo "control-plane e2e: $PASS checks passed"
