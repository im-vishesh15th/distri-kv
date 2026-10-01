#!/usr/bin/env bash
# End-to-end product check against a RUNNING stack (make demo-up first).
#
#   tenant + key creation -> PUT/GET -> tenant isolation -> CAS ->
#   usage endpoint -> metrics stay private -> burst => 429 -> revoke => 401
#
# Env: GATEWAY_URL (default http://localhost:8080)
set -euo pipefail
cd "$(dirname "$0")/.."

URL="${GATEWAY_URL:-http://localhost:${GATEWAY_PORT:-8080}}"
DB=/var/lib/gateway/gateway.db
ctl() { docker compose exec -T gateway gatewayctl -db "$DB" "$@"; }

PASS=0
fail() { echo "FAIL: $*" >&2; exit 1; }
ok()   { PASS=$((PASS+1)); echo "ok   $*"; }
expect() { # expect <description> <want> <got>
  [ "$2" = "$3" ] && ok "$1" || fail "$1: want '$2', got '$3'"
}
# call <key> <method> <path> [body] -> sets HTTP_CODE and BODY
call() {
  local key=$1 method=$2 path=$3 body=${4:-}
  local out
  if [ -n "$body" ]; then
    out=$(curl -s -w '\n%{http_code}' -X "$method" "$URL$path" -H "Authorization: Bearer $key" -d "$body")
  else
    out=$(curl -s -w '\n%{http_code}' -X "$method" "$URL$path" -H "Authorization: Bearer $key")
  fi
  HTTP_CODE=$(printf '%s' "$out" | tail -n1)
  BODY=$(printf '%s' "$out" | sed '$d')
}
newkey() { ctl create-key "$1" e2e | grep -o 'dkv_live_[0-9a-f]\{64\}'; }

# Unique tenants per run so reruns never collide with earlier state.
SUF="$(date +%s)-$$"
A="e2e-a-$SUF"; B="e2e-b-$SUF"

ctl create-tenant "$A" "E2E A" >/dev/null
ctl create-tenant "$B" "E2E B" >/dev/null
KEY_A=$(newkey "$A"); KEY_B=$(newkey "$B")
[ -n "$KEY_A" ] && [ -n "$KEY_B" ] || fail "key creation"
ok "created two tenants with API keys"

echo "waiting for the cluster to accept writes..."
for i in $(seq 1 60); do
  call "$KEY_A" PUT /kv/probe '{"value":"1"}'
  [ "$HTTP_CODE" = 200 ] && break
  [ "$i" = 60 ] && fail "cluster not ready after 60s (HTTP $HTTP_CODE: $BODY)"
  sleep 1
done
ok "cluster accepts writes through the gateway"

# --- basic CRUD
call "$KEY_A" PUT /kv/color '{"value":"blue"}'; expect "PUT returns 200" 200 "$HTTP_CODE"
call "$KEY_A" GET /kv/color;                   expect "GET returns 200" 200 "$HTTP_CODE"
expect "GET returns the value written" '{"value":"blue"}' "$BODY"

# --- tenant isolation
call "$KEY_B" GET /kv/color; expect "tenant B cannot see tenant A's key" 404 "$HTTP_CODE"
call "$KEY_B" PUT /kv/color '{"value":"red"}'
call "$KEY_A" GET /kv/color
expect "tenant A's value untouched by tenant B's write" '{"value":"blue"}' "$BODY"

# --- CAS (the safe pattern; see docs/gateway.md on retries)
call "$KEY_A" POST /kv '{"op":"cas","key":"lock","value":"me"}'
expect "CAS create-if-absent applies" '{"applied":true}' "$BODY"
call "$KEY_A" POST /kv '{"op":"cas","key":"lock","value":"you"}'
expect "second CAS create-if-absent is refused" '{"applied":false}' "$BODY"

# --- usage endpoint
call "$KEY_A" GET /v1/usage; expect "/v1/usage returns 200" 200 "$HTTP_CODE"
echo "$BODY" | grep -q '"requests":[1-9]' || fail "/v1/usage shows no requests: $BODY"
ok "/v1/usage counts this tenant's requests"

# --- metrics are not on the public port
call "$KEY_A" GET /metrics; expect "/metrics not served on the public port" 404 "$HTTP_CODE"
docker compose exec -T gateway wget -qO- http://localhost:9100/metrics \
  | grep -q "distrikv_gateway_requests_total{tenant=\"$A\"" \
  || fail "private metrics port has no series for tenant $A"
ok "private /metrics port reports per-tenant counters"

# --- burst => 429 with Retry-After (tenant B only)
ctl set-quota "$B" 1 2 >/dev/null
GOT429=0; RA=""
for i in $(seq 1 30); do
  hdr=$(curl -s -D - -o /dev/null "$URL/kv/color" -H "Authorization: Bearer $KEY_B")
  if printf '%s' "$hdr" | head -n1 | grep -q ' 429'; then
    GOT429=$((GOT429+1))
    RA=$(printf '%s' "$hdr" | tr -d '\r' | awk 'tolower($1)=="retry-after:"{print $2}')
  fi
done
[ "$GOT429" -ge 1 ] || fail "burst of 30 requests never returned 429"
[ -n "$RA" ] || fail "429 response had no Retry-After header"
ok "burst returns 429 with Retry-After ($GOT429 of 30 rejected)"
call "$KEY_A" GET /kv/color; expect "tenant A unaffected by B's rate limit" 200 "$HTTP_CODE"

# --- revoke => 401, immediately
PREFIX=$(echo "$KEY_A" | cut -c1-17)
ctl revoke-key "$A" "$PREFIX" >/dev/null
call "$KEY_A" GET /kv/color; expect "revoked key is rejected at once" 401 "$HTTP_CODE"

echo
echo "e2e passed ($PASS checks)"
