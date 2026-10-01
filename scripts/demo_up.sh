#!/usr/bin/env bash
# make demo-up: start 3 nodes + gateway, create a demo tenant and API key,
# wait until the cluster accepts writes, and print a ready-to-run curl example.
set -euo pipefail
cd "$(dirname "$0")/.."

GATEWAY_URL="${GATEWAY_URL:-http://localhost:${GATEWAY_PORT:-8080}}"
DB=/var/lib/gateway/gateway.db
ctl() { docker compose exec -T gateway gatewayctl -db "$DB" "$@"; }

echo "==> starting stack (first run builds the image)"
docker compose up -d --build --wait

echo "==> creating tenant 'demo' and a new API key"
ctl create-tenant demo "Demo Tenant" >/dev/null 2>&1 || echo "    (tenant 'demo' already exists)"
KEY=$(ctl create-key demo "demo-$(date +%s)" | grep -o 'dkv_live_[0-9a-f]\{64\}')
[ -n "$KEY" ] || { echo "could not create a key" >&2; exit 1; }

echo "==> waiting for the cluster to elect a leader and accept writes"
for i in $(seq 1 60); do
  code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$GATEWAY_URL/kv/hello" \
    -H "Authorization: Bearer $KEY" -d '{"value":"world"}' || true)
  [ "$code" = "200" ] && break
  [ "$i" = 60 ] && { echo "cluster not ready after 60s (last HTTP $code)" >&2; exit 1; }
  sleep 1
done

cat <<MSG

DistriKV is up. Only the gateway is published: $GATEWAY_URL

Your API key (shown once; create more with: docker compose exec gateway gatewayctl -db $DB create-key demo <name>):

  $KEY

Try it:

  export DKV_KEY=$KEY
  curl -s $GATEWAY_URL/kv/hello -H "Authorization: Bearer \$DKV_KEY"
  curl -s -X PUT $GATEWAY_URL/kv/greeting -H "Authorization: Bearer \$DKV_KEY" -d '{"value":"hi"}'
  curl -s $GATEWAY_URL/v1/usage -H "Authorization: Bearer \$DKV_KEY"

Dashboards:  make demo-monitoring   (Grafana on http://localhost:${GRAFANA_PORT:-3000})
Stop:        make demo-down
MSG
