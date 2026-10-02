#!/usr/bin/env bash
# generate_traffic.sh: Generate realistic cluster traffic for DistriKV dashboard telemetry.
# Usage: ./scripts/generate_traffic.sh [API_KEY] [GATEWAY_URL]

set -e

GATEWAY_URL="${2:-http://localhost:8080}"
KEY="${1:-}"

if [ -z "$KEY" ]; then
  # Try checking env var DKV_KEY
  KEY="${DKV_KEY:-}"
fi

if [ -z "$KEY" ]; then
  echo "Error: No API key provided."
  echo "Usage: ./scripts/generate_traffic.sh <API_KEY>"
  echo "Example: ./scripts/generate_traffic.sh dkv_live_... "
  exit 1
fi

echo "=========================================================="
echo " DistriKV Traffic Simulator"
echo " Gateway: $GATEWAY_URL"
echo " Key:     ${KEY:0:16}••••••••"
echo "=========================================================="

echo ""
echo "==> 1. Generating PUT write traffic (session & cache entries)..."
for i in $(seq 1 40); do
  curl -s -o /dev/null -X PUT "$GATEWAY_URL/kv/session:usr_$i" \
    -H "Authorization: Bearer $KEY" \
    -H "Content-Type: application/json" \
    -d "{\"value\":\"{\\\"user_id\\\":$i,\\\"token\\\":\\\"tok_$(date +%s)_$i\\\",\\\"status\\\":\\\"active\\\"}\"}" &
  if (( i % 10 == 0 )); then wait; fi
done
wait
echo "    ✓ 40 PUT requests completed."

echo ""
echo "==> 2. Generating GET read traffic (cache lookups)..."
for i in $(seq 1 100); do
  id=$(( (i % 40) + 1 ))
  curl -s -o /dev/null -X GET "$GATEWAY_URL/kv/session:usr_$id" \
    -H "Authorization: Bearer $KEY" &
  if (( i % 20 == 0 )); then wait; fi
done
wait
echo "    ✓ 100 GET requests completed."

echo ""
echo "==> 3. Generating CAS (Compare-And-Swap) atomic updates..."
for i in $(seq 1 25); do
  curl -s -o /dev/null -X POST "$GATEWAY_URL/kv" \
    -H "Authorization: Bearer $KEY" \
    -H "Content-Type: application/json" \
    -d "{\"op\":\"cas\",\"key\":\"counter:global\",\"value\":\"val_$i\"}" &
  if (( i % 5 == 0 )); then wait; fi
done
wait
echo "    ✓ 25 CAS requests completed."

echo ""
echo "==> 4. Generating DELETE requests (cache evictions)..."
for i in $(seq 1 15); do
  curl -s -o /dev/null -X DELETE "$GATEWAY_URL/kv/session:usr_$i" \
    -H "Authorization: Bearer $KEY" &
  if (( i % 5 == 0 )); then wait; fi
done
wait
echo "    ✓ 15 DELETE requests completed."

echo ""
echo "==> 5. Generating burst traffic to trigger rate limiting (429 Drops)..."
for i in $(seq 1 120); do
  curl -s -o /dev/null -X GET "$GATEWAY_URL/kv/session:usr_1" \
    -H "Authorization: Bearer $KEY" &
done
wait
echo "    ✓ Burst traffic complete."

echo ""
echo "==> Current Gateway Telemetry for this key:"
curl -s "$GATEWAY_URL/v1/usage" -H "Authorization: Bearer $KEY" | python3 -m json.tool 2>/dev/null || curl -s "$GATEWAY_URL/v1/usage" -H "Authorization: Bearer $KEY"

echo ""
echo "=========================================================="
echo " All traffic generated! Refresh your DistriKV Console dashboard to see the charts & metrics."
echo "=========================================================="
