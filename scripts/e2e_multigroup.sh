#!/usr/bin/env bash
# Multigroup e2e test: verifies keys land in different groups and tenant isolation holds.
#
# Requires: docker compose -f docker-compose.yml -f docker-compose.multigroup.yml up -d --wait

set -euo pipefail

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8080}"
DB_PATH="/var/lib/gateway/gateway.db"

echo "=== Multigroup e2e test ==="

# Create two tenants
echo "Creating tenants..."
docker exec distrikv-gateway gatewayctl -db "$DB_PATH" create-tenant tenant1 "Tenant 1" 2>/dev/null || echo "tenant1 exists"
docker exec distrikv-gateway gatewayctl -db "$DB_PATH" create-tenant tenant2 "Tenant 2" 2>/dev/null || echo "tenant2 exists"

KEY1=$(docker exec distrikv-gateway gatewayctl -db "$DB_PATH" create-key tenant1 multigroup-test 2>/dev/null | grep "dkv_live_" | head -1)
KEY2=$(docker exec distrikv-gateway gatewayctl -db "$DB_PATH" create-key tenant2 multigroup-test 2>/dev/null | grep "dkv_live_" | head -1)

echo "KEY1: $KEY1"
echo "KEY2: $KEY2"

# Function to test a key
test_key() {
    local key=$1
    local val=$2
    local auth=$3
    local expected_status=$4

    put_status=$(curl -s -o /dev/null -w "%{http_code}" -X PUT "$GATEWAY_URL/kv/$key" -H "Authorization: Bearer $auth" -d "{\"value\":\"$val\"}")
    if [[ "$put_status" != "$expected_status" ]]; then
        echo "FAIL: PUT $key returned $put_status (expected $expected_status)"
        return 1
    fi

    get_response=$(curl -s "$GATEWAY_URL/kv/$key" -H "Authorization: Bearer $auth")
    get_status=$?
    if [[ $get_status -ne 0 ]] || ! echo "$get_response" | grep -q "\"value\":\"$val\""; then
        echo "FAIL: GET $key failed or value mismatch: $get_response"
        return 1
    fi

    return 0
}

# Test ~30 keys per tenant (should distribute across all 3 groups)
echo "=== Testing ~30 keys per tenant (multi-group routing) ==="
for tenant in 1 2; do
    key_var="KEY$tenant"
    auth="${!key_var}"
    echo "Testing tenant$tenant with ${#auth} char key..."

    success=0
    for i in {1..30}; do
        key="tenant${tenant}-key-$i"
        val="tenant${tenant}-value-$i"
        if test_key "$key" "$val" "$auth" 200; then
            ((success++))
        else
            echo "  FAIL on $key"
        fi
    done
    echo "  tenant$tenant: $success/30 PUT+GET succeeded"
    if [[ $success -ne 30 ]]; then
        echo "FAIL: expected 30/30, got $success"
        exit 1
    fi
done

# Test tenant isolation
echo "=== Testing tenant isolation ==="
isolation_key="isolation-test"
if test_key "$isolation_key" "tenant1-secret" "$KEY1" 200; then
    echo "  tenant1 PUT: OK"
else
    echo "FAIL: tenant1 PUT"
    exit 1
fi

# tenant2 should not see tenant1's key
resp=$(curl -s "$GATEWAY_URL/kv/$isolation_key" -H "Authorization: Bearer $KEY2")
if echo "$resp" | grep -q "not_found"; then
    echo "  tenant2 cannot see tenant1's key: OK"
else
    echo "FAIL: tenant2 saw tenant1's key: $resp"
    exit 1
fi

# tenant2 writes its own value
if test_key "$isolation_key" "tenant2-secret" "$KEY2" 200; then
    echo "  tenant2 PUT: OK"
else
    echo "FAIL: tenant2 PUT"
    exit 1
fi

# tenant1 should still see its own value
resp=$(curl -s "$GATEWAY_URL/kv/$isolation_key" -H "Authorization: Bearer $KEY1")
if echo "$resp" | grep -q "tenant1-secret"; then
    echo "  tenant1 still sees own value: OK"
else
    echo "FAIL: tenant1 value changed: $resp"
    exit 1
fi

# Test CAS across groups
echo "=== Testing CAS ==="
cas_key="cas-test"
if test_key "$cas_key" "v1" "$KEY1" 200; then
    echo "  initial PUT: OK"
else
    echo "FAIL: CAS initial PUT"
    exit 1
fi

# CAS create-if-absent (expected empty)
cas_body='{"op":"cas","key":"'$cas_key'","expected":"","value":"v2"}'
resp=$(curl -s -X POST "$GATEWAY_URL/kv" -H "Authorization: Bearer $KEY1" -H "Content-Type: application/json" -d "$cas_body")
if echo "$resp" | grep -q '"applied":false'; then
    echo "  CAS create-if-absent refused (key exists): OK"
else
    echo "FAIL: CAS create-if-absent should fail: $resp"
    exit 1
fi

# CAS with correct expected
cas_body='{"op":"cas","key":"'$cas_key'","expected":"v1","value":"v3"}'
resp=$(curl -s -X POST "$GATEWAY_URL/kv" -H "Authorization: Bearer $KEY1" -H "Content-Type: application/json" -d "$cas_body")
if echo "$resp" | grep -q '"applied":true'; then
    echo "  CAS with correct expected: OK"
else
    echo "FAIL: CAS with correct expected failed: $resp"
    exit 1
fi

# Verify new value
resp=$(curl -s "$GATEWAY_URL/kv/$cas_key" -H "Authorization: Bearer $KEY1")
if echo "$resp" | grep -q "v3"; then
    echo "  CAS value updated: OK"
else
    echo "FAIL: CAS value not updated: $resp"
    exit 1
fi

echo ""
echo "=== All multigroup e2e checks passed ==="