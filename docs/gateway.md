# Gateway

The gateway is the only component customers talk to. It authenticates an API
key, maps it to a tenant, rate-limits that tenant, rewrites every key into the
tenant's namespace, and forwards the call to the cluster over gRPC.

```
customer server --HTTPS--> gateway --gRPC--> cluster (Raft ports stay private)
                              |
                         SQLite: tenants + hashed API keys
```

## Tenant isolation

Storage key = `"t:" + tenantID + ":" + key`. Tenant IDs must match
`^[a-z0-9][a-z0-9_-]{0,62}$`, so they can never contain `:`; the first `:`
after `t:` therefore always ends the tenant ID. Two different
`(tenantID, key)` pairs can never produce the same storage key
(`FuzzNamespaceInjective`). `NamespaceKey` is the only function that builds
data-plane keys. Customers never supply a tenant ID: it comes from the API key.

## API keys

Format `dkv_live_` + 64 hex chars. Only the SHA-256 hash is stored; the
display prefix (`dkv_live_` + 8 hex) identifies a key in `list/revoke/rotate`.
The plaintext is printed once at creation. Revocation, expiry and quota changes
are read from the database on every request, so they take effect immediately.

## HTTP API (all routes need `Authorization: Bearer <key>`)

| Request | Result |
|---|---|
| `GET /kv/{key}` | `200 {"value":"..."}` or `404` |
| `PUT /kv/{key}` body `{"value":"..."}` | `200 {"ok":true}` |
| `DELETE /kv/{key}` | `204` |
| `POST /kv` `{"op":"set","key":"k","value":"v"}` | `200 {"ok":true}` |
| `POST /kv` `{"op":"cas","key":"k","expected":"old","value":"new"}` | `200 {"applied":true/false}`; omit `expected` = only if absent |
| `POST /kv` `{"op":"incr"\|"decr","key":"k","delta":5}` | `200 {"value":N}`; delta defaults to 1, must be > 0 |
| `GET /healthz` | `200 ok` (no auth) |

Errors are JSON: `{"error":{"code":"...","message":"..."}}`. Status codes:
401 bad/missing/revoked/expired key, 400 bad input, 404 missing key, 409
not-integer/overflow, 413 body or value too large, 429 rate limited (with
`Retry-After`), 503 cluster unavailable, 504 cluster timeout.

## Known limitations (be upfront about these)

- **HTTP retries are not idempotent.** The cluster deduplicates retries by
  (client_id, sequence) for gateway-to-cluster calls, but a *customer's* retry
  after a timeout is a new request. `incr`/`decr` can apply twice; `cas` is the
  safe pattern. An `Idempotency-Key` header is future work.
- **No TLS in the gateway.** Terminate TLS in front of it (Caddy, a cloud load
  balancer) before exposing it.
- **Rate limits are per gateway process, in memory.** Running N gateways gives
  each tenant N times the limit.
- **Every request does two SQLite reads** (key, tenant). That keeps revocation
  instant; add a short-lived cache only if profiling shows it matters.
- **`gatewayctl` edits the database file directly**, so run it on the gateway's
  machine. The SQLite file is a single point of failure: back it up.
- The HTTP API carries UTF-8 text values only.
