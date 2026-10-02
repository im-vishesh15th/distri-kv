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
| `GET /v1/usage` | `200` this tenant's counters since the gateway started (see Usage) |
| `GET /healthz` | `200 ok` (no auth) |

Errors are JSON: `{"error":{"code":"...","message":"..."}}`. Status codes:
401 bad/missing/revoked/expired key, 400 bad input, 404 missing key, 409
not-integer/overflow, 413 body or value too large, 429 rate limited or too
many in-flight requests (both with `Retry-After`; the error `code` is
`rate_limited` or `too_many_concurrent_requests`), 503 cluster unavailable,
504 cluster timeout.

## Limits per tenant

Order of checks: auth, then token-bucket rate limit, then concurrency cap.

- **Rate limit:** `-rate-limit-rps` / `-rate-limit-burst` defaults, overridden
  per tenant with `gatewayctl set-quota`. Takes effect on the next request.
- **Concurrency cap:** `-max-concurrent-per-tenant` (default 128). The check is
  non-blocking: a request over the cap is rejected immediately with 429, never
  queued, so one tenant cannot pile up goroutines. Each tenant has its own
  slots.

## TLS

`-tls-cert cert.pem -tls-key key.pem` serves HTTPS (TLS 1.2+). Giving only one
of the two is a startup error. Without them the gateway speaks plain HTTP,
which is fine for local use or behind a TLS-terminating proxy (Caddy, a cloud
load balancer). For a quick local certificate use `mkcert`.

## Multi-group (sharding)

`-shard-config /path/to/shard.json` — optional JSON file with the shard map.
When provided, the gateway loads it at startup and calls `SetShardMap` on
every client in its pool, so the SDK routes each key to the correct Raft group.

File format:

```json
{"version": 1, "groups": 3}
```

or with explicit ranges:

```json
{"version": 2, "groups": 3,
 "ranges": [{"start": 0, "end": 5461, "group": 0}, ...]}
```

When omitted, the gateway runs in single-group mode (all keys route to group
0). See `docs/deploy.md` for the docker-compose override.

## Usage and metrics

`GET /v1/usage` returns the calling tenant's request counts by op and status,
plus `rate_limited` and `concurrency_limited`, and `since` (when counting
began). **Counters are in memory and reset when the gateway restarts.**

`-metrics-addr :9100` serves Prometheus `/metrics` on a separate listener. It is
never mounted on the public port, because it exposes tenant IDs; keep it on a
private network. Series: `distrikv_gateway_requests_total{tenant,op,status}`,
`distrikv_gateway_request_duration_seconds` (histogram by op),
`distrikv_gateway_rate_limited_total{tenant}`,
`distrikv_gateway_concurrency_limited_total{tenant}`. The tenant label is fine
for hundreds of tenants; cap or drop it before running thousands. Unauthenticated
requests are counted under `tenant="none"`.

## Control plane (web console API)

Signup, login, key management, quotas and operator tooling for a web frontend live in a separate
JSON API, enabled with `-control-bind`. See [control-api.md](control-api.md) and
[openapi.yaml](openapi.yaml). It shares the same SQLite database as the data plane, so a key
created, rotated or revoked in the console is honoured by the very next data-plane request.

## Known limitations (be upfront about these)

- **HTTP retries are not idempotent.** The cluster deduplicates retries by
  (client_id, sequence) for gateway-to-cluster calls, but a *customer's* retry
  after a timeout is a new request. `incr`/`decr` can apply twice; `cas` is the
  safe pattern. An `Idempotency-Key` header is future work.
- **No daily request budget or storage cap yet.** Only rate and concurrency
  limits exist.
- **TLS is optional.** Plain HTTP is the default; always enable TLS (or a
  proxy) before exposing the gateway to the internet.
- **Rate limits, concurrency caps and usage counters are per gateway process,
  in memory.** Running N gateways gives each tenant N times the limits, and
  usage resets on restart.
- **Every request does two SQLite reads** (key, tenant). That keeps revocation
  instant; add a short-lived cache only if profiling shows it matters.
- **`gatewayctl` edits the database file directly**, so run it on the gateway's
  machine. The SQLite file is a single point of failure: back it up.
- The HTTP API carries UTF-8 text values only.
