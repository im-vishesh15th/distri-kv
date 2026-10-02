# Control-plane API (web console)

The control plane is the JSON API behind a web frontend: signup and login,
API-key management, quota and usage views for customers, and tenant
administration for operators. It is **separate from the data plane**: the data
plane (`/kv`, `/v1/usage`) authenticates with API keys; the control plane
authenticates humans with a session cookie.

```
browser (console SPA) --cookie--> control API (:9091) --> SQLite (tenants, keys, users, sessions)
customer servers      --API key--> gateway   (:8080) --> cluster (gRPC)
```

Machine-readable spec: [`openapi.yaml`](openapi.yaml). Code: `internal/gateway/control*.go`,
`accounts*.go`, `password.go`. Disabled unless `-control-bind` is set.

## Running it

```
gateway -db gateway.db -control-bind :9091 \
        -control-origins https://console.example.com
```

| Flag | Default | Meaning |
|---|---|---|
| `-control-bind` | *(off)* | listen address for the control API |
| `-control-origins` | *(none)* | comma-separated browser origins allowed for CORS + CSRF (exact match, e.g. `http://localhost:5173`) |
| `-control-signup` | `true` | allow `POST /api/v1/auth/signup` |
| `-control-cookie-secure` | `false` | set the `Secure` cookie flag (forced on when `-tls-cert` is set) |
| `-control-trust-xff` | `false` | take the client IP from the **last** `X-Forwarded-For` hop (only behind your own proxy) |
| `-control-session-ttl` | `24h` | login session lifetime (absolute, not sliding) |

TLS reuses `-tls-cert/-tls-key`. In `docker-compose.yml` the control port is published
on **127.0.0.1 only** (`CONTROL_PORT`, default 9091; `CONTROL_ORIGINS`, default
`http://localhost:5173`). Put a reverse proxy in front for real deployments.

Create the first operator (there is deliberately no HTTP route that creates one):

```
printf '%s\n' "$PASSWORD" | gatewayctl -db gateway.db create-admin ops@example.com
```

## Conventions for the frontend

- Base path `/api/v1`. Bodies and responses are JSON. Errors are always
  `{"error":{"code":"...","message":"..."}}` (same shape as the data plane); switch on `code`.
- **Cookies:** call with `credentials: "include"` (fetch) / `withCredentials: true` (axios).
  The cookie `dkv_session` is `HttpOnly`, `SameSite=Strict`, `Path=/`; JavaScript cannot read it.
  Use `GET /me` on page load to learn whether the user is logged in.
- **CORS:** only origins listed in `-control-origins` get `Access-Control-Allow-Origin`
  (with `Allow-Credentials: true`). Preflights are answered. Dev and prod origins must be
  *same-site* (e.g. `localhost:5173` -> `localhost:9091`, or `app.example.com` ->
  `api.example.com`) because the cookie is `SameSite=Strict`.
- **CSRF:** state-changing requests need `Content-Type: application/json` (415 otherwise) and, when
  the browser sends `Origin`, it must be an allowed origin or equal to the request host (403
  `origin_not_allowed` otherwise).
- Every response carries `X-Request-ID` (an incoming valid one is echoed) and `Cache-Control: no-store`.
- Timestamps are RFC 3339 UTC strings. `null` means "not set".
- API-key plaintext is returned **once** (signup, create, rotate). Show it with a copy button and
  a "you won't see this again" notice; the list endpoint only ever returns the prefix.

## Endpoints

### Public

| Request | Success | Notes |
|---|---|---|
| `GET /health` | `200 {"status":"ok"}` | |
| `POST /auth/signup` `{email, password, tenant_id?, name?}` | `201 {user, tenant, api_key:{key,prefix,name,created_at}}` + session cookie | creates the account, its tenant and a first key named `default`; password >= 10 chars; `tenant_id` must match `^[a-z0-9][a-z0-9_-]{0,62}$` (auto-generated `t-xxxxxxxxxx` if omitted). `409 email_taken` / `tenant_taken`, `403 signup_disabled` |
| `POST /auth/login` `{email, password}` | `200 {user, tenant?}` + session cookie | `401 invalid_credentials` (same body for unknown email and wrong password) |

### Authenticated customer (session cookie)

| Request | Success | Notes |
|---|---|---|
| `POST /auth/logout` | `204` | ends this session |
| `GET /me` | `200 {user, tenant?}` | `401 unauthenticated` when not logged in |
| `PUT /me/password` `{current_password, new_password}` | `204` | all *other* sessions are ended |
| `GET /tenant` | `200 {tenant}` | includes `quota_rps/burst` (0 = default) and `effective_*` |
| `GET /tenant/usage` | `200 {tenant_id, usage}` | data-plane counters since the gateway process started (`usage.since`) |
| `GET /tenant/keys` | `200 {keys:[{prefix,name,status,created_at,expires_at,revoked_at}]}` | `status`: `active` / `expired` / `revoked` |
| `POST /tenant/keys` `{name, ttl_hours?}` | `201 {key, info}` | `ttl_hours` 0 or absent = no expiry, max 8760; `409 key_limit_reached` (default 10 active keys) |
| `POST /tenant/keys/{prefix}/rotate` `{ttl_hours?}` | `201 {key, info, revoked_prefix}` | body optional; old key is revoked immediately; `409 key_not_active` |
| `DELETE /tenant/keys/{prefix}` | `204` | takes effect on the data plane on the next request; `404 key_not_found` |

`{prefix}` is the 17-character display prefix, e.g. `dkv_live_ab12cd34`.

### Operator (`user.is_admin`, else `403 forbidden`)

| Request | Success |
|---|---|
| `GET /admin/tenants?limit=50&after=<id>` | `200 {tenants:[...], next_after}` (`next_after` is `""` on the last page; limit 1-200) |
| `GET /admin/tenants/{id}` | `200 {tenant}` |
| `PUT /admin/tenants/{id}/quota` `{rps, burst}` | `200 {tenant}`; both required; `0 0` = gateway default; takes effect on the next data-plane request |
| `GET /admin/tenants/{id}/usage` | `200 {tenant_id, usage}` |
| `GET /admin/tenants/{id}/keys` | `200 {keys}` |
| `DELETE /admin/tenants/{id}/keys/{prefix}` | `204` |

Operator accounts have no tenant, so the customer routes answer `404 no_tenant` for them.

## Security model

- **Passwords:** PBKDF2-HMAC-SHA256, 600 000 iterations, 16-byte random salt, stored as
  `pbkdf2-sha256$iters$salt$hash` (iterations are stored per hash so they can be raised later).
  Implemented in `password.go` and checked against RFC 7914 test vectors. Length-only policy (10-128).
- **Sessions:** 256-bit random token in the cookie; only its SHA-256 is stored, so a database leak does
  not yield usable sessions. Absolute lifetime (`-control-session-ttl`); expired rows are pruned every 10 minutes.
  Changing the password ends all other sessions; `gatewayctl reset-password` ends all of them.
- **Brute force:** per-IP limit on signup+login (10/min) and per-account limit on login and password
  change (5/min). While an account is limited, even the correct password is refused (no oracle).
  Unknown emails run a dummy hash so timing does not reveal which accounts exist.
- **Isolation:** every customer route derives the tenant from the session's user, never from the request;
  revoking or rotating another tenant's prefix returns `404`. Covered by `TestTenantIsolationInControlPlane`.
- **Audit log:** with a logger configured (the binary logs JSON to stderr) every signup, login, failed login,
  logout, password change, key create/rotate/revoke and quota change emits an `audit` line with
  request ID, client IP and user/tenant. No passwords, tokens or API keys are ever logged.

## Known limitations

- No email verification or password-reset-by-email (there is no mail infrastructure). Signups are
  therefore open to anyone who can reach the endpoint: leave `-control-signup=false` on a public
  deployment until you add verification, or front it with your own gate. Operators can reset a
  password with `gatewayctl reset-password`.
- One user per tenant, one tenant per user. No teams, roles beyond customer/operator, SSO or MFA.
- No tenant suspension/deletion endpoint (an operator can revoke keys and set the quota to a tiny value).
- Rate limiters and usage counters are in memory per gateway process; with several gateway
  replicas each enforces its own limits.
- Sessions use cookies only; non-browser clients should use the data-plane API keys instead.
