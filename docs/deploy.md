# Deployment

## Topology

```
internet --HTTPS--> gateway (:8080, the ONLY published port)
                       |  gRPC, private network
                       v
        distrikv-1   distrikv-2   distrikv-3     (Raft cluster)
                       ^
   prometheus --scrapes--> nodes :9101, gateway :9100   (private)
   grafana (opt-in profile) :3000
```

Raft/gRPC ports and both `/metrics` ports are not published. Grafana and
Prometheus start only with `--profile monitoring`.

## One-command demo

```bash
make demo-up           # build, start 3 nodes + gateway, create tenant + key, print curl
make e2e               # end-to-end checks against the running stack
make demo-monitoring   # add Prometheus + Grafana (http://localhost:3000)
make demo-down         # stop and DELETE all volumes
```

Requires Docker with Compose v2 and `curl`. Override ports with
`GATEWAY_PORT` / `GRAFANA_PORT`.

## What `make e2e` proves

Tenant and key creation; PUT then GET; tenant B cannot read tenant A's key and
cannot overwrite it; CAS create-if-absent; `/v1/usage` counts; `/metrics` is not
on the public port but is on the private one; a burst returns 429 with
`Retry-After` and does not affect other tenants; a revoked key gets 401
immediately. CI runs it on every push (`e2e` job).

## Multi-group cluster

For higher throughput, DistriKV can run with multiple Raft groups (shards).
The node count stays the same; each node participates in every group.

```bash
docker compose -f docker-compose.yml -f docker-compose.multigroup.yml up -d --wait
```

This mounts `deployments/shard.json` (`{"version":1,"groups":3}`) into all
nodes **and the gateway**. The gateway is started with `-shard-config`, so it
loads the same shard map and sets it on every pooled client via
`client.SetShardMap`. The SDK then routes each key to the correct Raft group.

Run the multigroup e2e:

```bash
make e2e-multigroup   # or ./scripts/e2e_multigroup.sh
```

This verifies ~30 keys per tenant round-trip across all groups, tenant
isolation, and CAS across groups.

### Gateway shard-map flag

`-shard-config=/path/to/shard.json` — optional JSON file with the shard map.
When omitted, the gateway runs in single-group mode (all keys group 0).
The file format is:

```json
{"version": 1, "groups": 3}
```

or with explicit ranges:

```json
{"version": 2, "groups": 3,
 "ranges": [{"start": 0, "end": 5461, "group": 0}, ...]}
```

The gateway loads this at startup and calls `SetShardMap` on every client in
its pool. If the file is missing or invalid, the gateway fails to start.

## Running on VMs (outline)

1. Put the three nodes on separate VMs; open the Raft port only between them.
2. Run the gateway on a fourth host (or alongside one node); only its HTTPS
   port is public. Pass `-tls-cert/-tls-key` or put Caddy in front.
3. Keep `-metrics-addr` bound to a private interface.
4. **Back up the gateway SQLite file** (`gateway-data` volume): it is the only
   record of tenants and hashed keys, and it is a single point of failure.
   Copying it while the gateway runs is safe only via `sqlite3 .backup`.
5. Rolling restart of nodes: one at a time, wait until the restarted node is
   caught up (`distrikv_raft_apply_lag` back to 0) before touching the next;
   with 3 nodes you can lose one, never two.

## Not done

Email verification and a signup web page (keys are issued with `gatewayctl`),
daily request budgets, idempotency keys for HTTP retries, and automatic
control-plane backups.
