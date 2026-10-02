# Deploying DistriKV on one Azure VM

This is a **single-VM** deployment. Three DistriKV node containers run on one
Azure VM. They are logically independent Raft replicas (separate processes,
separate data volumes, node-to-node traffic over a private Docker network), but
they share one machine, one disk and one failure domain. Do not describe it as
three physically distributed servers; a VM or disk failure takes all three down.
What it does demonstrate is real Raft behaviour: leader election, failover with
quorum, crash recovery from persisted state, and Multi-Raft.

## What gets deployed

```
Internet --80/443--> reverse-proxy (Caddy, automatic HTTPS)
                         |  /api/v1/*                      |  /kv /kv/* /v1/usage /healthz
                         v                                 v
                    gateway :9091 (control API)       gateway :8080 (data plane)
                                      |  gRPC over the private Docker network "distrikv-net"
              +-----------------------+-----------------------+
              v                       v                       v
         distrikv-1              distrikv-2              distrikv-3
         (node1)                 (node2)                 (node3)
          every node hosts a replica of:  group 0, group 1, group 2, metadata
```

| Item | Count | Source of truth |
|---|---|---|
| Azure VMs | 1 | `Standard_B2ats_v2`, 2 vCPU, 1 GiB |
| DistriKV node containers | 3 | `deployments/docker-compose.prod.yml` |
| Other containers | 2 (`gateway`, `reverse-proxy`) | same file |
| Data Raft groups | 3 (groups 0, 1, 2) | `deployments/shard.json` `{"version":1,"groups":3}` |
| Replicas per group | 3 (every node hosts every group) | `cmd/server/main.go`, same `-peers` for every group |
| Metadata Raft groups | 1 (replicated on all 3 nodes) | `cmd/server/main.go` always creates it |
| Logical data replicas | 3 x 3 = 9 | |
| Logical Raft replicas in total | 4 groups x 3 = 12 | |
| Fixed hash slots | 16384, split across the 3 groups | `internal/shard` |

The metadata group is not optional: the node binary creates it unconditionally
(`metadataGroupID`, a reserved ID) and it holds the versioned shard map. It is
not a separate container. Each group has its own leader; leaders are elected
independently, so group 0, group 1, group 2 and metadata can all be led by
different nodes.

## 0. Azure prerequisites

Network security group (NSG) inbound rules for the VM:

| Port | Source | Why |
|---|---|---|
| 22/tcp | **your own IP only** | SSH |
| 80/tcp | Any | Let's Encrypt HTTP challenge + redirect to HTTPS |
| 443/tcp | Any | HTTPS API |

Do **not** open 8080, 9090, 9091, 9100, 9101, 6060 or any Raft port. Nothing
except the reverse proxy is published by Docker (the healthcheck script
verifies this), but the NSG is a second wall.

**A DNS name is required for HTTPS.** Let's Encrypt cannot issue a certificate
for a bare IP address here. If you do not own a domain, use the free name Azure
gives you: Portal -> the VM -> Networking / Public IP address -> Configuration ->
**DNS name label** -> e.g. `distrikv`. Your name becomes
`distrikv.eastasia.cloudapp.azure.com` (use the region your IP is in). Also set
the public IP assignment to **Static** so the name never points elsewhere. If
you own a domain, create an `A` record to the VM's public IP instead.

## 1. Connect and install prerequisites (once)

```bash
ssh -i ~/.ssh/DistriKV_key.pem azureuser@<PUBLIC_IP_OR_DNS_NAME>
git clone <YOUR_REPO_URL> DistriKV && cd DistriKV
./scripts/setup_azure_vm.sh
exit          # log out and back in so the docker group applies
```

(The key file name/extension is whatever Azure gave you; `chmod 600` it on your
laptop.) `setup_azure_vm.sh` installs Docker Engine, the Compose plugin and Git,
adds a 2 GiB swap file and Docker log rotation. It installs nothing else.

## 2. Configure

```bash
cd ~/DistriKV
cp deployments/.env.example deployments/.env
nano deployments/.env
```

| Variable | Required | Meaning |
|---|---|---|
| `DOMAIN` | yes | DNS name pointing at the VM, e.g. `distrikv.eastasia.cloudapp.azure.com` |
| `CONTROL_ORIGINS` | yes | Browser origin(s) allowed to call the control API, e.g. `https://your-console.vercel.app` (exact, no trailing slash, comma-separated). Placeholder is fine until the frontend exists. |
| `CONTROL_SIGNUP` | no (default `false`) | Whether the control API lets anyone create an account. Keep `false` on a public VM until you have email verification. |

`deployments/.env` is git-ignored. There are no secrets in it. API keys,
passwords and sessions are created at runtime and live in the `gateway-data`
volume.

## 3. Build and start

### Option A: build on the VM (default; slow)

```bash
./scripts/deploy_vm.sh
```

The Go build of this project (it includes a pure-Go SQLite) is heavy for 1 GiB
RAM and 2 throttled vCPUs. The build uses `-p=1` and the swap file; expect
10-25 minutes the first time, and if it is killed (`signal: killed`), use B.

### Option B: build on your laptop, ship the image (no registry)

```bash
# on your laptop, in the repo (the VM is x86-64; on an Apple-silicon Mac this cross-builds):
docker buildx build --platform linux/amd64 --build-arg GO_BUILD_FLAGS="-trimpath" -t distrikv:latest --load .
docker save distrikv:latest | gzip | ssh -i ~/.ssh/DistriKV_key.pem azureuser@<HOST> 'gunzip | docker load'

# on the VM:
cd ~/DistriKV && ./scripts/deploy_vm.sh --no-build
```

`deploy_vm.sh` validates the compose file, starts the stack with `up -d`, waits
until all five containers are healthy and prints the status. It never deletes
data. Updating later: `git pull && ./scripts/deploy_vm.sh` (or Option B again).

## 4. Verify

```bash
./scripts/healthcheck_prod.sh
```

It checks that all 5 containers are healthy, every node hosts groups `0 1 2
metadata`, each group has exactly one leader, the gateway answers internally,
only ports 80/443 are published and only by the proxy, HTTPS works through
Caddy (resolved locally, so it also works before DNS propagates), `/metrics` is
not routed publicly, and a PUT/GET/DELETE round trip works (tenant
`healthcheck`, 30-minute key). Exit code 0 means everything passed.

See which node leads which group (read-only):

```bash
./scripts/raft_status_prod.sh           # once
./scripts/raft_status_prod.sh --watch   # refreshes every 2 s
```

## 5. Create your first tenant, key and operator

```bash
GW="docker exec distrikv-gateway gatewayctl -db /var/lib/gateway/gateway.db"
$GW create-tenant acme "Acme Inc"
$GW create-key acme laptop 720h        # prints the API key ONCE: copy it now
$GW list-keys acme
```

Use it over HTTPS (routes from `docs/gateway.md`):

```bash
export API=https://<DOMAIN>  KEY=dkv_live_...
curl -X PUT  "$API/kv/hello" -H "Authorization: Bearer $KEY" -d '{"value":"world"}'
curl         "$API/kv/hello" -H "Authorization: Bearer $KEY"
curl -X POST "$API/kv" -H "Authorization: Bearer $KEY" -d '{"op":"incr","key":"counter"}'
curl         "$API/v1/usage" -H "Authorization: Bearer $KEY"
```

Operator account for the console's admin routes (there is deliberately no HTTP
route that creates one; the password is read from stdin so it stays out of your
shell history and process list):

```bash
read -rs -p "operator password: " P; echo
printf '%s\n' "$P" | docker exec -i distrikv-gateway gatewayctl -db /var/lib/gateway/gateway.db create-admin ops@example.com
unset P
```

## 6. Start, stop, restart (read this before touching the VM)

All state lives in named Docker volumes: `distrikv-data-1/2/3` (one per node: Raft
logs, hard state and snapshots), `gateway-data` (SQLite) and `caddy-data`
(certificates). Define a shortcut:

```bash
cd ~/DistriKV
alias dkc='docker compose -f deployments/docker-compose.prod.yml --env-file deployments/.env'
```

| SAFE (keeps all data) | Effect |
|---|---|
| `dkc ps` | status |
| `dkc up -d` | start / apply changes |
| `dkc stop` | stop everything, keep containers and volumes |
| `dkc start` | start again after `stop` |
| `dkc restart` | restart everything |
| `docker restart distrikv-1` | restart one node; it recovers from its own volume |
| `dkc logs -f --tail=100 distrikv-1` | logs (JSON) |
| `docker stats --no-stream` | memory/CPU per container |
| `sudo reboot` | all containers have `restart: unless-stopped` and come back |

| DESTRUCTIVE (do not run unless you mean to wipe the cluster) | Effect |
|---|---|
| `dkc down -v` | **deletes every volume**: all Raft state, tenants, keys, certificates |
| `docker volume rm distrikv-data-1` (or any volume) | deletes that node's Raft state |
| `docker volume prune`, `docker system prune --volumes` | can delete volumes of stopped stacks |
| `rm -rf` on `/var/lib/docker` | everything |

`dkc down` (without `-v`) removes containers and the network but keeps volumes;
`up -d` brings it back. Prefer `stop`/`start`.

## 7. Failure and recovery test

Automated, with timings:

```bash
./scripts/failover_prod.sh        # stops the current leader of group 0, then restarts it
./scripts/failover_prod.sh 2      # or choose the victim node
```

It writes 30 keys, stops the victim, waits until every group has a leader among
the two remaining nodes, writes 30 more keys with the node down, reads all 60 back,
restarts the victim, waits until its commit index has caught up with each group's
leader, and reads all 60 keys again. It only stops/starts a container; it never
touches a volume.

The same thing by hand:

```bash
# A. All groups healthy, one leader each
./scripts/raft_status_prod.sh

# B. Write and read
GW="docker exec distrikv-gateway gatewayctl -db /var/lib/gateway/gateway.db"
$GW create-tenant demo "Failover demo" 2>/dev/null || true
KEY=$($GW create-key demo run1 1h | grep -o 'dkv_live_[0-9a-f]\{64\}')
curl -X PUT "https://<DOMAIN>/kv/demo1" -H "Authorization: Bearer $KEY" -d '{"value":"before"}'
curl        "https://<DOMAIN>/kv/demo1" -H "Authorization: Bearer $KEY"

# C. Identify the leader of group 0 (raw metric: is_leader=1 on exactly one node)
for n in 1 2 3; do docker exec distrikv-$n wget -q -O - http://127.0.0.1:9101/metrics | grep 'is_leader{.*group="0"}'; done

# D. Stop that leader (say it is distrikv-1)
docker stop distrikv-1

# E. Two nodes remain = quorum. A new leader appears within seconds:
./scripts/raft_status_prod.sh

# F. Write again while it is down
curl -X PUT "https://<DOMAIN>/kv/demo2" -H "Authorization: Bearer $KEY" -d '{"value":"during"}'

# G. Restart it. It reloads its persisted Raft log/snapshot, then catches up
docker start distrikv-1

# H. Its COMMIT row should reach the leaders' values; both keys still read back
./scripts/raft_status_prod.sh
curl "https://<DOMAIN>/kv/demo1" -H "Authorization: Bearer $KEY"
curl "https://<DOMAIN>/kv/demo2" -H "Authorization: Bearer $KEY"

# Optional: how often each node has seen the leader change
for n in 1 2 3; do docker exec distrikv-$n wget -q -O - http://127.0.0.1:9101/metrics | grep leader_changes_total; done
```

Stopping **two** nodes removes the majority: reads and writes then fail (503/504)
until one returns. That is correct Raft behaviour, not a bug.

## 8. Backups

```bash
./scripts/backup_gateway_db.sh     # -> ~/distrikv-backups/gateway-<UTC timestamp>.db, keeps 14
```

It uses `gatewayctl backup` (SQLite `VACUUM INTO`) inside the running gateway,
so it is consistent while serving traffic. The file contains hashed API keys and
password hashes: keep it private (it is created with mode 600). Daily via cron:

```bash
( crontab -l 2>/dev/null; echo '0 3 * * * cd $HOME/DistriKV && ./scripts/backup_gateway_db.sh >> $HOME/distrikv-backup.log 2>&1' ) | crontab -
```

Copy backups off the VM (`scp`) or they die with it.

Restore (replaces the live gateway database; stops only the gateway):

```bash
dkc stop gateway
docker run --rm -v gateway-data:/v -v "$HOME/distrikv-backups:/b:ro" alpine \
  sh -c 'rm -f /v/gateway.db /v/gateway.db-wal /v/gateway.db-shm && cp /b/gateway-<TIMESTAMP>.db /v/gateway.db && chown 1000:1000 /v/gateway.db && chmod 600 /v/gateway.db'
dkc start gateway
```

The Raft/KV data is **not** in this backup. It is replicated three times, but on
one disk. For protection against disk loss, take an Azure disk snapshot of the VM
OS disk (Portal -> Disks -> Create snapshot) after `dkc stop`, or accept that this
is a demo deployment.

## 9. Resource limits on a 1 GiB VM

* Each node is capped at 200 MiB (`GOMEMLIMIT` 160 MiB), the gateway at 160 MiB,
  Caddy at 96 MiB. These are starting points; check `docker stats --no-stream`.
  A container killed for memory shows `OOMKilled=true` in `docker inspect`.
* The KV engine is **in memory**: committed data is rebuilt from the persisted Raft
  log and snapshot on every start. Keep the data set small (tens of MiB), or RAM, not
  disk, is the limit.
* `B2ats_v2` is a burstable VM with a low CPU baseline. Under sustained load, CPU
  credits can run out; delayed heartbeats can then cause leader changes (watch
  `distrikv_raft_leader_changes_total`). The election/heartbeat timeouts are fixed in
  `cmd/server/main.go` and were deliberately not changed. Use this VM for demos and
  functional/failure testing, not for benchmark numbers.
* Prometheus and Grafana are not started. They remain available for a larger VM via
  the repo's development compose file.

## 10. What the shard map does and does not do

* Keys map to one of 16384 slots by hash; the slots are split across groups 0/1/2
  according to `shard.json` (read by every node and the gateway at start).
* The metadata Raft group stores the authoritative, versioned shard config and
  replicates `MoveSlots` commands. `MoveSlots` **refuses** to move a slot range that
  contains keys, unless the caller passes `--unsafe-no-migration`, which moves ownership
  without moving data (the keys then look lost until moved by hand).
* There is **no** migration worker: no key copy, checkpoint, retry, verification or
  cutover. Do not claim online shard migration. Moving an empty range works; moving data
  safely is not implemented. See `docs/rebalancing.md`.

## 11. Security checklist before making it public

1. SSH (22) restricted to your IP in the NSG; password login disabled (Azure default with a key).
2. Keep `CONTROL_SIGNUP=false` until email verification exists, otherwise anyone can create accounts
   and tenants on a 1 GiB VM.
3. `/api/v1/admin/*` is reachable on the public hostname (protected by operator login and a
   per-account/per-IP login limiter). If you do not need it from the internet, block it in the
   Caddyfile (`handle /api/v1/admin/* { respond 404 }` before the `/api/v1/*` block) or restrict by IP.
4. Set `CONTROL_ORIGINS` to your real console origin only. A wrong value does not break the data plane.
5. Session cookies are `SameSite=Strict`: a console on `*.vercel.app` calling an API on a different
   registrable domain will not get the cookie. Fix this when deploying the frontend (serve the console
   under the same registrable domain, or proxy `/api/v1` through the console's domain). This is why the
   frontend step comes after this one.
6. The gateway's rate limits and usage counters are per process and in memory (single gateway here, fine).
7. No email verification or password reset by email; operators reset passwords with
   `gatewayctl reset-password`.
8. Node-to-node Raft traffic is plain gRPC without TLS or authentication. That is acceptable only because
   it never leaves the private Docker network on a single VM. Do not publish node ports.
9. Keep the OS patched (`sudo apt-get update && sudo apt-get upgrade`); consider unattended upgrades.
10. API keys are shown once at creation. Rotate/revoke with `gatewayctl` or the console API if one leaks.

## Troubleshooting

| Symptom | Check |
|---|---|
| `deploy_vm.sh` build ends with `signal: killed` | out of memory: confirm swap (`free -h`), or use Option B |
| proxy never becomes healthy / HTTPS fails | `docker logs distrikv-proxy`; DNS must resolve to the VM; NSG must allow 80 and 443 |
| `set CONTROL_ORIGINS in deployments/.env` | the variable is missing from `deployments/.env` |
| a node stays `unhealthy` | `docker logs distrikv-N`; the healthcheck needs the node's metrics endpoint, which starts only after all four groups are created |
| writes return 503/504 right after a restart | election in progress; retry after a few seconds (`raft_status_prod.sh`) |
| permission error in a data directory | volumes are initialised from the image (uid 1000); never mount a host directory over them as root |

## Accurate wording for a resume or demo

> Deployed DistriKV as three independent Raft replicas in isolated Docker containers on one Azure VM, with
> Multi-Raft (three data groups plus a metadata group), persistent Raft state, gateway routing with
> API-key tenancy, automatic HTTPS, and scripted leader-failure and recovery testing.
