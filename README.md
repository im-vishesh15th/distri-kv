# DistriKV

Fault-tolerant, horizontally scalable distributed key-value database in Go —
and a self-serve service built on top of it (sign up → API key → use).

> **Depth > Breadth** — correctness first. Every feature ships with tests,
> fault injection, and a stated invariant. No claims without proof, no
> fabricated benchmark numbers.

## What it is

- **Engine:** custom persistent Raft consensus (election, replication, quorum
  commit), replicated state machine, client request deduplication,
  linearizable reads (ReadIndex), snapshots + InstallSnapshot, fixed-slot
  sharding (16384 slots) + Multi-Raft.
- **Assurance:** deterministic simulated network (seeded delay/drop/reorder/
  partition), automated fault tests, linearizability checking (custom checker).
- **Product:** hosted service — tenants, API keys, quotas/rate limiting,
  gateway, onboarding — so any business can use DistriKV over gRPC/HTTPS.

**Key decision:** no separate WAL — the persistent Raft log *is* the durable
write-ahead log (append / fsync / checksum / recovery / replay / truncate /
compaction). One log, one source of order.

## Quick Start

```bash
go run ./cmd/server &                # start a node (gRPC on :8080)
go run ./cmd/client set hello world  # write
go run ./cmd/client get hello        # read -> world
make test                            # unit + integration
make test-race                       # race detector (required before merge)
make vet && make fmt
make lint                            # gofmt -l check + go vet; fails if anything is unformatted
make proto                           # regenerate gen/ from proto/kv.proto
```

## Use it as a service (Docker)

```bash
make demo-up      # 3-node cluster + gateway; prints an API key and a curl example
make e2e          # end-to-end checks (tenant isolation, 429, revoked key -> 401, ...)
```

```bash
curl -X PUT localhost:8080/kv/hello -H "Authorization: Bearer $DKV_KEY" -d '{"value":"world"}'
curl localhost:8080/kv/hello        -H "Authorization: Bearer $DKV_KEY"
curl localhost:8080/v1/usage        -H "Authorization: Bearer $DKV_KEY"
```

Only the gateway port is published. See [Deployment](docs/deploy.md) and
[Gateway](docs/gateway.md).

## Roadmap

| Milestone | Phases | What you get | Status |
|---|---|---|---|
| **M0 Foundation** | 0 | Repo, design docs, baseline | ✅ done |
| **M1 Single-node DB** | 1–2 | In-memory engine + gRPC API | ✅ done |
| **M2 Durable consensus** | 3–7 | Persistent log, election, replication, state machine | ✅ done |
| **M3 Hardening** | 8–11 | Leader routing, dedup, atomics, linearizable reads | ✅ done |
| **M4 Lifecycle** | 12–13 | Snapshots, log compaction, InstallSnapshot | ✅ done |
| **M5 Proof** | 14–16 | Simulated network, fault tests, linearizability checking | ✅ done — **Tier 1 complete** |
| **M6 Scale** | 17–21 | Multi-Raft, fixed-slot sharding, cluster router | ✅ Phases 17–21 done |
| **M7 Measure** | 22–24 | Consistency spec, benchmarks, pprof | ✅ done |
| **M8 Product** | P1–P5 | API keys, tenancy, quotas, gateway, deploy, metrics | ✅ lean: keys, tenancy, rate + concurrency limits, TLS flags, usage endpoint, compose stack, Prometheus/Grafana, e2e in CI. control-plane API for a web console (`/api/v1`, [docs](docs/control-api.md)). Not done: the web frontend itself, email verification, daily budgets, idempotency keys |
| **M9 Launch** | 25–30 | Docker, observability, CI, full test suite, docs | ⬜ |

**Tier 1 (must-have) ends at M5.** Rebalancing, metadata Raft, and cloud
deployment are explicitly optional: a finished, correct core beats a
half-finished extra.

## Documentation

- [Architecture](docs/architecture.md) — layers, invariants, persistence, failure model
- [Persistence](docs/persistence.md) — on-disk format, fsync policy, recovery decision table
- [Snapshots](docs/snapshots.md) — applied-state capture, single-file sidecar, log compaction, InstallSnapshot catch-up
- [Raft](docs/raft.md) — election core: event-loop model, tick time, vote rules, tests
- [Consistency](docs/consistency.md) — precise guarantees and the tests that prove them
- [Client sessions](docs/client-sessions.md) — effectively-once mutations: session identity, sequence dedup, error contract, honest limits
- [Concurrency](docs/concurrency.md) — three levels (clients / event loop / shards) and who owns what
- [Simulation](docs/simulation.md) — deterministic in-process network: seeded faults, clock modes, reproducibility boundary
- [Faults](docs/faults.md) — spec §19 scenarios, seeded chaos, disk failure, and the pre-vote protections
- [Linearizability](docs/linearizability.md) — operation histories, the checker, and the end-to-end proof
- [Multi-Raft](docs/multiraft.md) — per-group Raft, group-multiplexed transport, group vs node failure model
- [Sharding](docs/shard.md) — fixed-slot key→group mapping, the shard map, and the routing chain
- [Cluster Router](docs/router.md) — key→slot→group→leader→node, per-group client state, `-shard-config`
- [Rebalancing](docs/rebalancing.md) — metadata Raft group, MoveSlots, data migration, client config sync
- [Gateway](docs/gateway.md) — HTTP API, API keys, tenant isolation, rate/concurrency limits, TLS, usage, metrics
- [Control-plane API](docs/control-api.md) — web-console JSON API: signup/login sessions, API-key management, quotas, usage, operator routes ([OpenAPI](docs/openapi.yaml))
- [Deployment](docs/deploy.md) — compose topology, demo, e2e, VM outline, backups
- [Benchmarks](docs/benchmarks.md) — real end-to-end results: 3-node cluster over gRPC, matrix + node scaling (1/3/5) + failover + snapshot recovery, with environment and exact commands

## Non-goals

No SQL, no ACID transaction engine, no Redis compatibility, no Kubernetes, no
RocksDB-class storage engine. Every exclusion above exists to protect the core
goal: a deep, provably correct distributed systems project.

## License

TBD.
