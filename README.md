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
  partition), automated fault tests, linearizability checking (Porcupine).
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
make proto                           # regenerate gen/ from proto/kv.proto
```

## Roadmap

| Milestone | Phases | What you get | Status |
|---|---|---|---|
| **M0 Foundation** | 0 | Repo, design docs, baseline | ✅ done |
| **M1 Single-node DB** | 1–2 | In-memory engine + gRPC API | ✅ done |
| **M2 Durable consensus** | 3–7 | Persistent log, election, replication, state machine | ✅ done |
| **M3 Hardening** | 8–11 | Leader routing, dedup, atomics, linearizable reads | ✅ done (Phase 12 next) |
| **M4 Lifecycle** | 12–13 | Snapshots, log compaction, InstallSnapshot | ⬜ |
| **M5 Proof** | 14–16 | Simulated network, fault tests, linearizability checking | ⬜ |
| **M6 Scale** | 17–21 | Multi-Raft, fixed-slot sharding, cluster router | ⬜ |
| **M7 Measure** | 22–24 | Consistency spec, benchmarks, pprof | ⬜ |
| **M8 Product** | P1–P5 | API keys, tenancy, quotas, gateway, onboarding, deploy | ⬜ |
| **M9 Launch** | 25–30 | Docker, observability, CI, full test suite, docs | ⬜ |

**Tier 1 (must-have) ends at M5.** Rebalancing, metadata Raft, and cloud
deployment are explicitly optional: a finished, correct core beats a
half-finished extra.

## Documentation

- [Architecture](docs/architecture.md) — layers, invariants, persistence, failure model
- [Persistence](docs/persistence.md) — on-disk format, fsync policy, recovery decision table
- [Raft](docs/raft.md) — election core: event-loop model, tick time, vote rules, tests
- [Consistency](docs/consistency.md) — precise guarantees and the tests that prove them
- [Concurrency](docs/concurrency.md) — three levels (clients / event loop / shards) and who owns what

## Non-goals

No SQL, no ACID transaction engine, no Redis compatibility, no Kubernetes, no
RocksDB-class storage engine. Every exclusion above exists to protect the core
goal: a deep, provably correct distributed systems project.

## License

TBD.
