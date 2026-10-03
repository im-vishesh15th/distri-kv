# DistriKV

<p align="center">
  <strong>A fault-tolerant, horizontally scalable distributed key-value database in Go — with a self-serve hosted service on top.</strong>
</p>

<p align="center">
  Persistent Raft · Multi-Raft · Sharding · Linearizable Reads · Snapshots · Fault Injection · Tenant Isolation · API Gateway
</p>

---

## Overview

**DistriKV** is a distributed key-value database built from first principles in Go.

The project is intentionally focused on **correctness, failure handling, and distributed-systems fundamentals** rather than breadth. It implements a persistent Raft-based replicated state machine, Multi-Raft sharding, deterministic fault simulation, linearizability checking, snapshots, client request deduplication, and a service layer with API keys, tenant isolation, quotas, and rate/concurrency limits.

DistriKV can be used in two ways:

1. **As a distributed database engine** — run nodes and interact with the KV API/gRPC interface.
2. **As a hosted service** — tenants sign up, receive API keys, and access isolated key-value data through an HTTP gateway.

> **Depth > Breadth:** every major feature is designed around explicit invariants, tests, fault injection, and reproducible behavior. The project avoids making unsupported correctness or benchmark claims.

---

## Architecture

```text
                         ┌──────────────────────────┐
                         │        Clients            │
                         │  CLI / SDK / HTTP / gRPC │
                         └────────────┬─────────────┘
                                      │
                                      ▼
                         ┌──────────────────────────┐
                         │      Caddy / HTTPS       │
                         └────────────┬─────────────┘
                                      │
                                      ▼
                         ┌──────────────────────────┐
                         │        Gateway           │
                         │                          │
                         │ API keys                 │
                         │ Authentication           │
                         │ Tenant isolation         │
                         │ Rate/concurrency limits  │
                         │ Usage & metrics          │
                         └────────────┬─────────────┘
                                      │
                                      ▼
                 ┌────────────────────────────────────────┐
                 │             Cluster Router             │
                 │                                        │
                 │ key → slot → group → leader → node    │
                 └───────────────┬────────────────────────┘
                                 │
             ┌───────────────────┼───────────────────┐
             ▼                   ▼                   ▼
      ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
      │   Node 1    │     │   Node 2    │     │   Node 3    │
      │             │     │             │     │             │
      │ Raft Group 1│◄───►│ Raft Group 1│◄───►│ Raft Group 1│
      │ Raft Group 2│     │ Raft Group 2│     │ Raft Group 2│
      │ Raft Group 3│     │ Raft Group 3│     │ Raft Group 3│
      │ Metadata    │     │ Metadata    │     │ Metadata    │
      └─────────────┘     └─────────────┘     └─────────────┘
```

### Production topology

The current Azure deployment uses:

- 3 DistriKV node containers
- 3 data Raft groups per node
- 1 metadata Raft group
- 12 logical Raft replicas across the cluster
- 1 gateway
- 1 Caddy reverse proxy
- HTTPS on ports 80/443
- Only the gateway/reverse-proxy surface is publicly exposed

> These are **logically independent replicas on one VM**, not three physical servers. A single-VM deployment demonstrates distributed behavior but does not provide physical-machine failure independence.

---

# Core Features

## 1. Persistent Raft Consensus

DistriKV implements a persistent Raft consensus layer including:

- Leader election
- Candidate voting
- Term management
- Log replication
- Quorum-based commit
- Leader routing
- Follower catch-up
- Persistent recovery
- Log truncation
- Log compaction
- Snapshots
- InstallSnapshot
- Pre-vote protections

The Raft log is also the durable write-ahead log.

### No separate WAL

DistriKV deliberately does **not** maintain a separate WAL:

```text
Client mutation
      │
      ▼
Raft log append
      │
      ├── checksum
      ├── fsync
      └── persistent ordering
      │
      ▼
Raft replication
      │
      ▼
Quorum commit
      │
      ▼
State machine apply
```

This gives the system a single durable ordering source:

> **The persistent Raft log is the WAL.**

---

## 2. Replicated State Machine

Committed Raft entries are applied to a replicated state machine.

The core invariant is:

```text
Only committed Raft entries are applied as durable state-machine mutations.
```

This allows replicas to converge on the same deterministic state when they process the same committed command sequence.

---

## 3. Linearizable Reads

DistriKV supports linearizable reads using **Raft ReadIndex** semantics.

A successful read is tied to a sufficiently current committed state rather than simply reading arbitrary local follower state.

Conceptually:

```text
Client
  │
  ▼
Leader
  │
  ├── establish read barrier
  │
  ▼
Committed state
  │
  ▼
Read result
```

See [`docs/consistency.md`](docs/consistency.md) for the exact consistency contract and tests.

---

## 4. Client Request Deduplication

DistriKV supports effectively-once mutation behavior through client sessions and sequence numbers.

A client request contains session identity and a monotonically increasing sequence number.

Conceptually:

```text
(client_id, sequence_number)
              │
              ▼
        Deduplication
              │
       ┌──────┴──────┐
       │             │
    new request    duplicate
       │             │
       ▼             ▼
    execute       return stored
                  result / error
```

This protects against retries causing the same mutation to be applied multiple times.

See [`docs/client-sessions.md`](docs/client-sessions.md).

---

# Multi-Raft and Sharding

## Fixed-Slot Sharding

DistriKV divides the keyspace into:

```text
16,384 fixed slots
```

A key is routed through:

```text
key
 │
 ▼
slot
 │
 ▼
Raft group
 │
 ▼
leader
 │
 ▼
node
```

This provides horizontal distribution across multiple Raft groups.

See:

- [`docs/shard.md`](docs/shard.md)
- [`docs/multiraft.md`](docs/multiraft.md)
- [`docs/router.md`](docs/router.md)

---

## Group vs Node

A **physical/logical node** hosts multiple Raft groups.

For example:

```text
Node 1
├── Raft Group 1
├── Raft Group 2
├── Raft Group 3
└── Metadata Group
```

A Raft group is a **logical consensus group**.

A node is a **process/container that hosts one or more groups**.

Therefore:

```text
3 nodes ≠ 3 Raft groups
```

The current production topology has multiple logical Raft groups distributed across three node containers.

---

# Failure Handling

DistriKV was designed around failure rather than assuming a perfect network.

The project includes a deterministic simulated network capable of modeling:

- Message delay
- Message drops
- Message reordering
- Network partitions
- Node failures
- Leader failures
- Disk failures
- Recovery
- Snapshot catch-up

Simulation is seeded so failures can be reproduced.

Example conceptual flow:

```text
Seed
 │
 ├── deterministic delays
 ├── deterministic drops
 ├── deterministic reorder
 └── deterministic partitions
          │
          ▼
     Fault scenario
          │
          ▼
     System behavior
          │
          ▼
     Invariant checker
```

See [`docs/simulation.md`](docs/simulation.md) and [`docs/faults.md`](docs/faults.md).

---

# Linearizability Checking

DistriKV includes a custom linearizability checker for operation histories.

The checker evaluates whether concurrent operations can be arranged into a valid sequential history consistent with real-time ordering and the specified operation semantics.

This is used to validate consistency claims rather than relying only on happy-path tests.

See [`docs/linearizability.md`](docs/linearizability.md).

---

# Snapshots and Log Compaction

Long-running Raft logs cannot grow indefinitely.

DistriKV therefore supports:

- State-machine snapshots
- Snapshot persistence
- Log compaction
- Snapshot installation
- Follower catch-up from snapshots

Conceptually:

```text
Raft Log
──────────────────────────────────────────────►

[old entries][old entries][old entries][new entries]
      │
      └──────────► snapshot
                    │
                    ▼
             compact old log
```

A lagging replica can recover through `InstallSnapshot` when the required historical log entries are no longer available.

See [`docs/snapshots.md`](docs/snapshots.md).

---

# Service Layer

DistriKV is not only a database engine. It also includes a self-serve service layer.

## Tenant Isolation

Each business operates within its own tenant boundary.

Conceptually:

```text
Business A
   │
   ├── API Key A
   └── Tenant A
        │
        └── isolated KV data

Business B
   │
   ├── API Key B
   └── Tenant B
        │
        └── isolated KV data
```

The gateway associates authenticated requests with their authorized tenant and enforces tenant scoping for data operations.

A client cannot obtain another tenant's data merely by changing a database, key, or request parameter.

Tenant isolation is covered by the end-to-end test suite.

---

## API Keys

The service supports API-key-based authentication.

Example:

```bash
curl -X PUT http://localhost:8080/kv/hello \
  -H "Authorization: Bearer $DKV_KEY" \
  -H "Content-Type: application/json" \
  -d '{"value":"world"}'
```

Read:

```bash
curl http://localhost:8080/kv/hello \
  -H "Authorization: Bearer $DKV_KEY"
```

Usage:

```bash
curl http://localhost:8080/v1/usage \
  -H "Authorization: Bearer $DKV_KEY"
```

API keys can be revoked, and revoked keys are rejected by the gateway.

---

## Rate and Concurrency Limits

The service layer supports:

- Per-tenant rate limiting
- Concurrency limits
- Quotas
- Usage tracking

This protects the database from uncontrolled client traffic and provides tenant-level resource controls.

---

# Gateway and Control Plane

The HTTP gateway provides the service-facing API.

It handles:

```text
Authentication
      │
Tenant resolution
      │
Authorization
      │
Rate/concurrency limits
      │
Usage accounting
      │
Cluster routing
      │
DistriKV
```

The control-plane API provides endpoints for:

- Signup
- Login sessions
- API-key management
- Quotas
- Usage
- Operator routes

See [`docs/gateway.md`](docs/gateway.md) and [`docs/control-api.md`](docs/control-api.md).

---

# Observability

DistriKV includes production-oriented observability components.

Current stack includes:

- Prometheus
- Grafana
- Gateway metrics
- Usage endpoint
- Health checks
- Deployment health scripts
- Failover verification
- pprof support

The goal is to make system behavior observable during normal operation and failure scenarios.

---

# Quick Start

## Requirements

Recommended environment:

- Go
- Docker
- Docker Compose
- Make
- `protoc` for protobuf regeneration
- Git

Check the toolchain:

```bash
go version
docker --version
docker compose version
make --version
```

---

## Run a Single Node

Start a server:

```bash
go run ./cmd/server &
```

The gRPC server runs on:

```text
:8080
```

Use the client:

```bash
go run ./cmd/client set hello world
```

Read:

```bash
go run ./cmd/client get hello
```

Expected:

```text
world
```

---

# Development Commands

Run tests:

```bash
make test
```

Run the race detector:

```bash
make test-race
```

Run static checks:

```bash
make vet
```

Format:

```bash
make fmt
```

Run lint checks:

```bash
make lint
```

Regenerate protobuf code:

```bash
make proto
```

Recommended pre-merge sequence:

```bash
make fmt
make vet
make lint
make test
make test-race
```

---

# Run the Full Service Stack

Start the demo environment:

```bash
make demo-up
```

The demo starts a multi-node DistriKV cluster and gateway and prints an API key with an example request.

Run end-to-end tests:

```bash
make e2e
```

The E2E suite covers service-level behavior including:

- Tenant isolation
- API-key authentication
- Revoked keys
- Rate limiting
- Concurrency limits
- KV operations
- Usage behavior

---

# Example Service Usage

After starting the service:

```bash
export DKV_KEY="<your-api-key>"
```

Write:

```bash
curl -X PUT http://localhost:8080/kv/hello \
  -H "Authorization: Bearer $DKV_KEY" \
  -H "Content-Type: application/json" \
  -d '{"value":"world"}'
```

Read:

```bash
curl http://localhost:8080/kv/hello \
  -H "Authorization: Bearer $DKV_KEY"
```

Usage:

```bash
curl http://localhost:8080/v1/usage \
  -H "Authorization: Bearer $DKV_KEY"
```

> Never commit real API keys, credentials, TLS private keys, or production secrets to Git.

---

# Docker Deployment

Start the service stack:

```bash
make demo-up
```

Run E2E verification:

```bash
make e2e
```

Only the gateway surface should be exposed to clients in the intended deployment topology.

See [`docs/deploy.md`](docs/deploy.md).

---

# Azure Production Deployment

DistriKV can be deployed to an Ubuntu 24.04 Azure VM.

## 1. Prepare the VM

```bash
./scripts/setup_azure_vm.sh
```

After the script completes, log out and back in if required.

## 2. Configure environment

```bash
cp deployments/.env.example deployments/.env
```

Configure:

```text
DOMAIN=...
CONTROL_ORIGINS=...
```

Do not commit the `.env` file.

## 3. Deploy

```bash
./scripts/deploy_vm.sh
```

## 4. Health check

```bash
./scripts/healthcheck_prod.sh
```

## 5. Failover verification

```bash
./scripts/failover_prod.sh
```

The failover procedure stops a leader, continues writes through the remaining quorum, restarts the failed component, and verifies catch-up.

See [`docs/deploy-azure.md`](docs/deploy-azure.md).

---

# Production Architecture

The current single-VM production topology is:

```text
                    Internet
                       │
                     HTTPS
                       │
                       ▼
               ┌──────────────┐
               │    Caddy     │
               │ TLS / Proxy  │
               └──────┬───────┘
                      │
                      ▼
               ┌──────────────┐
               │   Gateway    │
               │ Auth / API   │
               └──────┬───────┘
                      │
          ┌───────────┼───────────┐
          ▼           ▼           ▼
      ┌───────┐   ┌───────┐   ┌───────┐
      │ Node1 │   │ Node2 │   │ Node3 │
      │       │   │       │   │       │
      │ G1 G2 │   │ G1 G2 │   │ G1 G2 │
      │ G3 M  │   │ G3 M  │   │ G3 M  │
      └───────┘   └───────┘   └───────┘
```

Where:

```text
G1/G2/G3 = data Raft groups
M        = metadata Raft group
```

Again, these are separate logical replicas/processes hosted on one VM. This deployment does not protect against physical VM loss.

---

# Project Structure

A simplified repository layout:

```text
DistriKV/
├── cmd/
│   ├── server/
│   ├── client/
│   └── gateway/
│
├── internal/
│   ├── raft/
│   ├── storage/
│   ├── multiraft/
│   ├── shard/
│   ├── router/
│   ├── gateway/
│   ├── sessions/
│   └── ...
│
├── proto/
│   └── kv.proto
│
├── gen/
│   └── ...
│
├── deployments/
│   ├── docker-compose.yml
│   ├── docker-compose.prod.yml
│   └── .env.example
│
├── scripts/
│   ├── setup_azure_vm.sh
│   ├── deploy_vm.sh
│   ├── healthcheck_prod.sh
│   └── failover_prod.sh
│
├── docs/
│   ├── architecture.md
│   ├── persistence.md
│   ├── snapshots.md
│   ├── raft.md
│   ├── consistency.md
│   ├── client-sessions.md
│   ├── concurrency.md
│   ├── simulation.md
│   ├── faults.md
│   ├── linearizability.md
│   ├── multiraft.md
│   ├── shard.md
│   ├── router.md
│   ├── rebalancing.md
│   ├── gateway.md
│   ├── control-api.md
│   ├── deploy.md
│   ├── deploy-azure.md
│   └── benchmarks.md
│
├── Makefile
└── README.md
```

---

# Correctness Model

DistriKV is built around explicit invariants.

Important invariants include:

### Raft safety

Committed entries must not be overwritten by conflicting entries.

### State-machine safety

Only committed commands are applied as durable state-machine mutations.

### Linearizable reads

Successful linearizable reads must observe a state consistent with the required linearization point.

### Deduplication

A retried mutation with the same client session/sequence identity must not produce an additional state-machine mutation.

### Tenant isolation

An authenticated tenant must only access resources authorized for that tenant.

### Snapshot correctness

Installing a valid snapshot must produce state equivalent to applying the snapshot's represented committed state.

### Routing correctness

A key must consistently map through:

```text
key → slot → group → leader → node
```

subject to the current shard configuration.

---

# Testing Strategy

DistriKV uses multiple levels of testing.

## Unit tests

Validate individual components and invariants.

```bash
make test
```

## Integration tests

Validate interactions between storage, Raft, state machine, routing, and service components.

## Race detection

```bash
make test-race
```

Go's race detector is required before merge for concurrency-sensitive changes.

## Fault injection

Deterministic simulated failures include:

```text
message drop
message delay
message reorder
network partition
leader failure
node restart
disk failure
snapshot recovery
```

## Linearizability checking

Operation histories are checked against the system's consistency specification.

## End-to-end service testing

```bash
make e2e
```

This validates the complete path:

```text
client
  → gateway
  → authentication
  → tenant authorization
  → routing
  → Raft cluster
  → replicated state machine
```

---

# Benchmarks

Benchmark results are maintained separately in:

[`docs/benchmarks.md`](docs/benchmarks.md)

The benchmark documentation records:

- Hardware/environment
- Cluster topology
- Node count
- Workload
- Commands
- Failover scenarios
- Snapshot recovery scenarios
- Measurements

No benchmark number should be interpreted without its corresponding environment and methodology.

> **No fabricated benchmark numbers.** Results should only be reported when reproducible from the documented benchmark setup.

---


### M8 Product status

Implemented:

- API keys
- Tenant isolation
- Rate limits
- Concurrency limits
- TLS configuration
- Usage endpoint
- Gateway
- Control-plane API
- Docker Compose stack
- Prometheus/Grafana
- End-to-end CI checks

Not currently implemented:

- Web console frontend
- Email verification
- Daily budgets
- Idempotency keys

---

# Design Trade-offs

## Why a custom Raft implementation?

The purpose of DistriKV is to understand and demonstrate distributed-systems internals rather than simply assembling existing distributed database components.

The implementation therefore owns:

```text
elections
replication
persistent logs
quorum commit
snapshots
recovery
fault simulation
consistency checking
```

---

## Why no separate WAL?

Because the persistent Raft log already provides:

```text
durability
ordering
recovery
replay
replication
```

Maintaining a second independent WAL would introduce another durable ordering mechanism without being necessary for this design.

---

## Why fixed 16,384 slots?

Fixed slots make the routing model explicit and deterministic:

```text
key → slot → group
```

This provides a clean foundation for Multi-Raft and future slot movement/rebalancing.

---

## Why one VM for the current deployment?

The current Azure deployment is intended to demonstrate:

- Production-style containerization
- TLS
- gateway routing
- health checks
- failover behavior
- observability
- deployment automation

It is **not** presented as physical high availability.

Three containers on one VM cannot survive loss of that VM.

---


# Security Notes

Never commit:

```text
.env
API keys
private keys
TLS private certificates
cloud credentials
database credentials
```

Use environment variables or a secrets manager for production credentials.

The gateway should be the only publicly exposed application surface in the intended deployment.

For production deployments:

- Use HTTPS/TLS.
- Restrict administrative/control-plane access.
- Rotate credentials when compromised.
- Keep production `.env` files outside Git.
- Review exposed ports.
- Back up persistent state.
- Verify restoration procedures.

See [`docs/deploy-azure.md`](docs/deploy-azure.md) for deployment and security notes.

---

# Documentation

| Topic | Documentation |
|---|---|
| Architecture | [`docs/architecture.md`](docs/architecture.md) |
| Persistence | [`docs/persistence.md`](docs/persistence.md) |
| Raft | [`docs/raft.md`](docs/raft.md) |
| Consistency | [`docs/consistency.md`](docs/consistency.md) |
| Client sessions | [`docs/client-sessions.md`](docs/client-sessions.md) |
| Concurrency | [`docs/concurrency.md`](docs/concurrency.md) |
| Snapshots | [`docs/snapshots.md`](docs/snapshots.md) |
| Simulation | [`docs/simulation.md`](docs/simulation.md) |
| Fault injection | [`docs/faults.md`](docs/faults.md) |
| Linearizability | [`docs/linearizability.md`](docs/linearizability.md) |
| Multi-Raft | [`docs/multiraft.md`](docs/multiraft.md) |
| Sharding | [`docs/shard.md`](docs/shard.md) |
| Cluster Router | [`docs/router.md`](docs/router.md) |
| Rebalancing | [`docs/rebalancing.md`](docs/rebalancing.md) |
| Gateway | [`docs/gateway.md`](docs/gateway.md) |
| Control Plane | [`docs/control-api.md`](docs/control-api.md) |
| Deployment | [`docs/deploy.md`](docs/deploy.md) |
| Azure Deployment | [`docs/deploy-azure.md`](docs/deploy-azure.md) |
| Benchmarks | [`docs/benchmarks.md`](docs/benchmarks.md) |

---

# Project Philosophy

DistriKV follows a few principles:

### 1. Correctness before features

A smaller correct distributed system is more valuable than a large system with unclear guarantees.

### 2. Explicit invariants

Every important subsystem should have a clearly stated invariant.

### 3. Reproducible failures

Fault injection should be deterministic wherever possible.

### 4. Honest guarantees

Do not claim:

```text
"high availability"
"linearizability"
"fault tolerance"
"production ready"
```

without defining exactly what those terms mean and showing the corresponding tests or evidence.

### 5. Measure, don't invent

Performance claims must come from reproducible benchmarks with documented environments and workloads.

---
