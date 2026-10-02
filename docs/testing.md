# Testing Strategy

This document describes the testing philosophy, tools, and mutation testing approach used in DistriKV.

---

## Test Suite Overview

| Package | Purpose |
|---------|---------|
| `internal/raft` | Core Raft implementation tests |
| `internal/lincheck` | Custom linearizability checker (NOT Porcupine) |
| `internal/history` | Operation history recording |
| `internal/shard` | Shard mapping and config tests |
| `internal/multiraft` | Multi-Raft host and cluster tests |
| `internal/server` | gRPC service routing tests |
| `internal/kv` | Engine and state machine tests |
| `internal/transport/sim` | Deterministic fault-injection network |
| `internal/gateway` | HTTP gateway: auth, rate-limiting, tenancy, metrics, `/v1/usage` |
| `internal/nodemetrics` | Raft node metrics collector (Prometheus text format, no deps) |

---

## Fault Injection Testing (Phase 15)

The `internal/raft/faults_test.go` implements spec §19 scenarios using the deterministic simulation network:

- `TestSimLeaderKillRestartAgrees` — leader killed and restarted, state agrees
- `TestSimPartitionLeaderFromFollower` — one follower isolated from the leader
- `TestSimBlockLeaderFromFollower` — blocks single link (no partition); pre-vote keeps the leader in its original term throughout
- `TestSimPartitionedNodeDoesNotInflateTerm` — an isolated node's election timeouts change no term (pre-vote)
- `TestSimMinorityLeaderCannotCommit` — minority leader cannot commit
- `TestSimSeededChaosConverges` — random faults, system converges
- `TestSimDiskFailureHaltsNode` — disk failure halts node

All run over the deterministic sim network (`internal/transport/sim`).

---

## Linearizability Testing (Phase 16)

The `internal/lincheck` package provides a **custom backtracking linearizability checker** (NOT Porcupine). It:

1. Records operation histories with `internal/history.Recorder`
2. Uses two-phase search: successful ops alone first, then branches on failed writes
3. 5M-node budget, memoization for performance
4. Used by `TestLinearizableConcurrent` and `TestLinearizableChaos`

The checker verifies that operation histories admit a real-time-consistent total order.

---

## Mutation Testing (Verification of Test Effectiveness)

To verify that our tests actually catch bugs, we introduce deliberate
mutations (bugs) and confirm tests FAIL. The results below are **exact
observed failures** from full re-runs on 2026-10-02
(`go test -race -count=1 ./...` on a clean tree at `eb1f554`);
everything is reproducible with the commands shown.

### Mutation 1: Broken Quorum Math

**Location:** `internal/raft/replicate.go` — `maybeAdvanceCommit()`

**Change:** Replace correct majority calculation `len(peers)/2 + 1` with `len(peers)/2`

```go
// Correct:
if len(matches) < n.majorityN {  // n.majorityN = len(peers)/2 + 1
nIdx := matches[n.majorityN-1]

// Mutation:
brokenMajorityN := len(n.peers) / 2  // Missing +1
if len(matches) < brokenMajorityN {
nIdx := matches[brokenMajorityN-1]
```

**Observed results:**

| run | command | result |
|---|---|---|
| fault suite | `go test -race -count=1 ./internal/raft/ -run "TestSim"` | **FAIL** — `TestSimMinorityLeaderCannotCommit` (`faults_test.go:413: minority entry committed/applied without a majority: commit=2 applied=2 base=1`) |
| linearizability suite | `go test -race -count=1 ./internal/raft/ -run "TestLinearizable"` | **PASS** — this mutation is not caught by the lin suite |
| full suite | `go test -race -count=1 ./...` | **FAIL** — `TestCommitRuleRequiresCurrentTerm`, `TestSimMinorityLeaderCannotCommit`, `TestMultiGroupReplication` (all `internal/raft`); `internal/server` fails with a **panic**: `index out of range [-1]` in `maybeAdvanceCommit`, surfacing while `TestMultiSessionConcurrentIncr` runs — for a single-node group `len(peers)/2 = 0`, so `matches[-1]` |

The same drop applied at the **construction site** instead
(`raft.go`, `New → majorityN: len(peers)/2` — which additionally
breaks elections, pre-votes, and ReadIndex) fails every fault test
above plus `TestSimPartitionLeaderFromFollower`,
`TestSimBlockLeaderFromFollower`,
`TestSimPartitionedNodeDoesNotInflateTerm`, and
`TestSimSeededChaosConverges`.

> An earlier revision of this section claimed the quorum mutation did
> **not** fail `TestSimMinorityLeaderCannotCommit`. Re-runs on
> 2026-10-02 could not reproduce that: both formulations fail the test
> deterministically, alone and inside the suite (the test's
> `faults_test.go:413` assertion has been present since Phase 15).

### Mutation 2: Removed Current-Term Check

**Location:** `internal/raft/replicate.go` — `maybeAdvanceCommit()`

**Change:** Remove `term != n.term` check that prevents committing prior-term entries

```go
// Correct:
term, err := n.rlog.Term(nIdx)
if err != nil || term != n.term {
    return nil
}

// Mutation:
_, err := n.rlog.Term(nIdx)
if err != nil {
    return nil
}
// term != n.term check removed
```

**Observed results:**

| run | command | result |
|---|---|---|
| fault suite | `go test -race -count=1 ./internal/raft/ -run "TestSim"` | **PASS** — not caught here |
| linearizability suite | `go test -race -count=1 ./internal/raft/ -run "TestLinearizable"` | **PASS** — not caught here |
| full suite | `go test -race -count=1 ./...` | **FAIL** — exactly one test: `TestCommitRuleRequiresCurrentTerm` |

> An earlier revision of this section claimed the fault and
> linearizability suites fail under this mutation. They do not: the
> only catcher is the targeted Figure-8 unit test. The **full**
> suite, not a filtered run, is the gate.

---

## Running Mutation Tests

To run mutation tests locally:

```bash
# 1. Start from a clean tree: git status
# 2. Apply ONE mutation (exact diffs above)
# 3. Run all three, recording the exact --- FAIL lines:
go test -race -count=1 ./internal/raft/ -run "TestSim"
go test -race -count=1 ./internal/raft/ -run "TestLinearizable"
go test -race -count=1 ./...
# 4. Revert: git checkout internal/raft/replicate.go
#    (and internal/raft/raft.go for the construction-site variant),
#    re-run the full suite, confirm git status is clean.
```

**Important:** Both mutations MUST cause test failures somewhere in the
full suite — if they don't, the test suite has a gap. Note that
mutation 2 is invisible to the fault and linearizability suites: a
filtered run would miss it entirely.

---

## Three-Node Demo Script

`scripts/three_node_demo.sh` demonstrates a real 3-node cluster:
1. Starts 3 server processes on separate ports
2. Writes a key
3. Kills the leader
4. Confirms key is still readable and writable

Run with: `./scripts/three_node_demo.sh`

---

## CI Requirements

All PRs must pass:
- `gofmt -l .` — no formatting issues
- `go vet ./...` — no vet issues
- `go test -race -count=1 ./...` — all tests pass with race detector
- Mutation tests (documented above) — mutations must cause failures

---

## Gateway Tests (`internal/gateway`)

The gateway package has comprehensive tests covering:

- **Tenant lifecycle**: create/get tenants
- **API key lifecycle**: create, validate, reject invalid/expired/nonexistent
- **Rate limiter**: burst allowance, token refill over time, tenant-specific quota override
- **Key namespacing**: `t:{tenantID}:{key}` encoding with fuzz test proving injective + reversible mapping
- **HTTP auth middleware**: missing/malformed/valid auth, 401 responses
- **Concurrent rate limiter access**: thread-safe token bucket
- **Random key generation**: `dkv_live_` + 256 random bits, SHA-256 hashing stability
- **HTTP auth**: 401 for missing/malformed auth, 500 for missing KV (not 401)
- **HTTP rate limit**: burst requests allowed, then 429 with `Retry-After`
- **HTTP methods**: GET/PUT/POST/DELETE on `/kv/{key}` with proper auth

The control-plane API (`control_test.go`, `accounts_sqlite_test.go`) adds: PBKDF2 against RFC 7914
vectors; signup/login/logout/session expiry; uniform login errors; per-account and per-IP brute-force
limits; password change ending other sessions; key create/rotate/revoke being reflected on the data
plane immediately; cross-tenant isolation; admin pagination and quota; CSRF/CORS; and a store suite run
against both `MemStore` and `SQLiteStore` (including backup and upgrade of an existing database).
`scripts/e2e_control.sh` (`make e2e-control`) runs the same flows over HTTP against the compose stack.

Run with: `go test -race -count=1 ./internal/gateway/...`

---

## Node Metrics Tests (`internal/nodemetrics`)

The nodemetrics package has 2 tests for the Prometheus metrics collector:

- **`TestLeaderChangesAndGauges`**: verifies leader-change counting and gauge values
- **`TestGroupsAreIndependent`**: verifies metrics are independent per Raft group

Run with: `go test -race -count=1 ./internal/nodemetrics/...`

The concurrency-cap, TLS, and auth tests described in the Gateway section
above live in `internal/gateway` and are run there.