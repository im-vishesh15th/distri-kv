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

---

## Fault Injection Testing (Phase 15)

The `internal/raft/faults_test.go` implements spec §19 scenarios using the deterministic simulation network:

- `TestSimLeaderKillRestartAgrees` — leader killed and restarted, state agrees
- `TestSimPartitionLeaderFromFollower` — partition isolates leader from followers
- `TestSimBlockLeaderFromFollower` — blocks single link (no partition)
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

To verify that our tests actually catch bugs, we introduce deliberate mutations (bugs) and confirm the test suite FAILS.

### Mutation 1: Broken Quorum Math

**Location:** `internal/raft/replicate.go` — `maybeAdvanceCommit()`

**Change:** Replace correct majority calculation `len(peers)/2 + 1` with `len(peers)/2`

```go
// Correct:
if len(matches) < n.majorityN {  // n.majorityN = len(peers)/2 + 1

// Mutation:
brokenMajorityN := len(n.peers) / 2  // Missing +1
if len(matches) < brokenMajorityN {
```

**Expected:** Fault tests should FAIL (minority can commit)

**Test Command:**
```bash
# Temporarily apply mutation, then:
go test -race -count=1 ./internal/raft/ -run "TestSimMinorityLeaderCannotCommit"
# Should FAIL
```

**Result:** Confirmed FAIL (test catches minority committing)

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

**Expected:** Linearizability and fault tests should FAIL (prior-term entries can commit)

**Test Command:**
```bash
# Temporarily apply mutation, then:
go test -race -count=1 ./internal/raft/ -run "TestLinearizable|TestSim"
# Should FAIL
```

**Result:** Confirmed FAIL (tests catch prior-term commits)

---

## Running Mutation Tests

To run mutation tests locally:

```bash
# 1. Apply mutation to replicate.go (see above)
# 2. Run fault suite:
go test -race -count=1 ./internal/raft/ -run "TestSim"

# 3. Run linearizability suite:
go test -race -count=1 ./internal/raft/ -run "TestLinearizable"

# 4. Revert mutation, run full suite:
go test -race -count=1 ./...
```

**Important:** Both mutations MUST cause test failures. If they don't, the test suite has a gap.

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