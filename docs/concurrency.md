# Concurrency model — DistriKV

> Status: through Phase 12. Maps the spec's three concurrency levels
> (§9 CONCURRENCY MODEL) onto the actual code, and names the primitive
> that makes each level safe. Every claim names its test; see also
> guarantee A1 in docs/consistency.md.

## Level 1 — many clients, one process (goroutines)

- **gRPC handlers run concurrently.** Each RPC gets its own goroutine;
  two mutations on the same node's service arrive in arbitrary order
  relative to each other.
- **One SDK `Client` may be used from many goroutines.** `mutateMu`
  serializes mutations *and* sequence allocation — issue order equals
  sequence order, which is the ordering Phase 9's dedup rule needs.
  Reads do **not** take `mutateMu` (they are sessionless); routing
  state (`current` endpoint, connection map) is guarded by `mu`.
- **`kv.MemEngine` is safe for concurrent use**: one `sync.RWMutex`
  around every operation, values defensively copied in and out.
  Handler reads (`Get`/`Exists`, gated by the ReadIndex barrier since
  Phase 11) therefore run safely *while the apply loop writes*.

## Level 2 — the Raft total order (one goroutine owns the truth)

- **A single event-loop goroutine owns all Raft core state**: term,
  vote, log position, `commitIndex`, `lastApplied`. Nothing else
  mutates it — this is single-threaded *by construction*, not by lock.
- **All replicated state is written here, in log order.** Apply runs in
  the same loop turn as the commit advance: the engine *and* the
  session table change only inside `SM.Apply`, only from this
  goroutine. Concurrent proposals from N sessions are thereby turned
  into ONE total order — this loop is the only serializer across
  sessions, and it is why a lost update cannot hide anywhere between
  "client sent it" and "engine stored it".
- **Atomic operations linearize at their position in the committed
  log.** CAS/INCREMENT/DECREMENT are indivisible there: no interleaving
  can observe or interleave with half an operation. Business rules
  (spec §8's `DECREMENT_IF_POSITIVE`) are built by the application on
  top of these primitives — via a CAS retry loop — never inferred by
  the database.
- **Proposer waiters**: `Propose` blocks on per-index channels the loop
  releases when that entry applies; a step-down fails them en masse.
- **Linearizable reads (Phase 11)**: `ReadIndex` gates every data read
  in the same loop — fresh heartbeat acknowledgments (send-generation
  fenced), current-term no-op committed, applied ≥ read point — so a
  read never observes state below its read point, and a non-leader
  refuses instead of answering from a possibly stale replica.
- **Snapshots (Phase 12)**: `maybeSnapshot` runs at the *end* of
  `applyCommitted`, on this same goroutine — `SM.Snapshot()` therefore
  observes a quiescent state machine at an exact `lastApplied`, never a
  half-applied one, and no lock guards the capture. Restore is
  pre-loop: `raft.New` calls `Restore` before `Run`, so no event can
  race it. The cost is loop-blocking (O(state) per window) — accepted
  as the correctness-first choice; profiling (Phase 24) decides if it
  ever moves off-loop.

## Level 3 — Multi-Raft / fixed-slot sharding (Phases 17–21, planned)

Independent groups progress concurrently; a key's slot pins it to one
group, so cross-shard ordering is not a concern inside a single
operation. Not implemented yet — DistriKV is one group until Phase 17.

## Ownership table

| State | Written by | Read by | Sync primitive |
|---|---|---|---|
| Engine KV data | apply loop (via `SM.Apply`) | gRPC handlers (after the ReadIndex barrier) | `MemEngine` `sync.RWMutex` |
| Session table (Phase 9) | apply loop only | apply loop only | none needed — single goroutine, `-race` enforces |
| Snapshot capture + compaction (Phase 12) | event loop (end of apply turn) | `raft.New` at startup (pre-loop) | loop single-threading; startup before `Run` |
| Snapshot sidecar file | loop via `raftlog.SaveSnapshot` (atomic rename) | `Open`/`LoadSnapshot` | same file discipline as `hardstate` |
| Raft term/vote/commit/apply cursor | event loop only | `Status()` readers | loop single-threading; status snapshots |
| Propose waiters | loop releases them | waiting proposers | per-index channels, failed on step-down |
| SDK endpoint + conns | any caller goroutine | any caller goroutine | `Client.mu` |
| SDK mutation + sequence | any caller goroutine | — | `Client.mutateMu` |

## What proves each level (tests)

| Level | Tests |
|---|---|
| 1 — engine under concurrency | `TestMemEngineConcurrentReadWriters`, `TestMemEngineConcurrentApplyAtomicity`, `TestMemEngineConcurrentCASExactlyOneWinner` |
| 1 — handlers vs. apply loop | `TestConcurrentReadsDuringWrites` (reads never torn mid-apply) |
| 1 — one session, many goroutines | `TestConcurrentIncrThroughWire` (exactly N×M through one session) |
| 1 × 2 — many sessions, one loop | `TestMultiSessionConcurrentIncr`, `TestCASOptimisticLoopConvergence`, `TestDecrementIfPositiveExactlyOneWinner` (spec §8: exactly one winner) |
| 1 × 2 on a real cluster | `TestConcurrentSessionsThroughCluster` (3 nodes, exact count on every replica) |
| 2 — ReadIndex in the loop | `TestReadIndexContract` (returned index already applied), `TestFollowerRefusesDataReads`, `TestReadAfterFailoverSeesAcknowledgedWrite` |
| 2 — snapshots in the loop | `TestSnapshotCompactsLog` (capture between applies, boundary term served), `TestRestartFromSnapshot` (pre-loop restore + tail replay), `TestSnapshotCrashBeforeCompaction` (startup reconciliation) |

The whole suite runs under `go test -race`; any unsynchronized access
above fails it regardless of assertions.
