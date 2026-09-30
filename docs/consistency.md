# Consistency — DistriKV

> Status: through Phase 13 (InstallSnapshot + snapshot recovery).
> Implementations and their proofs are added phase by phase; every claim below
> must eventually name the test that demonstrates it. Vague phrases like
> "strong consistency" are forbidden in this project unless immediately
> defined.

## Terms

- **Committed:** a log entry stored durably on a majority of replicas of its
  Raft group. Committed entries are never lost or reordered away (Leader
  Completeness + Log Matching).
- **Applied:** a committed entry has been executed against the state machine
  (`lastApplied` advanced). Only applied state is served to clients.
- **Uncommitted:** an entry on fewer than all required replicas; may be
  truncated by a new leader. Never visible as user state.
- **Quorum:** strictly more than half the group members (e.g. 2 of 3, 3 of 5).
  Any two quorums intersect — the reason minority partitions cannot commit.
- **Linearizable read:** a read that appears to take effect atomically at some
  point between its invocation and response, and observes all writes
  acknowledged before the read began.
- **Stale read:** a read that returns a value older than what a completed
  write established. DistriKV never serves one on its primary path: data
  reads pass the ReadIndex barrier (R1) and non-leaders refuse to answer
  rather than risk it.
- **Session:** a client identified by `client_id` with a monotonic
  `sequence_number` per request; used for retry deduplication.

## Guarantees (to be verified by tests as phases land)

| # | Guarantee | Mechanism | Verified by | Since phase |
|---|---|---|---|---|
| W1 | An acknowledged write is durable across process crash | fsync'd persistent Raft log before ack | torn-tail/restart tests (log); commit-before-ack in replication tests | 3, 6 |
| W2 | Writes commit only with majority agreement | Raft majority commit rule | `TestCommitRequiresMajority`; `TestRaftOverSimulatedNetwork` (majority commits while a follower is partitioned); partition fault tests (Phase 15) | 6, 14 |
| W3 | No acknowledged write is lost across leader change | Raft safety + persisted hard state | `TestReplicationSurvivesLeaderKill` | 6 |
| R1 | Reads are linearizable | ReadIndex: fresh heartbeat quorum confirms leadership in the current term (send-generation fenced against delayed responses) → current-term no-op committed so `commitIndex` covers prior-term commits → wait for `lastApplied ≥ readIndex` → serve; non-leaders refuse (`Aborted`), SDK redirects | `TestFollowerRefusesDataReads`, `TestReadAfterFailoverSeesAcknowledgedWrite`, `TestReadIndexContract` (Raft layer: returned index already applied) | 11 |
| A1 | Atomic operations apply indivisibly: among concurrent attempts exactly one CAS wins, and INCREMENT/DECREMENT never lose an update | each command is one indivisible step in the event loop's in-order apply (linearization point = position in the committed log); the engine holds one lock per operation | `TestMemEngineConcurrentCASExactlyOneWinner`, `TestMemEngineConcurrentApplyAtomicity` (engine); `TestMultiSessionConcurrentIncr`, `TestCASOptimisticLoopConvergence`, `TestDecrementIfPositiveExactlyOneWinner`, `TestConcurrentReadsDuringWrites` (full stack, cross-session); `TestConcurrentSessionsThroughCluster` (3-node); `TestConcurrentIncrThroughWire` (one session) | 10 |
| S1 | A retried mutation applies at most once, and its original response is replayed | replicated session table in the SM: `(client_id → last seq, response)` decided from the logged command at apply time; table bounded at `sessionCap` with deterministic (birth-order) eviction | `TestDedupReplaysCachedResult`, `TestDedupRejectsSupersededSequence`, `TestDedupReplaysCachedError`, `TestDedupReplaysCachedCASFailure`; `TestDuplicateRetryAppliesOnce` (full stack); failover INCR in `TestClientRoutesWritesToLeader`; eviction horizon in `TestSessionCapEvictsOldest` | 9 |
| S2 | Session/dedup state survives replication and restart | session table is deterministic SM state: replicated via the log, rebuilt by replay (`TestSessionStateSurvivesReplay`); serialized into snapshots together with the engine key space and restored before tail-log replay (Phase 12); a far-behind follower receives the same bytes as a leader-shipped snapshot and reconstructs sessions from them (Phase 13) | `TestSessionStateSurvivesReplay`; `TestSMSnapshotRestoreRoundTrip` (session table + cached error identity across restore), `TestRestartFromSnapshot` (dedup works after snapshot restart); install restore in `TestInstallSnapshotFollowerHandler`, end-to-end in `TestInstallSnapshotCatchesFarBehindFollower` | 9/12/13 |
| D1 | All replicas converge to identical state | deterministic in-order apply | `TestStateMachinesConverge` (byte-identical state + restart replay), `TestFollowerAppliesOnlyCommitted` | 7 |
| P1 | Committed state survives the defined crash model | persistent log recovery (torn tail truncated, corruption loud) | byte-level truncation fuzz | 3 (planned) |

**On "exactly once":** DistriKV provides *at-most-once application of
mutating operations within a client session, with response replay*. The
precise contract (S1):

- **A session is one SDK `Client`**: a random `client_id` plus one
  `sequence_number` per logical operation — retries of that operation
  reuse its number, and the SDK serializes its mutations so a session's
  sequences reach the server strictly in order.
- **The replicated session table** records `client_id → (last sequence,
  response)` at apply time, from the logged command itself:
  - `sequence == last` → duplicate: the recorded response (including
    domain errors and CAS `applied=false`) is replayed; the engine is not
    touched.
  - `sequence < last` → superseded: refused with `ErrStaleSequence`,
    never applied (re-applying would rewind state written by newer
    requests).
  - `sequence > last` → applied (gaps are legal: an abandoned attempt
    consumes its sequence number).
- **Bounds:** dedup is scoped to `client_id` — a client that loses its
  identity starts a fresh session, and retries across that boundary are
  not recognized. A request the client gives up on without retrying is
  lost; dedup cannot resurrect it. The session table itself is bounded:
  at `sessionCap` (65536) live sessions, a new `client_id` evicts the
  earliest-created session (deterministic birth order — replicated state,
  so every replica evicts the same one). The at-most-once window for an
  evicted session ends at eviction: a retry whose original applied before
  eviction applies again. This never fires for a live retry in practice —
  SDK retries finish within seconds, while eviction requires 65536 other
  sessions to appear first — but it is the precise edge, stated here
  rather than hidden (`TestSessionCapEvictsOldest`).
- **Why the SDK may auto-retry `Unavailable` mutations (Phase 9):** the
  retry carries the same sequence, so a committed original and its retry
  can never both apply — but an *unretried* lost request is simply lost,
  which is why this is never called "exactly once".

## Behavior under quorum loss

If a majority is unavailable, the system **stops committing** and refuses to
serve unsafe reads. It does not fail over to a minority, and it does not serve
values from a possibly-stale replica. Availability is sacrificed before
consistency on the primary path.

## Explicitly out of scope (current)

Follower/stale reads as a configurable option, cross-shard transactions,
multi-region guarantees. If ever added, each becomes its own documented
guarantee with its own tests.
