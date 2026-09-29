# DistriKV's Raft core

Custom implementation — no Hashicorp Raft, no etcd/raft. This document covers
what exists **now (Phase 5: leader election)** and how it is built; log
replication and the commit rule append here in Phase 6.

## Concurrency model: one event loop owns all state

Every piece of Raft state — term, vote, role, timers, vote counts — is owned
by exactly one goroutine running `Node.Run`. Clock ticks, inbound RPCs,
outbound RPC responses, and status queries are all **events on a channel**
that the loop serially processes. Consequences:

- No locks around Raft state: there is one writer, ever. Every transition is
  sequential and can be reasoned about (and tested) in isolation.
- Handlers are synchronous rendezvous: an inbound `RequestVote` posts an
  event and waits for the loop's reply, so the RPC response is ordered
  *after* whatever the loop had to persist for it.
- Effects that need I/O (outbound RPCs) run on short-lived goroutines that
  post their response back as an event. Their lifetime is bounded by the
  node's RPC context, cancelled when `Run` exits — no goroutine outlives
  its node.

## Time: tick-driven, injectable

The node never reads a clock. An external driver calls `Tick()` at a fixed
period (`StartTicker` in production at 10 ms; manually in tests), and every
timeout is counted in ticks:

| Setting | Value (production) | Meaning |
|---|---|---|
| `ElectionTicks` | 10 | base election timeout; randomized to **[10, 19] ticks = 100–190 ms** per term |
| `HeartbeatTicks` | 3 | leader heartbeat period = 30 ms |

Randomization matters: if all followers shared one deadline, a split vote
would repeat forever. A fresh random deadline each term guarantees some
candidate wins the next round.

## State

**Persistent** (raftlog hard state — `currentTerm`, `votedFor`, fsync'd via
atomic rename before any message that assumes them):

| Field | Notes |
|---|---|
| `term` | bumped on campaign and on seeing a higher term; persisted *before* the vote request / response involving it goes out |
| `votedFor` | at most one vote per term per node, durably recorded |

**Volatile** (rebuilt at start): `role` (follower/candidate/leader),
`leaderID`, election/heartbeat timers, per-term vote set, and (declared,
advanced from Phase 6) `commitIndex`, `lastApplied`.

On `New`, term and vote are recovered from the log's hard state *before* the
node may participate — a restarted node can never forget a vote it cast.

## Election rules

A follower campaigns when its randomized election timeout elapses:

1. `term++`, `votedFor = self`, **persist**, reset the timer, then solicit
   votes from every peer with `lastLogIndex/lastLogTerm` from the log.
2. Majority reached (`len(peers)/2 + 1`, counting self) → become leader,
   immediately broadcast an empty heartbeat asserting authority.

A receiver grants a vote only if **all three** hold (Raft §5.1, §5.4.1):

1. **Term** — request term ≥ our term (a higher term is adopted, the old
   vote cleared, both persisted, before we can respond).
2. **One vote per term** — `votedFor` is empty or already this candidate.
3. **Up-to-date log** — candidate's last log term > ours, or equal terms and
   at least as many entries. *This is what makes an elected leader's log
   contain everything committed.*

Granting a vote also resets our election timer (the grantee may win and
start heartbeating; avoid a spurious campaign).

Any response or request advertising a **higher term** forces an immediate
step-down: adopt term, clear vote, persist, become follower, fresh randomized
timeout. Stale responses (older term) and duplicates (already-counted voter)
are ignored — vote counts are keyed by voter ID, never incremented blindly.

## AppendEntries in Phase 5

Phase 5's leader sends **empty heartbeats only**. The receiver implements
the liveness half plus an honest consistency reply:

- term < ours → reject, *no* timer reset (a partitioned ex-leader must not
  postpone our elections);
- term > ours → adopt + persist; valid-term message → step down if needed,
  learn `leaderID`, **reset the election timer** (even on a rejected
  consistency check — the leader is alive, that is what matters);
- `prevLogIndex/prevLogTerm` checked against our log (`index 0` is the base
  case; below the compaction point fails);
- **entries in the request are refused**, not silently ignored: replication
  lands in Phase 6, and the Phase-5 leader never sends them.

## What is deliberately not here yet (Phase 6+)

Entries in heartbeats, `nextIndex/matchIndex`, majority-based commit,
`LeaderCommit` propagation, `AppendEntries` response handling on the leader,
conflict-index fast backoff, proposals from clients.

## How this phase is tested

| Test | Property |
|---|---|
| `TestSingleNodeElectsItself` | majority-of-one self-elects; no RPCs sent |
| `TestCampaignBoundsAndVoteRPCFields` | no campaign before min timeout; campaign by max; vote RPC fields correct; self-vote durable **before** RPCs leave |
| `TestMajorityGrantsElectLeader` | grants → leader + immediate heartbeat |
| `TestMinorityNeverElects` | 2/5 never wins; re-campaigns at higher term |
| `TestSplitVoteResolvesOnRetry` | zero-grant round → new term → eventual leader |
| `TestVotePersistedBeforeResponse` | on-disk vote matches the response the instant it is sent; double vote in one term refused; stale term refused |
| `TestVoteDeniedWhenCandidateLogBehind` | election restriction (§5.4.1) incl. equal-term/shorter-log cases |
| `TestAppendEntriesHeartbeatAndConsistency` | stale heartbeat rejected without timer reset; valid one adopts leader + resets timer; entries refused; prevLog checked |
| `TestStaleVoteResponseIgnored`, `TestDuplicateGrantCountsOnce`, `TestHigherTermResponseStepDownClearsVote`, `TestUnreachablePeerDoesNotBlockElection` | response-handling safety, driven synchronously (no polling) |
| `TestThreeNodeElectsLeaderKillReelect` (real gRPC) | elect → kill leader → re-elect at higher term → restart old leader → converge to one leader |
| `TestFiveNodeMinorityCannotElect` (real gRPC) | 2-of-5 minority never elects; restoring a third node enables election |

Unit tests inject time (`Tick`) and the network (scripted transport)
directly, so all timeout/ordering claims are deterministic; the two
integration tests exercise the real gRPC transport under `go test -race`.
