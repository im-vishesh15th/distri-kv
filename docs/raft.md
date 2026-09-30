# DistriKV's Raft core

Custom implementation — no Hashicorp Raft, no etcd/raft. This document covers
what exists **now (Phases 5–7: leader election, log replication, and
state-machine apply)** and how it is built.

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
`leaderID`, election/heartbeat timers, per-term vote set, `commitIndex`,
`lastApplied`, and the leader's per-peer replication progress (below).

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

## AppendEntries: receiving side

A follower processes every AppendEntries in this order (Raft §5.3):

1. **Term handling** — term < ours → reject untouched, *no* timer reset (a
   partitioned ex-leader must not postpone our elections); term > ours →
   adopt + persist, old vote cleared.
2. **Leadership acceptance** — any valid-term message steps us down if
   needed, teaches `leaderID`, and **resets the election timer** — even if
   the request is rejected below. The timer measures leader liveness, not
   request validity.
3. **Consistency check** on `(prevLogIndex, prevLogTerm)` — *before any
   mutation*. A rejected request never truncates our log; index 0 is the
   base case (`Term(0) = 0`), below the compaction point fails.
4. **Append** — entries must form the exact continuation of `prevLog`
   (malformed indexes are refused untouched). We skip entries we already
   hold with the same term, truncate our suffix at the first divergent
   index (logged as `log_truncated`), append the rest and **fsync before
   acknowledging** — fsync-before-ack, so an acked entry survives our crash.
5. **Commit propagation** — `commitIndex = max(commitIndex,
   min(LeaderCommit, last index we now know about))`: never backward, never
   past what our log actually holds.

## Replication: the leader's side (Phase 6)

Each leader rebuilds per-peer progress at `becomeLeader` — never carried
across terms:

| Field | Meaning |
|---|---|
| `next` | next index to send that peer; starts optimistically at `lastIndex + 1` |
| `match` | highest index **confirmed durable** on that peer; starts at 0, only acks raise it |
| `inflight` | one AppendEntries RPC outstanding to this peer (responses can't reorder against each other) |
| `pending` | a trigger (proposal/heartbeat) arrived while busy — resend as soon as the response lands |

Sends are triggered by proposals, by the heartbeat tick (30 ms), and by
responses (remainder after the batch cap, or rejection backoff). Entries are
capped at 64 per RPC; a leader farther ahead continues on the response.
Heartbeats ride the same path: empty `Entries` when caught up, with
`LeaderCommit` piggybacked — the commit watermark reaches followers within
one heartbeat period.

On a **rejection**, `next` steps back one index and retries immediately;
because it strictly decreases, the loop terminates at the true match point.
(The dissertation's conflict-term hints — jumping whole terms in one round
trip — are deliberately deferred: correctness first, and the cost of the
baseline only shows up in measurement phases.)

Outbound RPC failures never spin: the slot is cleared and the next
heartbeat retries (~30 ms), which is also how transport recovery plays out.

## Commit rule (Raft §5.4.2, Figure 8)

`commitIndex` advances to the highest index `N` where **both** hold:

1. a **majority** of the cluster has `N` durable (leader's own synced log
   counts as one replica), and
2. `term(N) == currentTerm`.

The term condition is the whole point of Figure 8: counting replicas of a
*previous* term's entry can commit an entry that a later divergent leader
would overwrite. Previous-term entries commit **indirectly** — a current-term
entry above them commits, carrying them along.

That is why `becomeLeader` appends a **no-op entry** (empty payload, current
term) before anything else: it confirms leadership with current-term content
and gives the commit rule something in the current term to wait on, so a
restarted leader's `commitIndex` never stalls waiting for a client to write.

Followers advance `commitIndex` only via `LeaderCommit`; the leader only via
the rule above. Both are reflected in `Status()`.

## Proposals (Phases 6–7)

`Node.Propose(ctx, payload)` is leader-only and blocks until the entry is
**applied** — committed on a majority under the rule above *and* executed
against the local state machine in log order — returning its log index and
the state machine's result for that entry:

- follower/candidate → `ErrNotLeader` immediately (codes.Aborted — the SDK
  redirects via GetStatus and retries, safe because nothing was appended);
- leadership lost while in flight → `ErrLeadershipLost`, *fast* — the entry's
  fate is unknowable from the old leader (it may still commit under the new
  one), which is exactly why blind client retries wait for Phase 9 dedup;
- empty payload → `ErrEmptyProposal` (empty is reserved for internal no-ops);
- cancelling `ctx` does **not** retract an appended entry;
- a state-machine domain error (e.g. `ErrNotInteger`) is the entry's
  *outcome*, not a transport failure — the entry applied, `lastApplied`
  advanced, and the error is what the proposer observes.

Waiters are registered per index in the loop and released when that entry
applies — or failed en masse on step-down. A single-node cluster commits and
applies inside `Propose` itself (majority of one), so the caller's own
read-your-writes follows from the call returning.

## Apply: committed log → state machine (Phase 7)

`lastApplied` trails `commitIndex` by exactly the entries still owed to the
state machine. `applyCommitted` runs inside the same loop turn that advances
`commitIndex` — on the leader via the commit rule, on the follower via
`LeaderCommit` — so no observer of this node can ever see a committed entry
that has not been applied here:

1. read entry `lastApplied+1` from the persistent log;
2. **empty payload → skip** (leader no-ops are raft-internal, never state
   machine input — client empty proposals were already refused);
3. otherwise call `StateMachine.Apply(payload)` synchronously on the loop
   goroutine — strict log order, exactly once per entry;
4. advance `lastApplied`, release any proposer waiting on this index with
   the result (or domain error);
5. repeat until `lastApplied == commitIndex`.

Because commit and apply are one loop turn, `Propose` returning *is* the
apply acknowledgment, and a failed `rlog.Get` of a committed entry halts the
node (applying a suffix would silently diverge from every other replica).

A `nil` `StateMachine` is legal: entries still replicate and commit, apply
becomes a no-op, and `lastApplied` still tracks `commitIndex` (the Phase 5/6
configuration). Recovery needs no special path: a restarted node starts at
`lastApplied = 0` and, once `commitIndex` advances (its own no-op, or the
leader's `LeaderCommit`), re-applies the whole recovered log onto an empty
engine — identical order, identical commands, identical state.

The seam back toward clients: `kv.EncodeCommand` serializes a `kv.Command`
into the entry payload; `kv.SM` decodes and applies it to the engine and
returns `kv.Result`, which travels back through `Propose` to the gRPC
service. The service's mutations go through this path; reads (until
Phase 11) hit the engine directly.

## What is deliberately not here yet (Phase 9+)

Conflict-term backoff hints (deferred to measurement), snapshots/
`InstallSnapshot`, dedup, ReadIndex.

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
| `TestAppendEntriesHeartbeatAndConsistency` | stale heartbeat rejected without timer reset; valid one adopts leader + resets timer; prevLog checked |
| `TestStaleVoteResponseIgnored`, `TestDuplicateGrantCountsOnce`, `TestHigherTermResponseStepDownClearsVote`, `TestUnreachablePeerDoesNotBlockElection` | response-handling safety, driven synchronously (no polling) |
| `TestLeaderReplicatesAndCommits` | proposal ships with correct prevLog anchor; one follower ack commits a 3-node majority; entry durable in the log; commit watermark rides the next heartbeat |
| `TestCommitRequiresMajority` | 5-node: one ack commits nothing (however long we wait); the second completes 3-of-5 |
| `TestCommitRuleRequiresCurrentTerm` | Figure 8 in isolation: majority on a prior-term entry does **not** commit; a current-term entry above it does |
| `TestFollowerLogReplication` | divergent suffix replaced; rejected consistency check mutates nothing; extensions fsync'd; LeaderCommit clamped; malformed requests refused untouched |
| `TestNextIndexBacksOffOnReject` | rejection walks `next` back to the follower's real log; resend anchored correctly |
| `TestProposeValidation`, `TestPendingProposalFailsOnLeadershipLoss` | not-leader/empty rejections; in-flight proposal fails fast on step-down |
| `TestThreeNodeElectsLeaderKillReelect`, `TestFiveNodeMinorityCannotElect` (real gRPC) | election under the replication/no-op traffic |
| `TestReplicationSurvivesLeaderKill` (real gRPC) | sequential proposals commit; a killed follower restarts and converges byte-for-byte; killing the leader yields a newer leader holding every committed payload |
| `TestProposeDeliversApplyResult` | Propose returns index + state-machine result only after apply; `LastApplied == CommitIndex` on return; the election no-op never reaches the state machine |
| `TestApplyErrorIsAnOutcomeNotAFailure` | a domain error is delivered to the proposer while `lastApplied` advances and the loop keeps serving |
| `TestFollowerAppliesOnlyCommitted` | uncommitted entries apply nothing; `LeaderCommit` advances apply exactly once, in order |
| `TestLastAppliedTracksCommitWithoutSM` | nil state machine: apply cursor still tracks commit |
| `TestStateMachinesConverge` (real gRPC) | D1: mixed workload (set/incr/CAS) converges byte-identically on all replicas; a crashed node with a fresh empty engine replays the recovered log back to identical state |

Unit tests inject time (`Tick`) and the network (scripted transport)
directly, so all timeout/ordering claims are deterministic; the integration
tests exercise the real gRPC transport under `go test -race`.
