# DistriKV's Raft core

Custom implementation — no Hashicorp Raft, no etcd/raft. This document covers
what exists **now (Phases 5–13: leader election, log replication,
state-machine apply, ReadIndex reads, snapshots + compaction, and
InstallSnapshot)** and how it is built.

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
| `ElectionTicks` | 50 | base election timeout; randomized to **[50, 99] ticks = 500–990 ms** per term |
| `HeartbeatTicks` | 5 | leader heartbeat period = 50 ms |

Randomization matters: if all followers shared one deadline, a split vote
would repeat forever. A fresh random deadline each term guarantees some
candidate wins the next round.

The production values were 10/3 (100–190 ms) until the benchmark runs in
`docs/benchmarks.md` showed them to sit *inside* the single event loop's
fsync jitter: with ≥32 concurrent writers the loop can go >200 ms between
processing control events, so followers campaigned constantly (measured:
75 elections in 30 s and 25% failed ops at concurrency 128). At 500–990 ms
the same load runs with zero elections and zero errors. The tighter values
remain correct in tests, which drive `Tick()` themselves and control time.

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

Sends are triggered by proposals, by the heartbeat tick (50 ms), and by
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
heartbeat retries (~50 ms), which is also how transport recovery plays out.

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
  one); the SDK's retry reuses the same session sequence, so Phase 9's
  dedup keeps the outcome at-most-once either way;
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
service. The service's mutations go through this path; reads pass
`ReadIndex` first (below), then hit the engine.

Since Phase 9, `kv.SM` also owns the **replicated session table** (spec
§13): before touching the engine it checks the logged command's
`client_id`/`sequence_number` against the last sequence applied for that
client — a duplicate (`== last`) replays the recorded response without
re-applying, a superseded sequence (`< last`) is refused
(`ErrStaleSequence`), anything newer applies. Because the decision comes
from the entry bytes themselves, in log order, every replica makes it
identically — dedup is ordinary deterministic SM state, rebuilt by log
replay on restart and serialized into snapshots since Phase 12 (the table
rides `SM.Snapshot`, so a restored node keeps answering retries exactly
as before).

## Linearizable reads: ReadIndex (Phase 11)

"Reads go to the leader" is not a safety argument (spec §14) — a deposed
leader can still believe it leads while a newer leader commits writes it
has never seen. `Node.ReadIndex(ctx)` establishes the condition instead,
in three gates, all enforced by the event loop before it returns
(`internal/raft/readindex.go`):

1. **Fresh leadership proof.** Raft elects at most one leader per term,
   so a majority acknowledging *our* term — for `AppendEntries` requests
   sent *after* the read began — rules out any other leader existing as
   of that quorum. Freshness is enforced with a send-generation number
   (`peerProgress.gen`): each read records the generation it needs, and
   responses to pre-read requests are excluded, so a delayed message
   from before a partition cannot fake the proof.
2. **Current-term no-op committed** (Figure 8). Leader completeness
   already put every previously committed entry in this leader's log at
   election; the term's no-op committing is what pulls `commitIndex`
   over them, so the read point genuinely covers everything committed
   by earlier leaders.
3. **Applied ≥ read point**, guaranteed *before* `ReadIndex` returns —
   the state machine, not just the commit cursor, reflects the read.

The gRPC service calls this barrier before every `Get`/`Exists`; a
follower or candidate returns `ErrNotLeader` → `codes.Aborted`, and the
SDK discovers the leader and redirects (a read has no side effects, so
retrying is trivially safe). `GetStatus` deliberately skips the barrier:
it is a routing hint, not data. Reads cost one heartbeat round trip —
concurrent reads batch naturally onto the same forced send.

## Snapshots + log compaction (Phase 12)

The Raft log cannot grow forever (spec §15). Every `SnapshotEvery` applied
entries the node captures the state machine at `lastApplied` and compacts
the log prefix the capture covers (`internal/raft/snapshot.go`):

1. **Capture** — `Snapshotter.Snapshot()` on the loop goroutine, between
   apply batches, so `lastApplied` is the exact log position of the bytes
   (`kv.SM` serializes engine key space + session table + eviction counter
   into one protobuf payload).
2. **Persist** — `raftlog.SaveSnapshot({lastIncludedIndex,
   lastIncludedTerm}, payload)`: temp file → fsync → rename → fsync dir
   (the hard-state discipline), a single `snapshot` sidecar next to the
   log.
3. **Compact** — only after the snapshot is durable: `TruncatePrefix`
   discards entries `≤ lastIncludedIndex`, keeping `firstTerm` as the
   boundary's `prevLogTerm`. A crash between (2) and (3) leaves
   redundant-but-safe entries; startup reconciles by truncating them.
   The reverse order — compact before persist — could leave a log with
   nothing to restore from, which is why the order is structural, not
   advisory.

**Recovery** (`raft.New` → `restoreSnapshot`): load the sidecar, refuse
to start if the state machine cannot restore it or if the log's retained
start leaves a gap past `lastIncludedIndex` (entries lost = divergence
waiting to happen), compact any leftover covered entries, `Restore` the
payload, then adopt `commitIndex = lastApplied = lastIncludedIndex` (a
snapshot only ever comes from applied state, and applied implies
committed). Apply then resumes at the retained tail — recovery is
*snapshot + remaining log*, not full replay.

**Boundary term:** `Term(firstIndex-1)` answers from `firstTerm`
(`lastIncludedTerm`), so a follower acknowledged exactly at the
compaction point keeps receiving entries; deeper behind, replication
switches to `InstallSnapshot` (next section).

**Policy:** per-node, not consensus — each replica snapshots at its own pace;
safety invariants (never past `lastApplied`, never discard committed
entries uncaptured) hold locally. Failures to capture/persist are logged
(debug) and retried next window: snapshotting is capacity, not safety, and
never halts the node. `cmd/server` sets `-snapshot-every 1024` (0
disables).

## InstallSnapshot (Phase 13)

A follower may fall arbitrarily far behind (spec §16): the leader has
compacted past everything it would need to send. Instead of thousands of
missing entries, the leader ships the snapshot itself, and the follower
reconstructs its state machine from the payload, then continues with the
remaining log.

**Leader side** (`replicate.go`): two triggers, both inside `sendAppendTo`
and sharing the per-peer in-flight slot with normal appends:

1. `progress.match + 1 < firstIndex` (confirmed behind, `match > 0`) —
   install directly; probing down one reject per round trip would cost
   gap-many round trips. `match == 0` is a fresh term with no evidence of
   where the follower is — probe normally, so a caught-up follower after a
   leader change isn't shipped a needless full snapshot.
2. `prev + 1 < firstIndex` — the anchor crossed below the boundary during
   a reject walk-down (`prev == firstIndex-1` remains answerable from the
   boundary term; only below it do entries run out).

The payload comes from the durable sidecar (`LoadSnapshot`), so a leader
always still has what its compaction discarded; if none exists (impossible
when compaction ran — it saves first), the send is refused and logged
rather than guessed at. Success advances
`match = lastIncludedIndex` (monotonic — a redundant install must never
regress it), re-arms `next = match + 1`, counts as a ReadIndex quorum
acknowledgment (same accounting as an append success), and flows the tail
beyond the snapshot through normal appends. Refusals have no back-off to
walk: they retry on the heartbeat cadence, with pending triggers dropped
deliberately so a repeated refusal cannot spin a retry loop.

**Follower side** (`handlers.go` → `onInstallSnapshot`), in an order
chosen so every crash/retry window is safe:

1. Term gates + authority, exactly like AppendEntries: stale term ⇒
   refuse untouched (no election-timer reset); newer term adopted and
   persisted; a valid-term leader message steps us down if needed, sets
   `leaderID`, defers our election.
2. Guards before any mutation: position 0 or boundary term 0 is malformed
   ⇒ refuse; `lastIncludedIndex ≤ lastApplied` ⇒ **success without
   touching anything** (never regress applied state — this also answers
   the retry after a failed persistence attempt); a state machine that
   isn't snapshot-capable ⇒ refuse.
3. **`Restore` first** — all-or-nothing payload validation
   (`kv.SM.Restore` builds the new state before swapping). A failure
   mutates nothing durable: the follower stays behind and the leader
   retries with the same bytes (debug-level log each attempt — capacity,
   not safety).
4. Adopt `commitIndex = lastApplied = lastIncludedIndex` with the
   restore: the state machine already contains that position, so
   re-applying our old tail on top of it would double-apply.
5. `SaveSnapshot` — durable intent before the log it justifies. A
   persistence failure responds failure with the position kept in memory;
   the next attempt hits the "already installed" guard and answers
   success, so the leader's match still advances. A crash before that
   restarts the node from its old durable state (old log, old sidecar),
   which is self-consistent — the adoption was memory-only.
6. Rebase the log: a tail whose boundary entry matches the snapshot's
   term is compatible — `TruncatePrefix` keeps whatever follows
   (uncommitted entries a new leader may still want). A short or
   divergent tail means nothing held is trusted — `Reset` discards every
   entry and adopts the metadata. Structural disk failure halts, as
   anywhere else.

**Restart recovery** covers the install crash window: sidecar saved but
the log not yet rebased (the log ends *before* `lastIncludedIndex`) —
startup discards all retained entries (`Reset`) and adopts the boundary.
The Phase 12 `TruncatePrefix` catch-up handles the other window (sidecar
saved, covered entries still present); together every point between step 5
and step 6 restarts into the installed state.

`snapshot_installed` is an info-level lifecycle event (leader, position,
payload bytes, new log boundary); all refusal/persistence paths are
debug-level, since they can repeat every heartbeat.

## What is deliberately not here yet (deferred)

Conflict-term backoff hints (deferred until measurement shows catch-up
gaps actually cost — the reject walk-down terminates correctly without
them).

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
| `TestFollowerRefusesDataReads` (real gRPC) | R1's safety condition: a follower holding the value still answers `Aborted` (never a value) to raw `Get`/`Exists`; an SDK dialed at the follower reads by redirect |
| `TestReadAfterFailoverSeesAcknowledgedWrite` (real gRPC) | acknowledged write → leader killed → immediate read on the new leader sees it (no-op commit + applied-wait close the window) |
| `TestReadIndexContract` (real gRPC) | `ReadIndex` returns only with `lastApplied ≥ index` (and `commitIndex ≥ index`); a follower's call fails `ErrNotLeader` |
| `TestSnapshotCompactsLog` (real gRPC, 3-node) | every replica snapshots and truncates its prefix; `Term(firstIndex-1)` serves the boundary; writes after compaction replicate and converge |
| `TestRestartFromSnapshot` (real gRPC, standalone) | state restored from snapshot with covered entries gone; retained tail replays (`snapshot + remaining log`); session table + dedup survive the restore (retry replays, next seq applies) |
| `TestSnapshotCrashBeforeCompaction` (real gRPC) | the SaveSnapshot→TruncatePrefix crash window: startup truncates covered entries, adopts the snapshot position, keeps serving |
| `TestNewRefusesBrokenSnapshotStates` | snapshot without a restorable state machine ⇒ refuse; snapshot beyond the log's retained start (lost tail) ⇒ refuse |
| `TestInstallSnapshotFollowerHandler` | stale-term install refused untouched; fresh install restores the SM + persists the sidecar + rebases the log + adopts the position; a redundant snapshot at/below `lastApplied` answers success WITHOUT touching state (even with a payload the SM would refuse); a corrupt payload above our position is refused with sidecar/log/position unchanged; position-0 refused |
| `TestInstallSnapshotRefusedStates` | SM that cannot restore ⇒ refuse untouched, **no sidecar written** (would make the node unrestartable); SM without snapshot support ⇒ refuse untouched |
| `TestRestoreSnapshotReconcilesInstallCrashWindow` | install crash window (sidecar above the log's end): startup discards the old log via `Reset`, adopts the snapshot position, keeps serving |
| `TestInstallSnapshotCatchesFarBehindFollower` (real gRPC, 3-node) | spec §16 end-to-end: follower offline across 40 writes against a window of 2; leader compacts past its entire history; restart ⇒ the log is rebased BEYOND every index it ever held (installation, not entry catch-up — the proof), sidecar in place, state rebuilt incl. pre-outage keys, tail + further writes converge |
| `TestSnapshotSaveLoadRoundTrip`, `TestOpenRefusedOnCorruptSnapshot`, `TestLoadSnapshotRefusedOnCorruptSnapshot`, `TestSnapshotRejectsZeroIndex`, `TestSnapshotClosedLog`, `TestTruncatePrefix` (raftlog) | sidecar round trip + replace; CRC/length damage is loud on both Open and load; zero index refused; closed log refused; compaction point survives restart **with** a snapshot (no-snapshot fallback pinned) |
| `TestResetRebasesBoundary` (raftlog) | `Reset` discards every entry and rebases firstIndex/firstTerm on the metadata; the boundary answers with the metadata's term; the rebase survives reopen WITH a sidecar; the no-sidecar fallback (reopens at index 1) is pinned; zero index and closed log refused |

Unit tests inject time (`Tick`) and the network (scripted transport)
directly, so all timeout/ordering claims are deterministic; the integration
tests exercise the real gRPC transport under `go test -race`.
