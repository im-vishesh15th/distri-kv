# Automated fault testing

Phase 15 delivers spec §19: fault injection that never stops at "the
node crashed" — every test verifies resulting **system state** (logs,
engines, applied marks). All of it runs over the Phase 14 simulated
network; no containers anywhere.

Tests live in `internal/raft/faults_test.go` (plus the Phase 14
`TestRaftOverSimulatedNetwork`, which already proved majority commits
under a follower isolation).

## Spec §19 scenarios

### 1. Kill leader → re-elect → restart → agree

`TestSimLeaderKillRestartAgrees` — the spec's ten steps, verbatim:

1. 3-node cluster over the sim network; elect.
2. Write three keys; verify committed data on **every** replica
   (byte-identical logs + engine reads).
3. Kill the leader (`killSim`: handler detached → inbound RPCs fail like
   a refused connection; loop stops; log closes).
4. Survivors elect exactly one leader in a **newer term**.
5. More writes on the new leader.
6. Restart the old leader from the same directory — fresh engine, so
   only log replay can converge it.
7. It catches up to the leader's log.
8. All replicas agree: committed logs 1..commit byte-identical, every
   key visible everywhere.

### 2. Partition leader from one follower

Two tests, because our Raft has **no pre-vote** (the spec doesn't
require it) and the two cuts behave differently:

- `TestSimPartitionLeaderFromFollower` — `Isolate(follower)`: the cut
  node's campaign term-bumps cannot reach anyone, so the leader stays
  put (asserted for 2s), the majority keeps committing, the isolated
  node **provably never sees** the new entries (its `LastLogIndex`
  stays below the first post-isolation index), and after heal all three
  agree.
- `TestSimBlockLeaderFromFollower` — the literal one-link cut
  `Block(leader, follower)`: the cut follower's campaigns *do* reach
  the third node, so leadership churns between the two reachable nodes
  while the partition lasts (correct vanilla-Raft behavior). Availability
  is therefore asserted the way a client experiences it — writes keep
  landing within a 15s deadline through leadership changes — and heal
  must end in full agreement.

### 3. Minority cannot commit, majority continues

`TestSimMinorityLeaderCannotCommit` — the leader is partitioned into a
minority of one:

1. Its proposal appends locally (leaders run on heartbeat time, never
   self-demote) but must **not commit or apply** — asserted via
   `CommitIndex`/`LastApplied` staying at the pre-proposal mark.
2. The majority elects in a newer term and commits a write.
3. The minority proposal is still pending 300ms later — no quorum, no
   commit.
4. Heal: the stale leader steps down on the higher term; the proposal
   fails with `ErrLeadershipLost`/`ErrNotLeader` (never a lie), and the
   stray entry is gone from every log — the new leader's no-op occupies
   the same index in a newer term, so the entry can never commit
   anywhere. The minority key exists in **no** engine.

## §17 failure list, end to end

`TestSimSeededChaosConverges` (subtests per seed) — the "automated"
half: the cluster is **born under seeded delay/loss/duplication faults**
(§17's message failures exercised through elections and commits, not
just at the network layer), then a seeded random schedule runs 4 rounds
of one-fault-at-a-time actions (heal / random split / isolate / slow
node / fault flicker / crash-or-revive, max one crash at a time) while
best-effort writes race it. Invariants:

- **Nothing halts** — a live node's `runErr` must stay empty all run
  (disk isn't failing; a halt is a bug).
- **Progress** — at least one chaos write commits per seed.
- **After the final heal**: one leader, every committed write visible
  in every replica, committed logs byte-identical.

The schedule is a pure function of the seed (same seed replays the same
actions); node-side timing is outside the network's control — see
[docs/simulation.md](simulation.md) for the reproducibility boundary.

`TestSimDiskFailureHaltsNode` — §17's persistence failure, "simulated
disk failure where practical": closing a live follower's log makes every
subsequent write fail. The node must **halt with an error** (never ack
or apply anything the disk didn't take), it leaves the electorate
cleanly, the surviving majority keeps committing, and the victim's
engine provably has the pre-failure write but never the post-failure
one.

## Known limitation (documented, not hidden)

Without pre-vote, a partitioned node's term inflates with each failed
campaign. While partitioned this is harmless for `Isolate`d nodes
(their bumps reach no one) but causes the leadership churn exercised in
`TestSimBlockLeaderFromFollower`; on heal it costs one re-election round
before convergence. Pre-vote (Raft §9.6) would suppress both — it is a
liveness optimization, not a correctness requirement, and the spec
doesn't ask for it. Candidate for a later phase if production churn
measurements justify it.
