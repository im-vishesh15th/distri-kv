# Consistency — DistriKV

> Status: Phase 0 — definitions first. Implementations and their proofs are
> added phase by phase; every claim below must eventually name the test that
> demonstrates it. Vague phrases like "strong consistency" are forbidden in
> this project unless immediately defined.

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
  write established. DistriKV does not serve stale reads on its primary path.
- **Session:** a client identified by `client_id` with a monotonic
  `sequence_number` per request; used for retry deduplication.

## Guarantees (to be verified by tests as phases land)

| # | Guarantee | Mechanism | Verified by | Since phase |
|---|---|---|---|---|
| W1 | An acknowledged write is durable across process crash | fsync'd persistent Raft log before ack | torn-write/restart tests | 3 (planned) |
| W2 | Writes commit only with majority agreement | Raft majority commit rule | partition fault tests | 6 (planned) |
| W3 | No acknowledged write is lost across leader change | Raft safety + persisted hard state | leader-kill tests | 5–6 (planned) |
| R1 | Reads are linearizable | ReadIndex: confirm leadership in current term → wait for `lastApplied ≥ readIndex` → serve | stale-follower & leader-change read tests | 11 (planned) |
| S1 | A retried mutation applies at most once | replicated session table `(client_id, seq) → cached response` | lost-response retry tests | 9 (planned) |
| S2 | Session/dedup state survives restart and snapshots | session table is part of state-machine snapshot | restart & snapshot tests | 9/12 (planned) |
| D1 | All replicas converge to identical state | deterministic in-order apply | state-hash comparison tests | 7 (planned) |
| P1 | Committed state survives the defined crash model | persistent log recovery (torn tail truncated, corruption loud) | byte-level truncation fuzz | 3 (planned) |

**On "exactly once":** DistriKV provides *effectively-once application of
mutating operations within a client session* — duplicates are detected and the
original response is replayed. The precise bounds (session-table growth, key
scoping, what happens if a client loses its `client_id`) are documented when
Phase 9 lands. We never write "exactly once" without those bounds.

## Behavior under quorum loss

If a majority is unavailable, the system **stops committing** and refuses to
serve unsafe reads. It does not fail over to a minority, and it does not serve
values from a possibly-stale replica. Availability is sacrificed before
consistency on the primary path.

## Explicitly out of scope (current)

Follower/stale reads as a configurable option, cross-shard transactions,
multi-region guarantees. If ever added, each becomes its own documented
guarantee with its own tests.
