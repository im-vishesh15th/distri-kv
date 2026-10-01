# Client sessions: effectively-once mutations

The unit of exactly-once is a **session**: one SDK `Client`, one stable
random `client_id`, one monotonic sequence number per group. The server
remembers the outcome of the last applied mutation per
`(client_id, sequence_number)` and replays it instead of applying
again — so a retried operation (timeout, leader failover, SDK redirect)
cannot be applied twice. This page states the mechanism precisely,
including where the guarantee stops.

## The session, from the SDK side

- **`Dial` mints the identity.** Every `pkg/client.Dial` generates a
  128-bit crypto-random `client_id` (32 hex chars) — not user-supplied,
  not exposed through the API: one `Client` value == one session.
- **Sequence numbers are per (client, group), starting at 1**, allocated
  inside `mutate` under the session lock, *before* the retry loop
  (`nextSeq` — pinned by `TestPerGroupSequencesStartAtOne`). Sharded
  clients run independent sequences per Raft group.
- **One in-flight mutation per session.** Mutations serialize on
  `mutateMu`, which covers sequence allocation and the whole attempt
  loop. Reads and status calls never take it — they carry no session
  fields at all.
- **Every attempt of one logical operation sends the same sequence
  number.** The SDK auto-retries transient failures (5 retries,
  50 ms → 800 ms backoff) and redirects to a discovered leader (up to
  3 hops) *within* a single `mutate` call. That is what makes retries
  harmless: no matter how many attempts land, the server applies the
  operation at most once.

## What the server does with a sequence

The check is **inside `kv.SM.Apply`, in log order** — deterministic and
replicated, never a pre-proposal shortcut. The gRPC layer always
proposes, stamping `client_id`/`sequence_number` onto the logged
command (`internal/kv/codec.go`), so every replica reaches the same
verdict applying the same entry. A duplicate is still committed and
applied on every replica — as a no-op that returns the cached result
(the contract comment lives in `proto/kv.proto`).

The session table (`client_id → {birth, lastSeq, result, resultErr}`)
is in-memory **replicated state**: rebuilt by log replay, captured in
state-machine snapshots (below), never persisted as a separate file.

| incoming `sequence_number` vs `lastSeq` | verdict |
|---|---|
| `== lastSeq` | **duplicate** — return the cached result/error of the original; the engine is not touched |
| `< lastSeq` | **stale** — rejected with `kv.ErrStaleSequence` (gRPC `InvalidArgument`); nothing applied |
| `> lastSeq` (gaps allowed) | applies; `lastSeq` and the outcome are cached |

- **Gaps are legal**: an attempt the client abandoned before sending
  consumed its number — `seq > last+1` still applies
  (`TestDedupRejectsSupersededSequence`).
- **CAS duplicates replay the original bit**: a cached `applied=false`
  comes back as `applied=false`, not as an error
  (`TestDedupReplaysCachedCASFailure`).
- **Cached domain errors replay too** (`TestDedupReplaysCachedError`) —
  a retry of an operation that failed with a domain error returns the
  same error without re-attempting it.
- **Scope is per (group, client_id)**: each Raft group runs its own
  `kv.SM` with its own session table.

## Surviving restarts and snapshots

The session table rides every state-machine snapshot:
`StateMachineSnapshot{engine_state, sessions, next_session_birth}`
(`proto/kv.proto`) serializes it deterministically (sessions sorted by
`client_id`), and `SM.Restore` reloads it. `TestRestartFromSnapshot`
proves it end-to-end: after restarting from a snapshot, retrying the
last applied sequence replays the original response *without*
re-applying — the applied counter stays put. Without a snapshot,
`TestSessionStateSurvivesReplay` shows log replay rebuilding the same
table. See [snapshots.md](snapshots.md) for the snapshot path itself.

## Error contract (internal → gRPC → SDK)

One mapping function per side. The server's 1:1 table is
`internal/server/errors.go`, pinned by `internal/server/errors_test.go`:

| server condition | gRPC code | SDK sees (`pkg/client`) |
|---|---|---|
| `kv.ErrKeyNotFound` | `NotFound` | `client.ErrKeyNotFound` |
| `kv.ErrNotInteger` | `FailedPrecondition` | `client.ErrNotInteger` |
| `kv.ErrOverflow` | `OutOfRange` | `client.ErrOverflow` |
| `kv.ErrStaleSequence` | `InvalidArgument` | raw gRPC status (not remapped) |
| `raft.ErrNotLeader` | `Aborted` | `client.ErrNotLeader` |
| `raft.ErrLeadershipLost`, `raft.ErrStopped` | `Unavailable` | raw status passes through |
| `context.Canceled` / `DeadlineExceeded` | untouched | untouched |
| anything else | `Internal` | raw status |

- The composite string that appears throughout
  [benchmarks.md](benchmarks.md) —
  `client: not leader and no leader could be located: raft: not leader` —
  is the SDK sentinel with the server's text wrapped inside it. The
  benchmark JSONL shows it as `Unknown(client: not leader and no leader
  could be located: raft: not)` because the SDK error is no longer a
  gRPC status by then, and `cmd/bench` falls back to
  `Unknown(<message, truncated to 60 chars>)` for non-status errors.
- **CAS failure is not an error case**: the precondition mismatch
  returns `applied=false` on a normal `OK` response (proto comment: a
  normal outcome, not an error); the SDK surfaces `(false, nil)`.

## Where the guarantee stops (honest limits)

- **65,536-session horizon.** The table is capped (`sessionCap`). When
  it is full, the arrival of a *new* `client_id` evicts the
  earliest-born session — deterministic replicated eviction by `birth`
  order, no wall-clock anywhere. Inside the horizon effectively-once
  holds; a retry from an **evicted** session re-applies
  (`TestSessionCapEvictsOldest`). Sessions otherwise never expire: no
  TTL, no lease, no heartbeat.
- **A new `Dial` is a new session.** An SDK process restart mints a
  fresh `client_id`; dedup does not span process restarts.
- **`ErrStaleSequence` is a misbehaving-client signal.** The SDK's
  monotonic counter cannot produce it; the server-side comments scope
  it to "misbehaving or long-abandoned clients" — a comment-level
  claim: no test drives it through an SDK.
- **Ambiguous outcomes are retried, not guessed.** On `Unavailable`
  the SDK performs a leader-repair lookup and retries under the *same*
  sequence (`TestClientRoutesWritesToLeader` asserts exactly one
  application across a leader kill). If retries are exhausted, the raw
  `Unavailable` status is passed through unremapped.

## Tests

| test | property |
|---|---|
| `TestPerGroupSequencesStartAtOne` | per-group counters start at 1, allocated before the retry loop |
| `TestDedupRejectsSupersededSequence` | `seq < last` rejected; gaps apply |
| `TestDedupReplaysCachedError` | duplicate returns the original domain error |
| `TestDedupReplaysCachedCASFailure` | duplicate CAS replays `applied=false` |
| `TestSessionStateSurvivesReplay` | log replay rebuilds the table |
| `TestSMSnapshotRestoreRoundTrip` | replay / stale-reject / CAS behave identically after a snapshot round trip |
| `TestRestartFromSnapshot` | sessions ride the snapshot; no double-apply after restart |
| `TestSessionCapEvictsOldest` | cap eviction deterministic; evicted session re-applies |
| `TestClientRoutesWritesToLeader` | failover retry with the same sequence ⇒ exactly one application |
| `internal/server/errors_test.go` table | the 1:1 internal → gRPC mapping |

Related: [consistency.md](consistency.md) — guarantee rows S1/S2/G4 and
the "On exactly once" discussion; [raft.md](raft.md) — log replication
that carries the session fields; [benchmarks.md](benchmarks.md) — the
failover run's 8 client errors are this contract in action: server
`Aborted` → SDK sentinel after redirects and retries were exhausted —
a visible client failure, never a silently-lost acknowledged write.
