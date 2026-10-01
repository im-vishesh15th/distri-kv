# Snapshots and log compaction

A Raft log that never shrinks is a disk leak, and a restart that
replays everything forever is a availability bug. DistriKV snapshots
the **applied state machine** on a count of applied entries, atomically
replaces a single snapshot file, compacts the log prefix, and ships the
snapshot to followers whose logs that prefix no longer covers. Every
step on the write path is fsync'd — a snapshot never claims a position
the disk didn't take.

## Trigger

- `-snapshot-every=N` — applied entries per snapshot + compaction.
  Default **1024**; `0` disables compaction (the flag's own help text:
  legal for tests, "never what a server wants" — the log then grows
  without bound).
- The setting applies **per group**: the metadata group and every data
  group receive the same value and keep their own counter
  (`cmd/server/main.go`; [router.md](router.md)).
- The counter is over **applied** entries, and capture runs at the end
  of the apply batch on the event-loop goroutine — the state machine is
  quiescent (between applies), so `lastApplied` is the exact log
  position of the captured state (`maybeSnapshot`,
  `internal/raft/snapshot.go`). Capture never uses `commitIndex`;
  applied ⊆ committed is what makes it legal for restore to set
  `commitIndex = lastApplied = lastIncludedIndex`.

## What a snapshot contains

- **Everything the state machine owns.** For the KV state machine:
  the complete engine key space **plus the client-session (dedup)
  table** plus the session birth counter —
  `StateMachineSnapshot{engine_state, sessions, next_session_birth}`
  (`proto/kv.proto`). Bytes are deterministic (sessions sorted by
  `client_id`), so identical histories produce identical snapshots
  (`TestSMSnapshotRestoreRoundTrip`).
- **Metadata**: `lastIncludedIndex` (= `lastApplied`) and its term,
  persisted as the log's new boundary.
- **On disk: exactly one file**, `<data-dir>/snapshot`, beside the raft
  log and hard state (`internal/raftlog/snapshot.go`). Saves write a
  temp file and `os.Rename`; per the source comment, *exactly one
  snapshot exists at a time — `SaveSnapshot` atomically replaces it*,
  so there is no keep-N retention policy to prune: the old file is gone
  the moment the new one lands. Body cap **1 GiB** (`maxSnapshotBody`).
  A snapshot file that fails to parse is a **hard error** on Open,  never a silent fallback (`TestOpenRefusedOnCorruptSnapshot`).

## Compaction (immediately after the save)

- Prefix truncation: entries ≤ `lastIncludedIndex` are discarded;
  `FirstIndex()` becomes `lastIncludedIndex + 1`, and the log serves
  `Term(lastIncludedIndex)` from the sidecar so prevLog checks keep
  working at the boundary (`TestSnapshotCompactsLog`, which also
  verifies writes keep replicating *after* compaction).
- Invariants: the snapshot position never exceeds `lastApplied`, and
  compaction never discards a committed entry before it was captured —
  `lastApplied ≥ lastSnapIndex` always holds.
- Crash windows are reconciled at startup, each with a test:
  - snapshot saved, crash **before** compaction → `raft.New` catches
    the log up to the boundary (`TestSnapshotCrashBeforeCompaction`);
  - sidecar ahead of a stale log (install crash window) → `Reset`
    rebases position and boundary
    (`TestRestoreSnapshotReconcilesInstallCrashWindow`);
  - a snapshot/log **gap** is refused loudly at startup — `New` errors
    with `cannot restore snapshots` / `lost`
    (`TestNewRefusesBrokenSnapshotStates`).

## Restart: `snapshot_restored`

On startup (inside `raft.New`, before the event loop runs), a node
with a sidecar restores it first: `Restore(payload)` replaces the
state machine, `commitIndex = lastApplied = lastIncludedIndex` is
rebased, and log replay then covers only entries *after* the boundary.
The node emits INFO

```
snapshot_restored   last_included_index=… last_included_term=…
                    payload_bytes=… log_first_index=…
```

(defined at `internal/raft/raft.go:558`, visible at default log
verbosity — the server's slog handler is INFO unless `-debug`). A fresh
data dir has no sidecar, so it logs no marker and replays from index 1.

Measured in the recovery run ([benchmarks.md](benchmarks.md)): local
snapshot load **0.2 s** from process start.

## Follower catch-up: `snapshot_installed`

When a follower is so far behind that the leader's log no longer
covers its position — `nextIndex` below the leader's `FirstIndex()`
after compaction — the leader ships the snapshot instead of entries:

- One `InstallSnapshotRequest` (`proto/raft.proto`): term, leader id,
  `last_included_index`/`last_included_term`, and the opaque state
  machine payload. It is reached through the normal AppendEntries
  reject walk-down (including the `match == 0` case of a follower that
  was killed before ever matching).
- The receiver validates **before** touching anything: stale terms
  refused with log and `lastApplied` untouched; a redundant install
  (`≤ lastApplied`) answers success while changing nothing — even for
  a corrupt payload; a corrupt payload above the position is refused
  with the sidecar untouched; `last_included_index == 0` refused
  (`TestInstallSnapshotFollowerHandler`,
  `TestInstallSnapshotRefusedStates`).
- On acceptance: state machine replaced wholesale (restore, not
  merge), log rebased (`FirstIndex = lastIncluded + 1`, count 0),
  sidecar written, position set, leader adopted — and the node emits
  INFO

  ```
  snapshot_installed   leader=… last_included_index=… payload_bytes=…
  ```

  (`internal/raft/handlers.go:271`). Only the **receiver** logs a
  marker; the leader's snapshot lines are debug-level  (`snapshot_send_blocked`, `snapshot_rpc_failed`, `install_rejected`).
- End-to-end proofs over real gRPC:
  `TestInstallSnapshotCatchesFarBehindFollower` (follower dead, 40
  writes, leader compacts past it, restart → the node's own sidecar is
  beyond its old history, state rebuilt from the payload, further
  writes converge on all replicas) and
  `TestInstallSnapshotViaRejectWalkDown` (same via `match == 0`).

The recovery script (`scripts/bench_recovery.sh`) greps exactly these
two INFO messages — byte-exact matches of the source literals — and
**hard-fails if `snapshot_installed` was not observed**, which is how
the run in [benchmarks.md](benchmarks.md) proves a plain log catch-up
genuinely did not suffice: `snapshot_restored` at 0.2 s, then
`snapshot_installed` and rejoin at **3.1 s**, after which the restarted
node went on to win the next election.

## Honest limits

- **No chunking.** The snapshot ships as one gRPC message. The repo
  sets no message-size options, so grpc-go's default receive cap
  (4 MiB) applies, while the on-disk cap is 1 GiB — a legitimately
  large state machine can hit the transport ceiling long before the
  file cap. The 4 MiB figure is **derived from grpc-go defaults, not
  tested in this repo**: no test exercises a payload above it, so the
  failure mode for an oversized snapshot is unverified.
- **Single file, no history.** Exactly one snapshot exists at a time —
  no point-in-time restore of an older snapshot.
- **Compaction can be turned off** (`-snapshot-every=0`), with the log
  growth that implies; servers default to 1024.
- The leader has no success marker for a shipped snapshot (receiver
  only); its snapshot-related lines appear only under `-debug`.

## Tests

| test | property |
|---|---|
| `TestSnapshotCompactsLog` | prefix discarded; boundary term servable; writes keep flowing after compaction |
| `TestRestartFromSnapshot` | restart restores applied state + session table from the snapshot; tail entries still replay |
| `TestSnapshotCrashBeforeCompaction` | crash between save and truncate reconciles at startup |
| `TestNewRefusesBrokenSnapshotStates` | broken/gapped snapshot states fail `New` loudly |
| `TestInstallSnapshotFollowerHandler` | stale/redundant/corrupt/zero installs refused without mutation; valid install replaces state + rebases log |
| `TestInstallSnapshotRefusedStates` | non-restorable SM refused; no sidecar written on refusal |
| `TestRestoreSnapshotReconcilesInstallCrashWindow` | sidecar-ahead-of-log reconciled via `Reset` |
| `TestInstallSnapshotCatchesFarBehindFollower` | real-gRPC end-to-end catch-up past a compacted log |
| `TestInstallSnapshotViaRejectWalkDown` | `match == 0` walk-down ends in a whole-snapshot adoption |
| `internal/raftlog/snapshot_test.go` round-trip/corrupt/zero-index table | the sidecar file format itself |
| `TestSMSnapshotRestoreRoundTrip`, `TestMemEngineRestoreRejectsMalformed` | SM bytes deterministic; malformed payloads rejected with state untouched |

Related: [persistence.md](persistence.md) — on-disk format and fsync
policy; [client-sessions.md](client-sessions.md) — the dedup table that
rides snapshots; [benchmarks.md](benchmarks.md) — the measured recovery
run; [testing.md](testing.md) — test inventory.
