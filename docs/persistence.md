# Persistence — DistriKV's Persistent Raft Log

> Status: Phases 12–13 (log + hard state + snapshot sidecar + `Reset`,
> implemented and tested). This is the normative description of the
> on-disk format and recovery policy.

## 1. One log, no WAL

DistriKV has exactly **one** durable ordered log: the persistent Raft log. It
is simultaneously:

- the Raft consensus log (ordered replicated commands for consensus), and
- the write-ahead/command log for the replicated state machine (the role a
  "WAL" plays in traditional databases).

There is no `internal/wal/` and no second log to keep in sync — a deliberate
decision to eliminate a whole class of ordering/durability bugs.

## 2. Files

```
<data-dir>/raft/
├── raft.log      # framed entry records (the log)
├── hardstate     # currentTerm + votedFor (atomically replaced)
└── snapshot      # lastIncludedIndex + lastIncludedTerm + SM payload
                  # (atomically replaced; Phase 12 — exactly one exists)
```

## 3. Entry record format (little-endian)

```
0        4          8                16               24          24+N
+--------+----------+----------------+----------------+------------+
| len u32| crc32 u32| index u64      | term u64       | payload (N)
+--------+----------+----------------+----------------+------------+

len = 16 + N                    (size of index+term+payload)
crc = CRC-32/IEEE over len || index || term || payload
```

Properties:

- The `len` field itself is CRC-covered, so header damage cannot cause a
  silent mis-parse.
- `len` outside `[16, 16 MiB]` is impossible for our writer → treated as
  corruption, not as a torn tail.
- Payloads are opaque to this package; from Phase 7 they hold encoded
  state-machine commands (`kv.EncodeCommand`).

`hardstate` uses the same framing with body `term u64 | vlen u32 | votedFor`
and is rewritten atomically (temp file → fsync → rename → dir sync), so disk
always holds either the previous or the new hard state, never a mixture.

## 4. Snapshot sidecar format (Phase 12)

```
0        4          8                16               24          24+N
+--------+----------+----------------+----------------+------------+
| len u32| crc32 u32| lastIndex u64   | lastTerm u64   | payload (N)
+--------+----------+----------------+----------------+------------+

len = 16 + N                    (size of lastIncludedIndex+term+payload)
crc = CRC-32/IEEE over len || body
```

- `lastIndex`/`lastTerm` are Raft's `lastIncludedIndex`/`lastIncludedTerm`:
  the log position the payload reconstructs. Everything `≤ lastIndex` is
  discardable (prefix truncation), and `lastTerm` becomes `firstTerm` —
  the `prevLogTerm` anchor at the compaction boundary.
- The payload is opaque to this package: `kv.SM`'s serialized engine key
  space + session table (see `proto/kv.proto` `StateMachineSnapshot`).
- Same atomic-write discipline as `hardstate`, same CRC coverage of the
  `len` field. A damaged file refuses to load (`ErrSnapshotCorrupt`) — on
  **both** `Open` (which reads it to derive `firstIndex`) and `LoadSnapshot`.
  Silently proceeding would reconstruct the wrong state machine while
  believing it resumed from a snapshot.
- `lastIndex == 0` is reserved as "no snapshot" (Raft indexes start at 1)
  and is refused by the writer.
- **Ordering contract:** callers `SaveSnapshot` BEFORE `TruncatePrefix`ing
  the entries it covers — and before `Reset`ing a log that doesn't reach
  the snapshot at all (the Phase 13 install path). A crash in between
  leaves redundant-but-safe entries, or a sidecar ahead of the log's end;
  startup reconciles both (truncate the covered prefix, or discard the
  whole log and adopt the boundary). The reverse order could leave a
  compacted log with nothing to restore from.

## 5. Durability policy (fsync semantics)

| Operation | What it guarantees |
|---|---|
| `Append(entries)` | write(2) — **process-crash** safe immediately; contiguous-index validated *before* any bytes are written; not machine-crash safe |
| `Sync()` | fsync — the prefix written so far survives **process crash and machine crash** |
| `TruncateSuffix/Prefix/Reset` | full rewrite via temp+rename+fsync — discarded entries cannot reappear after a crash |
| `SetHardState` | atomic replace + fsync — safe to send the RPC that assumes the new term/vote only after it returns |
| `SaveSnapshot` | atomic replace + fsync — the snapshot is complete or absent, never torn; must precede the `TruncatePrefix` it justifies |

**The Raft rule this enables:** fsync-before-ack. Raft will call `Sync()`
before acknowledging entries to a leader, before counting its own vote, and
before serving state that depends on them. That ordering is precisely why the
"invalid final record" recovery case below is safe.

## 6. Recovery decision table

On `Open` the log is scanned sequentially; every record must pass CRC and
index-contiguity checks:

| Observation | Classification | Action |
|---|---|---|
| `< 8` trailing bytes | partial header (torn write) | truncate to last valid record, report `TornTail` |
| `len` out of `[16, 16MiB]` | impossible from our writer | **hard error `ErrCorrupt`** |
| record extends past EOF | incomplete final write | torn tail → truncate + report |
| CRC mismatch, record **last** | crash during that append | torn tail → truncate + report |
| CRC mismatch, **not** last | bit-rot / damage of an acked record | **hard error `ErrCorrupt`** |
| index discontinuity (e.g. 2 → 7) | structural damage | **hard error `ErrCorrupt`** |
| hard-state file damaged | term/vote uncertainty | **hard error `ErrHardStateCorrupt`** |
| hard-state file missing or 0 bytes | fresh node | zero `HardState` |
| snapshot file damaged (bad len/CRC) | state-machine uncertainty | **hard error `ErrSnapshotCorrupt`** (on `Open` and on `LoadSnapshot`) |
| snapshot file missing or 0 bytes | no snapshot ever taken | no snapshot (zero meta) |
| snapshot + empty log file | fully compacted | `firstIndex = lastIncludedIndex+1`, `firstTerm = lastIncludedTerm` |
| snapshot + retained entries | crash before prefix truncation | entries win for `firstIndex`; `raft.New` truncates the covered prefix (idempotent catch-up) |
| snapshot + log ending **before** the snapshot | install crash window: `SaveSnapshot` durable, `Reset` (log rebase) not yet run | `raft.New` discards all retained entries and adopts `firstIndex/firstTerm` from the sidecar |

`Recovery{Records, TornTail, TruncatedBytes}` is returned to the caller and
logged — recovery is never silent.

**Why "invalid final record" is torn, not corrupt:** a torn write is always a
*prefix* of one record (partial writes stop early; our appends are sequential).
A complete header from our writer always carries a valid `len`. So an invalid
final record is a write that never finished — and by the fsync-before-ack rule
it was never acknowledged. Every non-final record *was* acked, so damage there
stops the node.

**Honest limitation:** bit-rot that corrupts the *final* record's bytes is
indistinguishable from a crash-during-write and is discarded as a torn tail
(while being reported). The alternative — refusing to start — would make every
normal crash unrecoverable. Under DistriKV's stated crash model (process crash
+ torn machine writes, not silent bit-rot on acked data), this is the correct
trade-off.

## 7. Truncation

- **Suffix** (Raft conflict resolution): drop entries `≥ fromIndex`. Rewrite
  via temp+rename; Raft must never re-see discarded entries.
- **Prefix** (snapshot compaction, Phase 12): drop entries `≤ uptoIndex`;
  `firstIndex = uptoIndex+1`, `firstTerm = term(uptoIndex)` (= the snapshot's
  `lastIncludedTerm`, which `Term(firstIndex-1)` serves afterward).
- **Reset** (snapshot adoption, Phase 13): discard **every** entry and
  rebase `firstIndex = lastIncludedIndex+1`, `firstTerm =
  lastIncludedTerm` — the follower-side path when its tail is too short
  (or divergent) to keep. Same rewrite discipline. It only survives a
  restart with a sidecar (the caller saves one first); a bare reset on an
  empty, metadata-less file falls back to `firstIndex = 1` on reopen
  (pinned by `TestResetRebasesBoundary`).
- Rewrites are full-file: conflict truncation is rare and compaction is
  bounded by snapshot intervals, so simplicity beats segment bookkeeping. If
  profiles (Phase 24) ever disagree, segmentation is an optimization that
  doesn't change this format.

**Compaction point across restart (Phase 12):** with a snapshot, the
point survives — `Open` re-establishes `firstIndex`/`firstTerm` from
`lastIncludedIndex`/`lastIncludedTerm` (fully compacted log: from the
sidecar alone; retained entries: `firstIndex` from `entries[0]`,
`firstTerm` from a boundary-matching sidecar). Without a snapshot
(compacting before persisting — contract violation, kept working as a
fallback and pinned by `TestTruncatePrefix`), an empty file reverts
`firstIndex` to 1: nothing on disk recorded where compaction stopped.

## 8. Memory model

Valid records are loaded into an in-memory slice for O(1) `Get`/`Term`; the
file is the durable source of truth. Payloads are cloned in on `Append` and
out on `Get`/`Replay` — the log never aliases caller memory. Snapshots
(Phase 12) bound how much log exists at all; if Phase 24 profiling shows the
resident copy matters, it becomes an evidence-based change, not a guess.

## 9. Crash model covered by tests

| Test | What it proves |
|---|---|
| `TestTornTailMatrix` | for **every byte offset** of a 30-record log: complete prefix recovered, partial record discarded loudly, clean boundaries report no tear, log accepts continued appends and another restart |
| `TestMidFileCorruptionIsLoud` | flipped payload/index/term/CRC bytes, out-of-range len, and CRC-valid index discontinuity all refuse to open |
| `TestTornFirstRecord` | crash during the very first append → fresh-node semantics, not corruption |
| `TestHardStateCorruptionIsLoud` | damaged/truncated term-or-vote file refuses to open (silently zeroing could permit a double-vote) |
| `TestRaftVoteDurabilityScenario` | persist-vote → crash → restart still remembers the vote |
| `TestTruncateSuffix/Prefix` | truncations survive restart; discarded entries cannot be resurrected; with a snapshot the compaction point (`firstIndex`/`firstTerm`) survives a fully compacted reopen (the no-snapshot fallback is pinned too) |
| `TestSnapshotSaveLoadRoundTrip` | sidecar save→load round trip; newer save atomically replaces; temp file consumed |
| `TestOpenRefusedOnCorruptSnapshot`, `TestLoadSnapshotRefusedOnCorruptSnapshot` | damaged snapshot refuses to load — on `Open` and on `LoadSnapshot`, never a silent wrong-state resume |
| `TestSnapshotRejectsZeroIndex`, `TestSnapshotClosedLog` | zero `lastIndex` (the "none" marker) refused by the writer; use after `Close` is `ErrClosed` |
| `TestResetRebasesBoundary` (raftlog) | `Reset` rebases the boundary durably; survives reopen **with** a sidecar; the no-sidecar fallback (reopen at index 1) is pinned; zero index and closed log refused |
| `TestSnapshotCrashBeforeCompaction` (raft, real gRPC) | snapshot durable + compaction not yet run → startup truncates the covered prefix and resumes at the snapshot position |
| `TestRestoreSnapshotReconcilesInstallCrashWindow` (raft) | the install window: sidecar durable but the log still ends *before* it → startup discards the old log (`Reset`) and adopts the snapshot position instead of refusing to start |
