package raft

// Snapshots and log compaction (Phase 12, spec §15).
//
// The log cannot grow forever: every SnapshotEvery applied entries the node
// captures the state machine at lastApplied, persists it through the log's
// snapshot sidecar (raftlog.SaveSnapshot: lastIncludedIndex +
// lastIncludedTerm + payload), and only THEN compacts the log prefix the
// snapshot covers. That ordering is the whole safety story:
//
//	snapshot durable  ->  compact entries
//
// a crash between the two leaves redundant-but-safe entries that
// restoreSnapshot truncates at startup; the reverse order could leave a
// compacted log with nothing to restore from.
//
// Snapshots are per-node policy, not consensus: each replica snapshots its
// own applied prefix at its own pace. The safety invariants hold locally —
// the snapshot position never exceeds lastApplied, and compaction never
// discards a committed entry that was not captured first.

import "distrikv/internal/raftlog"

// maybeSnapshot captures and compacts if the apply window since the last
// snapshot has reached SnapshotEvery. Called on the event-loop goroutine at
// the end of applyCommitted, so the state machine is quiescent (between
// applies) and lastApplied is the exact log position of the captured state.
//
// Failure policy: snapshotting is capacity, not safety — failing to capture
// only means the log keeps growing (the next window retries). Every failure
// is logged (debug level: a persistently failing capture retries every
// apply, so info would spam — -debug surfaces it), never silently dropped,
// and never halts the node: a halt would take down a healthy replica over a
// snapshot-directory write error while its log still serves commits.
func (n *Node) maybeSnapshot() {
	if n.snapEvery == 0 || n.snapshottable == nil {
		return
	}
	if n.lastApplied <= n.lastSnapIndex || n.lastApplied < n.lastSnapIndex+n.snapEvery {
		return
	}

	payload, err := n.snapshottable.Snapshot()
	if err != nil {
		n.logf("snapshot_failed", "index", n.lastApplied, "stage", "capture", "err", err.Error())
		return
	}
	// The term of the lastApplied entry is lastIncludedTerm — Raft needs
	// it for prevLogTerm matching against a compacted prefix (Phase 13
	// ships it with InstallSnapshot). lastApplied > lastSnapIndex >= the
	// previous compaction point guarantees the entry is still retained.
	term, err := n.rlog.Term(n.lastApplied)
	if err != nil {
		n.logf("snapshot_failed", "index", n.lastApplied, "stage", "term", "err", err.Error())
		return
	}
	meta := raftlog.SnapshotMeta{
		LastIncludedIndex: n.lastApplied,
		LastIncludedTerm:  term,
	}

	if err := n.rlog.SaveSnapshot(meta, payload); err != nil {
		// Not durable: do NOT advance lastSnapIndex, do NOT compact —
		// both would outrun the only record of the compaction point.
		n.logf("snapshot_failed", "index", n.lastApplied, "stage", "persist", "err", err.Error())
		return
	}
	n.lastSnapIndex = n.lastApplied

	// The snapshot is durable; compacting now is safe even if this fails
	// (the entries become reclaimable on the next window or at restart).
	if err := n.rlog.TruncatePrefix(meta.LastIncludedIndex); err != nil {
		n.logf("snapshot_failed", "index", meta.LastIncludedIndex, "stage", "compact", "err", err.Error())
		return
	}
	n.logfInfo("snapshot_created",
		"last_included_index", meta.LastIncludedIndex,
		"last_included_term", meta.LastIncludedTerm,
		"payload_bytes", len(payload),
		"log_retained", n.rlog.Count(),
	)
}
