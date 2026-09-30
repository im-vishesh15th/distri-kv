package raftlog

import "errors"

// Errors returned by the log. Callers match with errors.Is.
var (
	// ErrCompacted indicates the requested index precedes firstIndex: the
	// entry was discarded by prefix truncation (snapshot compaction).
	ErrCompacted = errors.New("raftlog: index is compacted")

	// ErrUnavailable indicates the requested index is past the end of the log.
	ErrUnavailable = errors.New("raftlog: index not in log")

	// ErrCorrupt indicates structural damage to the log file (invalid CRC or
	// non-contiguous indexes on a record that is not the final one). It is a
	// hard error: corruption is never silently ignored or auto-repaired.
	ErrCorrupt = errors.New("raftlog: corrupt record")

	// ErrNonContiguous indicates Append was called with entries that do not
	// start exactly at lastIndex+1. Raft log indexes are contiguous; a gap
	// or overlap is a caller bug, not a recoverable condition.
	ErrNonContiguous = errors.New("raftlog: append not contiguous")

	// ErrClosed indicates use of a closed Log.
	ErrClosed = errors.New("raftlog: log is closed")

	// ErrHardStateCorrupt indicates the hard-state file exists but cannot be
	// parsed. Failing loudly is mandatory: silently falling back to a zero
	// term/vote could permit a double-vote after a crash.
	ErrHardStateCorrupt = errors.New("raftlog: corrupt hard state")

	// ErrSnapshotCorrupt indicates the snapshot sidecar exists but cannot be
	// parsed. Failing loudly is mandatory: silently proceeding would make the
	// node restore a wrong (or empty) state machine while believing it
	// resumed from a snapshot — silent state divergence.
	ErrSnapshotCorrupt = errors.New("raftlog: corrupt snapshot")
)
