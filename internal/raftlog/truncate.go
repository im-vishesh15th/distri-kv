package raftlog

import (
	"fmt"
	"os"
)

// TruncateSuffix discards every entry with Index >= fromIndex (conflict
// resolution: a follower drops entries a new leader contradicts).
//
// fromIndex must lie in [firstIndex, lastIndex+1]: truncating compacted
// entries is ErrCompacted, truncating past the end is ErrUnavailable,
// fromIndex == lastIndex+1 is a no-op.
//
// Durability: the rewritten file is fsynced before returning, so a conflict
// resolution cannot be undone by a crash (the discarded entries must never
// reappear).
func (l *Log) TruncateSuffix(fromIndex uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}

	last := l.lastIndexLocked()
	if fromIndex > last+1 {
		return ErrUnavailable
	}
	if fromIndex < l.firstIndex {
		return ErrCompacted
	}
	if fromIndex == last+1 {
		return nil // nothing to discard
	}

	keep := l.entries[:fromIndex-l.firstIndex]
	return l.rewriteLocked(keep, l.firstIndex, l.firstTerm)
}

// TruncatePrefix discards every entry with Index <= uptoIndex (log
// compaction after a snapshot, Phase 12).
//
// The log's firstIndex becomes uptoIndex+1 and firstTerm becomes the term of
// the discarded entry — the term Raft needs for lastIncludedTerm checks.
// uptoIndex must lie in [firstIndex, lastIndex].
func (l *Log) TruncatePrefix(uptoIndex uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}

	if uptoIndex < l.firstIndex {
		return nil // already compacted past this point
	}
	last := l.lastIndexLocked()
	if uptoIndex > last {
		return ErrUnavailable
	}

	newFirst := uptoIndex + 1
	newFirstTerm := l.entries[uptoIndex-l.firstIndex].Term
	keep := l.entries[uptoIndex-l.firstIndex+1:]
	return l.rewriteLocked(keep, newFirst, newFirstTerm)
}

// rewriteLocked atomically replaces the log file with only keep[] and resets
// firstIndex/firstTerm to match.
//
// Implementation: serialize to a temp file → fsync → rename over raft.log →
// fsync directory → reopen the append handle. Rename is atomic, so a crash at
// any point leaves either the old complete file or the new one. Suffix/prefix
// truncation happens on conflict/compaction (rare, bounded by snapshot
// intervals), so full-file rewrite is the right simplicity trade-off —
// segment-based partial rewrites are optimization, not correctness.
func (l *Log) rewriteLocked(keep []Entry, newFirst, newFirstTerm uint64) error {
	tmp := l.dir + "/raft.log.tmp"

	buf := make([]byte, 0, 64*len(keep))
	for _, e := range keep {
		buf = appendRecord(buf, e)
	}

	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("raftlog: rewrite temp: %w", err)
	}
	if err := writeFull(f, buf); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("raftlog: rewrite write: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("raftlog: rewrite sync: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("raftlog: rewrite close: %w", err)
	}

	// Close the current handle before swapping the file underneath it.
	if err := l.file.Close(); err != nil {
		return fmt.Errorf("raftlog: rewrite close old handle: %w", err)
	}
	if err := os.Rename(tmp, logPath(l.dir)); err != nil {
		return fmt.Errorf("raftlog: rewrite rename: %w", err)
	}
	if err := syncDir(l.dir); err != nil {
		return fmt.Errorf("raftlog: rewrite sync dir: %w", err)
	}

	f2, err := os.OpenFile(logPath(l.dir), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("raftlog: rewrite reopen: %w", err)
	}

	l.file = f2
	// Copy keep: it may alias l.entries, which is about to be replaced.
	newEntries := make([]Entry, len(keep))
	for i, e := range keep {
		newEntries[i] = e // payloads already log-owned
	}
	l.entries = newEntries
	l.firstIndex = newFirst
	l.firstTerm = newFirstTerm
	return nil
}
