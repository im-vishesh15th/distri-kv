// Package raftlog implements DistriKV's persistent Raft log — the single
// durable ordered log of the system.
//
// There is deliberately NO separate WAL: this log is both the Raft consensus
// log and the write-ahead/command log for the replicated state machine. It
// provides durable append, fsync semantics, index/term addressing, CRC-framed
// records, torn-write recovery, suffix truncation (conflict resolution),
// prefix truncation (snapshot compaction), and hard-state persistence for
// currentTerm/votedFor.
//
// See docs/persistence.md for the on-disk format, durability policy, and the
// recovery decision table.
package raftlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// Entry is one Raft log entry. Payload is opaque here; from Phase 7 it holds
// an encoded state-machine command.
type Entry struct {
	Index   uint64
	Term    uint64
	Payload []byte
}

// Recovery reports what Open had to do to reach a consistent state. It is
// returned even on success so the caller can log it — recovery is never
// silent.
type Recovery struct {
	// Records is the number of valid entries loaded.
	Records int64

	// TornTail is true when an incomplete final record was found and
	// discarded.
	TornTail bool

	// TruncatedBytes is how many bytes were discarded from the tail.
	TruncatedBytes int64
}

// Log is a persistent Raft log: valid records are held in memory for reads;
// the file on disk is the durable source of truth.
//
// Concurrency: all methods are safe for concurrent use (an internal mutex).
// In the Raft core a single event-loop goroutine will own the log; the lock
// exists so recovery/replay readers cannot race appenders, not to make the
// log a shared-write structure.
//
// Payload policy: Append clones caller payloads in; Get/Replay clone them
// out. The log never aliases caller memory.
type Log struct {
	mu      sync.Mutex
	dir     string
	file    *os.File // opened O_WRONLY|O_APPEND
	entries []Entry

	// firstIndex is the log's lowest retained index. After prefix
	// truncation this is >1 and firstTerm carries the term of the last
	// discarded entry (the snapshot's lastIncludedTerm).
	//
	// Restart caveat (documented, pinned by TestTruncatePrefix): when
	// compaction leaves at least one entry, firstIndex is recovered from
	// entries[0]. A FULLY compacted log has an empty file, so firstIndex
	// reverts to 1 on reopen. Phase 12 fixes that by re-establishing
	// firstIndex/firstTerm from snapshot metadata (lastIncludedIndex/
	// lastIncludedTerm) — which only exists once snapshots exist, i.e.
	// exactly when compaction becomes possible.
	firstIndex uint64
	firstTerm  uint64

	hard   HardState
	closed bool
}

func logPath(dir string) string  { return filepath.Join(dir, "raft.log") }
func hardPath(dir string) string { return filepath.Join(dir, "hardstate") }

// Open loads (or creates) the log in dir.
//
// Recovery sequence: read file → validate every record (CRC + contiguity) →
// discard an incomplete final record if present (truncating the file to the
// last valid boundary and syncing) → hard-fail on mid-file corruption →
// load hard state (missing hard-state file means a fresh node; a damaged one
// is a hard error, never silently zeroed).
func Open(dir string) (*Log, Recovery, error) {
	var rec Recovery

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, rec, fmt.Errorf("raftlog: mkdir %s: %w", dir, err)
	}

	data, err := os.ReadFile(logPath(dir))
	if err != nil && !os.IsNotExist(err) {
		return nil, rec, fmt.Errorf("raftlog: read log: %w", err)
	}

	entries, torn, perr := parseRecords(data)
	if perr != nil {
		return nil, rec, fmt.Errorf("raftlog: %w (manual inspection required)", perr)
	}

	file, err := os.OpenFile(logPath(dir), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, rec, fmt.Errorf("raftlog: open: %w", err)
	}

	if torn >= 0 {
		if err := file.Truncate(torn); err != nil {
			file.Close()
			return nil, rec, fmt.Errorf("raftlog: truncate torn tail: %w", err)
		}
		if err := file.Sync(); err != nil {
			file.Close()
			return nil, rec, fmt.Errorf("raftlog: sync after truncation: %w", err)
		}
		rec.TornTail = true
		rec.TruncatedBytes = int64(len(data)) - torn
	}
	rec.Records = int64(len(entries))

	hard, herr := readHardState(dir)
	if herr != nil {
		file.Close()
		return nil, rec, herr
	}

	l := &Log{
		dir:     dir,
		file:    file,
		entries: entries,
		hard:    hard,
	}
	if len(entries) > 0 {
		l.firstIndex = entries[0].Index
		l.firstTerm = entries[0].Term
	} else {
		l.firstIndex = 1
	}
	return l, rec, nil
}

// FirstIndex is the lowest retained index (1 for a fresh log).
func (l *Log) FirstIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.firstIndex
}

// LastIndex is the highest index in the log (firstIndex-1 when empty).
func (l *Log) LastIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastIndexLocked()
}

// LastTerm is the term of the entry at LastIndex (firstTerm when empty).
func (l *Log) LastTerm() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastTermLocked()
}

// Count is the number of entries currently retained.
func (l *Log) Count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

func (l *Log) lastIndexLocked() uint64 {
	if len(l.entries) == 0 {
		return l.firstIndex - 1
	}
	return l.entries[len(l.entries)-1].Index
}

func (l *Log) lastTermLocked() uint64 {
	if len(l.entries) == 0 {
		return l.firstTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// Term returns the term at index.
//
// Term(0) is (0, nil): index 0 is the empty-log base case Raft uses for
// prevLogTerm of the very first entry. Get(0) remains an error — there is no
// entry 0.
func (l *Log) Term(index uint64) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index == 0 {
		return 0, nil
	}
	e, err := l.getLocked(index)
	if err != nil {
		return 0, err
	}
	return e.Term, nil
}

// Get returns a copy of the entry at index, or ErrCompacted /
// ErrUnavailable.
func (l *Log) Get(index uint64) (Entry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.getLocked(index)
}

func (l *Log) getLocked(index uint64) (Entry, error) {
	if index < l.firstIndex {
		return Entry{}, ErrCompacted
	}
	li := l.lastIndexLocked()
	if index > li {
		return Entry{}, ErrUnavailable
	}
	e := l.entries[index-l.firstIndex]
	return cloneEntry(e), nil
}

// Append durably-frames entries at the end of the log.
//
// Durability: bytes are handed to the OS via write(2) but NOT fsynced —
// process-crash safe immediately, machine-crash safe after Sync. Raft must
// call Sync before acknowledging the entries or relying on them for
// correctness (fsync-before-ack policy, docs/persistence.md).
//
// Contiguity: entries[0].Index must equal lastIndex+1 and each subsequent
// entry must be exactly +1. Violations return ErrNonContiguous without
// writing anything.
func (l *Log) Append(entries []Entry) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if len(entries) == 0 {
		return nil
	}

	expected := l.lastIndexLocked() + 1
	if entries[0].Index != expected {
		return fmt.Errorf("%w: first index %d, want %d", ErrNonContiguous, entries[0].Index, expected)
	}
	for i := 1; i < len(entries); i++ {
		if entries[i].Index != entries[i-1].Index+1 {
			return fmt.Errorf("%w: index jumps %d -> %d", ErrNonContiguous, entries[i-1].Index, entries[i].Index)
		}
	}
	for _, e := range entries {
		if entryMetaSize+len(e.Payload) > maxBodySize {
			return fmt.Errorf("raftlog: payload of entry %d exceeds %d bytes", e.Index, maxBodySize)
		}
	}

	buf := make([]byte, 0, 64*len(entries))
	for _, e := range entries {
		buf = appendRecord(buf, e)
	}
	if err := writeFull(l.file, buf); err != nil {
		return fmt.Errorf("raftlog: append: %w", err)
	}

	for _, e := range entries {
		l.entries = append(l.entries, cloneEntry(e))
	}
	return nil
}

// Sync fsyncs the log file. After Sync returns nil, an acknowledged (i.e.
// previously Synced) prefix survives process crash and machine crash.
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("raftlog: sync: %w", err)
	}
	return nil
}

// Replay calls fn for every retained entry with Index >= from, in order.
// fn receives cloned payloads; returning an error from fn aborts Replay.
//
// from must lie within [firstIndex, lastIndex+1]; below firstIndex is
// ErrCompacted, past the end is ErrUnavailable.
func (l *Log) Replay(from uint64, fn func(Entry) error) error {
	l.mu.Lock()
	last := l.lastIndexLocked()
	if from < l.firstIndex {
		l.mu.Unlock()
		return ErrCompacted
	}
	if from > last+1 {
		l.mu.Unlock()
		return ErrUnavailable
	}
	// Snapshot the matching entries under the lock so fn runs unlocked.
	n := int(last - from + 1)
	batch := make([]Entry, 0, n)
	for _, e := range l.entries[from-l.firstIndex:] {
		batch = append(batch, cloneEntry(e))
	}
	l.mu.Unlock()

	for _, e := range batch {
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}

// Close syncs and closes the log file.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	if err := l.file.Sync(); err != nil {
		l.file.Close()
		return fmt.Errorf("raftlog: close sync: %w", err)
	}
	if err := l.file.Close(); err != nil {
		return fmt.Errorf("raftlog: close: %w", err)
	}
	return nil
}

// HardState returns the persisted Raft hard state (currentTerm, votedFor).
func (l *Log) HardState() HardState {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.hard
}

// cloneEntry copies an entry, including its payload.
func cloneEntry(e Entry) Entry {
	if e.Payload != nil {
		p := make([]byte, len(e.Payload))
		copy(p, e.Payload)
		e.Payload = p
	}
	return e
}

// writeFull writes all of buf to f.
func writeFull(f *os.File, buf []byte) error {
	for len(buf) > 0 {
		n, err := f.Write(buf)
		if err != nil {
			return err
		}
		buf = buf[n:]
	}
	return nil
}

// syncDir fsyncs a directory so renames/creations inside it are durable.
// Some platforms (notably macOS) reject fsync on directory fds with EINVAL;
// under DistriKV's process-crash model directory-sync failure is acceptable
// (rename atomicity still holds), so those errors are ignored while real
// I/O errors surface.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil && !isSyncDirUnsupported(err) {
		return err
	}
	return nil
}

// isSyncDirUnsupported reports whether err is a platform limitation on
// directory fsync rather than a real failure.
func isSyncDirUnsupported(err error) bool {
	return errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP)
}

// putU32/putU64 helpers re-exported for tests and hardstate encoding.
func putU32(b []byte, v uint32) { binary.LittleEndian.PutUint32(b, v) }
func putU64(b []byte, v uint64) { binary.LittleEndian.PutUint64(b, v) }
func getU32(b []byte) uint32    { return binary.LittleEndian.Uint32(b) }
func getU64(b []byte) uint64    { return binary.LittleEndian.Uint64(b) }
