package raftlog

import (
	"fmt"
	"os"
	"path/filepath"
)

// SnapshotMeta is the Raft-facing metadata of a persisted snapshot
// (spec §15): the log position the captured state represents.
//
// lastIndex/lastTerm are named lastIncludedIndex/lastIncludedTerm in the
// Raft paper. Everything at or before LastIncludedIndex is reconstructable
// from the snapshot payload, which is why TruncatePrefix may discard it.
type SnapshotMeta struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
}

// Zero reports whether no snapshot has been taken (the fresh-node state).
func (m SnapshotMeta) Zero() bool { return m.LastIncludedIndex == 0 }

// snapshotPath is the sidecar file next to raft.log/hardstate. Exactly one
// snapshot exists at a time: SaveSnapshot atomically replaces it, so the
// on-disk snapshot is always the newest complete one.
func snapshotPath(dir string) string { return filepath.Join(dir, "snapshot") }

// maxSnapshotBody bounds the len field of the snapshot frame. A snapshot is
// the serialized KV state machine; anything past 1 GiB cannot be something
// this system wrote and marks the file as garbage rather than inviting a
// giant allocation on a corrupt length.
const maxSnapshotBody = 1 << 30

// SaveSnapshot durably persists meta+payload as this log's snapshot,
// replacing any previous one.
//
// On-disk frame (little-endian), mirroring the hard-state sidecar:
//
//	0        4          8               16               24
//	+--------+----------+---------------+----------------+-----------
//	| len u32| crc32 u32| lastIndex u64 | lastTerm u64   | payload
//	+--------+----------+---------------+----------------+-----------
//
//	len = 16 + len(payload); crc covers len bytes || body bytes (so a
//	corrupted length is detected, not mis-parsed).
//
// Durability policy: temp file → fsync → atomic rename → fsync directory —
// the same ordering as SetHardState, so a crash leaves either the previous
// complete snapshot or the new one, never a torn mixture. Callers persist
// the snapshot BEFORE compacting the log entries it covers: a crash in
// between leaves an untruncated log (redundant but safe), while the reverse
// order could leave a compacted log with no snapshot to restore from.
func (l *Log) SaveSnapshot(meta SnapshotMeta, payload []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}

	body := 16 + len(payload)
	if meta.LastIncludedIndex == 0 {
		// Index 0 is the "no snapshot" marker (index numbering starts at 1);
		// a snapshot of the empty state at the log start needs index >= 1.
		return fmt.Errorf("raftlog: snapshot LastIncludedIndex must be >= 1")
	}
	if body > maxSnapshotBody {
		return fmt.Errorf("raftlog: snapshot body %d exceeds %d bytes", body, maxSnapshotBody)
	}

	buf := make([]byte, headerSize+body)
	putU32(buf[0:4], uint32(body))
	putU64(buf[8:16], meta.LastIncludedIndex)
	putU64(buf[16:24], meta.LastIncludedTerm)
	copy(buf[24:], payload)
	// CRC covers the len field + everything after the crc field.
	putU32(buf[4:8], crcOver(buf[0:4], buf[8:]))

	tmp := filepath.Join(l.dir, "snapshot.tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("raftlog: snapshot temp: %w", err)
	}
	if err := writeFull(f, buf); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("raftlog: snapshot write: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("raftlog: snapshot sync: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("raftlog: snapshot close: %w", err)
	}
	if err := os.Rename(tmp, snapshotPath(l.dir)); err != nil {
		return fmt.Errorf("raftlog: snapshot rename: %w", err)
	}
	if err := syncDir(l.dir); err != nil {
		return fmt.Errorf("raftlog: snapshot sync dir: %w", err)
	}
	return nil
}

// LoadSnapshot returns the persisted snapshot (any goroutine). A missing
// file means no snapshot has ever been taken: (zero meta, nil payload).
// An existing-but-unparseable file is a hard error — restoring from a
// damaged snapshot would silently reconstruct the wrong state.
func (l *Log) LoadSnapshot() (SnapshotMeta, []byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return SnapshotMeta{}, nil, ErrClosed
	}
	return readSnapshot(l.dir)
}

// readSnapshot loads the snapshot sidecar from dir (see LoadSnapshot).
func readSnapshot(dir string) (SnapshotMeta, []byte, error) {
	data, err := os.ReadFile(snapshotPath(dir))
	if err != nil {
		if os.IsNotExist(err) {
			return SnapshotMeta{}, nil, nil
		}
		return SnapshotMeta{}, nil, fmt.Errorf("raftlog: read snapshot: %w", err)
	}
	if len(data) == 0 {
		// Rename atomicity makes an empty file a "never written" state.
		return SnapshotMeta{}, nil, nil
	}
	if len(data) < headerSize {
		return SnapshotMeta{}, nil, fmt.Errorf("%w: snapshot truncated header (%d bytes)", ErrSnapshotCorrupt, len(data))
	}
	body := int(getU32(data[0:4]))
	end := headerSize + body
	if body < 16 || end != len(data) {
		return SnapshotMeta{}, nil, fmt.Errorf("%w: snapshot bad length %d (file %d bytes)", ErrSnapshotCorrupt, body, len(data))
	}
	if getU32(data[4:8]) != crcOver(data[0:4], data[8:]) {
		return SnapshotMeta{}, nil, fmt.Errorf("%w: snapshot CRC mismatch", ErrSnapshotCorrupt)
	}
	return SnapshotMeta{
		LastIncludedIndex: getU64(data[8:16]),
		LastIncludedTerm:  getU64(data[16:24]),
	}, data[24:], nil
}
