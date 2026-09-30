package raftlog

// Phase 12: the snapshot sidecar — save/load round trip, replacement
// semantics, corruption refusal, and closed-log behavior.

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestSnapshotSaveLoadRoundTrip covers the happy paths: missing file is the
// zero state, save→load returns identical meta and payload, and a newer
// save REPLACES the old one (exactly one snapshot exists at a time).
func TestSnapshotSaveLoadRoundTrip(t *testing.T) {
	l, _ := mustOpen(t, t.TempDir())
	t.Cleanup(func() { _ = l.Close() })

	// No snapshot yet: zero meta, nil payload, no error.
	meta, payload, err := l.LoadSnapshot()
	if err != nil || !meta.Zero() || payload != nil {
		t.Fatalf("fresh LoadSnapshot = (%+v, %q, %v), want zero/nil/nil", meta, payload, err)
	}

	// Save → load returns exactly what went in.
	if err := l.SaveSnapshot(SnapshotMeta{LastIncludedIndex: 7, LastIncludedTerm: 3}, []byte("payload-7")); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	meta, payload, err = l.LoadSnapshot()
	if err != nil || meta.LastIncludedIndex != 7 || meta.LastIncludedTerm != 3 || !bytes.Equal(payload, []byte("payload-7")) {
		t.Fatalf("LoadSnapshot = (%+v, %q, %v), want {7 3} \"payload-7\" nil", meta, payload, err)
	}

	// A newer snapshot replaces the previous one atomically — readers see
	// either the old complete file or the new one, never a mixture.
	if err := l.SaveSnapshot(SnapshotMeta{LastIncludedIndex: 9, LastIncludedTerm: 4}, []byte("payload-9")); err != nil {
		t.Fatalf("SaveSnapshot 2: %v", err)
	}
	meta, payload, err = l.LoadSnapshot()
	if err != nil || meta.LastIncludedIndex != 9 || !bytes.Equal(payload, []byte("payload-9")) {
		t.Fatalf("LoadSnapshot after replace = (%+v, %q, %v), want {9 4} \"payload-9\" nil", meta, payload, err)
	}

	// The temp file must not linger: rename consumed it.
	if _, err := os.Stat(filepath.Join(l.dir, "snapshot.tmp")); !os.IsNotExist(err) {
		t.Fatalf("snapshot.tmp still present after save (err=%v)", err)
	}
}

// TestOpenRefusedOnCorruptSnapshot: Open reads the sidecar to derive
// firstIndex/firstTerm, so a damaged snapshot must fail Open loudly —
// silently proceeding would address the log against a base it cannot trust.
func TestOpenRefusedOnCorruptSnapshot(t *testing.T) {
	dir := t.TempDir()
	l, _ := mustOpen(t, dir)
	if err := l.SaveSnapshot(SnapshotMeta{LastIncludedIndex: 4, LastIncludedTerm: 1}, []byte("good")); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := filepath.Join(dir, "snapshot")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	data[len(data)-1] ^= 0xff // flip a payload byte: CRC must catch it
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("corrupt snapshot: %v", err)
	}

	if _, _, err := Open(dir); !errors.Is(err, ErrSnapshotCorrupt) {
		t.Fatalf("Open on corrupt snapshot = %v, want ErrSnapshotCorrupt", err)
	}
}

// TestLoadSnapshotRefusedOnCorruptSnapshot: the same refusal on the
// direct load path (file corrupted while the log is open).
func TestLoadSnapshotRefusedOnCorruptSnapshot(t *testing.T) {
	dir := t.TempDir()
	l, _ := mustOpen(t, dir)
	t.Cleanup(func() { _ = l.Close() })

	path := filepath.Join(dir, "snapshot")
	if err := os.WriteFile(path, []byte("garbage"), 0o644); err != nil {
		t.Fatalf("write garbage snapshot: %v", err)
	}
	if _, _, err := l.LoadSnapshot(); !errors.Is(err, ErrSnapshotCorrupt) {
		t.Fatalf("LoadSnapshot on garbage = %v, want ErrSnapshotCorrupt", err)
	}
}

// TestSnapshotRejectsZeroIndex: index 0 is the "no snapshot" marker, so
// persisting it would make the snapshot invisible (Zero() would drop it).
func TestSnapshotRejectsZeroIndex(t *testing.T) {
	l, _ := mustOpen(t, t.TempDir())
	t.Cleanup(func() { _ = l.Close() })
	if err := l.SaveSnapshot(SnapshotMeta{}, []byte("x")); err == nil {
		t.Fatalf("SaveSnapshot{0} = nil, want an error")
	}
	if _, _, err := l.LoadSnapshot(); err != nil {
		t.Fatalf("LoadSnapshot after rejected save = %v, want nil", err)
	}
}

// TestSnapshotClosedLog: like every other log operation, use after Close
// is ErrClosed rather than a write into a directory the process may have
// released.
func TestSnapshotClosedLog(t *testing.T) {
	l, _ := mustOpen(t, t.TempDir())
	if err := l.SaveSnapshot(SnapshotMeta{LastIncludedIndex: 1, LastIncludedTerm: 1}, []byte("x")); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := l.SaveSnapshot(SnapshotMeta{LastIncludedIndex: 2, LastIncludedTerm: 1}, []byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("SaveSnapshot on closed log = %v, want ErrClosed", err)
	}
	if _, _, err := l.LoadSnapshot(); !errors.Is(err, ErrClosed) {
		t.Fatalf("LoadSnapshot on closed log = %v, want ErrClosed", err)
	}
}
