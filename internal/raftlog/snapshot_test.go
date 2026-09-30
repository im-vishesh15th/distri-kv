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
	if err := l.Reset(SnapshotMeta{LastIncludedIndex: 3, LastIncludedTerm: 1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Reset on closed log = %v, want ErrClosed", err)
	}
}

// TestResetRebasesBoundary (Phase 13): Reset discards every entry and
// rebases the boundary on the metadata — the snapshot-adoption path for a
// follower whose tail is too short (or divergent) to keep. The rebase
// survives a restart ONLY with a sidecar; the pinned fallback for a bare
// Reset is that an empty, metadata-less file opens as a fresh log at
// index 1.
func TestResetRebasesBoundary(t *testing.T) {
	t.Run("rebase with sidecar survives reopen", func(t *testing.T) {
		dir := t.TempDir()
		l, _ := mustOpen(t, dir)
		for i := uint64(1); i <= 5; i++ {
			if err := l.Append([]Entry{{Index: i, Term: 2, Payload: []byte("x")}}); err != nil {
				t.Fatalf("append %d: %v", i, err)
			}
		}
		// Caller contract: the sidecar is durable BEFORE the reset.
		meta := SnapshotMeta{LastIncludedIndex: 3, LastIncludedTerm: 7}
		if err := l.SaveSnapshot(meta, []byte("state-3")); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}
		if err := l.Reset(meta); err != nil {
			t.Fatalf("Reset: %v", err)
		}

		if l.Count() != 0 || l.FirstIndex() != 4 || l.LastIndex() != 3 {
			t.Fatalf("after Reset: count=%d first=%d last=%d, want 0/4/3", l.Count(), l.FirstIndex(), l.LastIndex())
		}
		// The boundary answers with the METADATA's term (the leader's
		// lastIncludedTerm), not anything our discarded entries held.
		if got, err := l.Term(3); err != nil || got != 7 {
			t.Fatalf("Term(3) = (%d, %v), want (7, nil)", got, err)
		}
		if _, err := l.Term(2); !errors.Is(err, ErrCompacted) {
			t.Fatalf("Term(2) = %v, want ErrCompacted (below the new boundary)", err)
		}
		if err := l.Append([]Entry{{Index: 4, Term: 7, Payload: []byte("y")}}); err != nil {
			t.Fatalf("append after reset: %v", err)
		}

		if err := l.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		l2, _ := mustOpen(t, dir)
		t.Cleanup(func() { _ = l2.Close() })
		if l2.FirstIndex() != 4 || l2.LastIndex() != 4 || l2.Count() != 1 {
			t.Fatalf("reopened: first=%d last=%d count=%d, want 4/4/1", l2.FirstIndex(), l2.LastIndex(), l2.Count())
		}
		if got, err := l2.Term(3); err != nil || got != 7 {
			t.Fatalf("reopened Term(3) = (%d, %v), want (7, nil) (from sidecar)", got, err)
		}
	})

	t.Run("bare reset does not survive reopen (pinned fallback)", func(t *testing.T) {
		dir := t.TempDir()
		l, _ := mustOpen(t, dir)
		for i := uint64(1); i <= 3; i++ {
			if err := l.Append([]Entry{{Index: i, Term: 1, Payload: []byte("x")}}); err != nil {
				t.Fatalf("append %d: %v", i, err)
			}
		}
		if err := l.Reset(SnapshotMeta{LastIncludedIndex: 6, LastIncludedTerm: 2}); err != nil {
			t.Fatalf("Reset: %v", err)
		}
		if l.FirstIndex() != 7 {
			t.Fatalf("FirstIndex after bare Reset = %d, want 7", l.FirstIndex())
		}
		if err := l.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		l2, _ := mustOpen(t, dir)
		t.Cleanup(func() { _ = l2.Close() })
		if l2.FirstIndex() != 1 {
			t.Fatalf("reopened FirstIndex = %d, want 1 (no sidecar ⇒ fresh fallback)", l2.FirstIndex())
		}
	})

	t.Run("zero index refused", func(t *testing.T) {
		l, _ := mustOpen(t, t.TempDir())
		t.Cleanup(func() { _ = l.Close() })
		if err := l.Reset(SnapshotMeta{}); err == nil {
			t.Fatalf("Reset{0} = nil, want an error")
		}
		if l.FirstIndex() != 1 {
			t.Fatalf("FirstIndex after refused reset = %d, want 1", l.FirstIndex())
		}
	})
}
