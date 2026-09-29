package raftlog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestHardStateFreshNode(t *testing.T) {
	l, _ := mustOpen(t, t.TempDir())
	if hs := l.HardState(); hs.Term != 0 || hs.VotedFor != "" {
		t.Fatalf("fresh hard state = %+v, want zero value", hs)
	}
}

func TestHardStatePersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	l, _ := mustOpen(t, dir)
	if err := l.SetHardState(HardState{Term: 1, VotedFor: "nodeA"}); err != nil {
		t.Fatalf("SetHardState: %v", err)
	}
	// Overwrite: later term/vote wins.
	if err := l.SetHardState(HardState{Term: 4, VotedFor: "nodeB"}); err != nil {
		t.Fatalf("SetHardState 2: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	l2, _ := mustOpen(t, dir)
	defer l2.Close()
	if hs := l2.HardState(); hs.Term != 4 || hs.VotedFor != "nodeB" {
		t.Fatalf("reopened hard state = %+v, want {4 nodeB}", hs)
	}
}

// TestHardStateCorruptionIsLoud: a damaged hard-state file must refuse to
// open. Silently resetting votedFor could allow a double-vote after restart —
// exactly the kind of silent recovery the spec forbids.
func TestHardStateCorruptionIsLoud(t *testing.T) {
	src := t.TempDir()
	l, _ := mustOpen(t, src)
	if err := l.SetHardState(HardState{Term: 9, VotedFor: "nodeC"}); err != nil {
		t.Fatalf("SetHardState: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(src, "hardstate"))
	if err != nil {
		t.Fatalf("read hardstate: %v", err)
	}
	if len(raw) < 24 {
		t.Fatalf("hardstate file suspiciously small: %d bytes", len(raw))
	}

	t.Run("flipped byte in term", func(t *testing.T) {
		dir := t.TempDir()
		damaged := append([]byte(nil), raw...)
		damaged[8] ^= 0xFF // term field
		if err := os.WriteFile(filepath.Join(dir, "hardstate"), damaged, 0o644); err != nil {
			t.Fatalf("seed: %v", err)
		}
		_, _, err := Open(dir)
		if !errors.Is(err, ErrHardStateCorrupt) {
			t.Fatalf("Open = %v, want ErrHardStateCorrupt", err)
		}
	})

	t.Run("flipped byte in votedFor", func(t *testing.T) {
		dir := t.TempDir()
		damaged := append([]byte(nil), raw...)
		damaged[20] ^= 0xFF // votedFor field
		if err := os.WriteFile(filepath.Join(dir, "hardstate"), damaged, 0o644); err != nil {
			t.Fatalf("seed: %v", err)
		}
		_, _, err := Open(dir)
		if !errors.Is(err, ErrHardStateCorrupt) {
			t.Fatalf("Open = %v, want ErrHardStateCorrupt", err)
		}
	})

	t.Run("truncated file", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "hardstate"), raw[:5], 0o644); err != nil {
			t.Fatalf("seed: %v", err)
		}
		_, _, err := Open(dir)
		if !errors.Is(err, ErrHardStateCorrupt) {
			t.Fatalf("Open = %v, want ErrHardStateCorrupt", err)
		}
	})

	t.Run("zero-length file is fresh", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "hardstate"), nil, 0o644); err != nil {
			t.Fatalf("seed: %v", err)
		}
		l2, _, err := Open(dir)
		if err != nil {
			t.Fatalf("Open with empty hardstate: %v", err)
		}
		defer l2.Close()
		if hs := l2.HardState(); hs.Term != 0 || hs.VotedFor != "" {
			t.Fatalf("hard state = %+v, want zero value", hs)
		}
	})
}

func TestHardStateValidation(t *testing.T) {
	l, _ := mustOpen(t, t.TempDir())
	huge := make([]byte, maxVotedForLen+1)
	if err := l.SetHardState(HardState{Term: 1, VotedFor: string(huge)}); err == nil {
		t.Fatal("SetHardState with oversized VotedFor = nil, want error")
	}
	// State must be unchanged after the rejected write.
	if hs := l.HardState(); hs.Term != 0 || hs.VotedFor != "" {
		t.Fatalf("hard state mutated by rejected write: %+v", hs)
	}
}

// TestRaftVoteDurabilityScenario exercises the ordering the Raft core will
// rely on in Phase 5: persist term+vote BEFORE responding to RequestVote,
// then verify a crash-restart still remembers the vote (no double vote).
func TestRaftVoteDurabilityScenario(t *testing.T) {
	dir := t.TempDir()

	l, _ := mustOpen(t, dir)
	// Node receives RequestVote for term 5 from nodeX: persist first...
	if err := l.SetHardState(HardState{Term: 5, VotedFor: "nodeX"}); err != nil {
		t.Fatalf("SetHardState: %v", err)
	}
	// ...then (conceptually) send the vote response, then crash.
	if err := l.Close(); err != nil {
		t.Fatalf("Close (crash): %v", err)
	}

	// Restart: the vote must still be remembered.
	l2, _ := mustOpen(t, dir)
	defer l2.Close()
	hs := l2.HardState()
	if hs.Term != 5 || hs.VotedFor != "nodeX" {
		t.Fatalf("vote lost across crash: %+v, want {5 nodeX}", hs)
	}
}
