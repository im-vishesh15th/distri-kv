package raftlog

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
)

// mkEntries builds contiguous entries starting at from with term baseTerm.
func mkEntries(from uint64, term uint64, n int) []Entry {
	es := make([]Entry, n)
	for i := range es {
		es[i] = Entry{
			Index:   from + uint64(i),
			Term:    term,
			Payload: []byte(fmt.Sprintf("payload-%d", from+uint64(i))),
		}
	}
	return es
}

func mustOpen(t *testing.T, dir string) (*Log, Recovery) {
	t.Helper()
	l, rec, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l, rec
}

func TestOpenFresh(t *testing.T) {
	l, rec := mustOpen(t, t.TempDir())

	if rec.Records != 0 || rec.TornTail || rec.TruncatedBytes != 0 {
		t.Fatalf("fresh recovery = %+v, want zero values", rec)
	}
	if l.FirstIndex() != 1 || l.LastIndex() != 0 || l.LastTerm() != 0 || l.Count() != 0 {
		t.Fatalf("fresh state: first=%d last=%d lastTerm=%d count=%d",
			l.FirstIndex(), l.LastIndex(), l.LastTerm(), l.Count())
	}
	if term, err := l.Term(0); err != nil || term != 0 {
		t.Fatalf("Term(0) = %d, %v; want 0, nil", term, err)
	}
	if _, err := l.Get(1); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Get(1) = %v, want ErrUnavailable", err)
	}
	if _, err := l.Get(0); !errors.Is(err, ErrCompacted) && !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Get(0) = %v, want an error", err)
	}
}

func TestAppendGetTerm(t *testing.T) {
	l, _ := mustOpen(t, t.TempDir())

	if err := l.Append(mkEntries(1, 3, 5)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if l.LastIndex() != 5 || l.LastTerm() != 3 || l.FirstIndex() != 1 || l.Count() != 5 {
		t.Fatalf("state: first=%d last=%d lastTerm=%d count=%d",
			l.FirstIndex(), l.LastIndex(), l.LastTerm(), l.Count())
	}

	e, err := l.Get(4)
	if err != nil {
		t.Fatalf("Get(4): %v", err)
	}
	if e.Index != 4 || e.Term != 3 || string(e.Payload) != "payload-4" {
		t.Fatalf("Get(4) = %+v", e)
	}
	if term, err := l.Term(4); err != nil || term != 3 {
		t.Fatalf("Term(4) = %d, %v", term, err)
	}
	if _, err := l.Get(6); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Get(6) = %v, want ErrUnavailable", err)
	}

	// Empty append is a no-op.
	if err := l.Append(nil); err != nil {
		t.Fatalf("Append(nil): %v", err)
	}
	if l.LastIndex() != 5 {
		t.Fatalf("LastIndex after empty append = %d", l.LastIndex())
	}
}

func TestAppendRejectsNonContiguous(t *testing.T) {
	l, _ := mustOpen(t, t.TempDir())

	if err := l.Append(mkEntries(1, 1, 3)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	tests := []struct {
		name string
		give []Entry
	}{
		{"gap", []Entry{{Index: 5, Term: 1}}},
		{"overlap", []Entry{{Index: 3, Term: 1}}},
		{"internal gap", []Entry{{Index: 4, Term: 1}, {Index: 6, Term: 1}}},
		{"zero index", []Entry{{Index: 0, Term: 1}}},
	}
	for _, tt := range tests {
		if err := l.Append(tt.give); !errors.Is(err, ErrNonContiguous) {
			t.Errorf("%s: Append = %v, want ErrNonContiguous", tt.name, err)
		}
	}
	// Nothing was written.
	if l.LastIndex() != 3 || l.Count() != 3 {
		t.Fatalf("log mutated by rejected append: last=%d count=%d", l.LastIndex(), l.Count())
	}
}

func TestReopenPreservesState(t *testing.T) {
	dir := t.TempDir()

	l, _ := mustOpen(t, dir)
	if err := l.Append(mkEntries(1, 7, 4)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.SetHardState(HardState{Term: 7, VotedFor: "nodeB"}); err != nil {
		t.Fatalf("SetHardState: %v", err)
	}
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	l2, rec := mustOpen(t, dir)
	if rec.TornTail || rec.Records != 4 {
		t.Fatalf("recovery = %+v, want 4 records, no tear", rec)
	}
	if l2.LastIndex() != 4 || l2.LastTerm() != 7 || l2.Count() != 4 {
		t.Fatalf("reopened state: last=%d lastTerm=%d count=%d", l2.LastIndex(), l2.LastTerm(), l2.Count())
	}
	if hs := l2.HardState(); hs.Term != 7 || hs.VotedFor != "nodeB" {
		t.Fatalf("reopened hard state = %+v", hs)
	}
	e, err := l2.Get(2)
	if err != nil || string(e.Payload) != "payload-2" {
		t.Fatalf("Get(2) after reopen = %q, %v", e.Payload, err)
	}
}

func TestPayloadIsolation(t *testing.T) {
	l, _ := mustOpen(t, t.TempDir())

	in := []byte("original")
	if err := l.Append([]Entry{{Index: 1, Term: 1, Payload: in}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	copy(in, "MUTATED!") // caller reuses its buffer

	e, err := l.Get(1)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(e.Payload, []byte("original")) {
		t.Fatalf("log aliased caller memory: %q", e.Payload)
	}

	copy(e.Payload, "MUTATED!") // caller mutates what it got
	e2, _ := l.Get(1)
	if !bytes.Equal(e2.Payload, []byte("original")) {
		t.Fatalf("caller aliased log memory: %q", e2.Payload)
	}
}

func TestReplay(t *testing.T) {
	l, _ := mustOpen(t, t.TempDir())
	if err := l.Append(mkEntries(1, 1, 5)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	collect := func(from uint64) ([]uint64, error) {
		var idxs []uint64
		err := l.Replay(from, func(e Entry) error {
			idxs = append(idxs, e.Index)
			return nil
		})
		return idxs, err
	}

	idxs, err := collect(1)
	if err != nil || len(idxs) != 5 {
		t.Fatalf("Replay(1) = %v, %v", idxs, err)
	}
	idxs, err = collect(3)
	if err != nil || len(idxs) != 3 || idxs[0] != 3 {
		t.Fatalf("Replay(3) = %v, %v", idxs, err)
	}
	idxs, err = collect(6) // last+1: empty but valid
	if err != nil || len(idxs) != 0 {
		t.Fatalf("Replay(6) = %v, %v; want empty, nil", idxs, err)
	}
	if _, err := collect(7); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Replay(7) = %v, want ErrUnavailable", err)
	}
	if _, err := collect(0); !errors.Is(err, ErrCompacted) {
		t.Fatalf("Replay(0) = %v, want ErrCompacted", err)
	}

	// An error from fn aborts Replay and propagates.
	sentinel := errors.New("stop")
	err = l.Replay(1, func(Entry) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("Replay fn error = %v, want propagated sentinel", err)
	}
}

// TestTruncateSuffix covers conflict-resolution truncation and its durability.
func TestTruncateSuffix(t *testing.T) {
	dir := t.TempDir()
	l, _ := mustOpen(t, dir)
	if err := l.Append(mkEntries(1, 1, 10)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// No-op at the end.
	if err := l.TruncateSuffix(11); err != nil {
		t.Fatalf("TruncateSuffix(11): %v", err)
	}
	// Beyond the end is invalid.
	if err := l.TruncateSuffix(12); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("TruncateSuffix(12) = %v, want ErrUnavailable", err)
	}
	// Real truncation: drop entries 5..10.
	if err := l.TruncateSuffix(5); err != nil {
		t.Fatalf("TruncateSuffix(5): %v", err)
	}
	if l.LastIndex() != 4 || l.Count() != 4 {
		t.Fatalf("after suffix truncate: last=%d count=%d, want 4/4", l.LastIndex(), l.Count())
	}
	if _, err := l.Get(5); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Get(5) after truncate = %v, want ErrUnavailable", err)
	}

	// The truncation must survive restart.
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	l2, _ := mustOpen(t, dir)
	if l2.LastIndex() != 4 || l2.Count() != 4 {
		t.Fatalf("after reopen: last=%d count=%d, want 4/4", l2.LastIndex(), l2.Count())
	}

	// Appending must continue from the new end (as if 5..10 never existed).
	if err := l2.Append(mkEntries(5, 2, 2)); err != nil {
		t.Fatalf("Append after truncate: %v", err)
	}
	if l2.LastIndex() != 6 || l2.LastTerm() != 2 {
		t.Fatalf("state after re-append: last=%d lastTerm=%d", l2.LastIndex(), l2.LastTerm())
	}
}

// TestTruncatePrefix covers snapshot compaction.
func TestTruncatePrefix(t *testing.T) {
	dir := t.TempDir()
	l, _ := mustOpen(t, dir)
	if err := l.Append(mkEntries(1, 1, 5)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Append(mkEntries(6, 2, 5)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Compact away entries 1..4.
	if err := l.TruncatePrefix(4); err != nil {
		t.Fatalf("TruncatePrefix(4): %v", err)
	}
	if l.FirstIndex() != 5 || l.LastIndex() != 10 || l.Count() != 6 {
		t.Fatalf("after prefix truncate: first=%d last=%d count=%d", l.FirstIndex(), l.LastIndex(), l.Count())
	}
	if _, err := l.Get(4); !errors.Is(err, ErrCompacted) {
		t.Fatalf("Get(4) = %v, want ErrCompacted", err)
	}
	if e, err := l.Get(5); err != nil || e.Term != 1 {
		t.Fatalf("Get(5) = %+v, %v", e, err)
	}
	// Compacting already-compacted region is a no-op; past the end is invalid.
	if err := l.TruncatePrefix(2); err != nil {
		t.Fatalf("TruncatePrefix(2) = %v, want nil (no-op)", err)
	}
	if err := l.TruncatePrefix(11); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("TruncatePrefix(11) = %v, want ErrUnavailable", err)
	}

	// Full compaction: everything discarded; firstIndex moves past the end.
	if err := l.TruncatePrefix(10); err != nil {
		t.Fatalf("TruncatePrefix(10): %v", err)
	}
	if l.FirstIndex() != 11 || l.LastIndex() != 10 || l.Count() != 0 || l.LastTerm() != 2 {
		t.Fatalf("after full compaction: first=%d last=%d count=%d lastTerm=%d",
			l.FirstIndex(), l.LastIndex(), l.Count(), l.LastTerm())
	}
	// Suffix truncation may not reach into the compacted region.
	if err := l.TruncateSuffix(10); !errors.Is(err, ErrCompacted) {
		t.Fatalf("TruncateSuffix(10) on compacted log = %v, want ErrCompacted", err)
	}

	// Restart with a FULLY compacted (zero-entry) log: the file is empty, so
	// firstIndex reverts to 1 — the compaction point exists only in memory
	// today. Phase 12 fixes this by re-establishing firstIndex/firstTerm from
	// snapshot metadata (lastIncludedIndex/lastIncludedTerm), which is where
	// the truth will live. This test pins that documented behavior.
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	l2, _ := mustOpen(t, dir)
	if l2.FirstIndex() != 1 || l2.LastIndex() != 0 || l2.Count() != 0 {
		t.Fatalf("reopened fully-compacted log: first=%d last=%d count=%d, want 1/0/0 (Phase-12 caveat)",
			l2.FirstIndex(), l2.LastIndex(), l2.Count())
	}

	// By contrast, PARTIAL compaction (entries remain) survives restart:
	// firstIndex is derived from the first retained record.
	dir3 := t.TempDir()
	l3, _ := mustOpen(t, dir3)
	if err := l3.Append(mkEntries(1, 1, 5)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l3.TruncatePrefix(4); err != nil {
		t.Fatalf("TruncatePrefix(4): %v", err)
	}
	if err := l3.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	l4, _ := mustOpen(t, dir3)
	if l4.FirstIndex() != 5 || l4.LastIndex() != 5 || l4.LastTerm() != 1 {
		t.Fatalf("reopened partially-compacted log: first=%d last=%d lastTerm=%d, want 5/5/1",
			l4.FirstIndex(), l4.LastIndex(), l4.LastTerm())
	}
}

// TestConcurrentAccess stresses the internal mutex: concurrent readers,
// appender, and status calls under -race.
func TestConcurrentAccess(t *testing.T) {
	l, _ := mustOpen(t, t.TempDir())
	if err := l.Append(mkEntries(1, 1, 100)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_, _ = l.Get(uint64(i%100 + 1))
				_, _ = l.Term(uint64(i%100 + 1))
				_ = l.LastIndex()
				_ = l.FirstIndex()
				_ = l.Count()
			}
		}(g)
	}
	// A second appender: appends after the seeded range would conflict, so
	// it only appends when contiguous — serialize via observed LastIndex.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			next := l.LastIndex() + 1
			_ = l.Append(mkEntries(next, 2, 1))
		}
	}()
	wg.Wait()

	if l.LastIndex() < 100 {
		t.Fatalf("last index = %d, want >= 100", l.LastIndex())
	}
}

func TestClosedLogRejectsOperations(t *testing.T) {
	l, _ := mustOpen(t, t.TempDir())
	if err := l.Append(mkEntries(1, 1, 1)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := l.Close(); err != nil { // idempotent
		t.Fatalf("second Close: %v", err)
	}
	if err := l.Append(mkEntries(2, 1, 1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Append after close = %v, want ErrClosed", err)
	}
	if err := l.Sync(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Sync after close = %v, want ErrClosed", err)
	}
	if err := l.SetHardState(HardState{Term: 1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("SetHardState after close = %v, want ErrClosed", err)
	}
}
