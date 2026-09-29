package raftlog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// buildCompleteLog writes n entries to dir and returns the raw file bytes
// plus the record end offsets (boundaries), for truncation experiments.
func buildCompleteLog(t *testing.T, dir string, n int) ([]byte, []int) {
	t.Helper()
	l, _, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	entries := mkEntries(1, 1, n)
	if err := l.Append(entries); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, "raft.log"))
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	boundaries := []int{0}
	off := 0
	for i := 0; i < n; i++ {
		off += recordLen(len(entries[i].Payload))
		boundaries = append(boundaries, off)
	}
	if off != len(data) {
		t.Fatalf("file size %d != computed %d", len(data), off)
	}
	return data, boundaries
}

// TestTornTailMatrix is the crash-injection test from the spec: for EVERY
// byte offset of the log file, truncate there (simulating a crash during
// append), reopen, and verify:
//
//  1. every fully-written record is recovered,
//  2. a partial final record is detected and discarded (loudly, via
//     Recovery), never partially trusted,
//  3. a record exactly on a boundary is a clean cut (no tear reported),
//  4. the recovered log accepts continued appends and survives another
//     restart.
func TestTornTailMatrix(t *testing.T) {
	srcDir := t.TempDir()
	const nEntries = 30
	data, boundaries := buildCompleteLog(t, srcDir, nEntries)

	boundaryAt := func(k int) bool {
		for _, b := range boundaries {
			if b == k {
				return true
			}
		}
		return false
	}
	recordsWithin := func(k int) int {
		count := 0
		for _, b := range boundaries {
			if b <= k && b > 0 {
				count++
			}
		}
		return count
	}

	for k := 0; k <= len(data); k++ {
		k := k
		t.Run(fmt.Sprintf("truncate@%d", k), func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "raft.log"), data[:k], 0o644); err != nil {
				t.Fatalf("seed: %v", err)
			}

			l, rec, err := Open(dir)
			if err != nil {
				t.Fatalf("Open after truncation at %d: %v", k, err)
			}
			defer l.Close()

			wantRecords := int64(recordsWithin(k))
			if rec.Records != wantRecords {
				t.Fatalf("recovered %d records, want %d (truncation at %d)", rec.Records, wantRecords, k)
			}
			if int64(l.Count()) != wantRecords {
				t.Fatalf("log count %d, want %d", l.Count(), wantRecords)
			}

			// Tear detection: only a mid-record cut is a torn tail.
			onBoundary := k == 0 || boundaryAt(k)
			if rec.TornTail == onBoundary {
				t.Fatalf("TornTail = %v at offset %d (onBoundary=%v), want opposite", rec.TornTail, k, onBoundary)
			}
			if rec.TornTail && rec.TruncatedBytes == 0 {
				t.Fatal("TornTail reported but TruncatedBytes = 0")
			}

			// State consistency: last index matches recovered count.
			wantLast := uint64(wantRecords)
			if wantRecords == 0 {
				wantLast = 0
			}
			if l.LastIndex() != wantLast {
				t.Fatalf("LastIndex = %d, want %d", l.LastIndex(), wantLast)
			}

			// The log remains usable: append past the end, sync, reopen.
			next := l.LastIndex() + 1
			if err := l.Append([]Entry{{Index: next, Term: 9, Payload: []byte("after-crash")}}); err != nil {
				t.Fatalf("Append after recovery: %v", err)
			}
			if err := l.Sync(); err != nil {
				t.Fatalf("Sync: %v", err)
			}
			if err := l.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			l2, rec2, err := Open(dir)
			if err != nil {
				t.Fatalf("re-open: %v", err)
			}
			defer l2.Close()
			if rec2.TornTail {
				t.Fatalf("second open reports tear: %+v", rec2)
			}
			if l2.LastIndex() != next {
				t.Fatalf("after re-append LastIndex = %d, want %d", l2.LastIndex(), next)
			}
			e, err := l2.Get(next)
			if err != nil || string(e.Payload) != "after-crash" {
				t.Fatalf("Get(%d) = %q, %v", next, e.Payload, err)
			}
		})
	}
}

// TestMidFileCorruptionIsLoud: damage that cannot be a torn tail must stop
// recovery with ErrCorrupt — never be truncated away silently.
func TestMidFileCorruptionIsLoud(t *testing.T) {
	mutations := map[string]func(data []byte){
		"flip payload byte": func(data []byte) {
			// Byte inside record #2's payload.
			data[recordLen(len("payload-1"))+24+5] ^= 0xFF
		},
		"flip index byte": func(data []byte) {
			// Low byte of record #3's index.
			off := recordLen(len("payload-1")) + recordLen(len("payload-2")) + 8
			data[off] ^= 0xFF
		},
		"flip term byte": func(data []byte) {
			off := recordLen(len("payload-1")) + recordLen(len("payload-2")) + 16
			data[off] ^= 0xFF
		},
		"flip crc byte": func(data []byte) {
			data[4] ^= 0xFF // crc of record #1 (mid-file once more follow)
		},
		"len out of range": func(data []byte) {
			// Corrupt record #2's len into an impossible value. Our writer
			// can never emit this, so it must be ErrCorrupt — even though
			// its extent would run past EOF.
			off := recordLen(len("payload-1"))
			data[off] = 0xFF
			data[off+1] = 0xFF
			data[off+2] = 0xFF
			data[off+3] = 0x7F
		},
		"index discontinuity": func(data []byte) {
			// Rewrite record #3's index from 3 to 300 (crc recomputed, so
			// CRC passes but contiguity fails).
			off := recordLen(len("payload-1")) + recordLen(len("payload-2"))
			putU64(data[off+8:], 300)
			putU32(data[off+4:], crcOver(data[off:off+4], data[off+8:off+recordLen(len("payload-3"))]))
		},
	}

	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			srcDir := t.TempDir()
			data, _ := buildCompleteLog(t, srcDir, 5)
			mutate(data)

			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "raft.log"), data, 0o644); err != nil {
				t.Fatalf("seed: %v", err)
			}
			_, _, err := Open(dir)
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("Open = %v, want ErrCorrupt", err)
			}
		})
	}
}

// TestTornFirstRecord: a crash during the very first append leaves a partial
// record at offset 0 — recovery must treat it as a torn tail (fresh-node
// semantics), not corruption.
func TestTornFirstRecord(t *testing.T) {
	srcDir := t.TempDir()
	data, _ := buildCompleteLog(t, srcDir, 3)

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "raft.log"), data[:10], 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	l, rec, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()
	if !rec.TornTail || rec.TruncatedBytes != 10 || rec.Records != 0 {
		t.Fatalf("recovery = %+v, want torn tail of 10 bytes, 0 records", rec)
	}
	if l.LastIndex() != 0 || l.FirstIndex() != 1 {
		t.Fatalf("state: first=%d last=%d, want 1/0", l.FirstIndex(), l.LastIndex())
	}
}
