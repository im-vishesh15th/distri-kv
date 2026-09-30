package kv

// Phase 12: Snapshot/Restore — the engine key space AND the client-session
// table serialize into one payload, restore replaces wholesale, dedup
// decisions are identical after a restore (spec §13/§15), and the encoding
// is deterministic (same state ⇒ same bytes).

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

// Compile-time: SM offers the raft.Snapshotter capability (structural
// interface — raft never imports kv, kv never imports raft).
var _ interface {
	Snapshot() ([]byte, error)
	Restore([]byte) error
} = (*SM)(nil)

// TestMemEngineSnapshotRestore: round trip preserves state, the encoding is
// byte-deterministic, and Restore REPLACES (post-snapshot additions vanish).
func TestMemEngineSnapshotRestore(t *testing.T) {
	e := NewMemEngine()
	if err := e.Put("keep", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := e.Put("empty", nil); err != nil { // empty value must survive
		t.Fatal(err)
	}
	if err := e.Put("bin", []byte{0x00, 0xff, 0x7f}); err != nil { // non-UTF8
		t.Fatal(err)
	}
	if err := e.Put("deleted", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := e.Delete("deleted"); err != nil {
		t.Fatal(err)
	}

	snap, err := e.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	// Determinism: same state ⇒ identical bytes (sorted keys).
	again, err := e.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot again: %v", err)
	}
	if !bytes.Equal(snap, again) {
		t.Fatalf("snapshot encoding not deterministic: %d vs %d bytes differ", len(snap), len(again))
	}

	// Diverge after the snapshot; restore must discard those changes.
	if err := e.Put("post-snapshot", []byte("gone")); err != nil {
		t.Fatal(err)
	}
	if err := e.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	for k, want := range map[string][]byte{
		"keep": []byte("v1"), "empty": nil, "bin": {0x00, 0xff, 0x7f},
	} {
		got, err := e.Get(k)
		if err != nil {
			t.Fatalf("Get(%q) after restore: %v", k, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get(%q) = %v, want %v", k, got, want)
		}
	}
	if _, err := e.Get("deleted"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("deleted key resurrected: Get = %v", err)
	}
	if e.Exists("post-snapshot") {
		t.Fatalf("post-snapshot key survived Restore (restore must replace, not merge)")
	}

	// A restored engine snapshots to the same bytes (round-trip stability).
	rsnap, err := e.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot after restore: %v", err)
	}
	if !bytes.Equal(rsnap, snap) {
		t.Fatalf("restore→snapshot bytes differ from original")
	}
}

// TestMemEngineRestoreRejectsMalformed: every truncation/misalignment fails
// WITHOUT touching current state — a half-restored engine must never reach
// the apply path.
func TestMemEngineRestoreRejectsMalformed(t *testing.T) {
	good := func() *MemEngine {
		e := NewMemEngine()
		if err := e.Put("stable", []byte("value")); err != nil {
			t.Fatal(err)
		}
		return e
	}
	valid, err := good().Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(valid) < 12 {
		t.Fatalf("valid snapshot implausibly short: %d bytes", len(valid))
	}

	cases := map[string][]byte{
		"truncated header":  valid[:4],
		"truncated entry":   valid[:len(valid)-2],
		"trailing garbage":  append(append([]byte{}, valid...), 0xAA),
		"implausible count": append([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x0f}, valid[8:]...),
	}
	for name, data := range cases {
		e := good()
		if err := e.Restore(data); err == nil {
			t.Fatalf("%s: Restore = nil, want an error", name)
		}
		if v, err := e.Get("stable"); err != nil || string(v) != "value" {
			t.Fatalf("%s: state damaged by rejected restore: (%q, %v)", name, v, err)
		}
	}
}

// buildHistory feeds one SM a fixed, session-bearing history and returns
// it: a normal write, a cached domain error (INCR on text), a failed CAS
// precondition, and a second write from the first session — the minimum
// where session ORDER and cached outcomes both matter.
func buildHistory(t *testing.T) *SM {
	t.Helper()
	sm := NewSM(NewMemEngine())
	mustResult(t, sm, mustEncode(t, sessioned(Set("a", []byte("1")), "client-A", 1)))
	mustResult(t, sm, mustEncode(t, Set("txt", []byte("hello")))) // non-integer target
	if _, err := sm.Apply(mustEncode(t, sessioned(IncrBy("txt", 1), "client-B", 1))); !errors.Is(err, ErrNotInteger) {
		t.Fatalf("setup: client-B incr = %v, want ErrNotInteger", err)
	}
	mustResult(t, sm, mustEncode(t, sessioned(CAS("a", []byte("99"), []byte("nope")), "client-C", 7)))
	mustResult(t, sm, mustEncode(t, sessioned(IncrBy("cnt", 2), "client-A", 2)))
	return sm
}

// TestSMSnapshotRestoreRoundTrip is the spec §13+§15 intersection: after a
// snapshot restore, the session table answers dedup exactly as before —
// the last-applied retry replays (not re-apply), cached domain errors keep
// errors.Is identity, superseded sequences stay refused, and identical
// histories produce identical snapshot bytes on independently built
// replicas.
func TestSMSnapshotRestoreRoundTrip(t *testing.T) {
	sm := buildHistory(t)
	snap, err := sm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Independent replica, same history ⇒ identical snapshot bytes.
	snap2, err := buildHistory(t).Snapshot()
	if err != nil {
		t.Fatalf("Snapshot 2: %v", err)
	}
	if !bytes.Equal(snap, snap2) {
		t.Fatalf("snapshots of identical histories differ (%d vs %d bytes)", len(snap), len(snap2))
	}

	// Restore into a fresh SM (empty engine, empty sessions).
	freshEngine := NewMemEngine()
	fresh := NewSM(freshEngine)
	if err := fresh.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Engine state carried over (a == "1" — the INCR was refused).
	if v, err := freshEngine.Get("a"); err != nil || string(v) != "1" {
		t.Fatalf("restored a = (%q, %v), want (1, nil)", v, err)
	}
	if v, err := freshEngine.Get("cnt"); err != nil || string(v) != "2" {
		t.Fatalf("restored cnt = (%q, %v), want (2, nil)", v, err)
	}

	// Retry of client-A's LAST op (seq 2): replayed, NOT re-applied.
	payloadA2 := mustEncode(t, sessioned(IncrBy("cnt", 2), "client-A", 2))
	out, err := fresh.Apply(payloadA2)
	if err != nil {
		t.Fatalf("client-A seq2 retry after restore: %v", err)
	}
	if res, ok := out.(Result); !ok || !res.Applied {
		t.Fatalf("client-A seq2 retry = %#v, want Applied result", out)
	}
	if v, _ := freshEngine.Get("cnt"); string(v) != "2" {
		t.Fatalf("cnt after retry = %q, want 2 (double-applied)", v)
	}

	// Retry from client-B: the cached domain error keeps errors.Is identity
	// (sentinel rebuilt from its Error() text, sm.go sessionError).
	if _, err := fresh.Apply(mustEncode(t, sessioned(IncrBy("txt", 1), "client-B", 1))); !errors.Is(err, ErrNotInteger) {
		t.Fatalf("client-B retry after restore = %v, want ErrNotInteger (sentinel identity lost)", err)
	}

	// Retry from client-C: the CAS precondition failure replays as
	// applied=false — a normal outcome, not an error.
	out, err = fresh.Apply(mustEncode(t, sessioned(CAS("a", []byte("99"), []byte("nope")), "client-C", 7)))
	if err != nil {
		t.Fatalf("client-C retry after restore: %v", err)
	}
	if res := out.(Result); res.Applied {
		t.Fatalf("client-C retry = %+v, want Applied=false replay", res)
	}

	// A superseded sequence (client-A seq 1 < last 2) stays refused.
	if _, err := fresh.Apply(mustEncode(t, sessioned(Set("a", []byte("stale")), "client-A", 1))); !errors.Is(err, ErrStaleSequence) {
		t.Fatalf("client-A seq1 after restore = %v, want ErrStaleSequence", err)
	}
	if v, _ := freshEngine.Get("a"); string(v) != "1" {
		t.Fatalf("stale write landed: a = %q, want 1", v)
	}

	// Unsessioned writes still apply post-restore.
	mustResult(t, fresh, mustEncode(t, Set("b", []byte("2"))))
	if v, _ := freshEngine.Get("b"); string(v) != "2" {
		t.Fatalf("post-restore unsessioned write = %q, want 2", v)
	}
}

// TestSessionCapEvictsOldest pins the bounded-table behavior (S1's
// documented horizon): arriving at sessionCap, a NEW session evicts the
// earliest-created one — deterministic (birth order is replicated state) —
// while retained sessions keep deduping. The evicted client's next retry is
// no longer recognized: the at-most-once window ends at eviction, exactly
// as documented, never silently.
func TestSessionCapEvictsOldest(t *testing.T) {
	engine := NewMemEngine()
	sm := NewSM(engine)

	inc := func(clientID string, seq uint64) {
		t.Helper()
		if _, err := sm.Apply(mustEncode(t, sessioned(IncrBy("c", 1), clientID, seq))); err != nil {
			t.Fatalf("apply %s#%d: %v", clientID, seq, err)
		}
	}
	client := func(i int) string { return fmt.Sprintf("client-%d", i) }

	// Fill the table exactly to capacity: one session each, c == sessionCap.
	for i := 0; i < sessionCap; i++ {
		inc(client(i), 1)
	}
	if got := len(sm.sessions); got != sessionCap {
		t.Fatalf("sessions at capacity = %d, want %d", got, sessionCap)
	}
	if v, _ := engine.Get("c"); string(v) != fmt.Sprint(sessionCap) {
		t.Fatalf("c = %q, want %d", v, sessionCap)
	}

	// One more NEW session arrives: evicts client-0 (oldest birth), stays
	// at capacity, and its own increment applies.
	inc(client(sessionCap), 1)
	if got := len(sm.sessions); got != sessionCap {
		t.Fatalf("sessions after overflow = %d, want %d (eviction must keep the cap)", got, sessionCap)
	}
	if _, ok := sm.sessions[client(0)]; ok {
		t.Fatalf("client-0 survived at capacity; want eviction of the earliest session")
	}
	if _, ok := sm.sessions[client(sessionCap)]; !ok {
		t.Fatalf("newest session missing after eviction")
	}
	if v, _ := engine.Get("c"); string(v) != fmt.Sprint(sessionCap+1) {
		t.Fatalf("c after overflow session = %q, want %d", v, sessionCap+1)
	}

	// A RETAINED session still dedups: its retry replays without applying.
	if _, err := sm.Apply(mustEncode(t, sessioned(IncrBy("c", 1), client(sessionCap), 1))); err != nil {
		t.Fatalf("retained session retry: %v", err)
	}
	if v, _ := engine.Get("c"); string(v) != fmt.Sprint(sessionCap+1) {
		t.Fatalf("c after retained retry = %q, want %d (must not re-apply)", v, sessionCap+1)
	}

	// The EVICTED session is no longer recognized — its retry applies as a
	// fresh session (documented: the at-most-once window ends at eviction).
	if _, err := sm.Apply(mustEncode(t, sessioned(IncrBy("c", 1), client(0), 1))); err != nil {
		t.Fatalf("evicted session retry: %v", err)
	}
	if v, _ := engine.Get("c"); string(v) != fmt.Sprint(sessionCap+2) {
		t.Fatalf("c after evicted retry = %q, want %d (re-applies by design after eviction)", v, sessionCap+2)
	}
}
