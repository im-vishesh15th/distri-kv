package kv

// Phase 9: client-session deduplication in the replicated state machine —
// the spec §13 requirements: a retried logical request must not apply
// twice, its original response must be replayed, and the session state is
// ordinary SM state (deterministic, rebuilt from log replay).

import (
	"errors"
	"testing"
)

// sessioned stamps a session onto a command (test helper mirroring what
// the service does per request).
func sessioned(cmd Command, clientID string, seq uint64) Command {
	cmd.ClientID, cmd.Sequence = clientID, seq
	return cmd
}

func mustEncode(t *testing.T, cmd Command) []byte {
	t.Helper()
	b, err := EncodeCommand(cmd)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return b
}

func mustResult(t *testing.T, sm *SM, payload []byte) Result {
	t.Helper()
	out, err := sm.Apply(payload)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	res, ok := out.(Result)
	if !ok {
		t.Fatalf("result type = %T, want kv.Result", out)
	}
	return res
}

// TestDedupReplaysCachedResult is the §13 example: leader commits, the
// response is lost, the client retries the same sequence — the retry must
// be recognized and answered from the session table WITHOUT applying again.
func TestDedupReplaysCachedResult(t *testing.T) {
	engine := NewMemEngine()
	sm := NewSM(engine)
	payload := mustEncode(t, sessioned(IncrBy("counter", 1), "client-A", 42))

	first := mustResult(t, sm, payload)
	if v, _ := engine.Get("counter"); string(v) != "1" {
		t.Fatalf("engine after first = %q, want 1", v)
	}

	// The retry: identical bytes, identical session.
	second := mustResult(t, sm, payload)
	if string(second.Value) != string(first.Value) {
		t.Fatalf("replayed result = %q, want original %q", second.Value, first.Value)
	}
	if v, _ := engine.Get("counter"); string(v) != "1" {
		t.Fatalf("engine after retry = %q, want 1 (double-applied)", v)
	}

	// A NEW sequence from the same session applies normally.
	next := mustResult(t, sm, mustEncode(t, sessioned(IncrBy("counter", 1), "client-A", 43)))
	if string(next.Value) != "2" {
		t.Fatalf("seq 43 result = %q, want 2", next.Value)
	}
}

// TestDedupRejectsSupersededSequence: a sequence older than one already
// applied is never applied (it would rewind state written by newer
// requests) — it is refused with ErrStaleSequence. Gaps (abandoned
// attempts) are legal.
func TestDedupRejectsSupersededSequence(t *testing.T) {
	engine := NewMemEngine()
	sm := NewSM(engine)

	mustResult(t, sm, mustEncode(t, sessioned(Set("k", []byte("v3")), "c", 3)))

	// Superseded: would clobber v3 with v1.
	_, err := sm.Apply(mustEncode(t, sessioned(Set("k", []byte("v1")), "c", 1)))
	if !errors.Is(err, ErrStaleSequence) {
		t.Fatalf("seq 1 err = %v, want ErrStaleSequence", err)
	}
	if v, _ := engine.Get("k"); string(v) != "v3" {
		t.Fatalf("engine = %q, want v3 (superseded write applied)", v)
	}

	// Gap: seq 5 was skipped (an abandoned attempt consumed seq 4) — legal.
	mustResult(t, sm, mustEncode(t, sessioned(Set("k", []byte("v5")), "c", 5)))
	if v, _ := engine.Get("k"); string(v) != "v5" {
		t.Fatalf("engine = %q, want v5", v)
	}
}

// TestDedupReplaysCachedError: the recorded outcome of a FAILED request is
// replayed too — a retry must not observe state that has since changed
// (here: another writer fixes the value between the original and the
// retry; the retry still reports its own original failure).
func TestDedupReplaysCachedError(t *testing.T) {
	engine := NewMemEngine()
	sm := NewSM(engine)
	if _, err := engine.Apply(Set("s", []byte("hello"))); err != nil {
		t.Fatalf("seed: %v", err)
	}

	payload := mustEncode(t, sessioned(IncrBy("s", 1), "c", 1))
	if _, err := sm.Apply(payload); !errors.Is(err, ErrNotInteger) {
		t.Fatalf("original err = %v, want ErrNotInteger", err)
	}

	// Another writer repairs the value between original and retry.
	mustResult(t, sm, mustEncode(t, Set("s", []byte("5"))))

	// The retry replays ITS recorded outcome (ErrNotInteger), not a fresh
	// evaluation against the repaired state (which would return 6).
	if _, err := sm.Apply(payload); !errors.Is(err, ErrNotInteger) {
		t.Fatalf("retry err = %v, want cached ErrNotInteger", err)
	}
	if v, _ := engine.Get("s"); string(v) != "5" {
		t.Fatalf("engine = %q, want 5 (retry re-applied)", v)
	}
}

// TestDedupReplaysCachedCASFailure: applied=false is a normal outcome and
// is cached like any other response (proto/kv.proto contract).
func TestDedupReplaysCachedCASFailure(t *testing.T) {
	engine := NewMemEngine()
	sm := NewSM(engine)
	mustResult(t, sm, mustEncode(t, Set("k", []byte("v"))))

	payload := mustEncode(t, sessioned(CAS("k", []byte("other"), []byte("new")), "c", 1))
	first := mustResult(t, sm, payload)
	if first.Applied {
		t.Fatal("original CAS applied, want precondition failure")
	}
	second := mustResult(t, sm, payload)
	if second.Applied {
		t.Fatal("replayed CAS applied, want cached precondition failure")
	}
	if v, _ := engine.Get("k"); string(v) != "v" {
		t.Fatalf("engine = %q, want v (CAS must not have stored)", v)
	}
}

// TestNoSessionOptsOut: client_id == "" is the internal/test path — always
// applied, no session bookkeeping (the gRPC edge rejects client mutations
// without session fields, so clients never take this path).
func TestNoSessionOptsOut(t *testing.T) {
	engine := NewMemEngine()
	sm := NewSM(engine)
	mustResult(t, sm, mustEncode(t, IncrBy("n", 1)))
	mustResult(t, sm, mustEncode(t, IncrBy("n", 1)))
	if v, _ := engine.Get("n"); string(v) != "2" {
		t.Fatalf("engine = %q, want 2 (sessionless commands must not dedup)", v)
	}
}

// TestSessionStateSurvivesReplay: sessions are derived state — a fresh SM
// that replays the same log (node restart, new replica) rebuilds the
// session table and keeps deduplicating (S2: replication half; snapshot
// serialization is Phase 12).
func TestSessionStateSurvivesReplay(t *testing.T) {
	payload := mustEncode(t, sessioned(IncrBy("counter", 1), "client-A", 42))

	original := NewSM(NewMemEngine())
	mustResult(t, original, payload)

	// A brand-new state machine replays the log into a brand-new engine.
	recovered := NewSM(NewMemEngine())
	first := mustResult(t, recovered, payload)
	if string(first.Value) != "1" {
		t.Fatalf("replay result = %q, want 1", first.Value)
	}
	// The rebuilt session table still recognizes the duplicate...
	second := mustResult(t, recovered, payload)
	if string(second.Value) != "1" {
		t.Fatalf("retry after replay = %q, want 1", second.Value)
	}
	if v, _ := recovered.engine.Get("counter"); string(v) != "1" {
		t.Fatalf("engine after replayed retry = %q, want 1 (double-applied)", v)
	}
	// ...and still refuses superseded sequences.
	if _, err := recovered.Apply(mustEncode(t, sessioned(IncrBy("counter", 1), "client-A", 1))); !errors.Is(err, ErrStaleSequence) {
		t.Fatalf("stale after replay err = %v, want ErrStaleSequence", err)
	}
}
