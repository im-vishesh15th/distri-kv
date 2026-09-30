package kv

// Phase 7: SM is the seam between the Raft log and the engine — payload in,
// Command decoded, applied, kv.Result out.

import (
	"errors"
	"testing"
)

func TestSMAppliesEncodedCommands(t *testing.T) {
	engine := NewMemEngine()
	sm := NewSM(engine)

	payload, err := EncodeCommand(Set("k", []byte("v")))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	result, aerr := sm.Apply(payload)
	if aerr != nil {
		t.Fatalf("apply: %v", aerr)
	}
	res, ok := result.(Result)
	if !ok {
		t.Fatalf("result type = %T, want kv.Result", result)
	}
	if v, gerr := engine.Get("k"); gerr != nil || string(v) != "v" {
		t.Fatalf("engine after apply: (%q, %v), want (v, nil)", v, gerr)
	}
	_ = res // Set's Result carries no client-visible outcome.
}

func TestSMDomainErrorIsDeliveredNotFatal(t *testing.T) {
	engine := NewMemEngine()
	sm := NewSM(engine)
	if _, err := engine.Apply(Set("s", []byte("hello"))); err != nil {
		t.Fatalf("seed: %v", err)
	}

	payload, err := EncodeCommand(IncrBy("s", 1))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, aerr := sm.Apply(payload); !errors.Is(aerr, ErrNotInteger) {
		t.Fatalf("apply err = %v, want ErrNotInteger", aerr)
	}
	// The failed entry changed nothing — but it was still processed.
	if v, gerr := engine.Get("s"); gerr != nil || string(v) != "hello" {
		t.Fatalf("state after failed incr: (%q, %v)", v, gerr)
	}
}

func TestSMRefusesGarbagePayload(t *testing.T) {
	engine := NewMemEngine()
	sm := NewSM(engine)
	if _, aerr := sm.Apply([]byte("garbage")); aerr == nil {
		t.Fatal("garbage payload accepted")
	}
	if engine.Exists("anything") {
		t.Fatal("state mutated by garbage payload")
	}
}
