package kv

import (
	"bytes"
	"sync"
)

// MemEngine is the initial in-memory Engine implementation: a map[string][]byte
// guarded by a sync.RWMutex.
//
// Ownership rule: values are defensively copied on the way in (Put, Apply) and
// on the way out (Get). Callers may freely reuse their buffers after calling
// into the engine; engine state is never aliased by caller memory.
//
// MemEngine is safe for concurrent use. It holds no disk state — durability
// comes exclusively from the persistent Raft log (Phase 3), never from a
// second log inside the engine.
type MemEngine struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewMemEngine returns an empty, ready-to-use in-memory engine.
func NewMemEngine() *MemEngine {
	return &MemEngine{data: make(map[string][]byte)}
}

// Get returns a copy of the value for key, or ErrKeyNotFound.
func (e *MemEngine) Get(key string) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	v, ok := e.data[key]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return clone(v), nil
}

// Put stores a copy of value under key, overwriting any existing value.
func (e *MemEngine) Put(key string, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.data[key] = clone(value)
	return nil
}

// Delete removes key. Deleting a missing key is a no-op, not an error:
// Delete must be idempotent because state-machine commands may be retried.
func (e *MemEngine) Delete(key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.data, key)
	return nil
}

// Exists reports whether key is present.
func (e *MemEngine) Exists(key string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	_, ok := e.data[key]
	return ok
}

// Apply executes a state-machine command atomically against the engine.
//
// This is the single-writer entry point for all mutations: from Phase 7 on,
// every mutating request flows client -> Raft log -> commit -> Apply, on every
// replica, in log order. Reads (GET/EXISTS) do NOT go through Apply — they are
// served directly from the engine under the linearizable-read path (Phase 11).
//
// Apply must remain deterministic: identical commands on identical prior state
// must produce identical results and identical resulting state on all replicas.
func (e *MemEngine) Apply(cmd Command) (Result, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	switch cmd.Op {
	case OpSet:
		e.data[cmd.Key] = clone(cmd.Value)
		return Result{Applied: true}, nil

	case OpDelete:
		delete(e.data, cmd.Key)
		return Result{Applied: true}, nil

	case OpCAS:
		return e.applyCAS(cmd)

	case OpIncr:
		return e.applyIncr(cmd)

	default:
		return Result{}, ErrUnknownOp
	}
}

// applyCAS implements compare-and-swap. Semantics (see command.go):
//   - Expected == nil  => the key must be ABSENT for the swap to succeed.
//   - Expected != nil  => the key must be PRESENT and byte-equal to Expected.
//
// A failed precondition returns Result{Applied: false} with a nil error —
// it is a normal outcome, not a failure, and replicas must agree on it.
func (e *MemEngine) applyCAS(cmd Command) (Result, error) {
	cur, exists := e.data[cmd.Key]

	var match bool
	switch {
	case cmd.Expected == nil:
		match = !exists
	case !exists:
		match = false
	default:
		match = bytes.Equal(cur, cmd.Expected)
	}
	if !match {
		return Result{Applied: false}, nil
	}

	e.data[cmd.Key] = clone(cmd.Value)
	return Result{Applied: true}, nil
}

// applyIncr atomically adds cmd.Delta (a signed value; negative = decrement)
// to the integer stored at cmd.Key. Absent keys are treated as 0, so Incr on
// a fresh key creates it with the delta and Decr creates it with -delta.
//
// The value must be a base-10 int64 in the canonical form this function
// writes; anything else returns ErrNotInteger. Overflow returns ErrOverflow.
func (e *MemEngine) applyIncr(cmd Command) (Result, error) {
	var n int64

	if cur, exists := e.data[cmd.Key]; exists {
		parsed, err := parseInt64(cur)
		if err != nil {
			return Result{}, err
		}
		n = parsed
	}

	// Overflow checks: compute the bounds without negating math.MinInt64.
	if cmd.Delta > 0 && n > (1<<63-1)-cmd.Delta {
		return Result{}, ErrOverflow
	}
	if cmd.Delta < 0 && n < -(1<<63)-cmd.Delta {
		return Result{}, ErrOverflow
	}
	n += cmd.Delta

	encoded := formatInt64(n)
	e.data[cmd.Key] = encoded
	return Result{Applied: true, Value: clone(encoded)}, nil
}

// clone returns a copy of b. clone(nil) returns nil.
func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
