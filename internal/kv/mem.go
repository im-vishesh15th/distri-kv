package kv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
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

// --- Snapshot / Restore (Phase 12) ---

// MemEngine snapshot encoding (little-endian, engine-internal):
//
//	[u64 count][u32 klen][key][u32 vlen][value] × count
//
// Keys are written in sorted order so the encoding is deterministic: the
// same map state always produces the same bytes (snapshot tests compare
// encodings directly). The encoding is self-contained: everything is copied
// out under the read lock, so callers may snapshot while writes proceed.
const memSnapMaxEntries = 1 << 28 // sanity bound: count can't exceed 256M

// Snapshot serializes the full key space. See Engine.Snapshot.
func (e *MemEngine) Snapshot() ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	keys := make([]string, 0, len(e.data))
	for k := range e.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(len(e.data)))
	for _, k := range keys {
		if uint64(len(k)) > uint64(^uint32(0)) || uint64(len(e.data[k])) > uint64(^uint32(0)) {
			return nil, fmt.Errorf("kv: snapshot entry exceeds 4 GiB")
		}
		var lenbuf [4]byte
		binary.LittleEndian.PutUint32(lenbuf[:], uint32(len(k)))
		buf = append(buf, lenbuf[:]...)
		buf = append(buf, k...)
		binary.LittleEndian.PutUint32(lenbuf[:], uint32(len(e.data[k])))
		buf = append(buf, lenbuf[:]...)
		buf = append(buf, e.data[k]...)
	}
	return buf, nil
}

// Restore replaces all engine state with data (a Snapshot encoding). A
// malformed encoding fails before any state is touched: a partially
// restored engine must never reach the apply path. See Engine.Restore.
func (e *MemEngine) Restore(data []byte) error {
	if len(data) < 8 {
		return fmt.Errorf("kv: restore engine: truncated header (%d bytes)", len(data))
	}
	count := binary.LittleEndian.Uint64(data)
	if count > memSnapMaxEntries {
		return fmt.Errorf("kv: restore engine: implausible entry count %d", count)
	}

	next := make(map[string][]byte, count)
	off := 8
	readField := func() ([]byte, error) {
		if off+4 > len(data) {
			return nil, fmt.Errorf("kv: restore engine: truncated at offset %d", off)
		}
		n := int(binary.LittleEndian.Uint32(data[off:]))
		off += 4
		if off+n > len(data) {
			return nil, fmt.Errorf("kv: restore engine: field of %d bytes overruns %d-byte snapshot", n, len(data))
		}
		f := data[off : off+n]
		off += n
		return f, nil
	}
	for i := uint64(0); i < count; i++ {
		k, err := readField()
		if err != nil {
			return err
		}
		v, err := readField()
		if err != nil {
			return err
		}
		next[string(k)] = clone(v)
	}
	if off != len(data) {
		return fmt.Errorf("kv: restore engine: %d trailing bytes after %d entries", len(data)-off, count)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.data = next
	return nil
}
