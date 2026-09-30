// Package kv defines the key-value engine contract for DistriKV — the
// replicated state machine's storage layer.
//
// Status (Phase 7): Engine interface + MemEngine (map behind RWMutex) +
// Command/Apply single-writer path + wire codec (codec.go) and the SM
// adapter (sm.go) that lets Raft's apply path drive Engine.Apply. Phase 12
// adds Snapshot/Restore on this interface and session-table serialization
// in sm.go so Raft can compact its log.
//
// Design notes (see docs/architecture.md):
//
//   - Raft commits an ordered log of Commands; every replica applies them to
//     its Engine in the same order. Determinism of Apply is an invariant.
//   - All mutations flow through Apply. Reads (Get/Exists) bypass it and are
//     served under the linearizable-read path (Phase 11).
//   - The initial implementation is map[string][]byte behind a sync.RWMutex,
//     hidden behind this interface so storage internals can improve later
//     without touching Raft or the state machine. Snapshot/Restore joined
//     the interface in Phase 12 (log compaction).
//   - There is intentionally no separate WAL: the persistent Raft log is the
//     only durable ordered log in the system.
package kv

import "errors"

// ErrKeyNotFound is returned when a key does not exist.
var ErrKeyNotFound = errors.New("kv: key not found")

// ErrKeyExists is returned by Create-style operations when a key already exists.
var ErrKeyExists = errors.New("kv: key already exists")

// Engine is the storage contract of a DistriKV node.
//
// Implementations must be safe for concurrent use. Get/Put/Delete/Exists are
// the Phase 1 surface; Apply has been driven by the Raft apply path since
// Phase 7; Snapshot/Restore (Phase 12) serialize the full key space so Raft
// can compact log prefixes and a restarted node resumes from captured state
// plus the log tail.
type Engine interface {
	// Get returns the value for key, or ErrKeyNotFound.
	Get(key string) ([]byte, error)

	// Put stores value under key, overwriting any existing value.
	Put(key string, value []byte) error

	// Delete removes key. Deleting a missing key is not an error.
	Delete(key string) error

	// Exists reports whether key is present.
	Exists(key string) bool

	// Apply executes cmd atomically and deterministically, returning the
	// result recorded for the client response. This is the sole mutation
	// entry point for the replicated state machine.
	Apply(cmd Command) (Result, error)

	// Snapshot serializes the engine's entire key space. The encoding is
	// engine-specific (only Restore will read it back); it must be
	// self-contained (no aliases into live engine memory) and deterministic
	// (same state ⇒ same bytes, so snapshot tests can compare encodings).
	Snapshot() ([]byte, error)

	// Restore replaces the engine's state with one produced by Snapshot.
	// After it returns, the engine holds exactly the captured keys — any
	// keys added since the snapshot was taken are gone. A malformed
	// encoding is an error, never a partial application: restoring half a
	// state machine would silently diverge from every other replica.
	Restore(data []byte) error
}
