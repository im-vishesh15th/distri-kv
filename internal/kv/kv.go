// Package kv defines the key-value engine contract for DistriKV.
//
// Phase 0 status: interface only — no implementation yet.
//
// Design notes (see docs/architecture.md):
//
//   - The engine is the replicated state machine's storage layer: Raft commits
//     an ordered log of commands, and each replica applies those commands to an
//     Engine in the same order. Determinism of Apply is therefore an invariant.
//   - The initial implementation (Phase 1) is map[string][]byte behind a
//     sync.RWMutex, hidden behind this interface so storage internals can
//     improve later without touching Raft or the state machine.
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
// the Phase 1 surface; Apply/Snapshot/Restore are exercised starting Phase 7
// and Phase 12 respectively.
type Engine interface {
	// Get returns the value for key, or ErrKeyNotFound.
	Get(key string) ([]byte, error)

	// Put stores value under key, overwriting any existing value.
	Put(key string, value []byte) error

	// Delete removes key. Deleting a missing key is not an error.
	Delete(key string) error

	// Exists reports whether key is present.
	Exists(key string) bool
}
