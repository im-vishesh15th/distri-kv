package kv

import "testing"

// TestEngineContract documents the Phase 0 state: the Engine interface exists
// but no implementation is required yet. Phase 1 will add a shared conformance
// suite that every Engine implementation must pass (table-driven CRUD plus
// concurrent -race tests).
func TestEngineContract(t *testing.T) {
	// Compile-time assertion that the error sentinels the API depends on exist.
	if ErrKeyNotFound == nil || ErrKeyExists == nil {
		t.Fatal("kv package must expose ErrKeyNotFound and ErrKeyExists")
	}
	if ErrKeyNotFound == ErrKeyExists {
		t.Fatal("error sentinels must be distinct")
	}
}
