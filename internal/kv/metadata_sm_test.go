package kv

import (
	"fmt"
	
	"testing"

	"distrikv/internal/raft"
	"distrikv/internal/shard"
)

// TestMetadataSMMoveSlotsSafety tests that MoveSlots rejects moves
// when the source range contains keys, unless unsafe_no_migration is set.
func TestMetadataSMMoveSlotsSafety(t *testing.T) {
	// Create engines for 2 groups
	engines := map[raft.GroupID]Engine{
		0: NewMemEngine(),
		1: NewMemEngine(),
	}

	// Find keys that hash to group 0's range
	shardMap := shard.NewMap(2)
	var group0Keys []string
	for i := 0; i < 1000 && len(group0Keys) < 2; i++ {
		k := fmt.Sprintf("testkey-%d", i)
		if shardMap.Group(k) == 0 {
			group0Keys = append(group0Keys, k)
		}
	}
	if len(group0Keys) < 2 {
		t.Fatal("Could not find enough keys for group 0")
	}

	// Put some keys in group 0's range
	engines[0].Put(group0Keys[0], []byte("value-a"))
	engines[0].Put(group0Keys[1], []byte("value-b"))

	// Create metadata SM with 2 groups, engines provided
	m := NewMetadataSM(MetadataSMConfig{
		InitialConfig: shard.NewMap(2),
		Engines:       engines,
	})

	// Try to move a range from group 0 to group 1 that contains keys
	// without unsafe flag - should fail
	req := MoveSlotsRequest{
		StartSlot:        0,
		EndSlot:          shard.NumSlots / 2, // First half (group 0's range)
		FromGroup:        0,
		ToGroup:          1,
		NewVersion:       2,
		UnsafeNoMigration: false,
	}
	cmd := MoveSlots(req)
	payload, _ := EncodeCommand(cmd)

	_, err := m.Apply(payload)
	if err == nil {
		t.Fatal("MoveSlots should reject move of non-empty range without unsafe flag")
	}
	if err.Error() == "" || !contains(err.Error(), "contains keys") {
		t.Fatalf("Expected error about keys in range, got: %v", err)
	}

	// Now try with unsafe flag - should succeed
	req.UnsafeNoMigration = true
	cmd = MoveSlots(req)
	payload, _ = EncodeCommand(cmd)

	res, err := m.Apply(payload)
	if err != nil {
		t.Fatalf("MoveSlots with unsafe flag should succeed: %v", err)
	}
	if res == nil {
		t.Fatal("Expected result from MoveSlots")
	}

	// Verify config was updated
	cfg := m.Config()
	if cfg.Version != 2 {
		t.Fatalf("Expected version 2, got %d", cfg.Version)
	}
	// The moved range should now be owned by group 1
	for slot := uint64(0); slot < shard.NumSlots/2; slot++ {
		if cfg.GroupSlot(slot) != 1 {
			t.Fatalf("Slot %d should be in group 1, got %d", slot, cfg.GroupSlot(slot))
		}
	}
}

// TestMetadataSMMoveSlotsEmptyRange tests that MoveSlots succeeds
// when the source range is empty (no keys).
func TestMetadataSMMoveSlotsEmptyRange(t *testing.T) {
	engines := map[raft.GroupID]Engine{
		0: NewMemEngine(),
		1: NewMemEngine(),
	}

	m := NewMetadataSM(MetadataSMConfig{
		InitialConfig: shard.NewMap(2),
		Engines:       engines,
	})

	// Move empty range from group 0 to group 1
	req := MoveSlotsRequest{
		StartSlot:        0,
		EndSlot:          shard.NumSlots / 2,
		FromGroup:        0,
		ToGroup:          1,
		NewVersion:       2,
		UnsafeNoMigration: false,
	}
	cmd := MoveSlots(req)
	payload, _ := EncodeCommand(cmd)

	res, err := m.Apply(payload)
	if err != nil {
		t.Fatalf("MoveSlots of empty range should succeed: %v", err)
	}
	if res == nil {
		t.Fatal("Expected result")
	}

	// Verify config updated
	cfg := m.Config()
	if cfg.Version != 2 {
		t.Fatalf("Expected version 2")
	}
}

// TestMetadataSMMoveSlotsWithoutEngines tests that without engines
// the check is skipped (backward compatibility).
func TestMetadataSMMoveSlotsWithoutEngines(t *testing.T) {
	// No engines provided
	m := NewMetadataSM(MetadataSMConfig{
		InitialConfig: shard.NewMap(2),
		Engines:       nil,
	})

	req := MoveSlotsRequest{
		StartSlot:        0,
		EndSlot:          shard.NumSlots / 2,
		FromGroup:        0,
		ToGroup:          1,
		NewVersion:       2,
		UnsafeNoMigration: false,
	}
	cmd := MoveSlots(req)
	payload, _ := EncodeCommand(cmd)

	// Should succeed even with non-empty range because no engines to check
	res, err := m.Apply(payload)
	if err != nil {
		t.Fatalf("MoveSlots without engines should skip key check: %v", err)
	}
	if res == nil {
		t.Fatal("Expected result")
	}
}

// contains checks if a string contains a substring.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || containsMiddle(s, substr)))
}

func containsMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}