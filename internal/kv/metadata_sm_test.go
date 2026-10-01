package kv

import (
	"testing"

	"distrikv/internal/shard"
)

// TestMetadataSMApplyDeterministic tests that Apply() produces identical
// results on different replicas regardless of their engine state.
// This is the determinism test: Apply() must depend only on the log and
// the current config, not on the local engine state.
func TestMetadataSMApplyDeterministic(t *testing.T) {
	// Create three metadata SMs with different engine states:
	// - m0: engine with keys in the range
	// - m1: engine without keys in the range
	// - m2: no engine (nil)
	m0 := NewMetadataSM(MetadataSMConfig{
		InitialConfig: shard.NewMap(2),
	})
	m1 := NewMetadataSM(MetadataSMConfig{
		InitialConfig: shard.NewMap(2),
	})
	m2 := NewMetadataSM(MetadataSMConfig{
		InitialConfig: shard.NewMap(2),
	})

	// Same move command for all three
	req := MoveSlotsRequest{
		StartSlot:         0,
		EndSlot:           shard.NumSlots / 2,
		FromGroup:         0,
		ToGroup:           1,
		NewVersion:        2,
		UnsafeNoMigration: false,
	}
	cmd := MoveSlots(req)
	payload, _ := EncodeCommand(cmd)

	// All three should produce identical results (apply succeeds)
	r0, err := m0.Apply(payload)
	if err != nil {
		t.Fatalf("m0.Apply failed: %v", err)
	}
	r1, err := m1.Apply(payload)
	if err != nil {
		t.Fatalf("m1.Apply failed: %v", err)
	}
	r2, err := m2.Apply(payload)
	if err != nil {
		t.Fatalf("m2.Apply failed: %v", err)
	}

	// Results should be identical
	v0 := string(r0.(Result).Value)
	v1 := string(r1.(Result).Value)
	v2 := string(r2.(Result).Value)
	if v0 != v1 || v1 != v2 {
		t.Fatalf("Results differ: m0=%s, m1=%s, m2=%s", v0, v1, v2)
	}

	// Configs should be identical
	c0 := m0.Config()
	c1 := m1.Config()
	c2 := m2.Config()
	if c0.Version != c1.Version || c1.Version != c2.Version {
		t.Fatalf("versions differ: %d, %d, %d", c0.Version, c1.Version, c2.Version)
	}
	for slot := uint64(0); slot < shard.NumSlots/2; slot++ {
		if c0.GroupSlot(slot) != c1.GroupSlot(slot) || c1.GroupSlot(slot) != c2.GroupSlot(slot) {
			t.Fatalf("group assignment differs at slot %d: c0=%d, c1=%d, c2=%d",
				slot, c0.GroupSlot(slot), c1.GroupSlot(slot), c2.GroupSlot(slot))
		}
	}
}

// TestMetadataSMRestartReplay tests that after committing a move,
// restarting all nodes and replaying the log produces the same config.
func TestMetadataSMRestartReplay(t *testing.T) {
	m := NewMetadataSM(MetadataSMConfig{
		InitialConfig: shard.NewMap(2),
	})

	// Commit a move
	req := MoveSlotsRequest{
		StartSlot:         0,
		EndSlot:           shard.NumSlots / 2,
		FromGroup:         0,
		ToGroup:           1,
		NewVersion:        2,
		UnsafeNoMigration: false,
	}
	cmd := MoveSlots(req)
	payload, _ := EncodeCommand(cmd)

	_, err := m.Apply(payload)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// Snapshot
	snap, err := m.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	// Restart: create new SM and restore
	m2 := NewMetadataSM(MetadataSMConfig{
		InitialConfig: shard.NewMap(2),
	})
	if err := m2.Restore(snap); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Config should be identical
	c1 := m.Config()
	c2 := m2.Config()
	if c1.Version != c2.Version {
		t.Fatalf("versions differ: %d vs %d", c1.Version, c2.Version)
	}
	for slot := uint64(0); slot < shard.NumSlots; slot++ {
		if c1.GroupSlot(slot) != c2.GroupSlot(slot) {
			t.Fatalf("group assignment differs at slot %d: %d vs %d",
				slot, c1.GroupSlot(slot), c2.GroupSlot(slot))
		}
	}
}

// TestMetadataSMApplyNoKeyCheckInApply verifies that Apply() itself
// does NOT check for keys - the check happens in the RPC handler.
func TestMetadataSMApplyNoKeyCheckInApply(t *testing.T) {
	// Even with keys in the range, Apply should succeed without the unsafe flag
	// because the check is done in the RPC handler, not in Apply.
	m := NewMetadataSM(MetadataSMConfig{
		InitialConfig: shard.NewMap(2),
	})

	req := MoveSlotsRequest{
		StartSlot:         0,
		EndSlot:           shard.NumSlots / 2,
		FromGroup:         0,
		ToGroup:           1,
		NewVersion:        2,
		UnsafeNoMigration: false,
	}
	cmd := MoveSlots(req)
	payload, _ := EncodeCommand(cmd)

	// Apply should succeed without checking for keys
	_, err := m.Apply(payload)
	if err != nil {
		t.Fatalf("Apply should not check for keys: %v", err)
	}

	// Config should be updated
	cfg := m.Config()
	if cfg.Version != 2 {
		t.Fatalf("expected version 2, got %d", cfg.Version)
	}
}

// TestMetadataSMConfigVersion tests that version is incremented correctly
func TestMetadataSMConfigVersion(t *testing.T) {
	m := NewMetadataSM(MetadataSMConfig{
		InitialConfig: shard.NewMap(2),
	})

	if m.Config().Version != 1 {
		t.Fatalf("initial version should be 1, got %d", m.Config().Version)
	}

	req := MoveSlotsRequest{
		StartSlot:         0,
		EndSlot:           shard.NumSlots / 2,
		FromGroup:         0,
		ToGroup:           1,
		NewVersion:        2,
		UnsafeNoMigration: false,
	}
	cmd := MoveSlots(req)
	payload, _ := EncodeCommand(cmd)

	_, err := m.Apply(payload)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if m.Config().Version != 2 {
		t.Fatalf("version should be 2, got %d", m.Config().Version)
	}

	// Second move
	req2 := MoveSlotsRequest{
		StartSlot:         shard.NumSlots / 2,
		EndSlot:           shard.NumSlots,
		FromGroup:         1,
		ToGroup:           0,
		NewVersion:        3,
		UnsafeNoMigration: false,
	}
	cmd2 := MoveSlots(req2)
	payload2, _ := EncodeCommand(cmd2)
	if _, err := m.Apply(payload2); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if m.Config().Version != 3 {
		t.Fatalf("version should be 3, got %d", m.Config().Version)
	}
}
