package shard

import (
	"fmt"
	"testing"

	"distrikv/internal/raft"
)

func TestSlotDeterministic(t *testing.T) {
	if Slot("foo") != Slot("foo") {
		t.Fatal("Slot must be deterministic: same key, same slot")
	}
}

func TestSlotInRange(t *testing.T) {
	for _, k := range []string{"a", "b", "foo", "bar", "hello", "world", "key1", "key2", "tenant:x"} {
		if s := Slot(k); s >= NumSlots {
			t.Fatalf("Slot(%q) = %d, want < %d", k, s, NumSlots)
		}
	}
}

func TestSlotDistribution(t *testing.T) {
	// Distinct keys should spread across the slot space (no clustering that
	// would unbalance the shard map).
	seen := make(map[uint64]bool)
	for i := 0; i < 2000; i++ {
		seen[Slot(fmt.Sprintf("key%d", i))] = true
	}
	if len(seen) < 1800 {
		t.Fatalf("Slot distribution too concentrated: %d distinct slots for 2000 keys", len(seen))
	}
}

func TestMapSingleGroup(t *testing.T) {
	m := NewMap(1)
	for _, k := range []string{"a", "b", "foo", "anything"} {
		if g := m.Group(k); g != 0 {
			t.Fatalf("1-group map: Group(%q) = %d, want 0", k, g)
		}
	}
}

func TestMapBalanced(t *testing.T) {
	for _, groups := range []uint64{1, 2, 3, 4, 8, 16} {
		m := NewMap(groups)
		counts := make([]uint64, groups)
		for s := uint64(0); s < NumSlots; s++ {
			g := m.GroupSlot(s)
			if g >= raft.GroupID(groups) {
				t.Fatalf("groups=%d: GroupSlot(%d) = %d, out of range", groups, s, g)
			}
			counts[g]++
		}
		// Balanced: each group gets NumSlots/groups or NumSlots/groups+1.
		base := NumSlots / groups
		for g, c := range counts {
			if c != base && c != base+1 {
				t.Fatalf("groups=%d: group %d has %d slots, want %d or %d", groups, g, c, base, base+1)
			}
		}
	}
}

func TestMapFullCoverage(t *testing.T) {
	for _, groups := range []uint64{1, 2, 3, 4, 8, 16} {
		m := NewMap(groups)
		seen := make([]bool, groups)
		for s := uint64(0); s < NumSlots; s++ {
			seen[m.GroupSlot(s)] = true
		}
		for g := range seen {
			if !seen[g] {
				t.Fatalf("groups=%d: group %d owns no slots", groups, g)
			}
		}
	}
}

func TestMapContiguous(t *testing.T) {
	// Each group's slots form one contiguous range (so a metadata group can
	// move a whole range atomically later).
	m := NewMap(4)
	var prev raft.GroupID
	for s := uint64(0); s < NumSlots; s++ {
		g := m.GroupSlot(s)
		if s > 0 && g != prev && g != prev+1 {
			t.Fatalf("non-contiguous: slot %d → group %d after group %d", s, g, prev)
		}
		prev = g
	}
}

func TestGroupConsistent(t *testing.T) {
	m := NewMap(3)
	for _, k := range []string{"a", "b", "foo", "bar", "baz"} {
		if m.Group(k) != m.GroupSlot(Slot(k)) {
			t.Fatalf("Group(%q) != GroupSlot(Slot(%q))", k, k)
		}
	}
}

func TestNewMapPanicsOnZero(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewMap(0) must panic")
		}
	}()
	NewMap(0)
}
