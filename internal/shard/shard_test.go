package shard

import (
	"fmt"
	"os"
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

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := t.TempDir() + "/shard.json"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestLoadConfigValid(t *testing.T) {
	path := writeConfig(t, `{"version": 1, "groups": 3}`)
	c, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if c.Version != 1 || c.Groups != 3 {
		t.Fatalf("got version=%d groups=%d, want 1/3", c.Version, c.Groups)
	}
	// Default contiguous split (slot·groups/NumSlots): group 0 owns
	// [0, 5461], group 1 [5462, 10922], group 2 [10923, 16383].
	if g := c.GroupSlot(0); g != 0 {
		t.Fatalf("GroupSlot(0) = %d, want 0", g)
	}
	if g := c.GroupSlot(5461); g != 0 {
		t.Fatalf("GroupSlot(5461) = %d, want 0", g)
	}
	if g := c.GroupSlot(5462); g != 1 {
		t.Fatalf("GroupSlot(5462) = %d, want 1", g)
	}
}

func TestLoadConfigExplicitRanges(t *testing.T) {
	body := `{"version": 2, "groups": 2, "ranges": [
		{"start": 0, "end": 100, "group": 1},
		{"start": 100, "end": 16384, "group": 0}
	]}`
	c, err := LoadConfig(writeConfig(t, body))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if g := c.GroupSlot(50); g != 1 {
		t.Fatalf("GroupSlot(50) = %d, want 1 (explicit range)", g)
	}
	if g := c.GroupSlot(100); g != 0 {
		t.Fatalf("GroupSlot(100) = %d, want 0 (explicit range)", g)
	}
}

func TestLoadConfigInvalid(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"zero groups", `{"version": 1, "groups": 0}`},
		{"overlapping ranges", `{"groups": 2, "ranges": [
			{"start": 0, "end": 100, "group": 0},
			{"start": 50, "end": 200, "group": 1}
		]}`},
		{"uncovered slot", `{"groups": 2, "ranges": [
			{"start": 0, "end": 100, "group": 0}
		]}`},
		{"group out of range", `{"groups": 2, "ranges": [
			{"start": 0, "end": 16384, "group": 5}
		]}`},
		{"range out of bounds", `{"groups": 2, "ranges": [
			{"start": 0, "end": 20000, "group": 0}
		]}`},
		{"bad json", `{"version": 1,`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := LoadConfig(writeConfig(t, tc.body)); err == nil {
				t.Fatalf("LoadConfig(%s) should fail", tc.name)
			}
		})
	}
	if _, err := LoadConfig(t.TempDir() + "/missing.json"); err == nil {
		t.Fatal("LoadConfig of a missing file should fail")
	}
}

func TestNewMapDefaultContiguous(t *testing.T) {
	m := NewMap(4)
	for s := uint64(0); s < NumSlots; s++ {
		if m.GroupSlot(s) != m.GroupSlot(s) {
			t.Fatal("not deterministic")
		}
	}
	// Contiguous: group 0 owns [0, 4096), group 1 [4096, 8192), etc.
	for g := uint64(0); g < 4; g++ {
		lo := g * NumSlots / 4
		if got := m.GroupSlot(lo); uint64(got) != g {
			t.Fatalf("GroupSlot(%d) = %d, want %d", lo, got, g)
		}
	}
}
