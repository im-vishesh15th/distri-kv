package raft_test

// Phase 18 (fixed-slot sharding): the key→group mapping end to end. Keys are
// routed to Raft groups via shard.Slot + shard.Map over a multiraft.Cluster,
// and each key must land in — and only in — its own group's state.

import (
	"errors"
	"fmt"
	"testing"

	"distrikv/internal/kv"
	"distrikv/internal/shard"
)

// TestShardRoutingIntegration routes a spread of keys to their groups over
// a 3-node, 2-group cluster, writes each key to its group, and verifies the
// key is present in its group's engine and absent from the other group's.
func TestShardRoutingIntegration(t *testing.T) {
	c := mgBoot(t, 2, 1801)
	m := shard.NewMap(2)

	keys := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "user:1", "user:2"}
	for _, k := range keys {
		mgPropose(t, c, m.Group(k), mustEncode(t, kv.Set(k, []byte("v-"+k))))
	}
	// Let both groups converge.
	mgWaitForApplied(t, c, 0)
	mgWaitForApplied(t, c, 1)

	for _, k := range keys {
		gid := m.Group(k)
		other := (gid + 1) % 2
		if got, err := mgLeaderEngine(t, c, gid).Get(k); err != nil || string(got) != "v-"+k {
			t.Fatalf("key %q not in group %d: (%q, %v)", k, gid, got, err)
		}
		if _, err := mgLeaderEngine(t, c, other).Get(k); !errors.Is(err, kv.ErrKeyNotFound) {
			t.Fatalf("key %q leaked into group %d: %v", k, other, err)
		}
	}
}

// TestShardRoutingIsDeterministic verifies the mapping is a pure function:
// the same key always routes to the same group, and the assignment is
// balanced across the groups.
func TestShardRoutingIsDeterministic(t *testing.T) {
	m := shard.NewMap(3)
	keys := make([]string, 200)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%d", i)
	}
	// Same key → same group, every time.
	for _, k := range keys {
		if m.Group(k) != m.Group(k) {
			t.Fatalf("Group(%q) is not deterministic", k)
		}
	}
	// Balanced: each of the 3 groups gets a roughly equal share.
	counts := make(map[uint64]int)
	for _, k := range keys {
		counts[uint64(m.Group(k))]++
	}
	for g := uint64(0); g < 3; g++ {
		if counts[g] < 40 || counts[g] > 100 {
			t.Fatalf("group %d got %d of 200 keys, want roughly balanced", g, counts[g])
		}
	}
}

// TestShardSlotSpace verifies the slot function covers the full fixed slot
// space and that the shard map's group assignment is total (every slot is
// owned).
func TestShardSlotSpace(t *testing.T) {
	m := shard.NewMap(4)
	seen := make(map[uint64]bool)
	for s := uint64(0); s < shard.NumSlots; s++ {
		g := m.GroupSlot(s)
		if uint64(g) >= 4 {
			t.Fatalf("GroupSlot(%d) = %d, out of range", s, g)
		}
		seen[uint64(g)] = true
	}
	for g := uint64(0); g < 4; g++ {
		if !seen[g] {
			t.Fatalf("group %d owns no slots", g)
		}
	}
}
