// Package shard maps keys to Raft groups via fixed slots (spec: fixed-slot
// sharding, not consistent hashing). The mapping is the first half of the
// routing chain — key → slot → shard → Raft group → leader — and is
// deterministic so every node in the cluster computes the same assignment.
//
// slot = hash(key) % NumSlots, with NumSlots = 16384 (the fixed slot
// space). The Map assigns contiguous, balanced slot ranges to Raft groups:
// with G groups, group g owns slots [g·NumSlots/G, (g+1)·NumSlots/G). The
// mapping is pure (no state, no I/O), so it is identical on every node and
// needs no coordination; making it durable and movable is the metadata
// Raft group's job (Phase 20+, optional Tier 3).
package shard

import (
	"hash/fnv"

	"distrikv/internal/raft"
)

// NumSlots is the fixed slot space (spec: 16384 slots).
const NumSlots = 16384

// Slot returns the slot for a key: a deterministic hash (FNV-1a) mod
// NumSlots. Every node computes the same slot for the same key — that is
// what makes the shard map consistent cluster-wide without coordination.
func Slot(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64() % NumSlots
}

// Map assigns slots to Raft groups in contiguous, balanced ranges. With G
// groups, group g owns slots [g·NumSlots/G, (g+1)·NumSlots/G): every group
// gets NumSlots/G or NumSlots/G+1 slots, every slot is owned by exactly one
// group, and each group's slots are contiguous (so a future metadata group
// can move a whole range at once).
type Map struct {
	groups uint64
}

// NewMap builds a shard map for the given number of Raft groups. groups
// must be at least 1.
func NewMap(groups uint64) *Map {
	if groups == 0 {
		panic("shard: NewMap requires at least one group")
	}
	return &Map{groups: groups}
}

// Groups returns the number of Raft groups in the map.
func (m *Map) Groups() uint64 { return m.groups }

// GroupSlot returns the Raft group that owns slot: contiguous ranges, so
// group = slot·groups/NumSlots. The result is always in [0, groups).
func (m *Map) GroupSlot(slot uint64) raft.GroupID {
	if slot >= NumSlots {
		panic("shard: slot out of range")
	}
	return raft.GroupID(slot * m.groups / NumSlots)
}

// Group returns the Raft group that owns key.
func (m *Map) Group(key string) raft.GroupID {
	return m.GroupSlot(Slot(key))
}
