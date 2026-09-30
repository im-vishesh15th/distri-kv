// Package shard maps keys to Raft groups via fixed slots (spec: fixed-slot
// sharding, not consistent hashing). The mapping is the first half of the
// routing chain — key → slot → shard → Raft group → leader — and is
// deterministic so every node in the cluster computes the same assignment.
//
// slot = hash(key) % NumSlots, with NumSlots = 16384 (the fixed slot
// space). The Config assigns contiguous, balanced slot ranges to Raft groups:
// with G groups, group g owns slots [g·NumSlots/G, (g+1)·NumSlots/G). The
// mapping is pure (no state, no I/O), so it is identical on every node and
// needs no coordination; making it durable and movable is the metadata
// Raft group's job (Phase 20+, optional Tier 3).
package shard

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"

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

// Range is one contiguous slot range owned by one group. Used to express an
// explicit (non-default) assignment in a Config.
type Range struct {
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
	Group uint64 `json:"group"`
}

// Config is a versioned shard map: the slot→group assignment that all nodes
// load from a static file. Versioned so a future metadata group can ship a
// new assignment and every node can tell which version it has. With no
// Ranges, the assignment is the default contiguous split by Groups.
type Config struct {
	Version uint64  `json:"version"`
	Groups  uint64  `json:"groups"`
	Ranges  []Range `json:"ranges,omitempty"`
}

// Map is the Phase 18 name for a Config (a computed shard map is just a
// versioned config with the default assignment).
type Map = Config

// NewMap returns a Config with the default contiguous split by `groups`.
// It panics on invalid input (groups == 0) — a constructor should reject an
// invalid config loudly.
func NewMap(groups uint64) *Config {
	c := &Config{Groups: groups}
	if err := c.validate(); err != nil {
		panic(err)
	}
	return c
}

// LoadConfig reads and validates a shard map from a JSON file. The file
// format is Config's JSON form, e.g.:
//
//	{"version": 1, "groups": 3}
//
// or with an explicit assignment:
//
//	{"version": 2, "groups": 3,
//	 "ranges": [{"start": 0, "end": 5461, "group": 0}, ...]}
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("shard: read config: %w", err)
	}
	var c Config
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("shard: parse config: %w", err)
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// validate checks the assignment is total (every slot owned), non-overlapping,
// and in range. The default (no Ranges) is always valid.
func (c *Config) validate() error {
	if c.Groups == 0 {
		return fmt.Errorf("shard: config has 0 groups")
	}
	if len(c.Ranges) == 0 {
		return nil // default contiguous split is always valid
	}
	// Sort-free coverage check: every slot must be owned exactly once.
	covered := make([]bool, NumSlots)
	for _, r := range c.Ranges {
		if r.Start >= r.End || r.End > NumSlots {
			return fmt.Errorf("shard: range [%d,%d) out of bounds", r.Start, r.End)
		}
		if r.Group >= c.Groups {
			return fmt.Errorf("shard: range [%d,%d) group %d out of range", r.Start, r.End, r.Group)
		}
		for s := r.Start; s < r.End; s++ {
			if covered[s] {
				return fmt.Errorf("shard: slot %d owned twice", s)
			}
			covered[s] = true
		}
	}
	for s := uint64(0); s < NumSlots; s++ {
		if !covered[s] {
			return fmt.Errorf("shard: slot %d not owned by any group", s)
		}
	}
	return nil
}

// GroupSlot returns the Raft group that owns slot: the explicit Ranges if
// set, else the contiguous split slot·groups/NumSlots. The result is always
// in [0, groups).
func (c *Config) GroupSlot(slot uint64) raft.GroupID {
	if slot >= NumSlots {
		panic("shard: slot out of range")
	}
	for _, r := range c.Ranges {
		if slot >= r.Start && slot < r.End {
			return raft.GroupID(r.Group)
		}
	}
	return raft.GroupID(slot * c.Groups / NumSlots)
}

// Group returns the Raft group that owns key.
func (c *Config) Group(key string) raft.GroupID {
	return c.GroupSlot(Slot(key))
}
