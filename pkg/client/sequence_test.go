package client

import (
	"fmt"
	"testing"

	"distrikv/internal/raft"
	"distrikv/internal/shard"
)

// newTestClient builds a Client without dialing (no server needed): the
// sequence-counter logic under test is transport-free.
func newTestClient() *Client {
	return &Client{seqs: make(map[raft.GroupID]uint64)}
}

// TestGroupForRoutesKeys verifies the client routes a key to exactly the
// group the loaded shard map says (the client and cluster must agree, or the
// per-group counters mismatch).
func TestGroupForRoutesKeys(t *testing.T) {
	m := shard.NewMap(3)
	c := newTestClient()
	c.SetShardMap(m)
	for _, k := range []string{"a", "foo", "bar", "tenant:x", "key-0", "key-1"} {
		if got, want := c.groupFor(k), m.Group(k); got != want {
			t.Fatalf("groupFor(%q) = %d, want %d", k, got, want)
		}
	}
}

// TestNoShardMapFallsBackToGroup0: without a shard map every key uses the
// group-0 counter — the single-group behavior, unchanged.
func TestNoShardMapFallsBackToGroup0(t *testing.T) {
	c := newTestClient()
	for _, k := range []string{"a", "foo", "anything"} {
		if got := c.groupFor(k); got != 0 {
			t.Fatalf("groupFor(%q) without shard map = %d, want 0", k, got)
		}
	}
}

// findKeysInGroups returns one key per group for the first `want` groups the
// map covers (scanning keys until each group has a representative).
func findKeysInGroups(t *testing.T, m *shard.Config, want int) map[raft.GroupID]string {
	t.Helper()
	keys := make(map[raft.GroupID]string)
	for i := 0; i < 200000 && len(keys) < want; i++ {
		k := fmt.Sprintf("key-%d", i)
		if _, ok := keys[m.Group(k)]; !ok {
			keys[m.Group(k)] = k
		}
	}
	if len(keys) != want {
		t.Fatalf("could not find keys in %d distinct groups (got %d)", want, len(keys))
	}
	return keys
}

// seqOf is test-only inspection of the current counter for a group WITHOUT
// incrementing it.
func (c *Client) seqOf(gid raft.GroupID) uint64 {
	c.seqMu.Lock()
	defer c.seqMu.Unlock()
	return c.seqs[gid]
}

// TestPerGroupSequenceCounters is the Phase 19 property: two keys that route
// to different groups draw from INDEPENDENT per-group counters. Each group's
// sequence starts at 1 and grows by one per write to THAT group. With a
// single global counter, group B's third write would carry sequence 6 — a
// number group B has never seen, which is harmless only while writes to B
// stay in order, but the moment requests to different groups interleave in
// flight the per-group monotonicity argument breaks. Per-group counters make
// each group's sequence numbers strictly 1,2,3,... in issue order.
func TestPerGroupSequenceCounters(t *testing.T) {
	m := shard.NewMap(2)
	c := newTestClient()
	c.SetShardMap(m)

	// Two keys, one per distinct group the map covers.
	ka, ga, kb, gb := "", raft.GroupID(0), "", raft.GroupID(0)
	for g, k := range findKeysInGroups(t, m, 2) {
		if ka == "" {
			ka, ga = k, g
		} else {
			kb, gb = k, g
		}
	}
	if ga == gb {
		t.Fatalf("test keys must map to distinct groups, got %d and %d", ga, gb)
	}

	// Interleave writes across the two groups: A B A B A B, exactly the way
	// a client issuing concurrent mutations to different keys would.
	for n := 0; n < 6; n++ {
		g := gb
		if n%2 == 0 {
			g = ga
		}
		want := c.seqOf(g) + 1
		if got := c.nextSeq(g); got != want {
			t.Fatalf("group %d write #%d: nextSeq = %d, want %d", g, want, got, want)
		}
	}
	_ = ka
	_ = kb
	// After 3 writes to each group, BOTH counters are at 3 — independent.
	// A single global counter would instead hand one group sequence 4..6
	// while the other sat at 3, i.e. sequences that are not 1,2,3,... per
	// group — exactly the false-stale-sequence hazard Phase 19 removes.
	if a, b := c.seqOf(ga), c.seqOf(gb); a != 3 || b != 3 {
		t.Fatalf("per-group counters not independent: group %d=%d group %d=%d, want 3 and 3", ga, a, gb, b)
	}
}

// TestPerGroupSequencesStartAtOne: the first write to a previously unused
// group carries sequence 1 (session sequences start at 1, per the server
// contract), regardless of what other groups have done.
func TestPerGroupSequencesStartAtOne(t *testing.T) {
	m := shard.NewMap(4)
	c := newTestClient()
	c.SetShardMap(m)
	_ = findKeysInGroups(t, m, 4) // sanity: the map covers all four groups

	// Drive group 0 high, then touch the other groups for the first time.
	for i := 0; i < 5; i++ {
		c.nextSeq(0)
	}
	for g := raft.GroupID(1); g <= 3; g++ {
		if got := c.nextSeq(g); got != 1 {
			t.Fatalf("first write to group %d: sequence %d, want 1 (independent of other groups)", g, got)
		}
	}
	if c.seqOf(0) != 5 {
		t.Fatalf("group 0 counter = %d, want 5", c.seqOf(0))
	}
}
