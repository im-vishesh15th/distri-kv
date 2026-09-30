package raft_test

// Phase 10 through a REAL 3-node cluster (gRPC transport, three separate
// event loops and logs): independent sessions dialed at DIFFERENT
// endpoints race INCRs through leader routing, and every replica must
// land on the exact count. This crosses Level 1 (many clients) with
// Level 2 (one event loop's total order) while Phase 8 routing and
// Phase 9 sessions are in play — a single lost or double-applied update
// anywhere in client -> routing -> Raft -> SM shows as a wrong total.

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"distrikv/pkg/client"
)

func TestConcurrentSessionsThroughCluster(t *testing.T) {
	nodes := startCluster(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	waitForLeadership(t, 10*time.Second, "initial leader", nodes)

	// One session per endpoint — the leader itself and both followers —
	// all mutating concurrently.
	clients := make([]*client.Client, 0, len(nodes))
	for _, n := range nodes {
		c, err := client.Dial(ctx, n.addr)
		if err != nil {
			t.Fatalf("dial %s: %v", n.id, err)
		}
		clients = append(clients, c)
	}
	t.Cleanup(func() {
		for _, c := range clients {
			_ = c.Close()
		}
	})

	const perClient = 25
	var wg sync.WaitGroup
	for _, c := range clients {
		wg.Add(1)
		go func(c *client.Client) {
			defer wg.Done()
			for i := 0; i < perClient; i++ {
				if _, err := c.IncrBy(ctx, "cluster-counter"); err != nil {
					t.Errorf("Incr: %v", err)
					return
				}
			}
		}(c)
	}
	wg.Wait()

	// Exact total on EVERY replica — the deterministic convergence
	// assertion used throughout: applied >= expected is not enough here,
	// a double-apply would overshoot, so equality is the whole test.
	want := strconv.Itoa(len(clients) * perClient)
	waitFor(t, 10*time.Second, "exact count on all replicas", func() bool {
		for _, n := range nodes {
			v, err := n.engine.Get("cluster-counter")
			if err != nil || string(v) != want {
				return false
			}
		}
		return true
	})
	if v, err := clients[0].Get(ctx, "cluster-counter"); err != nil || string(v) != want {
		t.Fatalf("sdk read: (%s, %v), want %s", v, err, want)
	}
}
