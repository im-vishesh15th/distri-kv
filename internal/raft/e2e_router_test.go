package raft_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	kv1 "distrikv/gen/kv/v1"
	"distrikv/internal/kv"
	"distrikv/internal/multiraft"
	"distrikv/internal/raft"
	"distrikv/internal/server"
	"distrikv/internal/shard"
	"distrikv/internal/transport"
	"distrikv/pkg/client"
	"google.golang.org/grpc"
)

// TestE2EMultiGroupRouter is the Phase 20 end-to-end test:
// 3 physical nodes × 2 Raft groups, real gRPC listeners, real GroupedService,
// real client SDK with shard map. Verifies per-group routing, leader discovery,
// and no false ErrStaleSequence under interleaved writes.
func TestE2EMultiGroupRouter(t *testing.T) {
	// Use a deterministic seed for reproducibility.
	seed := int64(2001)

	// 1) Boot the sim-based 3-node, 2-group cluster.
	c, err := multiraft.BootCluster(multiraft.ClusterConfig{
		Nodes:  3,
		Groups: 2,
		Seed:   seed,
		Dir:    t.TempDir(),
	})
	if err != nil {
		t.Fatalf("boot: %v", err)
	}
	defer c.Close()

	// 2) Load the shard config (2 groups, default contiguous split).
	m := shard.NewMap(2)

	// 3) Create real gRPC listeners first to get their addresses.
	lis := make([]net.Listener, 3)
	grpcAddrs := make([]string, 3)
	for i := 0; i < 3; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("node %d: listen: %v", i, err)
		}
		lis[i] = l
		grpcAddrs[i] = l.Addr().String()
	}

	// leaderAddrs maps sim transport NodeID ("n1", "n2", "n3") to real gRPC address.
	// This is used by GroupedService.GetStatus to provide leader hints.
	leaderAddrs := map[transport.NodeID]string{
		"n1": grpcAddrs[0],
		"n2": grpcAddrs[1],
		"n3": grpcAddrs[2],
	}

	// 4) For each node, create GroupedService and start gRPC server.
	grpcServers := make([]*grpc.Server, 3)
	clients := make([]*client.Client, 3)

	for i := 0; i < 3; i++ {
		// Build per-group engines from the cluster.
		engines := make(map[raft.GroupID]kv.Engine)
		for gid := raft.GroupID(0); gid < 2; gid++ {
			engines[gid] = c.Engines(gid)[i]
		}

		host := c.Host(i)
		svc := server.NewGroupedService(
			m,
			host,
			host.Status,
			leaderAddrs,
			nil, // no logger
			engines,
		)

		grpcServers[i] = grpc.NewServer()
		kv1.RegisterKVServiceServer(grpcServers[i], svc)

		// Start the gRPC server.
		go func(idx int) {
			if err := grpcServers[idx].Serve(lis[idx]); err != nil {
				// Server stopped is expected on test cleanup.
			}
		}(i)

		// Create a client dialing this node.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cli, err := client.Dial(ctx, grpcAddrs[i])
		cancel()
		if err != nil {
			t.Fatalf("node %d: client dial: %v", i, err)
		}
		// Load the shard map into the client.
		cli.SetShardMap(m)
		clients[i] = cli
	}

	// 5) Wait for leaders to be elected in both groups.
	for gid := raft.GroupID(0); gid < 2; gid++ {
		if leader := mgWaitForLeader(t, c, gid); leader == nil {
			t.Fatalf("group %d elected no leader", gid)
		}
	}

	// 6) Verify keys span both groups and land in exactly the right group's engine.
	keys := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "user:1", "user:2"}
	for _, k := range keys {
		// Use client 0 for all writes (it will discover per-group leaders).
		if err := clients[0].Put(context.Background(), k, []byte("v-"+k)); err != nil {
			t.Fatalf("Put %q: %v", k, err)
		}
	}

	// Let both groups converge.
	mgWaitForApplied(t, c, 0)
	mgWaitForApplied(t, c, 1)

	// Verify each key is in its group and NOT in the other group.
	for _, k := range keys {
		gid := m.Group(k)
		other := (gid + 1) % 2

		if got, err := mgLeaderEngine(t, c, gid).Get(k); err != nil || string(got) != "v-"+k {
			t.Fatalf("key %q not in group %d: (%q, %v)", k, gid, got, err)
		}
		if _, err := mgLeaderEngine(t, c, other).Get(k); err == nil {
			t.Fatalf("key %q leaked into group %d", k, other)
		}
	}

	// 7) Per-group leader discovery: verify the client correctly discovers
	//    each group's leader and only updates that group's routing.

	// Find a key in group 1.
	var group1Key string
	for _, k := range keys {
		if m.Group(k) == 1 {
			group1Key = k
			break
		}
	}
	if group1Key == "" {
		t.Fatal("no key found in group 1")
	}

	// Get group 1's leader.
	leader1 := mgWaitForLeader(t, c, 1)
	leader1Idx := -1
	for i, h := range c.Hosts() {
		if h == leader1 {
			leader1Idx = i
			break
		}
	}
	if leader1Idx < 0 {
		t.Fatal("group 1 leader not found")
	}

	// Get group 0's leader.
	leader0 := mgWaitForLeader(t, c, 0)
	leader0Idx := -1
	for i, h := range c.Hosts() {
		if h == leader0 {
			leader0Idx = i
			break
		}
	}
	if leader0Idx < 0 {
		t.Fatal("group 0 leader not found")
	}

	t.Logf("Group 0 leader: node %d, Group 1 leader: node %d", leader0Idx, leader1Idx)

	// 8) Test per-group leader discovery.
	//    Client 0 connects to node 0. Write to a group-1 key should redirect
	//    to group 1's leader. The client should update only group 1's endpoint.

	newGroup1Key := "group1-new-key"
	if m.Group(newGroup1Key) != 1 {
		for i := 1000; i < 2000; i++ {
			k := fmt.Sprintf("testkey%d", i)
			if m.Group(k) == 1 {
				newGroup1Key = k
				break
			}
		}
	}

	// This write should work and the client should discover group 1's leader.
	if err := clients[0].Put(context.Background(), newGroup1Key, []byte("value")); err != nil {
		t.Fatalf("Put to group 1 via node 0: %v", err)
	}

	// Verify the key landed in group 1.
	if got, err := mgLeaderEngine(t, c, 1).Get(newGroup1Key); err != nil || string(got) != "value" {
		t.Fatalf("group 1 missing write: (%q, %v)", got, err)
	}

	// 9) Interleaved writes across groups must not cause false ErrStaleSequence.
	for i := 0; i < 10; i++ {
		key0 := findKeyInGroup(t, m, 0)
		key1 := findKeyInGroup(t, m, 1)

		if err := clients[0].Put(context.Background(), key0, []byte(fmt.Sprintf("v0-%d", i))); err != nil {
			t.Fatalf("interleaved Put group 0: %v", err)
		}
		if err := clients[0].Put(context.Background(), key1, []byte(fmt.Sprintf("v1-%d", i))); err != nil {
			t.Fatalf("interleaved Put group 1: %v", err)
		}
	}

	// Final verification.
	mgWaitForApplied(t, c, 0)
	mgWaitForApplied(t, c, 1)
	for _, k := range keys {
		gid := m.Group(k)
		if got, err := mgLeaderEngine(t, c, gid).Get(k); err != nil || string(got) != "v-"+k {
			t.Fatalf("final check key %q in group %d: (%q, %v)", k, gid, got, err)
		}
	}

	t.Log("E2E multi-group router test passed")
}

// findKeyInGroup finds a key that hashes to the given group.
func findKeyInGroup(t *testing.T, m *shard.Config, wantGroup raft.GroupID) string {
	t.Helper()
	for i := 0; i < 10000; i++ {
		k := fmt.Sprintf("key-%d", i)
		if m.Group(k) == wantGroup {
			return k
		}
	}
	t.Fatalf("could not find key for group %d", wantGroup)
	return ""
}
