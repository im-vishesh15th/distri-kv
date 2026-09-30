package raft_test

// Phase 8: leader-aware client routing over the real gRPC transport —
// a client dialed at a FOLLOWER completes writes by following leader
// redirects (codes.Aborted = rejected before append = provably safe to
// retry), and after the leader dies the client repairs its routing while
// the ambiguous-class failure surfaces to the caller.

import (
	"context"
	"testing"
	"time"

	kv1 "distrikv/gen/kv/v1"
	"distrikv/pkg/client"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestClientRoutesWritesToLeader(t *testing.T) {
	nodes := startCluster(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := waitForLeadership(t, 10*time.Second, "initial leader", nodes)
	var follower *liveNode
	for _, n := range nodes {
		if n != leader {
			follower = n
			break
		}
	}

	// Dial a follower: routing must be invisible to the caller.
	c, err := client.Dial(ctx, follower.addr)
	if err != nil {
		t.Fatalf("dial follower: %v", err)
	}
	defer func() { _ = c.Close() }()

	st, err := c.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.NodeID != string(follower.id) {
		t.Fatalf("status answered by %q, want follower %q", st.NodeID, follower.id)
	}

	// Mutation against a follower: Aborted -> GetStatus -> redial leader ->
	// retry lands. If routing were broken this would surface ErrNotLeader.
	if err := c.Put(ctx, "route", []byte("v1")); err != nil {
		t.Fatalf("routed put: %v", err)
	}
	waitFor(t, 10*time.Second, "routed value replicated", func() bool {
		for _, n := range nodes {
			v, gerr := n.engine.Get("route")
			if gerr != nil || string(v) != "v1" {
				return false
			}
		}
		return true
	})
	if v, err := c.Get(ctx, "route"); err != nil || string(v) != "v1" {
		t.Fatalf("get after routed put: (%q, %v)", v, err)
	}

	// The safety class itself: a RAW mutation RPC on a follower must be
	// codes.Aborted (rejected BEFORE append) and must leave no trace on any
	// replica — that provable no-op is what licenses the SDK's write retry.
	rawConn, err := grpc.NewClient("passthrough:///"+follower.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("raw dial: %v", err)
	}
	defer func() { _ = rawConn.Close() }()
	if _, err := kv1.NewKVServiceClient(rawConn).Put(ctx, &kv1.PutRequest{
		Key: "ghost", Value: []byte("x"), ClientId: "probe", SequenceNumber: 1,
	}); status.Code(err) != codes.Aborted {
		t.Fatalf("raw put on follower: code %v err %v, want Aborted", status.Code(err), err)
	}
	assertFor(t, 500*time.Millisecond, "aborted write leaked into a replica", func() bool {
		for _, n := range nodes {
			if n.engine.Exists("ghost") {
				return false
			}
		}
		return true
	})

	// Kill the leader: the client's cached endpoint dies with it. Phase 9
	// turns Phase 8's surfaced Unavailable into a RETRIED write — the SDK
	// re-sends the same session sequence, so the outcome is at-most-once
	// (S1) whether or not the original attempt survived the old leader.
	leader.kill()
	survivors := make([]*liveNode, 0, 2)
	for _, n := range nodes {
		if n != leader {
			survivors = append(survivors, n)
		}
	}
	waitForLeadership(t, 10*time.Second, "new leader after kill", survivors)

	// One call — the SDK rides the failover out on its own (bounded
	// transient retries within ctx), and the INCR applies EXACTLY once.
	v, err := c.Incr(ctx, "failover-counter", 1)
	if err != nil {
		t.Fatalf("post-kill write should ride out failover: %v", err)
	}
	if v != 1 {
		t.Fatalf("failover incr = %d, want 1 (retry double-applied)", v)
	}
	// Every surviving replica converges on exactly one application: the
	// session table is replicated SM state, identical on all nodes. (The
	// killed leader is excluded — its engine is frozen at the moment of
	// death, as it must be.)
	waitFor(t, 10*time.Second, "exactly-once replication", func() bool {
		for _, n := range survivors {
			got, gerr := n.engine.Get("failover-counter")
			if gerr != nil || string(got) != "1" {
				return false
			}
		}
		return true
	})
	if v, err := c.Get(ctx, "failover-counter"); err != nil || string(v) != "1" {
		t.Fatalf("read after failover: (%q, %v), want 1", v, err)
	}
}
