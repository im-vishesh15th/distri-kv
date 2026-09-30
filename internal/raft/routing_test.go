package raft_test

// Phase 8: leader-aware client routing over the real gRPC transport —
// a client dialed at a FOLLOWER completes writes by following leader
// redirects (codes.Aborted = rejected before append = provably safe to
// retry), and after the leader dies the client repairs its routing while
// the ambiguous-class failure surfaces to the caller.

import (
	"context"
	"errors"
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

	// Kill the leader: the client's cached endpoint dies with it. The first
	// write afterwards is the AMBIGUOUS class — it must surface (no
	// auto-retry pre-Phase 9) while routing is repaired for the next call.
	leader.kill()
	survivors := make([]*liveNode, 0, 2)
	for _, n := range nodes {
		if n != leader {
			survivors = append(survivors, n)
		}
	}
	waitForLeadership(t, 10*time.Second, "new leader after kill", survivors)

	perr := c.Put(ctx, "after", []byte("kill"))
	if perr == nil {
		t.Fatal("write to the dead leader endpoint unexpectedly succeeded")
	}
	if code := status.Code(perr); code != codes.Unavailable {
		t.Fatalf("post-kill write code = %v (%v), want Unavailable (ambiguous class)", code, perr)
	}

	// Every failure while the cluster settles is ErrNotLeader (Aborted ->
	// nothing applied), so re-attempting is safe by construction; routing
	// has been repaired to a live endpoint and follows the new leader.
	waitFor(t, 10*time.Second, "routed write after failover", func() bool {
		err := c.Put(ctx, "after", []byte("kill"))
		if err == nil {
			return true
		}
		if errors.Is(err, client.ErrNotLeader) {
			return false // election/heartbeat settling — safe to re-attempt
		}
		t.Fatalf("post-failover write: %v", err)
		return false
	})
	if v, err := c.Get(ctx, "after"); err != nil || string(v) != "kill" {
		t.Fatalf("read after failover: (%q, %v)", v, err)
	}
}
