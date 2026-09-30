package raft_test

// Phase 11: linearizable reads (spec §14). "Reads go to the leader" is
// not a claim — these tests pin the safety condition: a non-leader
// REFUSES a data read (Aborted, never a value), the leader's ReadIndex
// guarantees applied >= read point before returning, and a read racing
// a leader kill still sees every acknowledged write (the window where a
// naive engine-direct read goes stale).

import (
	"context"
	"errors"
	"testing"
	"time"

	kv1 "distrikv/gen/kv/v1"
	"distrikv/internal/raft"
	"distrikv/pkg/client"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// rawKV dials a node with a bare stub — for assertions the SDK would
// paper over (it redirects instead of surfacing Aborted).
func rawKV(t *testing.T, addr string) kv1.KVServiceClient {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///"+addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("raw dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return kv1.NewKVServiceClient(conn)
}

// TestFollowerRefusesDataReads: the safety condition itself. A follower
// holding the value must still refuse to serve it — answering from a
// replica that may lag an acknowledged write is exactly the stale-read
// window spec §14 forbids. Aborted (not a value, not NotFound) is the
// proof: the SDK turns it into a redirect to the leader.
func TestFollowerRefusesDataReads(t *testing.T) {
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

	// Write through the SDK (routes to the leader) and wait for it to
	// replicate — the follower below HAS the value, and still must not
	// serve it: the danger is the next write it hasn't seen.
	c, err := client.Dial(ctx, leader.addr)
	if err != nil {
		t.Fatalf("dial leader: %v", err)
	}
	defer func() { _ = c.Close() }()
	if err := c.Put(ctx, "k", []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	waitFor(t, 10*time.Second, "value on all replicas", func() bool {
		for _, n := range nodes {
			v, gerr := n.engine.Get("k")
			if gerr != nil || string(v) != "v1" {
				return false
			}
		}
		return true
	})

	// Raw read on the leader: passes the ReadIndex barrier, returns the value.
	leaderReads := rawKV(t, leader.addr)
	resp, err := leaderReads.Get(ctx, &kv1.GetRequest{Key: "k"})
	if err != nil {
		t.Fatalf("leader read: %v", err)
	}
	if string(resp.Value) != "v1" {
		t.Fatalf("leader read = %q, want v1", resp.Value)
	}

	// Raw read on the follower: must be Aborted — never a value.
	followerReads := rawKV(t, follower.addr)
	_, ferr := followerReads.Get(ctx, &kv1.GetRequest{Key: "k"})
	if status.Code(ferr) != codes.Aborted {
		t.Fatalf("follower read code = %v (%v), want Aborted (non-leaders must refuse data reads)",
			status.Code(ferr), ferr)
	}
	// Same for Exists — it is a data read too.
	if _, eerr := followerReads.Exists(ctx, &kv1.ExistsRequest{Key: "k"}); status.Code(eerr) != codes.Aborted {
		t.Fatalf("follower exists code = %v (%v), want Aborted", status.Code(eerr), eerr)
	}

	// And the SDK, dialed AT the follower, still reads successfully —
	// by redirect, not by the follower answering.
	fc, err := client.Dial(ctx, follower.addr)
	if err != nil {
		t.Fatalf("dial follower: %v", err)
	}
	defer func() { _ = fc.Close() }()
	if v, gerr := fc.Get(ctx, "k"); gerr != nil || string(v) != "v1" {
		t.Fatalf("sdk read via follower: (%q, %v), want (v1, nil)", v, gerr)
	}
}

// TestReadAfterFailoverSeesAcknowledgedWrite: an acknowledged write,
// then an immediate kill of its leader, then a read on the survivor that
// took over — it must see the write. The new leader has the entry in
// its LOG (leader completeness), but before its own term's no-op
// commits it may not have applied it; ReadIndex's gates (current-term
// commit + applied >= read point) are what close that window. A naive
// engine-direct read can return nothing here.
func TestReadAfterFailoverSeesAcknowledgedWrite(t *testing.T) {
	nodes := startCluster(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := waitForLeadership(t, 10*time.Second, "initial leader", nodes)

	c, err := client.Dial(ctx, leader.addr)
	if err != nil {
		t.Fatalf("dial leader: %v", err)
	}
	defer func() { _ = c.Close() }()
	if err := c.Put(ctx, "survivor", []byte("acked")); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Kill the acknowledging leader; the entry is on a majority, so it
	// is committed — and a read MUST see it.
	leader.kill()
	survivors := make([]*liveNode, 0, 2)
	for _, n := range nodes {
		if n != leader {
			survivors = append(survivors, n)
		}
	}
	newLeader := waitForLeadership(t, 10*time.Second, "new leader", survivors)

	r, err := client.Dial(ctx, newLeader.addr)
	if err != nil {
		t.Fatalf("dial new leader: %v", err)
	}
	defer func() { _ = r.Close() }()

	v, gerr := r.Get(ctx, "survivor")
	if gerr != nil {
		t.Fatalf("read after failover: %v", gerr)
	}
	if string(v) != "acked" {
		t.Fatalf("read after failover = %q, want acked (stale: apply not waited for)", v)
	}
}

// TestReadIndexContract pins the ReadIndex contract at the Raft layer:
// when it returns, lastApplied has already reached the returned index
// (gate 3), and a non-leader refuses outright (ErrNotLeader — the code
// the SDK redirects on).
func TestReadIndexContract(t *testing.T) {
	nodes := startCluster(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := waitForLeadership(t, 10*time.Second, "initial leader", nodes)

	idx, err := leader.rnode.ReadIndex(ctx)
	if err != nil {
		t.Fatalf("leader ReadIndex: %v", err)
	}
	st := leader.rnode.Status()
	if st.LastApplied < idx {
		t.Fatalf("ReadIndex returned %d but lastApplied = %d (read point not applied)", idx, st.LastApplied)
	}
	if st.CommitIndex < idx {
		t.Fatalf("ReadIndex returned %d but commitIndex = %d (read point past commit)", idx, st.CommitIndex)
	}

	for _, n := range nodes {
		if n == leader {
			continue
		}
		if _, err := n.rnode.ReadIndex(ctx); !errors.Is(err, raft.ErrNotLeader) {
			t.Fatalf("follower ReadIndex err = %v, want ErrNotLeader", err)
		}
		break // one follower is enough — the rule is per-role, not per-node
	}
}
