package raft_test

// Phase 17 (Multi-Raft): a node hosts N independent Raft groups. These
// tests boot multi-group clusters via multiraft.Cluster and verify the
// per-group guarantees: independent elections, state isolation, correct
// routing (including misrouting), group-level and node-level faults,
// restart recovery, and per-group replication.

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	raftpb "distrikv/gen/raft/v1"
	"distrikv/internal/history"
	"distrikv/internal/kv"
	"distrikv/internal/lincheck"
	"distrikv/internal/multiraft"
	"distrikv/internal/raft"
	"distrikv/internal/server"
	"distrikv/internal/transport"
)

// mgBoot boots a 3-node, N-group cluster and registers cleanup.
func mgBoot(t *testing.T, groups int, seed int64) *multiraft.Cluster {
	t.Helper()
	c, err := multiraft.BootCluster(multiraft.ClusterConfig{
		Nodes:  3,
		Groups: groups,
		Seed:   seed,
		Dir:    t.TempDir(),
	})
	if err != nil {
		t.Fatalf("boot: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

// mgLeaders returns the hosts currently leading the given group.
func mgLeaders(c *multiraft.Cluster, gid raft.GroupID) []*multiraft.Host {
	var out []*multiraft.Host
	for _, h := range c.Hosts() {
		st, err := h.Status(gid)
		if err != nil {
			continue
		}
		if st.Role == raft.RoleLeader {
			out = append(out, h)
		}
	}
	return out
}

// mgWaitForLeader polls until exactly one host leads the group and every
// other LIVE host acknowledges it. Dead hosts (Role == "" after Run exits)
// are skipped — they cannot acknowledge, by definition.
func mgWaitForLeader(t *testing.T, c *multiraft.Cluster, gid raft.GroupID) *multiraft.Host {
	t.Helper()
	var leader *multiraft.Host
	waitFor(t, 10*time.Second, fmt.Sprintf("group %d leader", gid), func() bool {
		ls := mgLeaders(c, gid)
		if len(ls) != 1 {
			return false
		}
		for _, h := range c.Hosts() {
			if h == ls[0] {
				continue
			}
			st, err := h.Status(gid)
			if err != nil {
				return false
			}
			if st.Role == "" {
				continue // dead host — cannot acknowledge
			}
			if st.LeaderID != ls[0].ID() {
				return false
			}
		}
		leader = ls[0]
		return true
	})
	return leader
}

// mgPropose routes a write to the group's leader and blocks until applied.
func mgPropose(t *testing.T, c *multiraft.Cluster, gid raft.GroupID, payload []byte) {
	t.Helper()
	leader := mgWaitForLeader(t, c, gid)
	if _, _, err := leader.Propose(context.Background(), gid, payload); err != nil {
		t.Fatalf("propose to group %d: %v", gid, err)
	}
}

// mgStateHash hashes a group's applied state (replicas within the group
// must match, so hashing all of them is a strong equality check).
func mgStateHash(t *testing.T, c *multiraft.Cluster, gid raft.GroupID) string {
	t.Helper()
	h := sha256.New()
	for _, e := range c.Engines(gid) {
		b, err := e.Snapshot()
		if err != nil {
			t.Fatalf("engine snapshot: %v", err)
		}
		h.Write(b)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// mgEngine returns the engine of group gid on host i.
func mgEngine(c *multiraft.Cluster, i int, gid raft.GroupID) kv.Engine {
	return c.Engines(gid)[i]
}

// mgLeaderEngine returns the engine of the group's current leader (the
// replica Propose blocks on, so it definitely has the write).
func mgLeaderEngine(t *testing.T, c *multiraft.Cluster, gid raft.GroupID) kv.Engine {
	t.Helper()
	leader := mgWaitForLeader(t, c, gid)
	for i, h := range c.Hosts() {
		if h == leader {
			return c.Engines(gid)[i]
		}
	}
	t.Fatalf("group %d leader not found", gid)
	return nil
}

// mgWaitForApplied waits until every LIVE replica of the group has applied
// at least as far as the leader's commit index — so a state hash taken
// after this is over converged (not lagging) replicas.
func mgWaitForApplied(t *testing.T, c *multiraft.Cluster, gid raft.GroupID) {
	t.Helper()
	leader := mgWaitForLeader(t, c, gid)
	leaderSt, _ := leader.Status(gid)
	target := leaderSt.CommitIndex
	waitFor(t, 10*time.Second, fmt.Sprintf("group %d applied %d", gid, target), func() bool {
		for i := range c.Hosts() {
			st, err := c.Host(i).Status(gid)
			if err != nil {
				return false
			}
			if st.Role == "" {
				continue // dead host — cannot apply
			}
			if st.LastApplied < target {
				return false
			}
		}
		return true
	})
}

// TestMultiGroupElection: N groups on 3 nodes each elect a leader
// independently (separate elections, separate terms).
func TestMultiGroupElection(t *testing.T) {
	c := mgBoot(t, 2, 1701)
	for gid := raft.GroupID(0); gid < 2; gid++ {
		if leader := mgWaitForLeader(t, c, gid); leader == nil {
			t.Fatalf("group %d elected no leader", gid)
		}
	}
}

// TestMultiGroupIndependence: a write to group 0 never appears in group 1,
// and the two groups' state hashes stay separate.
func TestMultiGroupIndependence(t *testing.T) {
	c := mgBoot(t, 2, 1702)
	mgPropose(t, c, 0, mustEncode(t, kv.Set("only0", []byte("x"))))

	if got, err := mgLeaderEngine(t, c, 0).Get("only0"); err != nil || string(got) != "x" {
		t.Fatalf("group 0 missing its write: (%q, %v)", got, err)
	}
	if _, err := mgEngine(c, 0, 1).Get("only0"); !errors.Is(err, kv.ErrKeyNotFound) {
		t.Fatalf("group 1 has group 0's write: %v", err)
	}
	if mgStateHash(t, c, 0) == mgStateHash(t, c, 1) {
		t.Fatal("group 0 and group 1 state hashes must differ")
	}
}

// TestMultiGroupMisrouting: a message addressed to group A never changes
// group B's state, and an unknown group ID returns an error (never a
// panic, never a state change).
func TestMultiGroupMisrouting(t *testing.T) {
	c := mgBoot(t, 2, 1703)
	mgPropose(t, c, 0, mustEncode(t, kv.Set("a", []byte("1"))))

	// A group-0 message never changed group 1.
	if _, err := mgEngine(c, 0, 1).Get("a"); !errors.Is(err, kv.ErrKeyNotFound) {
		t.Fatal("group 1 changed by a group-0 message")
	}

	// An unknown group ID returns an error on every entry point.
	h := c.Host(0)
	if _, _, err := h.Propose(context.Background(), 99, []byte("x")); err == nil {
		t.Fatal("propose to unknown group 99: expected error")
	}
	if _, err := h.Status(99); err == nil {
		t.Fatal("status of unknown group 99: expected error")
	}
	if _, err := h.HandleAppendEntries(context.Background(),
		&raftpb.AppendEntriesRequest{Term: 1, GroupId: 99}); err == nil {
		t.Fatal("append to unknown group 99: expected error")
	}
	// The rejected message changed nothing.
	if got, err := mgLeaderEngine(t, c, 0).Get("a"); err != nil || string(got) != "1" {
		t.Fatalf("group 0 disturbed by an unknown-group message: (%q, %v)", got, err)
	}
}

// TestMultiGroupGroupLevelFault: isolate group 0's leader (group-scoped
// fault). Group 0 re-elects; group 1 keeps serving with no leader or term
// change.
func TestMultiGroupGroupLevelFault(t *testing.T) {
	c := mgBoot(t, 2, 1704)
	leader0 := mgWaitForLeader(t, c, 0)
	leader1 := mgWaitForLeader(t, c, 1)
	term1 := uint64(0)
	if st, err := leader1.Status(1); err == nil {
		term1 = st.Term
	}

	// Isolate group 0 on its leader only.
	term0 := uint64(0)
	if st, err := leader0.Status(0); err == nil {
		term0 = st.Term
	}
	c.Network().IsolateGroup(leader0.ID(), 0)

	// Group 0 re-elects at a higher term among the other two hosts (the
	// isolated stale leader still reports Role=Leader — it cannot hear the
	// higher term — so wait for a non-stale leader with a higher term).
	waitFor(t, 10*time.Second, "group 0 re-elects after isolation", func() bool {
		for _, l := range mgLeaders(c, 0) {
			if l == leader0 {
				continue // stale isolated leader
			}
			if st, err := l.Status(0); err == nil && st.Term > term0 {
				return true
			}
		}
		return false
	})

	// Group 1 keeps serving, same leader, same term.
	mgPropose(t, c, 1, mustEncode(t, kv.Set("still", []byte("here"))))
	st, err := leader1.Status(1)
	if err != nil {
		t.Fatalf("group 1 status: %v", err)
	}
	if st.Role != raft.RoleLeader || st.Term != term1 {
		t.Fatalf("group 1 changed under a group-0 fault: role=%v term=%d want leader term=%d",
			st.Role, st.Term, term1)
	}
}

// TestMultiGroupNodeLevelFault: crash a whole node. Every group it hosted
// re-elects independently; no acknowledged write is lost.
func TestMultiGroupNodeLevelFault(t *testing.T) {
	c := mgBoot(t, 2, 1705)
	mgPropose(t, c, 0, mustEncode(t, kv.Set("n0", []byte("v0"))))
	mgPropose(t, c, 1, mustEncode(t, kv.Set("n1", []byte("v1"))))

	// Crash node 0 entirely (all its groups).
	c.CrashNode(0)

	// Both groups re-elect independently among the survivors.
	for gid := raft.GroupID(0); gid < 2; gid++ {
		waitFor(t, 10*time.Second, fmt.Sprintf("group %d re-elects", gid), func() bool {
			return len(mgLeaders(c, gid)) == 1
		})
	}
	// Wait for the new leaders to apply the acknowledged writes (the log
	// has them; the engine catches up after the no-op commits).
	mgWaitForApplied(t, c, 0)
	mgWaitForApplied(t, c, 1)
	// The acknowledged writes survive on the live replicas.
	if got, err := mgLeaderEngine(t, c, 0).Get("n0"); err != nil || string(got) != "v0" {
		t.Fatalf("group 0 lost an acknowledged write: (%q, %v)", got, err)
	}
	if got, err := mgLeaderEngine(t, c, 1).Get("n1"); err != nil || string(got) != "v1" {
		t.Fatalf("group 1 lost an acknowledged write: (%q, %v)", got, err)
	}
}

// TestMultiGroupRestart: stop the whole cluster, restart it, and confirm
// each group recovers from its own data/g<id>/ directory with matching
// state hashes.
func TestMultiGroupRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")

	c1, err := multiraft.BootCluster(multiraft.ClusterConfig{
		Nodes: 3, Groups: 2, Seed: 1706, Dir: dir,
	})
	if err != nil {
		t.Fatalf("boot 1: %v", err)
	}
	mgPropose(t, c1, 0, mustEncode(t, kv.Set("r0", []byte("v0"))))
	mgPropose(t, c1, 1, mustEncode(t, kv.Set("r1", []byte("v1"))))
	mgWaitForApplied(t, c1, 0)
	mgWaitForApplied(t, c1, 1)
	h0 := mgStateHash(t, c1, 0)
	h1 := mgStateHash(t, c1, 1)
	c1.Close()

	// Restart from the same directories: each group recovers
	// independently.
	c2, err := multiraft.BootCluster(multiraft.ClusterConfig{
		Nodes: 3, Groups: 2, Seed: 1706, Dir: dir,
	})
	if err != nil {
		t.Fatalf("boot 2: %v", err)
	}
	defer c2.Close()

	// Recovery (log replay) is asynchronous in the Run loop — wait for
	// every replica of both groups to have applied at least the no-op.
	waitFor(t, 10*time.Second, "recovered groups replayed their logs", func() bool {
		for i := range c2.Hosts() {
			for gid := raft.GroupID(0); gid < 2; gid++ {
				if st, err := c2.Host(i).Status(gid); err != nil || st.LastApplied < 1 {
					return false
				}
			}
		}
		return true
	})

	if got := mgStateHash(t, c2, 0); got != h0 {
		t.Fatalf("group 0 state hash mismatch after restart: got %s want %s", got, h0)
	}
	if got := mgStateHash(t, c2, 1); got != h1 {
		t.Fatalf("group 1 state hash mismatch after restart: got %s want %s", got, h1)
	}
}

// TestMultiGroupReplication: each group commits independently, and logs
// are identical across replicas within a group.
func TestMultiGroupReplication(t *testing.T) {
	c := mgBoot(t, 2, 1707)
	mgPropose(t, c, 0, mustEncode(t, kv.Set("r0", []byte("v0"))))
	mgPropose(t, c, 1, mustEncode(t, kv.Set("r1", []byte("v1"))))

	for gid := raft.GroupID(0); gid < 2; gid++ {
		logs := c.Logs(gid)
		if len(logs) != 3 {
			t.Fatalf("group %d: %d logs, want 3", gid, len(logs))
		}
		// Compare every replica's log against the first, entry by entry.
		l0 := logs[0]
		for i := uint64(1); ; i++ {
			e0, err0 := l0.Get(i)
			if err0 != nil {
				// End of the first replica's log: every replica must be done.
				for r := 1; r < len(logs); r++ {
					if _, errR := logs[r].Get(i); errR == nil {
						t.Fatalf("group %d log divergence at %d: replica 0 ended but replica %d has it", gid, i, r)
					}
				}
				break
			}
			for r := 1; r < len(logs); r++ {
				er, errR := logs[r].Get(i)
				if errR != nil {
					t.Fatalf("group %d log divergence at %d: replica %d ended early", gid, i, r)
				}
				if e0.Term != er.Term || string(e0.Payload) != string(er.Payload) {
					t.Fatalf("group %d log divergence at %d: replica %d (%+v vs %+v)", gid, i, r, e0, er)
				}
			}
		}
	}
}

// TestMultiGroupLinearizabilityUnderFault: run a Phase 16 style
// linearizability workload on group 0 while group 1 is under a
// group-level partition. Group 0's history must be linearizable.
func TestMultiGroupLinearizabilityUnderFault(t *testing.T) {
	c := mgBoot(t, 2, 1708)

	// Wrap group 0's leader in a KV service (group 0 has no faults during
	// the run, so its leader is stable).
	leader := mgWaitForLeader(t, c, 0)
	leaderIdx := -1
	for i, h := range c.Hosts() {
		if h == leader {
			leaderIdx = i
			break
		}
	}
	if leaderIdx < 0 {
		t.Fatal("group 0 leader not found")
	}
	svc := server.NewService(c.Engines(0)[leaderIdx], c.Group(leaderIdx, 0), c.Group(leaderIdx, 0).Status, nil, nil)

	rec := history.NewRecorder()
	var wg sync.WaitGroup
	for cl := 0; cl < 4; cl++ {
		wg.Add(1)
		go func(cl int) {
			defer wg.Done()
			crng := rand.New(rand.NewSource(1708 + int64(cl)))
			runClient(rec, func() *server.Service { return svc }, fmt.Sprintf("client%d", cl), genOps(crng, 15))
		}(cl)
	}

	// While the clients work on group 0, partition group 1 (group-level
	// fault): {n1,n2} | {n3}. Group 1's leader (if on n3) re-elects; group 0
	// is untouched.
	c.Network().PartitionGroup(1,
		[]transport.NodeID{"n1", "n2"}, []transport.NodeID{"n3"})

	wg.Wait()
	c.Network().Heal()

	// Group 0's history must be linearizable.
	if _, err := lincheck.Check(rec.Ops()); err != nil {
		failed := 0
		for _, op := range rec.Ops() {
			if !op.Success {
				failed++
			}
		}
		t.Fatalf("group 0 history not linearizable under a group-1 fault (%d failed ops): %v", failed, err)
	}
}
