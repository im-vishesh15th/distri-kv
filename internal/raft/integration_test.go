package raft_test

// Phase 5's first fault tests, over the REAL gRPC transport and real time:
// a 3-node cluster elects, survives its leader being killed, re-elects, and
// absorbs the old leader back; a 5-node cluster whose majority is killed
// never elects from the remaining minority.

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"distrikv/internal/raft"
	"distrikv/internal/raftlog"
	"distrikv/internal/transport"
	grpctransport "distrikv/internal/transport/grpc"

	"google.golang.org/grpc"
)

const (
	tickPeriod     = 10 * time.Millisecond
	electionTicks  = 10 // randomized timeout: 100–190 ms real time
	heartbeatTicks = 3  // 30 ms heartbeats
)

type liveNode struct {
	id    transport.NodeID
	addr  string
	dir   string
	peers []transport.Peer

	lis    net.Listener
	srv    *grpc.Server
	tpt    *grpctransport.RealTransport
	rlog   *raftlog.Log
	rnode  *raft.Node
	cancel context.CancelFunc
	runErr chan error
	dead   bool
}

// debugLogger writes per-node Raft debug logs next to the log directory for
// diagnosing test failures.
func debugLogger(dir string, id transport.NodeID) *slog.Logger {
	f, err := os.Create(filepath.Join(dir, "raftdebug.log"))
	if err != nil {
		return nil
	}
	return slog.New(slog.NewJSONHandler(f, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// bootNode starts one full node (log + transport + raft + gRPC server +
// ticker). lis is pre-allocated by the cluster builder; pass nil to listen
// on addr itself (restart path).
func bootNode(t *testing.T, id transport.NodeID, addr, dir string, peers []transport.Peer, lis net.Listener) *liveNode {
	t.Helper()

	rlog, _, err := raftlog.Open(dir)
	if err != nil {
		t.Fatalf("%s: open raftlog: %v", id, err)
	}

	ids := make([]transport.NodeID, len(peers))
	for i, p := range peers {
		ids[i] = p.ID
	}
	tpt, err := grpctransport.New(id, peers, nil)
	if err != nil {
		t.Fatalf("%s: transport: %v", id, err)
	}

	rnode, err := raft.New(raft.Config{
		ID:             id,
		Peers:          ids,
		Transport:      tpt,
		Log:            rlog,
		ElectionTicks:  electionTicks,
		HeartbeatTicks: heartbeatTicks,
		// Distinct seed per boot: identical seeds across nodes would make
		// their randomized timeouts collide every round (livelock).
		RNG:    rand.New(rand.NewSource(time.Now().UnixNano() + int64(len(dir)))),
		Logger: debugLogger(dir, id),
	})
	if err != nil {
		t.Fatalf("%s: raft.New: %v", id, err)
	}
	tpt.SetHandler(rnode) // inbound Raft RPCs now reach the core

	if lis == nil {
		lis, err = net.Listen("tcp", addr)
		if err != nil {
			t.Fatalf("%s: re-listen %s: %v", id, addr, err)
		}
	}

	srv := grpc.NewServer()
	grpctransport.RegisterRaftService(srv, tpt)
	go func() { _ = srv.Serve(lis) }()

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() {
		err := rnode.Run(ctx)
		if err != nil {
			// A halted loop is a test-visible failure (disk error or logic
			// bug): print loudly rather than letting nodes silently vanish.
			fmt.Fprintf(os.Stderr, "RAFT_HALTED %s: %v\n", id, err)
		}
		runErr <- err
	}()
	raft.StartTicker(ctx, rnode, tickPeriod)

	return &liveNode{
		id: id, addr: lis.Addr().String(), dir: dir, peers: peers,
		lis: lis, srv: srv, tpt: tpt, rlog: rlog, rnode: rnode,
		cancel: cancel, runErr: runErr,
	}
}

// kill simulates a crash: loop stops, server drops (peers see Unavailable),
// outbound conns close, log closes. Idempotent.
func (ln *liveNode) kill() {
	if ln.dead {
		return
	}
	ln.dead = true
	ln.cancel()
	select {
	case <-ln.runErr:
	case <-time.After(2 * time.Second):
		panic(fmt.Sprintf("%s: raft Run did not exit", ln.id))
	}
	ln.srv.Stop()
	_ = ln.tpt.Close()
	if err := ln.rlog.Close(); err != nil {
		panic(fmt.Sprintf("%s: close log: %v", ln.id, err))
	}
}

// restart reopens the same directory and re-serves the same address — a
// crashed node coming back.
func restartNode(t *testing.T, ln *liveNode) {
	t.Helper()
	fresh := bootNode(t, ln.id, ln.addr, ln.dir, ln.peers, nil)
	*ln = *fresh
	ln.dead = false
}

// startCluster boots n nodes on loopback with static membership n1..nN.
func startCluster(t *testing.T, n int) []*liveNode {
	t.Helper()
	root := t.TempDir()

	lis := make([]net.Listener, n)
	peers := make([]transport.Peer, n)
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("pre-listen: %v", err)
		}
		lis[i] = l
		peers[i] = transport.Peer{ID: transport.NodeID(fmt.Sprintf("n%d", i+1)), Addr: l.Addr().String()}
	}

	nodes := make([]*liveNode, n)
	for i := 0; i < n; i++ {
		dir := fmt.Sprintf("%s/%s", root, peers[i].ID)
		nodes[i] = bootNode(t, peers[i].ID, peers[i].Addr, dir, peers, lis[i])
	}
	t.Cleanup(func() {
		for _, ln := range nodes {
			ln.kill()
		}
	})
	return nodes
}

func leadersOf(nodes []*liveNode) []*liveNode {
	var out []*liveNode
	for _, n := range nodes {
		if n.dead {
			continue
		}
		if n.rnode.Status().Role == raft.RoleLeader {
			out = append(out, n)
		}
	}
	return out
}

// waitForLeadership polls until exactly one node leads and every other live
// node acknowledges it.
func waitForLeadership(t *testing.T, d time.Duration, what string, nodes []*liveNode) *liveNode {
	t.Helper()
	var leader *liveNode
	waitFor(t, d, what, func() bool {
		ls := leadersOf(nodes)
		if len(ls) != 1 {
			return false
		}
		lead := ls[0]
		for _, n := range nodes {
			if n.dead || n == lead {
				continue
			}
			if s := n.rnode.Status(); s.LeaderID != lead.id {
				return false
			}
		}
		leader = lead
		return true
	})
	return leader
}

// TestThreeNodeElectsLeaderKillReelect covers the core liveness loop over
// real gRPC: elect → kill leader → re-elect among survivors → restart the
// old leader → the cluster converges on exactly one leader again.
func TestThreeNodeElectsLeaderKillReelect(t *testing.T) {
	nodes := startCluster(t, 3)

	// 1. Initial election.
	old := waitForLeadership(t, 10*time.Second, "initial leader", nodes)
	oldTerm := old.rnode.Status().Term
	// Let any transient duplicate claim settle before we start sampling
	// "exactly one leader" as a postcondition.
	assertFor(t, 300*time.Millisecond, "stable single leader before kill", func() bool {
		return len(leadersOf(nodes)) == 1
	})

	// 2. Kill the leader; the two survivors (a majority of 3) must elect.
	old.kill()
	survivors := make([]*liveNode, 0, 2)
	for _, n := range nodes {
		if n != old {
			survivors = append(survivors, n)
		}
	}
	waitFor(t, 10*time.Second, "re-election among survivors", func() bool {
		ls := leadersOf(survivors)
		if len(ls) != 1 {
			return false
		}
		return ls[0].rnode.Status().Term > oldTerm
	})

	// 3. Restart the killed node: it must rejoin without causing a split
	// brain — one leader, everyone else following it.
	restartNode(t, old)
	waitForLeadership(t, 10*time.Second, "converged cluster after restart", nodes)

	if s := old.rnode.Status(); s.Term < oldTerm {
		t.Fatalf("restarted node lost its persisted term: %+v", s)
	}
}

// TestReplicationSurvivesLeaderKill is the Phase 6 end-to-end: committed
// proposals survive the leader dying, a down follower catches up on restart,
// and the newly elected leader provably contains every committed entry (the
// election restriction doing its job).
func TestReplicationSurvivesLeaderKill(t *testing.T) {
	nodes := startCluster(t, 3)
	ctx := context.Background()

	leader := waitForLeadership(t, 10*time.Second, "initial leader", nodes)

	var followers []*liveNode
	for _, n := range nodes {
		if n != leader {
			followers = append(followers, n)
		}
	}

	// Eight sequential proposals: no-op occupies index 1, payloads v0..v7
	// land at 2..9. One follower dies after the first five — a majority of
	// 3 still holds with the remaining one.
	for i := 0; i < 5; i++ {
		idx, err := leader.rnode.Propose(ctx, []byte(fmt.Sprintf("v%d", i)))
		if err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
		if want := uint64(i + 2); idx != want {
			t.Fatalf("propose %d got index %d, want %d", i, idx, want)
		}
	}

	down := followers[0]
	down.kill()
	for i := 5; i < 8; i++ {
		if _, err := leader.rnode.Propose(ctx, []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("propose %d with follower down: %v", i, err)
		}
	}

	// Restart the dead follower: it must converge to the leader's log.
	restartNode(t, down)
	waitFor(t, 10*time.Second, "restarted follower caught up", func() bool {
		return down.rnode.Status().LastLogIndex >= 9
	})
	lst, flst := leader.rnode.Status().LastLogIndex, down.rnode.Status().LastLogIndex
	for i := uint64(1); i <= lst && i <= flst; i++ {
		le, lerr := leader.rlog.Get(i)
		fe, ferr := down.rlog.Get(i)
		if lerr != nil || ferr != nil || le.Term != fe.Term || string(le.Payload) != string(fe.Payload) {
			t.Fatalf("log divergence at %d: leader=%+v(%v) follower=%+v(%v)", i, le, lerr, fe, ferr)
		}
	}

	// Kill whoever currently leads (leadership may have churned during the
	// restart — a real possibility, since peers take ~200ms of grpc backoff
	// to reconnect while election timeouts are 100–190ms; churn is correct
	// Raft behavior and must not break our guarantees). The survivors must
	// then elect a strictly newer leader that has committed everything.
	cur := waitForLeadership(t, 10*time.Second, "stable leader before kill", nodes)
	prevTerm := cur.rnode.Status().Term
	cur.kill()
	survivors := make([]*liveNode, 0, 2)
	for _, n := range nodes {
		if n != cur {
			survivors = append(survivors, n)
		}
	}
	var newLeader *liveNode
	waitFor(t, 10*time.Second, "survivors elect a new leader", func() bool {
		ls := leadersOf(survivors)
		if len(ls) != 1 {
			return false
		}
		if s := ls[0].rnode.Status(); s.Term <= prevTerm || s.CommitIndex < 9 {
			return false
		}
		newLeader = ls[0]
		return true
	})

	// All eight committed payloads survive the leader change.
	for i := 0; i < 8; i++ {
		e, err := newLeader.rlog.Get(uint64(i + 2))
		if err != nil {
			t.Fatalf("committed entry %d missing on new leader: %v", i+2, err)
		}
		if want := fmt.Sprintf("v%d", i); string(e.Payload) != want {
			t.Fatalf("entry %d payload %q, want %q", i+2, e.Payload, want)
		}
	}
}

// TestFiveNodeMinorityCannotElect kills 3 of 5 nodes (including the leader).
// The 2 survivors are a minority and must never elect; restoring one node
// (a majority of 3) makes an election possible again.
func TestFiveNodeMinorityCannotElect(t *testing.T) {
	nodes := startCluster(t, 5)

	leader := waitForLeadership(t, 10*time.Second, "initial 5-node leader", nodes)
	assertFor(t, 300*time.Millisecond, "stable single leader before partition", func() bool {
		return len(leadersOf(nodes)) == 1
	})

	// Kill leader + 2 others → 3 down, 2 alive (minority of 5, majority 3).
	victims := []*liveNode{leader}
	for _, n := range nodes {
		if len(victims) == 3 {
			break
		}
		if n != leader {
			victims = append(victims, n)
		}
	}
	alive := make([]*liveNode, 0, 2)
	for _, n := range nodes {
		if n != victims[0] && n != victims[1] && n != victims[2] {
			alive = append(alive, n)
		}
	}
	for _, v := range victims {
		v.kill()
	}

	// Minority: repeated campaigns must never yield a leader. This window
	// spans ~6–10 full election timeouts, so a broken majority check would
	// surface quickly.
	assertFor(t, time.Second, "minority elected a leader", func() bool {
		return len(leadersOf(alive)) == 0
	})
	for _, n := range alive {
		if s := n.rnode.Status(); s.Role == raft.RoleLeader {
			t.Fatalf("%s leads from the minority: %+v", n.id, s)
		}
	}

	// Restore one node → 3 of 5 alive → majority election succeeds.
	restartNode(t, victims[1])
	waitForLeadership(t, 10*time.Second, "election after majority restored", nodes)
}
