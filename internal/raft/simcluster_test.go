package raft_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"distrikv/internal/kv"
	"distrikv/internal/raft"
	"distrikv/internal/raftlog"
	"distrikv/internal/transport"
	"distrikv/internal/transport/sim"
)

// Phase 14: the same Raft core over the SimulatedTransport — election,
// replication, partition, and heal with every RPC routed through the
// deterministic in-process network (spec §18). No gRPC, no sockets, no
// kernel in the loop; the transport seam is the only thing that changed
// versus the integration harness.

type simNode struct {
	// mu guards the fields below across kill/restart: restartSimNode
	// replaces the raft loop, log, and engine while client goroutines may
	// be reading them. Readers take a view() snapshot (never hold the
	// lock across a blocking call like Status()).
	mu     sync.RWMutex
	id     transport.NodeID
	rnode  *raft.Node
	rlog   *raftlog.Log
	engine kv.Engine
	dir    string             // durable directory: survives kill/restart
	ids    []transport.NodeID // cluster membership, for reboot
	cancel context.CancelFunc
	runErr chan error
	dead   bool
}

// view returns a snapshot of the mutable harness fields.
func (sn *simNode) view() (dead bool, rnode *raft.Node, rlog *raftlog.Log, engine kv.Engine, cancel context.CancelFunc, runErr chan error) {
	sn.mu.RLock()
	defer sn.mu.RUnlock()
	return sn.dead, sn.rnode, sn.rlog, sn.engine, sn.cancel, sn.runErr
}

// markDead sets the dead flag (killSim, disk-failure halt).
func (sn *simNode) markDead() {
	sn.mu.Lock()
	sn.dead = true
	sn.mu.Unlock()
}

// bootComponents builds the durable + raft pieces for one node: open the
// log, build the engine + Raft core, register with the network, start the
// loop and ticker. Shared by bootSimNode and restartSimNode.
func bootComponents(t *testing.T, network *sim.Network, id transport.NodeID, dir string, ids []transport.NodeID) (*raft.Node, *raftlog.Log, kv.Engine, context.CancelFunc, chan error) {
	t.Helper()

	rlog, _, err := raftlog.Open(dir)
	if err != nil {
		t.Fatalf("%s: open raftlog: %v", id, err)
	}
	engine := kv.NewMemEngine()
	tpt := sim.NewTransport(network, id)
	rnode, err := raft.New(raft.Config{
		ID:             id,
		Peers:          ids,
		Transport:      tpt,
		Log:            rlog,
		ElectionTicks:  electionTicks,
		HeartbeatTicks: heartbeatTicks,
		// Distinct seed per boot: identical seeds would make the
		// randomized election timeouts collide every round. The directory
		// length differentiates reboot seeds from the originals'.
		RNG:          rand.New(rand.NewSource(time.Now().UnixNano() + int64(len(dir)))),
		StateMachine: kv.NewSM(engine),
	})
	if err != nil {
		t.Fatalf("%s: raft.New: %v", id, err)
	}
	network.SetHandler(id, rnode) // inbound RPCs now reach the core

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() {
		err := rnode.Run(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "SIM_RAFT_HALTED %s: %v\n", id, err)
		}
		runErr <- err
	}()
	raft.StartTicker(ctx, rnode, tickPeriod)

	return rnode, rlog, engine, cancel, runErr
}

// bootSimNode builds one node — log, engine, sim transport, Raft core —
// registers it with the shared network, and starts its loop + ticker.
// Shared by startSimCluster and restartSimNode (a reboot is "new process,
// same directory").
func bootSimNode(t *testing.T, network *sim.Network, id transport.NodeID, dir string, ids []transport.NodeID) *simNode {
	t.Helper()
	rnode, rlog, engine, cancel, runErr := bootComponents(t, network, id, dir, ids)
	return &simNode{
		id: id, rnode: rnode, rlog: rlog, engine: engine,
		dir: dir, ids: ids, cancel: cancel, runErr: runErr,
	}
}

// killSim crashes the node like a process death: inbound RPCs fail the
// way a refused connection does (handler detached), the loop stops, the
// log closes. Idempotent.
func (sn *simNode) killSim(t *testing.T, network *sim.Network) {
	if sn.dead {
		return
	}
	sn.markDead()
	network.SetHandler(sn.id, nil)
	sn.cancel()
	select {
	case <-sn.runErr:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: sim raft Run did not exit after kill", sn.id)
	}
	if err := sn.rlog.Close(); err != nil {
		t.Fatalf("%s: close log: %v", sn.id, err)
	}
}

// restartSimNode reboots the same directory as a fresh process would: a
// brand-new empty engine (recovery = replay of the recovered log), a new
// Raft core, the same sim identity re-registered. The struct fields are
// swapped under the lock so concurrent readers never see a torn state.
func restartSimNode(t *testing.T, network *sim.Network, sn *simNode) {
	t.Helper()
	rnode, rlog, engine, cancel, runErr := bootComponents(t, network, sn.id, sn.dir, sn.ids)
	sn.mu.Lock()
	sn.rnode, sn.rlog, sn.engine = rnode, rlog, engine
	sn.cancel, sn.runErr, sn.dead = cancel, runErr, false
	sn.mu.Unlock()
}

// startSimCluster boots n nodes whose RPCs all cross one shared
// simulated network. The baseline is fault-free (delay 0, no drops):
// tests inject faults and partitions explicitly, so every disturbance in
// a test body is deliberate.
func startSimCluster(t *testing.T, n int, seed int64) ([]*simNode, *sim.Network) {
	return startSimClusterFaults(t, n, seed, sim.Faults{})
}

// startSimClusterFaults is startSimCluster with probabilistic faults
// active from birth — the Phase 15 chaos tests prove elections and
// commits survive seeded delay/loss/duplication end to end.
func startSimClusterFaults(t *testing.T, n int, seed int64, faults sim.Faults) ([]*simNode, *sim.Network) {
	t.Helper()
	network := sim.New(sim.Config{Seed: seed, Faults: faults, Auto: true})

	ids := make([]transport.NodeID, n)
	for i := range ids {
		ids[i] = transport.NodeID(fmt.Sprintf("n%d", i+1))
	}

	nodes := make([]*simNode, n)
	for i := 0; i < n; i++ {
		nodes[i] = bootSimNode(t, network, ids[i], t.TempDir(), ids)
	}
	t.Cleanup(func() {
		for _, sn := range nodes {
			sn.shutdown(t)
		}
		network.Close()
	})
	return nodes, network
}

// shutdown stops the node's loop, waits for Run to exit, and closes the
// log. Idempotent (teardown may overlap a failed test's early return).
func (sn *simNode) shutdown(t *testing.T) {
	dead, _, rlog, _, cancel, runErr := sn.view()
	if dead {
		return
	}
	sn.markDead()
	cancel()
	select {
	case <-runErr:
	case <-time.After(2 * time.Second):
		t.Errorf("%s: sim raft Run did not exit", sn.id)
	}
	if err := rlog.Close(); err != nil {
		t.Errorf("%s: close log: %v", sn.id, err)
	}
}

func simLeadersOf(nodes []*simNode) []*simNode {
	var out []*simNode
	for _, n := range nodes {
		dead, rnode, _, _, _, _ := n.view()
		if dead {
			continue
		}
		if rnode.Status().Role == raft.RoleLeader {
			out = append(out, n)
		}
	}
	return out
}

// waitForSimLeadership polls until exactly one node leads and every other
// live node acknowledges it (same contract as waitForLeadership, over
// simNode instead of liveNode).
func waitForSimLeadership(t *testing.T, d time.Duration, what string, nodes []*simNode) *simNode {
	t.Helper()
	var leader *simNode
	waitFor(t, d, what, func() bool {
		ls := simLeadersOf(nodes)
		if len(ls) != 1 {
			return false
		}
		lead := ls[0]
		for _, n := range nodes {
			dead, rnode, _, _, _, _ := n.view()
			if dead || n == lead {
				continue
			}
			if s := rnode.Status(); s.LeaderID != lead.id {
				return false
			}
		}
		leader = lead
		return true
	})
	return leader
}

// TestRaftOverSimulatedNetwork proves the seam end to end: the identical
// Raft core elects a leader, replicates, keeps committing with a follower
// partitioned away (majority of 3 is 2), and after heal the isolated
// replica catches up with byte-identical logs — every RPC having crossed
// the deterministic in-process network, none gRPC.
func TestRaftOverSimulatedNetwork(t *testing.T) {
	nodes, network := startSimCluster(t, 3, 20260930)
	ctx := context.Background()

	// 1. Election over the sim network.
	leader := waitForSimLeadership(t, 10*time.Second, "leader over sim network", nodes)

	// 2. Replication: no-op holds index 1, k0..k2 land at 2..4. Propose
	// blocks until applied, so the leader is at 4 when these return.
	for i := 0; i < 3; i++ {
		idx, _, err := leader.rnode.Propose(ctx, mustEncode(t, kv.Set(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i)))))
		if err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
		if want := uint64(i + 2); idx != want {
			t.Fatalf("propose %d index = %d, want %d", i, idx, want)
		}
	}
	waitFor(t, 10*time.Second, "all replicas applied first batch", func() bool {
		hw := leader.rnode.Status().LastApplied
		for _, n := range nodes {
			if n.rnode.Status().LastApplied < hw {
				return false
			}
		}
		return true
	})

	// 3. Isolate one follower. The majority side (leader + the other
	// follower) must keep committing; the isolated node must not see
	// entries 5..7 — those never existed before the partition closed, and
	// every leader→isolated path is blocked from here.
	var isolated *simNode
	for _, n := range nodes {
		if n != leader {
			isolated = n
			break
		}
	}
	network.Isolate(isolated.id)
	for i := 3; i < 6; i++ {
		if _, _, err := leader.rnode.Propose(ctx, mustEncode(t, kv.Set(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i))))); err != nil {
			t.Fatalf("propose %d with follower isolated: %v", i, err)
		}
	}
	if got := isolated.rnode.Status().LastLogIndex; got >= 5 {
		t.Fatalf("isolated follower at index %d: writes committed in its absence crossed the partition", got)
	}

	// 4. Heal: the isolated node catches up, all logs are byte-identical,
	// and applied state converges to the final high-water.
	network.Heal()
	leader = waitForSimLeadership(t, 10*time.Second, "stable leader after heal", nodes)
	waitFor(t, 10*time.Second, "isolated follower caught up after heal", func() bool {
		return isolated.rnode.Status().LastLogIndex >= leader.rnode.Status().LastLogIndex
	})

	lst := leader.rnode.Status().LastLogIndex
	for i := uint64(1); i <= lst; i++ {
		le, lerr := leader.rlog.Get(i)
		if lerr != nil {
			t.Fatalf("leader log get %d: %v", i, lerr)
		}
		for _, n := range nodes {
			if n == leader {
				continue
			}
			ne, nerr := n.rlog.Get(i)
			if nerr != nil || le.Term != ne.Term || string(le.Payload) != string(ne.Payload) {
				t.Fatalf("log divergence at %d: leader=%+v %s=%+v(%v)", i, le, n.id, ne, nerr)
			}
		}
	}
	waitFor(t, 10*time.Second, "final applied convergence", func() bool {
		hw := leader.rnode.Status().LastApplied
		for _, n := range nodes {
			if n.rnode.Status().LastApplied < hw {
				return false
			}
		}
		return true
	})
}
