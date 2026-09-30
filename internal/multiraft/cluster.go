package multiraft

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math/rand"
	"time"

	"distrikv/internal/kv"
	"distrikv/internal/raft"
	"distrikv/internal/raftlog"
	"distrikv/internal/transport"
	"distrikv/internal/transport/sim"
	"os"
)

// Timing defaults for booted groups (mirrors the single-group test
// harness: 10ms ticks, 100–190ms election timeout, 30ms heartbeats).
const (
	defaultElectionTicks  = 10
	defaultHeartbeatTicks = 3
	defaultTickPeriod     = 10 // ms
)

// ClusterConfig configures a multi-group test cluster.
type ClusterConfig struct {
	Nodes  int        // number of hosts (nodes)
	Groups int        // number of Raft groups per host
	Seed   int64      // base seed for the sim network
	Faults sim.Faults // birth faults (delay/drop/duplicate)
	Dir    string     // root data dir; group g's files live under Dir/g<g>/

	// ElectionTicks / HeartbeatTicks / TickPeriod override the defaults.
	ElectionTicks  int
	HeartbeatTicks int
	TickPeriod     int // ms
}

// Cluster manages a set of hosts (nodes), each serving the same set of
// Raft groups. Group g on all hosts forms one Raft group (they replicate
// with each other). This is the test-facing multi-group harness.
type Cluster struct {
	network *sim.Network
	hosts   []*Host
	// groups maps groupID -> the raft.Group on each host (index = host index).
	groups map[raft.GroupID][]*raft.Group
	// engines maps groupID -> the kv.Engine on each host (for state reads).
	engines map[raft.GroupID][]kv.Engine
	// logs maps groupID -> the raftlog.Log on each host (for log comparison).
	logs map[raft.GroupID][]*raftlog.Log
	// cancels[host] stops every group's event loop on that host (CrashNode,
	// Close).
	cancels [][]context.CancelFunc
}

// groupRNGSeed derives a distinct election-RNG seed per (node, group) so
// groups on the same node do not campaign in lockstep.
func groupRNGSeed(nodeID transport.NodeID, gid raft.GroupID) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(nodeID))
	_ = binary.Write(h, binary.LittleEndian, uint64(gid))
	return int64(h.Sum64())
}

// BootCluster creates the sim network, the hosts, and the groups, and
// starts every group's event loop + ticker. Group g's log and snapshots
// live under Dir/g<g>/ — no files are shared between groups.
func BootCluster(cfg ClusterConfig) (*Cluster, error) {
	electTO, heartTO, tickPeriod := cfg.ElectionTicks, cfg.HeartbeatTicks, cfg.TickPeriod
	if electTO == 0 {
		electTO = defaultElectionTicks
	}
	if heartTO == 0 {
		heartTO = defaultHeartbeatTicks
	}
	if tickPeriod == 0 {
		tickPeriod = defaultTickPeriod
	}

	network := sim.New(sim.Config{Seed: cfg.Seed, Faults: cfg.Faults, Auto: true})

	ids := make([]transport.NodeID, cfg.Nodes)
	for i := range ids {
		ids[i] = transport.NodeID(fmt.Sprintf("n%d", i+1))
	}

	c := &Cluster{
		network: network,
		hosts:   make([]*Host, cfg.Nodes),
		groups:  make(map[raft.GroupID][]*raft.Group, cfg.Groups),
		engines: make(map[raft.GroupID][]kv.Engine, cfg.Groups),
		logs:    make(map[raft.GroupID][]*raftlog.Log, cfg.Groups),
		cancels: make([][]context.CancelFunc, cfg.Nodes),
	}

	for i := 0; i < cfg.Nodes; i++ {
		host := NewHost(ids[i])
		tpt := sim.NewTransport(network, ids[i])
		network.SetHandler(ids[i], host) // inbound RPCs demux to the host

		for g := 0; g < cfg.Groups; g++ {
			gid := raft.GroupID(g)
			// Per-(group, node) directory: data/g<id>/<node>/. The group
			// dir is the top-level scope (no files shared between groups);
			// within a group, each node's replica gets its own subdirectory
			// so replicas never overwrite each other's log or hard state.
			dir := fmt.Sprintf("%s/g%d/%s", cfg.Dir, g, ids[i])
			rlog, _, err := raftlog.Open(dir)
			if err != nil {
				return nil, fmt.Errorf("node %s group %d: open raftlog: %w", ids[i], gid, err)
			}
			engine := kv.NewMemEngine()
			group, err := raft.New(raft.Config{
				ID:             ids[i],
				GroupID:        gid,
				Peers:          ids,
				Transport:      tpt,
				Log:            rlog,
				ElectionTicks:  electTO,
				HeartbeatTicks: heartTO,
				RNG:            rand.New(rand.NewSource(groupRNGSeed(ids[i], gid))),
				StateMachine:   kv.NewSM(engine),
			})
			if err != nil {
				return nil, fmt.Errorf("node %s group %d: raft.New: %w", ids[i], gid, err)
			}
			host.AddGroup(group)
			c.groups[gid] = append(c.groups[gid], group)
			c.engines[gid] = append(c.engines[gid], engine)
			c.logs[gid] = append(c.logs[gid], rlog)
		}
		c.hosts[i] = host
	}

	// Start every group's event loop + ticker.
	for i := 0; i < cfg.Nodes; i++ {
		for g := 0; g < cfg.Groups; g++ {
			group := c.groups[raft.GroupID(g)][i]
			ctx, cancel := context.WithCancel(context.Background())
			c.cancels[i] = append(c.cancels[i], cancel)
			go func(g *raft.Group, ctx context.Context) {
				if err := g.Run(ctx); err != nil {
					fmt.Fprintf(os.Stderr, "MULTIRAFT_HALTED %s: %v\n", g.ID(), err)
				}
			}(group, ctx)
			raft.StartTicker(ctx, group, time.Duration(tickPeriod)*time.Millisecond)
		}
	}
	return c, nil
}

// Network is the shared simulated network (for fault injection).
func (c *Cluster) Network() *sim.Network { return c.network }

// Hosts returns the cluster's hosts (index = node index).
func (c *Cluster) Hosts() []*Host { return c.hosts }

// Host returns the i-th host.
func (c *Cluster) Host(i int) *Host { return c.hosts[i] }

// Groups returns the replicas of one Raft group (index = host index).
func (c *Cluster) Groups(gid raft.GroupID) []*raft.Group {
	return c.groups[gid]
}

// Group returns the replica of gid on the i-th host.
func (c *Cluster) Group(i int, gid raft.GroupID) *raft.Group {
	return c.groups[gid][i]
}

// Engines returns the state machines' engines of one Raft group (index =
// host index) — for reading applied state in tests.
func (c *Cluster) Engines(gid raft.GroupID) []kv.Engine {
	return c.engines[gid]
}

// Logs returns the Raft logs of one group (index = host index) — for
// comparing log contents across replicas in tests.
func (c *Cluster) Logs(gid raft.GroupID) []*raftlog.Log {
	return c.logs[gid]
}

// CrashNode stops every group's event loop on one host (a whole-node
// crash). The groups it hosted re-elect independently on the survivors.
func (c *Cluster) CrashNode(i int) {
	for _, cancel := range c.cancels[i] {
		cancel()
	}
}

// Close stops every group's event loop, then the network.
func (c *Cluster) Close() {
	for _, hostCancels := range c.cancels {
		for _, cancel := range hostCancels {
			cancel()
		}
	}
	c.network.Close()
}
