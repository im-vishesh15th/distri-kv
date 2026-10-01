package multiraft

import (
	"context"
	"testing"
	"time"

	"distrikv/internal/kv"
	"distrikv/internal/raft"
	"distrikv/internal/raftlog"
	"distrikv/internal/transport"
	"distrikv/internal/transport/sim"
)

// BenchmarkSingleGroupPropose measures single-group Raft propose throughput
// over the simulated network (no network latency).
func BenchmarkSingleGroupPropose(b *testing.B) {
	network := sim.New(sim.Config{Seed: 42, Auto: true, Faults: sim.Faults{}})
	defer network.Close()

	hosts := make([]*Host, 3)
	ids := []transport.NodeID{"n1", "n2", "n3"}
	for i := range ids {
		host := NewHost(ids[i])
		tpt := sim.NewTransport(network, ids[i])
		network.SetHandler(ids[i], host)

		dir := b.TempDir() + "/n" + string(rune('1'+i))
		rlog, _, _ := raftlog.Open(dir)
		engine := kv.NewMemEngine()
		g, _ := raft.New(raft.Config{
			ID:             ids[i],
			GroupID:        0,
			Peers:          ids,
			Transport:      tpt,
			Log:            rlog,
			ElectionTicks:  10,
			HeartbeatTicks: 3,
			StateMachine:   kv.NewSM(engine),
		})
		host.AddGroup(g)
		hosts[i] = host
		go g.Run(context.Background())
		raft.StartTicker(context.Background(), g, 10*time.Millisecond)
	}

	// Wait for leader election.
	leader := hosts[0]
	for i := 0; i < 50; i++ {
		st, _ := leader.Status(0)
		if st.Role == raft.RoleLeader {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	payload := make([]byte, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = leader.Propose(context.Background(), 0, payload)
	}
}

// BenchmarkMultiGroupPropose measures per-group propose throughput
// in a multi-group cluster.
func BenchmarkMultiGroupPropose(b *testing.B) {
	network := sim.New(sim.Config{Seed: 42, Auto: true, Faults: sim.Faults{}})
	defer network.Close()

	const groups = 4
	hosts := make([]*Host, 3)
	ids := []transport.NodeID{"n1", "n2", "n3"}

	for i := range ids {
		host := NewHost(ids[i])
		tpt := sim.NewTransport(network, ids[i])
		network.SetHandler(ids[i], host)

		for g := 0; g < groups; g++ {
			dir := b.TempDir() + "/g" + string(rune('0'+g)) + "/n" + string(rune('1'+i))
			rlog, _, _ := raftlog.Open(dir)
			engine := kv.NewMemEngine()
			gid := raft.GroupID(g)
			group, _ := raft.New(raft.Config{
				ID:             ids[i],
				GroupID:        gid,
				Peers:          ids,
				Transport:      tpt,
				Log:            rlog,
				ElectionTicks:  10,
				HeartbeatTicks: 3,
				StateMachine:   kv.NewSM(engine),
			})
			host.AddGroup(group)
		}
		hosts[i] = host
		for g := 0; g < groups; g++ {
			go host.Group(raft.GroupID(g)).Run(context.Background())
		}
		for g := 0; g < groups; g++ {
			raft.StartTicker(context.Background(), host.Group(raft.GroupID(g)), 10*time.Millisecond)
		}
	}

	// Wait for leaders.
	time.Sleep(2 * time.Second)

	// Find leader for group 0.
	var leader *Host
	for _, h := range hosts {
		st, _ := h.Status(0)
		if st.Role == raft.RoleLeader {
			leader = h
			break
		}
	}

	payload := make([]byte, 100)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, _ = leader.Propose(context.Background(), 0, payload)
		}
	})
}
