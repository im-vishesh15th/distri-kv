package multiraft

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"distrikv/internal/kv"
	"distrikv/internal/raft"
	"distrikv/internal/raftlog"
	"distrikv/internal/transport"
	"distrikv/internal/transport/sim"
)

// awaitLeader waits until some host leads gid, or fails the benchmark.
// Timing must never start before leadership is confirmed: proposing as a
// follower fails immediately and would measure error latency, not commit
// latency.
func awaitLeader(b *testing.B, hosts []*Host, gid raft.GroupID) *Host {
	b.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, h := range hosts {
			st, err := h.Status(gid)
			if err == nil && st.Role == raft.RoleLeader {
				return h
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	b.Fatalf("group %d: no leader elected within 10s", gid)
	return nil
}

// benchSetPayload encodes an OpSet command with a unique session sequence
// number. Unique matters: the state machine dedups on (client_id, sequence),
// so a constant sequence would replay a cached response instead of applying
// — the original benchmark's payloads also failed codec validation outright
// and every proposal errored (silently, since errors were discarded).
// Returns an error rather than failing: callers may run in parallel
// goroutines, where b.Fatalf is not permitted.
func benchSetPayload(seq uint64) ([]byte, error) {
	return kv.EncodeCommand(kv.Command{
		Op:       kv.OpSet,
		Key:      "bench-key",
		Value:    make([]byte, 100),
		ClientID: "bench-client",
		Sequence: seq,
	})
}

// BenchmarkSingleGroupPropose measures single-group Raft propose throughput
// over the simulated network (no network latency). Propose blocks until the
// entry is applied on the leader, so each iteration is a full
// propose→replicate→commit→apply round trip. Failures are counted and
// reported as "failures"; a run where every proposal fails is an error, not
// a measurement.
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
		rlog, _, err := raftlog.Open(dir)
		if err != nil {
			b.Fatalf("open raft log: %v", err)
		}
		engine := kv.NewMemEngine()
		g, err := raft.New(raft.Config{
			ID:             ids[i],
			GroupID:        0,
			Peers:          ids,
			Transport:      tpt,
			Log:            rlog,
			ElectionTicks:  10,
			HeartbeatTicks: 3,
			StateMachine:   kv.NewSM(engine),
		})
		if err != nil {
			b.Fatalf("raft.New: %v", err)
		}
		host.AddGroup(g)
		hosts[i] = host
		go g.Run(context.Background())
		raft.StartTicker(context.Background(), g, 10*time.Millisecond)
	}

	leader := awaitLeader(b, hosts, 0)

	var failures int64
	var firstErr error
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload, perr := benchSetPayload(uint64(i + 1))
		if perr != nil {
			b.Fatalf("encode command: %v", perr)
		}
		if _, _, err := leader.Propose(context.Background(), 0, payload); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			atomic.AddInt64(&failures, 1)
		}
	}
	b.StopTimer()
	n := atomic.LoadInt64(&failures)
	b.ReportMetric(float64(n), "failures")
	if int(n) == b.N {
		b.Fatalf("all %d proposals failed (first: %v)", b.N, firstErr)
	}
}

// BenchmarkMultiGroupPropose measures propose throughput across all groups:
// proposals are distributed round-robin over the groups, each to that
// group's leader. Every group must have a confirmed leader before timing.
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
			rlog, _, err := raftlog.Open(dir)
			if err != nil {
				b.Fatalf("group %d: open raft log: %v", g, err)
			}
			engine := kv.NewMemEngine()
			gid := raft.GroupID(g)
			group, err := raft.New(raft.Config{
				ID:             ids[i],
				GroupID:        gid,
				Peers:          ids,
				Transport:      tpt,
				Log:            rlog,
				ElectionTicks:  10,
				HeartbeatTicks: 3,
				StateMachine:   kv.NewSM(engine),
			})
			if err != nil {
				b.Fatalf("group %d: raft.New: %v", g, err)
			}
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

	// Every group needs a confirmed leader before timing (leaders may differ
	// per group); the original found a leader for group 0 only, then
	// proposed to a possibly-nil host for the rest.
	leaders := make([]*Host, groups)
	for g := 0; g < groups; g++ {
		leaders[g] = awaitLeader(b, hosts, raft.GroupID(g))
	}

	var failures, proposals int64
	var firstErr error
	var errMu sync.Mutex
	var rr atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			seq := rr.Add(1)
			g := int(seq % groups)
			payload, perr := benchSetPayload(seq)
			if perr == nil {
				if _, _, err := leaders[g].Propose(context.Background(), raft.GroupID(g), payload); err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					atomic.AddInt64(&failures, 1)
				}
			} else {
				atomic.AddInt64(&failures, 1)
			}
			atomic.AddInt64(&proposals, 1)
		}
	})
	b.StopTimer()
	f, p := atomic.LoadInt64(&failures), atomic.LoadInt64(&proposals)
	b.ReportMetric(float64(f), "failures")
	if p == 0 || f == p {
		b.Fatalf("no successful proposals (%d/%d failed, first: %v)", f, p, firstErr)
	}
}
