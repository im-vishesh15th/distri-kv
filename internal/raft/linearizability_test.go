package raft_test

// Phase 16: operation histories + linearizability checking (spec §20/§21).
//
// Concurrent clients drive the REAL client path — server.Service with
// client sessions, so dedup and the ReadIndex read barrier are the
// production ones — over the Phase 14 simulated network while a seeded
// chaos schedule injects faults and crashes. Every logical operation is
// recorded (history.Op, spec §20's fields) and the finished history must
// be linearizable (lincheck.Check): there must exist a real-time-
// consistent total order explaining every observed output against the KV
// model. That is the end-to-end consistency proof — a lost write, a
// stale read, or a phantom value all show up as a non-linearizable
// history.

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	kv1 "distrikv/gen/kv/v1"
	"distrikv/internal/history"
	"distrikv/internal/lincheck"
	"distrikv/internal/server"
	"distrikv/internal/transport/sim"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// linOp is one logical client operation.
type linOp struct {
	kind  history.OpType
	key   string
	value string // set
	delta int64  // incrby
	old   string // cas expected value (lincheck.Absent = must be absent)
	new   string // cas new value
}

// input renders the op's recorded input (history.Op's format).
func (op linOp) input() string {
	switch op.kind {
	case history.OpSet:
		return op.value
	case history.OpIncrBy:
		return strconv.FormatInt(op.delta, 10)
	case history.OpCAS:
		return op.old + "\x00" + op.new
	}
	return ""
}

// call executes the op against svc and returns the recorded output.
func (op linOp) call(svc *server.Service, ctx context.Context, clientID string, seq uint64) (string, error) {
	switch op.kind {
	case history.OpSet:
		_, err := svc.Put(ctx, &kv1.PutRequest{
			Key: op.key, Value: []byte(op.value),
			ClientId: clientID, SequenceNumber: seq,
		})
		return "", err

	case history.OpGet:
		resp, err := svc.Get(ctx, &kv1.GetRequest{Key: op.key})
		if err != nil {
			if status.Code(err) == codes.NotFound {
				return lincheck.Absent, nil // a definitive answer: the key is absent
			}
			return "", err
		}
		return string(resp.Value), nil

	case history.OpIncrBy:
		resp, err := svc.Incr(ctx, &kv1.IncrRequest{
			Key: op.key, Delta: op.delta,
			ClientId: clientID, SequenceNumber: seq,
		})
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(resp.Value, 10), nil

	case history.OpCAS:
		resp, err := svc.CAS(ctx, &kv1.CASRequest{
			Key:            op.key,
			ExpectedExists: op.old != lincheck.Absent,
			ExpectedValue:  []byte(op.old),
			NewValue:       []byte(op.new),
			ClientId:       clientID,
			SequenceNumber: seq,
		})
		if err != nil {
			return "", err
		}
		if resp.Applied {
			return "true", nil
		}
		return "false", nil
	}
	return "", fmt.Errorf("unknown op type %v", op.kind)
}

// retryable reports whether the client may retry the SAME logical op
// (same session sequence): leadership moves and ambiguous outcomes.
// Dedup makes the retry safe — a retried op that had committed returns
// its original result instead of applying twice.
func retryable(err error) bool {
	switch status.Code(err) {
	case codes.Aborted, codes.Unavailable, codes.DeadlineExceeded:
		return true
	}
	return false
}

// callWithRetry runs one logical op to a definitive answer (or the
// deadline), retrying with the same (clientID, seq) — exactly what the
// SDK does.
func callWithRetry(svc func() *server.Service, clientID string, seq uint64, op linOp) (string, error) {
	deadline := time.Now().Add(3 * time.Second)
	for {
		if s := svc(); s != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			output, err := op.call(s, ctx, clientID, seq)
			cancel()
			if err == nil {
				return output, nil
			}
			if !retryable(err) {
				return "", err
			}
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("op %s/%d got no definitive answer within 30s", clientID, seq)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// runClient executes one client's workload sequentially, recording each
// logical op once with its final outcome.
func runClient(rec *history.Recorder, svc func() *server.Service, clientID string, ops []linOp) {
	for i, op := range ops {
		seq := uint64(i + 1)
		start := time.Now()
		output, err := callWithRetry(svc, clientID, seq, op)
		finish := time.Now()
		rec.Record(clientID, op.kind, op.key, op.input(), output, start, finish, err == nil)
	}
}

// genOps builds a deterministic per-client workload over a small key
// space so operations genuinely contend.
func genOps(rng *rand.Rand, n int) []linOp {
	strKeys := []string{"a", "b", "c", "d"}
	cntKeys := []string{"cnt0", "cnt1"}
	values := []string{"v0", "v1", "v2", "v3", "v4"}
	ops := make([]linOp, n)
	for i := range ops {
		switch rng.Intn(20) {
		case 0, 1, 2, 3, 4, 5, 6: // 7/20 set
			ops[i] = linOp{kind: history.OpSet, key: strKeys[rng.Intn(len(strKeys))], value: values[rng.Intn(len(values))]}
		case 7, 8, 9, 10, 11, 12: // 6/20 get
			ops[i] = linOp{kind: history.OpGet, key: strKeys[rng.Intn(len(strKeys))]}
		case 13, 14, 15, 16: // 4/20 incr
			ops[i] = linOp{kind: history.OpIncrBy, key: cntKeys[rng.Intn(len(cntKeys))], delta: int64(rng.Intn(7) - 3)}
		default: // 3/20 cas
			old := values[rng.Intn(len(values))]
			if rng.Intn(2) == 0 {
				old = lincheck.Absent
			}
			ops[i] = linOp{kind: history.OpCAS, key: strKeys[rng.Intn(len(strKeys))], old: old, new: values[rng.Intn(len(values))]}
		}
	}
	return ops
}

// linHarness wires a sim cluster's nodes into KV services and returns a
// leader-finding function for the clients. The service is built from the
// node's CURRENT view on every call, so a restarted node (new Raft core +
// engine) is picked up automatically.
func linHarness(nodes []*simNode) func() *server.Service {
	return func() *server.Service {
		ls := simLeadersOf(nodes)
		if len(ls) != 1 {
			return nil
		}
		_, rnode, _, engine, _, _ := ls[0].view()
		return server.NewService(engine, rnode, rnode.Status, nil, nil)
	}
}

// checkLinearizable runs the analysis half on a recorded history and
// fails the test with detail if it is not linearizable.
func checkLinearizable(t *testing.T, rec *history.Recorder) {
	t.Helper()
	ops := rec.Ops()
	failed := 0
	for _, op := range ops {
		if !op.Success {
			failed++
		}
	}
	t.Logf("history: %d ops (%d failed), %d clients", len(ops), failed, countClients(ops))
	start := time.Now()
	lin, err := lincheck.Check(ops)
	if err != nil {
		t.Fatalf("recorded history is NOT linearizable: %v", err)
	}
	t.Logf("linearizable: %d ops explained in %s", len(lin), time.Since(start).Round(time.Millisecond))
}

func countClients(ops []history.Op) int {
	seen := make(map[string]bool)
	for _, op := range ops {
		seen[op.ClientID] = true
	}
	return len(seen)
}

// TestLinearizableConcurrent is the fault-free baseline: concurrent
// clients, no chaos — the recording and the checker must agree on a
// healthy run.
func TestLinearizableConcurrent(t *testing.T) {
	nodes, _ := startSimCluster(t, 3, 1603)
	svc := linHarness(nodes)

	rec := history.NewRecorder()
	var wg sync.WaitGroup
	for c := 0; c < 4; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			crng := rand.New(rand.NewSource(1603 + int64(c)))
			runClient(rec, svc, fmt.Sprintf("client%d", c), genOps(crng, 20))
		}(c)
	}
	wg.Wait()

	waitForSimLeadership(t, 10*time.Second, "single leader", nodes)
	requireConverged(t, nodes, nil)
	checkLinearizable(t, rec)
}

// TestLinearizableChaos is the main event: the same workload while a
// seeded chaos schedule injects partitions, isolations, slow nodes,
// fault flickers, and crashes — and the history must STILL be
// linearizable. Linearizability is a safety property: no timing or
// fault schedule can make a correct system produce a non-linearizable
// history, so any failure here is a real bug (or a checker bug, which
// lincheck_test.go pins down).
func TestLinearizableChaos(t *testing.T) {
	for _, seed := range []int64{1601, 1602} {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			nodes, network := startSimClusterFaults(t, 3, seed, sim.Faults{
				DropRate: 0.08, DupRate: 0.04,
				MinDelay: 5 * time.Millisecond, MaxDelay: 25 * time.Millisecond,
			})
			svc := linHarness(nodes)

			rec := history.NewRecorder()
			var wg sync.WaitGroup
			for c := 0; c < 4; c++ {
				wg.Add(1)
				go func(c int) {
					defer wg.Done()
					crng := rand.New(rand.NewSource(seed + int64(c)))
					runClient(rec, svc, fmt.Sprintf("client%d", c), genOps(crng, 20))
				}(c)
			}

			// Chaos while the clients work: a seeded schedule, one fault
			// at a time, ticking faster than the clients' ops. Every ~10th
			// tick is a total outage (all nodes isolated for 5s): no
			// leader can be elected, so the ops in flight fail — that is
			// what exercises the checker's optional-effect path for
			// failed writes end to end (a failure shorter than the 3s
			// per-op deadline never produces one).
			stopChaos := make(chan struct{})
			var chaosWg sync.WaitGroup
			chaosWg.Add(1)
			var dead *simNode
			go func() {
				defer chaosWg.Done()
				crng := rand.New(rand.NewSource(seed + 1))
				ticker := time.NewTicker(150 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-stopChaos:
						return
					case <-ticker.C:
						if crng.Intn(10) == 0 {
							for _, n := range nodes {
								network.Isolate(n.id)
							}
							time.Sleep(5 * time.Second)
							network.Heal()
							continue
						}
						chaosAction(t, nodes, network, crng, &dead, false)
					}
				}
			}()
			wg.Wait()
			close(stopChaos)
			chaosWg.Wait()

			// Heal everything, revive anything the chaos killed, converge.
			network.Heal()
			network.SetFaults(sim.Faults{})
			for _, n := range nodes {
				network.SetSlow(n.id, 0)
				dead, _, _, _, _, _ := n.view()
				if dead {
					restartSimNode(t, network, n)
				}
			}
			waitForSimLeadership(t, 15*time.Second, "single leader after chaos", nodes)
			requireConverged(t, nodes, nil)
			checkLinearizable(t, rec)
		})
	}
}
