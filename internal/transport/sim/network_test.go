package sim

// Engine tests for the deterministic simulated network (spec §18): every
// capability the spec lists — delay, drop, reorder, duplication,
// partition, isolation, slow node, seeded determinism — is exercised
// directly against the Network in manual mode, where Advance provides
// deterministic quiescence points and no goroutine or wall clock can
// influence what happens.

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	raftpb "distrikv/gen/raft/v1"
	"distrikv/internal/transport"
)

// stubHandler is the inbound side: a canned responder. Deliveries are
// recorded by the test's invoke closures, so the recorder observes
// exactly what the network actually delivered (and when it didn't).
type stubHandler struct{}

func (stubHandler) HandleRequestVote(context.Context, *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	return &raftpb.RequestVoteResponse{}, nil
}
func (stubHandler) HandlePreVote(context.Context, *raftpb.PreVoteRequest) (*raftpb.PreVoteResponse, error) {
	return &raftpb.PreVoteResponse{}, nil
}
func (stubHandler) HandleAppendEntries(context.Context, *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	return &raftpb.AppendEntriesResponse{}, nil
}
func (stubHandler) HandleInstallSnapshot(context.Context, *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	return &raftpb.InstallSnapshotResponse{}, nil
}

// recorder captures delivery order (test-goroutine reads are guarded: the
// auto-mode driver records from its own goroutine).
type recorder struct {
	mu        chan struct{} // 1-buffer used as a mutex: cheap and obvious
	delivered []int
}

func newRecorder() *recorder { return &recorder{mu: make(chan struct{}, 1)} }

func (r *recorder) record(tag int) {
	r.mu <- struct{}{}
	r.delivered = append(r.delivered, tag)
	<-r.mu
}

func (r *recorder) snapshot() []int {
	r.mu <- struct{}{}
	defer func() { <-r.mu }()
	return append([]int(nil), r.delivered...)
}

// invoke records the delivery then calls through to the handler.
func invoke(rec *recorder, tag int) func(transport.Handler) (any, error) {
	return func(h transport.Handler) (any, error) {
		rec.record(tag)
		return h.HandleRequestVote(context.Background(), &raftpb.RequestVoteRequest{})
	}
}

type callRes struct {
	v   any
	err error
}

// waitEnqueued blocks until a scripted call has arrived at the network —
// the hook that makes serialized send histories reproducible.
func waitEnqueued(t *testing.T, n *Network) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for n.Pending() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("call never reached the network")
		}
		time.Sleep(100 * time.Microsecond)
	}
}

// send runs one scripted call to completion in manual mode: enqueue it,
// drive the network, return the outcome. Serial by construction — the
// next call starts only after this one is fully answered.
func send(t *testing.T, n *Network, from, to transport.NodeID, tag int, rec *recorder, drive time.Duration) callRes {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan callRes, 1)
	go func() {
		v, err := n.Call(ctx, from, to, invoke(rec, tag))
		done <- callRes{v, err}
	}()
	waitEnqueued(t, n)
	if err := n.Advance(drive); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	select {
	case r := <-done:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("call never completed")
	}
	return callRes{}
}

func TestFaultFreeDelivery(t *testing.T) {
	n := New(Config{Seed: 1})
	defer n.Close()
	n.SetHandler("b", stubHandler{})
	rec := newRecorder()

	r := send(t, n, "a", "b", 0, rec, 0)
	if r.err != nil {
		t.Fatalf("fault-free call: %v", r.err)
	}
	if _, ok := r.v.(*raftpb.RequestVoteResponse); !ok {
		t.Fatalf("response type = %T, want *raftpb.RequestVoteResponse", r.v)
	}
	if got := rec.snapshot(); !slices.Equal(got, []int{0}) {
		t.Fatalf("deliveries = %v, want [0]", got)
	}
}

func TestDropSkipsHandlerAndErrors(t *testing.T) {
	n := New(Config{Seed: 7, Faults: Faults{DropRate: 1}})
	defer n.Close()
	n.SetHandler("b", stubHandler{})
	rec := newRecorder()

	r := send(t, n, "a", "b", 1, rec, 0)
	if !errors.Is(r.err, ErrDropped) {
		t.Fatalf("err = %v, want ErrDropped", r.err)
	}
	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("dropped message reached the handler: %v", got)
	}

	// Rate back to 0: the same path delivers.
	n.SetFaults(Faults{DropRate: 0})
	r = send(t, n, "a", "b", 2, rec, 0)
	if r.err != nil {
		t.Fatalf("DropRate=0 call: %v", r.err)
	}
	if got := rec.snapshot(); !slices.Equal(got, []int{2}) {
		t.Fatalf("deliveries = %v, want [2]", got)
	}
}

func TestDelayBoundsDelivery(t *testing.T) {
	const delay = 50 * time.Millisecond
	n := New(Config{Seed: 3, Faults: Faults{MinDelay: delay, MaxDelay: delay}})
	defer n.Close()
	n.SetHandler("b", stubHandler{})
	rec := newRecorder()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan callRes, 1)
	go func() {
		v, err := n.Call(ctx, "a", "b", invoke(rec, 0))
		done <- callRes{v, err}
	}()
	waitEnqueued(t, n)

	if err := n.Advance(delay - time.Millisecond); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("delivered %v before its delay elapsed", got)
	}
	select {
	case r := <-done:
		t.Fatalf("call completed before delivery: %+v", r)
	default:
	}

	if err := n.Advance(2 * time.Millisecond); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if got := rec.snapshot(); !slices.Equal(got, []int{0}) {
		t.Fatalf("deliveries = %v, want [0]", got)
	}
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("delivered call: %v", r.err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("caller never received the response")
	}
}

// TestReorderingByDelay pins the mechanism: message B sent AFTER message
// A arrives BEFORE it — reordering is a consequence of sampled delays and
// the (deliverAt, seq) queue order, not of timing luck.
func TestReorderingByDelay(t *testing.T) {
	n := New(Config{Seed: 5})
	defer n.Close()
	n.SetHandler("b", stubHandler{})
	rec := newRecorder()

	// A: 100ms delay, scheduled now.
	n.SetFaults(Faults{MinDelay: 100 * time.Millisecond, MaxDelay: 100 * time.Millisecond})
	doneA := make(chan callRes, 1)
	go func() {
		v, err := n.Call(context.Background(), "a", "b", invoke(rec, 10))
		doneA <- callRes{v, err}
	}()
	waitEnqueued(t, n)
	if err := n.Advance(0); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("A delivered before its delay elapsed: %v", got)
	}

	// B: fault-free, sent after A.
	n.SetFaults(Faults{})
	if r := send(t, n, "a", "b", 20, rec, 0); r.err != nil {
		t.Fatalf("B: %v", r.err)
	}
	if got := rec.snapshot(); !slices.Equal(got, []int{20}) {
		t.Fatalf("after B: deliveries = %v, want [20] (B overtakes A)", got)
	}
	select {
	case <-doneA:
		t.Fatal("A completed before B despite later delivery")
	default:
	}

	// A's turn.
	if err := n.Advance(100 * time.Millisecond); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if got := rec.snapshot(); !slices.Equal(got, []int{20, 10}) {
		t.Fatalf("final deliveries = %v, want [20 10]", got)
	}
	select {
	case r := <-doneA:
		if r.err != nil {
			t.Fatalf("A: %v", r.err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("A never completed")
	}
}

// TestDuplicateDeliversTwice: DupRate=1 invokes the responder twice for
// one call; the caller still gets exactly one response.
func TestDuplicateDeliversTwice(t *testing.T) {
	n := New(Config{Seed: 11, Faults: Faults{DupRate: 1}})
	defer n.Close()
	n.SetHandler("b", stubHandler{})
	rec := newRecorder()

	r := send(t, n, "a", "b", 4, rec, 0)
	if r.err != nil {
		t.Fatalf("duplicated call: %v", r.err)
	}
	if got := rec.snapshot(); !slices.Equal(got, []int{4, 4}) {
		t.Fatalf("deliveries = %v, want [4 4] (delivered twice)", got)
	}
	if _, ok := r.v.(*raftpb.RequestVoteResponse); !ok {
		t.Fatalf("response type = %T", r.v)
	}
}

func TestBlockPairAndHeal(t *testing.T) {
	n := New(Config{Seed: 2})
	defer n.Close()
	n.SetHandler("a", stubHandler{})
	n.SetHandler("b", stubHandler{})
	n.SetHandler("c", stubHandler{})
	rec := newRecorder()

	n.Block("a", "b")
	if r := send(t, n, "a", "b", 0, rec, 0); !errors.Is(r.err, ErrBlocked) {
		t.Fatalf("a→b: err = %v, want ErrBlocked", r.err)
	}
	if r := send(t, n, "b", "a", 1, rec, 0); !errors.Is(r.err, ErrBlocked) {
		t.Fatalf("b→a: err = %v, want ErrBlocked (symmetric)", r.err)
	}
	if r := send(t, n, "a", "c", 2, rec, 0); r.err != nil {
		t.Fatalf("a→c must stay open: %v", r.err)
	}

	n.Heal()
	if r := send(t, n, "a", "b", 3, rec, 0); r.err != nil {
		t.Fatalf("a→b after heal: %v", r.err)
	}
	if got := rec.snapshot(); !slices.Equal(got, []int{2, 3}) {
		t.Fatalf("deliveries = %v, want [2 3] (blocked calls never delivered)", got)
	}
}

func TestPartitionGroupsAndIsolate(t *testing.T) {
	n := New(Config{Seed: 4})
	defer n.Close()
	for _, id := range []transport.NodeID{"n1", "n2", "n3", "n4"} {
		n.SetHandler(id, stubHandler{})
	}
	rec := newRecorder()

	// Majority/minority split: {n1,n2} | {n3,n4}.
	majority := []transport.NodeID{"n1", "n2"}
	minority := []transport.NodeID{"n3", "n4"}
	n.Partition(majority, minority)
	if r := send(t, n, "n1", "n3", 0, rec, 0); !errors.Is(r.err, ErrBlocked) {
		t.Fatalf("cross-group n1→n3: %v, want ErrBlocked", r.err)
	}
	if r := send(t, n, "n4", "n2", 1, rec, 0); !errors.Is(r.err, ErrBlocked) {
		t.Fatalf("cross-group n4→n2: %v, want ErrBlocked", r.err)
	}
	if r := send(t, n, "n1", "n2", 2, rec, 0); r.err != nil {
		t.Fatalf("within-majority must stay open: %v", r.err)
	}
	if r := send(t, n, "n3", "n4", 3, rec, 0); r.err != nil {
		t.Fatalf("within-minority stays open (it just cannot talk to the majority): %v", r.err)
	}
	n.Heal()

	// Node isolation: one node cut from the world, world unaffected.
	n.Isolate("n4")
	if r := send(t, n, "n1", "n4", 4, rec, 0); !errors.Is(r.err, ErrBlocked) {
		t.Fatalf("to isolated n4: %v, want ErrBlocked", r.err)
	}
	if r := send(t, n, "n4", "n1", 5, rec, 0); !errors.Is(r.err, ErrBlocked) {
		t.Fatalf("from isolated n4: %v, want ErrBlocked", r.err)
	}
	if r := send(t, n, "n1", "n2", 6, rec, 0); r.err != nil {
		t.Fatalf("rest of cluster unaffected: %v", r.err)
	}
	n.Heal()

	if got := rec.snapshot(); !slices.Equal(got, []int{2, 3, 6}) {
		t.Fatalf("deliveries = %v, want [2 3 6] (blocked calls never delivered)", got)
	}
}

// TestInFlightSurvivesPartition pins the semantic: partition rules are
// decided when the driver samples the call, not at delivery — a message
// already in flight completes, like a real packet already on the wire.
func TestInFlightSurvivesPartition(t *testing.T) {
	const delay = 50 * time.Millisecond
	n := New(Config{Seed: 6, Faults: Faults{MinDelay: delay, MaxDelay: delay}})
	defer n.Close()
	n.SetHandler("b", stubHandler{})
	rec := newRecorder()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan callRes, 1)
	go func() {
		v, err := n.Call(ctx, "a", "b", invoke(rec, 9))
		done <- callRes{v, err}
	}()
	waitEnqueued(t, n)
	if err := n.Advance(0); err != nil { // scheduled, in flight
		t.Fatalf("Advance: %v", err)
	}

	n.Block("a", "b") // partition closes after the message left
	if err := n.Advance(delay); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if got := rec.snapshot(); !slices.Equal(got, []int{9}) {
		t.Fatalf("in-flight delivery lost at partition: %v", got)
	}
	// The next call, however, is blocked.
	if r := send(t, n, "a", "b", 10, rec, 0); !errors.Is(r.err, ErrBlocked) {
		t.Fatalf("post-partition call: %v, want ErrBlocked", r.err)
	}
}

func TestNoRouteUntilRegistered(t *testing.T) {
	n := New(Config{Seed: 8})
	defer n.Close()
	rec := newRecorder()

	r := send(t, n, "a", "ghost", 0, rec, 0)
	if !errors.Is(r.err, ErrNoRoute) {
		t.Fatalf("unregistered target: %v, want ErrNoRoute", r.err)
	}
	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("no-route call delivered: %v", got)
	}

	// The node boots (or restarts): same address now answers.
	n.SetHandler("ghost", stubHandler{})
	if r := send(t, n, "a", "ghost", 1, rec, 0); r.err != nil {
		t.Fatalf("after registration: %v", r.err)
	}
}

func TestSlowNodeAddsDelay(t *testing.T) {
	const extra = 50 * time.Millisecond
	n := New(Config{Seed: 10})
	defer n.Close()
	n.SetHandler("b", stubHandler{})
	n.SetSlow("b", extra)
	rec := newRecorder()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan callRes, 1)
	go func() {
		v, err := n.Call(ctx, "a", "b", invoke(rec, 3))
		done <- callRes{v, err}
	}()
	waitEnqueued(t, n)
	if err := n.Advance(0); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("slow node answered instantly: %v", got)
	}
	if err := n.Advance(extra); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if got := rec.snapshot(); !slices.Equal(got, []int{3}) {
		t.Fatalf("deliveries = %v, want [3]", got)
	}
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("call: %v", r.err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("caller never completed")
	}
}

// TestDeterminismSameSeedSameHistory is the spec's core promise: same
// seed + same serialized call history => identical fault decisions and
// identical delivery order; a different seed diverges.
func TestDeterminismSameSeedSameHistory(t *testing.T) {
	faults := Faults{DropRate: 0.3, DupRate: 0.2, MinDelay: 0, MaxDelay: 40 * time.Millisecond}
	run := func(seed int64) (decisions []string, delivered []int) {
		n := New(Config{Seed: seed, Faults: faults})
		defer n.Close()
		n.SetHandler("b", stubHandler{})
		rec := newRecorder()
		for i := 0; i < 40; i++ {
			r := send(t, n, "a", "b", i, rec, time.Second) // 1s > max delay: everything resolves
			if r.err != nil {
				decisions = append(decisions, r.err.Error())
			} else {
				decisions = append(decisions, "ok")
			}
		}
		return decisions, rec.snapshot()
	}

	d1, l1 := run(42)
	d2, l2 := run(42)
	if !slices.Equal(d1, d2) {
		t.Fatalf("same seed diverged in decisions:\n%v\n%v", d1, d2)
	}
	if !slices.Equal(l1, l2) {
		t.Fatalf("same seed diverged in delivery order:\n%v\n%v", l1, l2)
	}

	d3, l3 := run(43)
	if slices.Equal(d1, d3) && slices.Equal(l1, l3) {
		t.Fatalf("different seed produced an identical run — seeded randomness not in effect")
	}
}

func TestAutoModeDeliversInRealTime(t *testing.T) {
	n := New(Config{Seed: 9, Auto: true, Faults: Faults{MinDelay: 30 * time.Millisecond, MaxDelay: 30 * time.Millisecond}})
	defer n.Close()
	n.SetHandler("b", stubHandler{})
	rec := newRecorder()

	if err := n.Advance(time.Second); err == nil {
		t.Fatal("Advance must be rejected while the auto driver runs")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan callRes, 1)
	go func() {
		v, err := n.Call(ctx, "a", "b", invoke(rec, 0))
		done <- callRes{v, err}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("auto delivery: %v", r.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("auto driver never delivered (30ms delay, 3s budget)")
	}
	if got := rec.snapshot(); !slices.Equal(got, []int{0}) {
		t.Fatalf("deliveries = %v, want [0]", got)
	}
}

func TestCloseFailsCalls(t *testing.T) {
	n := New(Config{Seed: 12})
	n.Close()
	n.Close() // idempotent

	if _, err := n.Call(context.Background(), "a", "b", nil); !errors.Is(err, transport.ErrClosed) {
		t.Fatalf("Call after Close: %v, want ErrClosed", err)
	}
	if err := n.Advance(0); !errors.Is(err, transport.ErrClosed) {
		t.Fatalf("Advance after Close: %v, want ErrClosed", err)
	}
}

func TestInvalidFaultsPanic(t *testing.T) {
	expectPanic := func(what string, fn func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatalf("%s: expected panic", what)
			}
		}()
		fn()
	}
	expectPanic("DropRate > 1", func() { New(Config{Faults: Faults{DropRate: 1.5}}) })
	expectPanic("negative delay", func() { New(Config{Faults: Faults{MinDelay: -time.Second}}) })
	expectPanic("min > max delay", func() {
		n := New(Config{})
		defer n.Close()
		n.SetFaults(Faults{MinDelay: 10 * time.Second, MaxDelay: time.Second})
	})
	expectPanic("Block self", func() {
		n := New(Config{})
		defer n.Close()
		n.Block("a", "a")
	})
}
