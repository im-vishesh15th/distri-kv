package raft_test

// Phase 13 (spec §16): the InstallSnapshot mechanism. The follower side —
// stale terms and redundant snapshots change nothing, a fresh install
// restores the state machine + persists the sidecar + rebases the log +
// adopts the position, unfakeable failures leave the node untouched — and
// the startup crash window where the sidecar sits above the log's end.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	raftpb "distrikv/gen/raft/v1"
	"distrikv/internal/kv"
	"distrikv/internal/raft"
	"distrikv/internal/raftlog"
	"distrikv/internal/transport"
)

// snapSM is a minimal snapshot-capable state machine for install tests:
// Apply appends payloads, Snapshot serializes them, Restore replaces them
// wholesale (all-or-nothing — validate before swapping, like kv.SM).
type snapSM struct {
	mu    sync.Mutex
	items []string
}

func (s *snapSM) Apply(payload []byte) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = append(s.items, string(payload))
	return "ok", nil
}

func (s *snapSM) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return []byte(strings.Join(s.items, "\n")), nil
}

func (s *snapSM) Restore(data []byte) error {
	if bytes.HasPrefix(data, []byte("CORRUPT")) {
		return errors.New("snapSM: corrupt payload")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(data) == 0 {
		s.items = nil
		return nil
	}
	s.items = strings.Split(string(data), "\n")
	return nil
}

func (s *snapSM) itemsSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.items...)
}

// noRestoreSM claims to be snapshot-capable but can never restore — the
// decode-failure path a real version-skew bug would take.
type noRestoreSM struct{}

func (s *noRestoreSM) Apply(payload []byte) (any, error) { return "ok", nil }
func (s *noRestoreSM) Snapshot() ([]byte, error)         { return []byte("state"), nil }
func (s *noRestoreSM) Restore([]byte) error              { return errors.New("noRestoreSM: cannot restore") }

// seedFollowerHistory drives committed entries 1..3 through the normal
// AppendEntries path so the node sits at lastApplied == 3 with term 1.
func seedFollowerHistory(t *testing.T, h *hnode) {
	t.Helper()
	r, err := h.n.HandleAppendEntries(context.Background(), &raftpb.AppendEntriesRequest{
		Term: 1, LeaderId: "n2", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []*raftpb.LogEntry{
			{Index: 1, Term: 1, Payload: []byte("a")},
			{Index: 2, Term: 1, Payload: []byte("b")},
			{Index: 3, Term: 1, Payload: []byte("c")},
		},
		LeaderCommit: 3,
	})
	if err != nil || !r.Success {
		t.Fatalf("seed append: %+v err=%v", r, err)
	}
	if st := h.n.Status(); st.LastApplied != 3 {
		t.Fatalf("after seed lastApplied = %d, want 3", st.LastApplied)
	}
}

// TestInstallSnapshotFollowerHandler walks the whole handler decision
// tree: stale term refuses, fresh install mutates everything it must (SM,
// sidecar, log boundary, position) and nothing it must not, a redundant
// snapshot at/below our position answers success WITHOUT touching state
// (never regress applied state — even with a payload the SM would refuse),
// and a corrupt payload above our position is refused with the node left
// exactly as it was.
func TestInstallSnapshotFollowerHandler(t *testing.T) {
	sm := &snapSM{}
	h := startNodeSM(t, "n1", 1, sm, "n1", "n2", "n3") // follower: no ticks
	ctx := context.Background()
	seedFollowerHistory(t, h)

	// (a) Stale term: an install from term 4 after we adopted term 5.
	if _, err := h.n.HandleAppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term: 5, LeaderId: "n2", PrevLogIndex: 3, PrevLogTerm: 1, LeaderCommit: 3,
	}); err != nil {
		t.Fatalf("term-5 append: %v", err)
	}
	r, err := h.n.HandleInstallSnapshot(ctx, &raftpb.InstallSnapshotRequest{
		Term: 4, LeaderId: "n2", LastIncludedIndex: 10, LastIncludedTerm: 3,
		Data: []byte("stale-state"),
	})
	if err != nil || r.Success {
		t.Fatalf("stale-term install: %+v err=%v, want refusal", r, err)
	}
	if got := h.rlog.FirstIndex(); got != 1 {
		t.Fatalf("stale install mutated log: FirstIndex = %d, want 1", got)
	}
	if st := h.n.Status(); st.LastApplied != 3 {
		t.Fatalf("stale install moved lastApplied = %d, want 3", st.LastApplied)
	}

	// (b) Fresh install at position 10: the leader's state replaces ours.
	r, err = h.n.HandleInstallSnapshot(ctx, &raftpb.InstallSnapshotRequest{
		Term: 5, LeaderId: "n2", LastIncludedIndex: 10, LastIncludedTerm: 3,
		Data: []byte("x\ny"),
	})
	if err != nil || !r.Success {
		t.Fatalf("fresh install: %+v err=%v, want success", r, err)
	}
	if got := sm.itemsSnapshot(); len(got) != 2 || got[0] != "x" || got[1] != "y" {
		t.Fatalf("SM after install = %q, want [x y] (old history replaced)", got)
	}
	st := h.n.Status()
	if st.CommitIndex != 10 || st.LastApplied != 10 {
		t.Fatalf("position after install = commit %d applied %d, want 10/10", st.CommitIndex, st.LastApplied)
	}
	if st.LeaderID != "n2" {
		t.Fatalf("leaderID = %q, want n2 (authority accepted)", st.LeaderID)
	}
	if got := h.rlog.FirstIndex(); got != 11 || h.rlog.Count() != 0 {
		t.Fatalf("log after install: first=%d count=%d, want 11/0 (rebased past our old tail)", got, h.rlog.Count())
	}
	if got, err := h.rlog.Term(10); err != nil || got != 3 {
		t.Fatalf("boundary Term(10) = (%d, %v), want (3, nil)", got, err)
	}
	meta, payload, err := h.rlog.LoadSnapshot()
	if err != nil || meta.LastIncludedIndex != 10 || meta.LastIncludedTerm != 3 || string(payload) != "x\ny" {
		t.Fatalf("sidecar = (%+v, %q, %v), want {10 3} \"x\ny\" nil", meta, payload, err)
	}

	// (c) Redundant install at/below our position: success, but NOTHING
	// changes — even a payload the SM would refuse never reaches it.
	r, err = h.n.HandleInstallSnapshot(ctx, &raftpb.InstallSnapshotRequest{
		Term: 5, LeaderId: "n2", LastIncludedIndex: 5, LastIncludedTerm: 1,
		Data: []byte("CORRUPT"),
	})
	if err != nil || !r.Success {
		t.Fatalf("redundant install: %+v err=%v, want success", r, err)
	}
	if got := sm.itemsSnapshot(); len(got) != 2 || got[0] != "x" {
		t.Fatalf("SM after redundant install = %q, want [x y] untouched", got)
	}
	if got := h.rlog.FirstIndex(); got != 11 {
		t.Fatalf("redundant install mutated log: FirstIndex = %d, want 11", got)
	}
	if st := h.n.Status(); st.LastApplied != 10 {
		t.Fatalf("redundant install moved lastApplied = %d, want 10", st.LastApplied)
	}

	// (d) Corrupt payload ABOVE our position: refused, node untouched —
	// in particular the sidecar still holds the position-10 snapshot.
	r, err = h.n.HandleInstallSnapshot(ctx, &raftpb.InstallSnapshotRequest{
		Term: 5, LeaderId: "n2", LastIncludedIndex: 20, LastIncludedTerm: 4,
		Data: []byte("CORRUPT-again"),
	})
	if err != nil || r.Success {
		t.Fatalf("corrupt install: %+v err=%v, want refusal", r, err)
	}
	if got := sm.itemsSnapshot(); len(got) != 2 || got[0] != "x" {
		t.Fatalf("SM after corrupt install = %q, want [x y] untouched", got)
	}
	if st := h.n.Status(); st.LastApplied != 10 || st.CommitIndex != 10 {
		t.Fatalf("position after corrupt install = %d/%d, want 10/10", st.CommitIndex, st.LastApplied)
	}
	if meta, _, err := h.rlog.LoadSnapshot(); err != nil || meta.LastIncludedIndex != 10 {
		t.Fatalf("sidecar after corrupt install = (%+v, %v), want {10 3}", meta, err)
	}

	// (e) Malformed request (position 0): refused without mutation.
	r, err = h.n.HandleInstallSnapshot(ctx, &raftpb.InstallSnapshotRequest{
		Term: 5, LeaderId: "n2", Data: []byte("anything"),
	})
	if err != nil || r.Success {
		t.Fatalf("zero-index install: %+v err=%v, want refusal", r, err)
	}
	if got := h.rlog.FirstIndex(); got != 11 {
		t.Fatalf("zero-index install mutated log: FirstIndex = %d, want 11", got)
	}
}

// TestInstallSnapshotRefusedStates: two node configurations where an
// install can never be honored — an SM that cannot restore (decode bug)
// and an SM without snapshot support at all. Both must refuse WITHOUT
// mutating anything, and without persisting a sidecar that would make the
// node unrestartable.
func TestInstallSnapshotRefusedStates(t *testing.T) {
	ctx := context.Background()
	install := func(t *testing.T, h *hnode) {
		t.Helper()
		seedFollowerHistory(t, h)
		r, err := h.n.HandleInstallSnapshot(ctx, &raftpb.InstallSnapshotRequest{
			Term: 1, LeaderId: "n2", LastIncludedIndex: 10, LastIncludedTerm: 3,
			Data: []byte("state"),
		})
		if err != nil || r.Success {
			t.Fatalf("install: %+v err=%v, want refusal", r, err)
		}
		if got := h.rlog.FirstIndex(); got != 1 {
			t.Fatalf("FirstIndex = %d, want 1 (log untouched)", got)
		}
		if st := h.n.Status(); st.LastApplied != 3 {
			t.Fatalf("lastApplied = %d, want 3 (position untouched)", st.LastApplied)
		}
		if meta, _, err := h.rlog.LoadSnapshot(); err != nil || !meta.Zero() {
			t.Fatalf("sidecar written despite refusal: (%+v, %v)", meta, err)
		}
	}

	t.Run("state machine cannot restore", func(t *testing.T) {
		h := startNodeSM(t, "n1", 1, &noRestoreSM{}, "n1", "n2", "n3")
		install(t, h)
	})

	t.Run("no snapshot support", func(t *testing.T) {
		h := startNode(t, "n1", 1, "n1", "n2", "n3") // SM nil
		install(t, h)
	})
}

// TestRestoreSnapshotReconcilesInstallCrashWindow: the follower crashed
// between SaveSnapshot and the log rebase during an install — the sidecar
// sits ABOVE the log's end (no Reset ran). raft.New must recover by
// discarding the old log and adopting the snapshot position, not refuse to
// start (the Phase 12 TruncatePrefix catch-up can't help: there is nothing
// to truncate TO).
func TestRestoreSnapshotReconcilesInstallCrashWindow(t *testing.T) {
	rlog, _, err := raftlog.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = rlog.Close() })
	engine := kv.NewMemEngine()
	sm := kv.NewSM(engine)
	for i := uint64(1); i <= 3; i++ {
		if err := rlog.Append([]raftlog.Entry{{Index: i, Term: 1, Payload: []byte("x")}}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	// The install's durable intent landed; the rebase did not.
	payload, err := sm.Snapshot()
	if err != nil {
		t.Fatalf("SM snapshot: %v", err)
	}
	if err := rlog.SaveSnapshot(raftlog.SnapshotMeta{
		LastIncludedIndex: 10, LastIncludedTerm: 3,
	}, payload); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	n, err := raft.New(raft.Config{
		ID:             "n1",
		Peers:          []transport.NodeID{"n1"},
		Transport:      newFakeTransport("n1", nil),
		Log:            rlog,
		ElectionTicks:  10,
		HeartbeatTicks: 3,
		StateMachine:   sm,
	})
	if err != nil {
		t.Fatalf("raft.New = %v, want reconciliation (Reset catch-up)", err)
	}
	// Loop running so Status is answerable; no ticks → stays a follower.
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	runErr := make(chan error, 1)
	go func() { runErr <- n.Run(runCtx) }()

	if st := n.Status(); st.LastApplied != 10 || st.CommitIndex != 10 {
		t.Fatalf("position after recovery = %d/%d, want 10/10 (snapshot adopted)", st.CommitIndex, st.LastApplied)
	}
	if rlog.FirstIndex() != 11 || rlog.Count() != 0 {
		t.Fatalf("log after recovery: first=%d count=%d, want 11/0 (old entries discarded)", rlog.FirstIndex(), rlog.Count())
	}
	if got, err := rlog.Term(10); err != nil || got != 3 {
		t.Fatalf("boundary Term(10) = (%d, %v), want (3, nil)", got, err)
	}
	stop()
	<-runErr
}

// TestInstallSnapshotCatchesFarBehindFollower is spec §16's scenario end
// to end over real gRPC: a follower goes offline, the cluster writes far
// past it (many snapshot windows — the entries it would need are compacted
// away), the follower restarts and is caught up by InstallSnapshot, then
// continues with the tail through normal appends.
//
// The proof that installation — not entry catch-up — did the work: the
// follower's log is rebased BEYOND every index it ever held, and its
// sidecar's position lies above its pre-death log end.
func TestInstallSnapshotCatchesFarBehindFollower(t *testing.T) {
	nodes := startClusterSnap(t, 3, 2) // snapshot window: 2 applied entries
	ctx := context.Background()
	leader := waitForLeadership(t, 10*time.Second, "initial leader", nodes)

	// Pick a follower and confirm a match > 0 first: the leader then
	// installs directly (match+1 below its compaction point) instead of
	// probing down one reject per round trip.
	var follower *liveNode
	for _, n := range nodes {
		if n.id != leader.id {
			follower = n
			break
		}
	}
	// Pre-kill writes: retry on leadership loss the way a client would —
	// under package-wide load (e.g. the Phase 15 chaos tests) a heartbeat
	// gap can churn leadership between the two proposes, and the test's
	// intent is "two committed writes", not "leadership never moves".
	for i := 0; i < 2; i++ {
		deadline := time.Now().Add(10 * time.Second)
		for {
			_, _, err := leader.rnode.Propose(ctx, mustEncode(t,
				kv.Set(fmt.Sprintf("w%d", i), []byte("pre"))))
			if err == nil {
				break
			}
			if !errors.Is(err, raft.ErrLeadershipLost) || time.Now().After(deadline) {
				t.Fatalf("pre-kill propose %d: %v", i, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	waitFor(t, 5*time.Second, "follower synced before it dies", func() bool {
		return follower.rnode.Status().LastApplied >= 3
	})

	preLogLast := follower.rlog.LastIndex()
	follower.kill()

	// Long outage: 40 writes against a window of 2 — the leader compacts
	// well past the follower's entire history.
	for i := 0; i < 40; i++ {
		if _, _, err := leader.rnode.Propose(ctx, mustEncode(t,
			kv.Set(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i))))); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	wantApplied := leader.rnode.Status().LastApplied
	waitFor(t, 10*time.Second, "leader compacts past the dead follower's log", func() bool {
		return leader.rlog.FirstIndex() > preLogLast+1
	})

	restartNode(t, follower)

	// Install + catch-up: sidecar beyond the follower's old history, log
	// rebased past every index it ever held, state machine rebuilt from
	// the payload (early keys readable), tail applied on top.
	waitFor(t, 15*time.Second, "follower installs the snapshot and catches up", func() bool {
		meta, _, err := follower.rlog.LoadSnapshot()
		if err != nil || meta.LastIncludedIndex <= preLogLast {
			return false // not installed yet (its own sidecar is older)
		}
		if follower.rlog.FirstIndex() <= preLogLast+1 {
			return false // log must be rebased beyond its own history
		}
		if v, err := follower.engine.Get("k0"); err != nil || string(v) != "v0" {
			return false
		}
		return follower.rnode.Status().LastApplied >= wantApplied
	})

	// The cluster keeps working THROUGH the install: more writes replicate
	// to the rejoined follower. (Re-read leadership defensively — an
	// election racing the restart is rare but legal.)
	leader = waitForLeadership(t, 10*time.Second, "leader stable after reinstall", nodes)
	for i := 40; i < 44; i++ {
		if _, _, err := leader.rnode.Propose(ctx, mustEncode(t,
			kv.Set(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i))))); err != nil {
			t.Fatalf("post-catch-up propose %d: %v", i, err)
		}
	}
	high := leader.rnode.Status().LastApplied
	waitFor(t, 10*time.Second, "all replicas converge past the install", func() bool {
		for _, n := range nodes {
			if n.rnode.Status().LastApplied < high {
				return false
			}
			if v, err := n.engine.Get("k43"); err != nil || string(v) != "v43" {
				return false
			}
		}
		return true
	})
}

// TestInstallSnapshotViaRejectWalkDown: the leader has NO confirmed match
// for the follower (it died before the election — the no-op ack never
// landed), so the direct-install shortcut can't fire. The reject walk-down
// must carry prev below the compaction point, where the second trigger
// installs — and a follower with a completely EMPTY log adopts the
// snapshot wholesale (Reset from a fresh boundary). The same restart
// checks as above apply.
func TestInstallSnapshotViaRejectWalkDown(t *testing.T) {
	nodes := startClusterSnap(t, 3, 2)
	ctx := context.Background()

	// Kill BEFORE any election: this node never acknowledges anything, so
	// the eventual leader's progress.match for it stays 0 throughout.
	nodes[2].kill()
	leader := waitForLeadership(t, 10*time.Second, "leader with a dead peer", nodes)

	for i := 0; i < 40; i++ {
		if _, _, err := leader.rnode.Propose(ctx, mustEncode(t,
			kv.Set(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i))))); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	wantApplied := leader.rnode.Status().LastApplied
	if leader.rlog.FirstIndex() <= 1 {
		t.Fatalf("leader FirstIndex = %d, want compaction to have happened", leader.rlog.FirstIndex())
	}

	restartNode(t, nodes[2])
	waitFor(t, 15*time.Second, "walked-down follower installs the snapshot", func() bool {
		meta, _, err := nodes[2].rlog.LoadSnapshot()
		if err != nil || meta.LastIncludedIndex == 0 {
			return false // never led/applied ⇒ its own sidecar cannot exist
		}
		if nodes[2].rlog.FirstIndex() <= 1 {
			return false
		}
		if v, err := nodes[2].engine.Get("k0"); err != nil || string(v) != "v0" {
			return false
		}
		return nodes[2].rnode.Status().LastApplied >= wantApplied
	})

	// Fully rejoined: subsequent writes replicate to it. Re-read
	// leadership defensively — the restarting peer can bump terms for a
	// round or two while the leader's reconnect backoff elapses (legal
	// Raft churn, not what this test is about).
	leader = waitForLeadership(t, 10*time.Second, "leader stable after walk-down install", nodes)
	for i := 40; i < 42; i++ {
		if _, _, err := leader.rnode.Propose(ctx, mustEncode(t,
			kv.Set(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i))))); err != nil {
			t.Fatalf("post-install propose %d: %v", i, err)
		}
	}
	high := leader.rnode.Status().LastApplied
	waitFor(t, 10*time.Second, "converged after walk-down install", func() bool {
		for _, n := range nodes {
			if n.rnode.Status().LastApplied < high {
				return false
			}
			if v, err := n.engine.Get("k41"); err != nil || string(v) != "v41" {
				return false
			}
		}
		return true
	})
}
