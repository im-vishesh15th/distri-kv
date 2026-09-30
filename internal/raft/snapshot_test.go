package raft_test

// Phase 12 (spec §15): snapshots + log compaction over the real stack —
// creation triggers compaction, a restarted node resumes from the snapshot
// (state AND session table) plus the retained log tail, the crash window
// between SaveSnapshot and TruncatePrefix is reconciled at startup, and
// unrecoverable snapshot/log combinations refuse to start loudly.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"distrikv/internal/kv"
	"distrikv/internal/raft"
	"distrikv/internal/raftlog"
	"distrikv/internal/transport"
)

// sessionCmd stamps a session onto a command (mirrors what the service
// does per request; kv-side helper lives in the kv package's tests).
func sessionCmd(cmd kv.Command, clientID string, seq uint64) kv.Command {
	cmd.ClientID, cmd.Sequence = clientID, seq
	return cmd
}

// TestSnapshotCompactsLog: with SnapshotEvery=2 every replica captures its
// state machine and truncates the covered log prefix, and the cluster
// keeps replicating writes AFTER compaction (prevLogTerm anchors ride
// firstTerm at the boundary).
func TestSnapshotCompactsLog(t *testing.T) {
	nodes := startClusterSnap(t, 3, 2)
	ctx := context.Background()
	leader := waitForLeadership(t, 10*time.Second, "initial leader", nodes)

	for i := 0; i < 6; i++ {
		if _, _, err := leader.rnode.Propose(ctx, mustEncode(t,
			kv.Set(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i))))); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	wantApplied := leader.rnode.Status().LastApplied
	if wantApplied < 7 {
		t.Fatalf("leader applied = %d, want >= 7 (no-op + 6 writes)", wantApplied)
	}

	// Every node snapshots its applied prefix (its own window) and compacts.
	waitFor(t, 10*time.Second, "every node compacts its log", func() bool {
		for _, n := range nodes {
			if n.rlog.FirstIndex() <= 1 {
				return false
			}
			if n.rnode.Status().LastApplied < wantApplied {
				return false
			}
		}
		return true
	})
	for _, n := range nodes {
		st := n.rnode.Status()
		if n.rlog.Count() >= int(st.LastApplied) {
			t.Fatalf("%s: log holds %d entries at applied %d — prefix was not compacted",
				n.id, n.rlog.Count(), st.LastApplied)
		}
		// Boundary anchor: the last compacted index's term is servable
		// (lastIncludedTerm), below it ErrCompacted.
		first := n.rlog.FirstIndex()
		if _, err := n.rlog.Term(first - 1); err != nil {
			t.Fatalf("%s: Term(%d) at compaction boundary = %v, want the lastIncludedTerm",
				n.id, first-1, err)
		}
	}

	// The cluster still operates after compaction: more writes replicate
	// and converge on every replica (this crosses the compacted boundary
	// on the wire as heartbeats/appends anchor at firstIndex-1).
	for i := 6; i < 10; i++ {
		if _, _, err := leader.rnode.Propose(ctx, mustEncode(t,
			kv.Set(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i))))); err != nil {
			t.Fatalf("post-compaction propose %d: %v", i, err)
		}
	}
	high := leader.rnode.Status().LastApplied
	waitFor(t, 10*time.Second, "post-compaction writes converge", func() bool {
		for _, n := range nodes {
			if n.rnode.Status().LastApplied < high {
				return false
			}
			if v, err := n.engine.Get("k9"); err != nil || string(v) != "v9" {
				return false
			}
		}
		return true
	})
}

// TestRestartFromSnapshot is the spec's "restart from snapshot" +
// "recovery using snapshot + remaining log": five sessioned increments
// compact away, one tail entry survives, and after a crash+restart the
// node restores state AND dedup sessions from the snapshot, then replays
// the tail — indistinguishable from a node that never restarted.
func TestRestartFromSnapshot(t *testing.T) {
	nodes := startClusterSnap(t, 1, 2)
	n := nodes[0]
	ctx := context.Background()
	waitForLeadership(t, 10*time.Second, "standalone leader", nodes)

	// Entries 2..6: five sessioned increments (client-X seq 1..5).
	var origResults []kv.Result
	for seq := uint64(1); seq <= 5; seq++ {
		_, res, err := n.rnode.Propose(ctx, mustEncode(t,
			sessionCmd(kv.IncrBy("cnt", 1), "client-X", seq)))
		if err != nil {
			t.Fatalf("propose seq %d: %v", seq, err)
		}
		origResults = append(origResults, res.(kv.Result))
	}
	// Entry 7: the tail write that will remain AFTER compaction.
	if _, _, err := n.rnode.Propose(ctx, mustEncode(t, kv.Set("tail", []byte("7")))); err != nil {
		t.Fatalf("propose tail: %v", err)
	}

	// Snapshots fire at applied 2, 4, 6 (one propose per apply turn on a
	// standalone leader): compaction to 6 leaves only entry 7.
	waitFor(t, 5*time.Second, "log compacted past the increments", func() bool {
		return n.rlog.FirstIndex() == 7
	})
	if v, err := n.engine.Get("cnt"); err != nil || string(v) != "5" {
		t.Fatalf("cnt before crash = (%q, %v), want 5", v, err)
	}

	n.kill()
	restartNode(t, n)

	// State restored from the snapshot: the increments' entries are GONE
	// from the log (FirstIndex == 7), the engine is brand new, so cnt can
	// only have come from Restore. lastApplied resumes at the snapshot
	// position (>= 6; the post-restart election no-op may already apply).
	st := n.rnode.Status()
	if st.LastApplied < 6 {
		t.Fatalf("after restart lastApplied = %d, want >= 6 (snapshot position)", st.LastApplied)
	}
	if v, err := n.engine.Get("cnt"); err != nil || string(v) != "5" {
		t.Fatalf("cnt after restart = (%q, %v), want 5 (from snapshot)", v, err)
	}

	// Recovery using snapshot + remaining log: entry 7 replays once the
	// node re-elects and its commit watermark covers the tail.
	waitFor(t, 10*time.Second, "tail entry applies after restart", func() bool {
		v, err := n.engine.Get("tail")
		return err == nil && string(v) == "7"
	})

	// The session table rode the snapshot: a retry of client-X's last
	// applied sequence (5) replays the original response without
	// re-applying (cnt must not move).
	waitForLeadership(t, 10*time.Second, "re-elected leader", nodes)
	_, res, err := n.rnode.Propose(ctx, mustEncode(t,
		sessionCmd(kv.IncrBy("cnt", 1), "client-X", 5)))
	if err != nil {
		t.Fatalf("seq5 retry after restart: %v", err)
	}
	if got := res.(kv.Result); got.Value == nil || string(got.Value) != string(origResults[4].Value) {
		t.Fatalf("replayed result = %v, want original %+v", got, origResults[4])
	}
	if v, _ := n.engine.Get("cnt"); string(v) != "5" {
		t.Fatalf("cnt after retry = %q, want 5 (double-applied)", v)
	}

	// Session continuity: the NEXT sequence applies normally.
	if _, _, err := n.rnode.Propose(ctx, mustEncode(t,
		sessionCmd(kv.IncrBy("cnt", 1), "client-X", 6))); err != nil {
		t.Fatalf("seq6 after restart: %v", err)
	}
	if v, _ := n.engine.Get("cnt"); string(v) != "6" {
		t.Fatalf("cnt after seq6 = %q, want 6", v)
	}
}

// TestSnapshotCrashBeforeCompaction: the crash window between
// SaveSnapshot (durable) and TruncatePrefix (not yet run). The log still
// holds covered entries at restart; restoreSnapshot must reconcile by
// compacting them, then resume from the snapshot position.
func TestSnapshotCrashBeforeCompaction(t *testing.T) {
	nodes := startClusterSnap(t, 1, 0) // auto-snapshots off: manual save
	n := nodes[0]
	ctx := context.Background()
	waitForLeadership(t, 10*time.Second, "standalone leader", nodes)

	for i := 0; i < 4; i++ {
		if _, _, err := n.rnode.Propose(ctx, mustEncode(t,
			kv.Set(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i))))); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	idx := n.rnode.Status().LastApplied // 5 (no-op + 4 writes)
	term, err := n.rlog.Term(idx)
	if err != nil {
		t.Fatalf("Term(%d): %v", idx, err)
	}
	payload, err := n.sm.Snapshot()
	if err != nil {
		t.Fatalf("SM snapshot: %v", err)
	}
	if err := n.rlog.SaveSnapshot(raftlog.SnapshotMeta{
		LastIncludedIndex: idx, LastIncludedTerm: term,
	}, payload); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	// The window's signature: snapshot durable, log NOT compacted.
	if n.rlog.FirstIndex() != 1 {
		t.Fatalf("FirstIndex = %d before crash, want 1 (compaction has not run)", n.rlog.FirstIndex())
	}

	n.kill()
	restartNode(t, n)

	// Startup reconciles: covered entries truncated, position adopted.
	if got := n.rlog.FirstIndex(); got != idx+1 {
		t.Fatalf("after restart FirstIndex = %d, want %d (crash-window catch-up)", got, idx+1)
	}
	if st := n.rnode.Status(); st.LastApplied < idx {
		t.Fatalf("after restart lastApplied = %d, want >= %d", st.LastApplied, idx)
	}
	for i := 0; i < 4; i++ {
		v, err := n.engine.Get(fmt.Sprintf("k%d", i))
		if err != nil || string(v) != fmt.Sprintf("v%d", i) {
			t.Fatalf("k%d after restart = (%q, %v), want v%d (from snapshot)", i, v, err, i)
		}
	}

	// Still a working node after reconciliation.
	waitForLeadership(t, 10*time.Second, "re-elected leader", nodes)
	if _, _, err := n.rnode.Propose(ctx, mustEncode(t, kv.Set("after", []byte("ok")))); err != nil {
		t.Fatalf("post-recovery propose: %v", err)
	}
	if v, err := n.engine.Get("after"); err != nil || string(v) != "ok" {
		t.Fatalf("post-recovery write = (%q, %v)", v, err)
	}
}

// TestNewRefusesBrokenSnapshotStates: two snapshot/log combinations that
// cannot be recovered safely must fail raft.New loudly — running anyway
// would fabricate a state machine that diverges from the cluster.
func TestNewRefusesBrokenSnapshotStates(t *testing.T) {
	newCfg := func(dir string, sm raft.StateMachine) (raft.Config, func()) {
		rlog, _, err := raftlog.Open(dir)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		return raft.Config{
			ID:             "n1",
			Peers:          []transport.NodeID{"n1"},
			Transport:      newFakeTransport("n1", nil),
			Log:            rlog,
			ElectionTicks:  10,
			HeartbeatTicks: 3,
			StateMachine:   sm,
		}, func() { _ = rlog.Close() }
	}

	t.Run("snapshot without a restorable state machine", func(t *testing.T) {
		cfg, cleanup := newCfg(t.TempDir(), &recordingSM{}) // no Snapshot()
		defer cleanup()
		if err := cfg.Log.SaveSnapshot(raftlog.SnapshotMeta{
			LastIncludedIndex: 3, LastIncludedTerm: 1,
		}, []byte("state")); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}
		_, err := raft.New(cfg)
		if err == nil || !strings.Contains(err.Error(), "cannot restore snapshots") {
			t.Fatalf("raft.New = %v, want a refusal to start", err)
		}
	})

	t.Run("snapshot beyond the log's retained start (lost tail)", func(t *testing.T) {
		cfg, cleanup := newCfg(t.TempDir(), kv.NewSM(kv.NewMemEngine()))
		defer cleanup()
		// Entries 1..15, then a snapshot at 3 — but compaction discards up
		// to 14, leaving a gap: 4..14 no longer exist anywhere.
		for i := uint64(1); i <= 15; i++ {
			if err := cfg.Log.Append([]raftlog.Entry{{Index: i, Term: 1, Payload: []byte("x")}}); err != nil {
				t.Fatalf("append %d: %v", i, err)
			}
		}
		if err := cfg.Log.SaveSnapshot(raftlog.SnapshotMeta{
			LastIncludedIndex: 3, LastIncludedTerm: 1,
		}, []byte("state")); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}
		if err := cfg.Log.TruncatePrefix(14); err != nil {
			t.Fatalf("TruncatePrefix: %v", err)
		}
		_, err := raft.New(cfg)
		if err == nil || !strings.Contains(err.Error(), "lost") {
			t.Fatalf("raft.New = %v, want a lost-entries refusal", err)
		}
	})
}
