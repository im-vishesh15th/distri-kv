package raft

// The commit rule in isolation (Raft §5.4.2, Figure 8): majority match is
// necessary but NOT sufficient — term(N) must equal the current term.
// Driven synchronously with no network, so the assertions are exact. Also
// covers the response-gating that protects match accounting.

import (
	"errors"
	"testing"

	"distrikv/internal/raftlog"
	"distrikv/internal/transport"
)

// asLeader puts a node into leadership for commit-rule tests without
// broadcasting (state is loop-owned; the test goroutine is the loop here).
func asLeader(t *testing.T, n *Group, term uint64, peers ...transport.NodeID) {
	t.Helper()
	n.role = RoleLeader
	n.leaderID = n.id
	n.term = term
	n.votedFor = n.id
	n.votes = nil
	n.commitIndex = 0
	n.progress = make(map[transport.NodeID]*peerProgress, len(peers))
	for _, p := range peers {
		if p == n.id {
			continue
		}
		n.progress[p] = &peerProgress{next: n.rlog.LastIndex() + 1}
	}
	if err := n.persistHardState(); err != nil {
		t.Fatalf("persist: %v", err)
	}
}

// Log: [1@T1, 2@T1, 3@T2]; we lead at term 2.
func TestCommitRuleRequiresCurrentTerm(t *testing.T) {
	n := newTestNode(t, "n1", "n2", "n3") // majorityN = 2
	if err := n.rlog.Append([]raftlog.Entry{
		{Index: 1, Term: 1, Payload: []byte("a")},
		{Index: 2, Term: 1, Payload: []byte("b")},
		{Index: 3, Term: 2, Payload: []byte("c")},
	}); err != nil {
		t.Fatalf("seed log: %v", err)
	}
	asLeader(t, n, 2, "n1", "n2", "n3")

	// Only self has index 3: majority match value is 0 → nothing.
	if err := n.maybeAdvanceCommit(); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if n.commitIndex != 0 {
		t.Fatalf("committed without peer acks: %d", n.commitIndex)
	}

	// Figure 8: a MAJORITY holds index 2 — but it's from term 1. Committing
	// it directly could later conflict with a leader from a divergent
	// history; it must wait for a current-term entry above it.
	n.progress["n2"].match = 2
	n.progress["n3"].match = 2
	if err := n.maybeAdvanceCommit(); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if n.commitIndex != 0 {
		t.Fatalf("prior-term entry committed directly: %d", n.commitIndex)
	}

	// One peer also holds the term-2 entry: now the majority match value is
	// 3, term(3)==current term → commit. Entries 1 and 2 commit with it,
	// exactly as Figure 8 prescribes (indirect commit).
	n.progress["n2"].match = 3
	if err := n.maybeAdvanceCommit(); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if n.commitIndex != 3 {
		t.Fatalf("current-term majority did not commit: %d", n.commitIndex)
	}

	// Commit index never moves backward.
	if err := n.maybeAdvanceCommit(); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if n.commitIndex != 3 {
		t.Fatalf("commit index regressed: %d", n.commitIndex)
	}
}

// A transport error from a PREVIOUS leadership epoch must not clear the
// current epoch's in-flight slot: overlapping RPCs to one peer would
// corrupt sentLast/match, and a false match feeds the commit rule.
func TestStaleAppendErrorIgnored(t *testing.T) {
	n := newTestNode(t, "n1", "n2", "n3")
	if err := n.rlog.Append([]raftlog.Entry{{Index: 1, Term: 1, Payload: []byte("a")}}); err != nil {
		t.Fatalf("seed log: %v", err)
	}
	asLeader(t, n, 2, "n1", "n2", "n3")

	prog := n.progress["n2"]
	prog.inflight = true
	prog.sentLast = 1

	// Error attributed to an older term: ignored entirely.
	if err := n.onAppendResponse(appRespEvent{from: "n2", term: 1, err: errTestNet}); err != nil {
		t.Fatalf("stale error: %v", err)
	}
	if !prog.inflight {
		t.Fatal("stale error cleared the current in-flight slot")
	}

	// Same error from the current term: the slot clears, next heartbeat
	// will retry.
	if err := n.onAppendResponse(appRespEvent{from: "n2", term: 2, err: errTestNet}); err != nil {
		t.Fatalf("current error: %v", err)
	}
	if prog.inflight {
		t.Fatal("current-term error did not clear in-flight")
	}
}

var errTestNet = errors.New("test network failure")
