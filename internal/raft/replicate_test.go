package raft_test

// Phase 6 replication tests: proposals flow to followers, commit requires a
// majority, the Figure-8 term rule holds, and rejection drives nextIndex
// back to the match point. Same harness as the election tests — manual
// ticks, scripted transport — so every ordering claim is deterministic.

import (
	"context"
	"testing"
	"time"

	raftpb "distrikv/gen/raft/v1"
	"distrikv/internal/raft"
	"distrikv/internal/raftlog"
	"distrikv/internal/transport"
)

// electLeader drives h's node through campaign and grants from every other
// peer, then completes the leader's no-op round (all peers ack it) so the
// node sits at CommitIndex == 1, ready for proposals.
func electLeader(t *testing.T, h *hnode, peers ...transport.NodeID) {
	t.Helper()
	h.tickN(20)
	waitFor(t, 2*time.Second, "campaign", func() bool {
		return h.n.Status().Role == raft.RoleCandidate
	})
	collectVoteCalls(t, h.tr.voteCalls, len(peers))
	for _, p := range peers {
		h.tr.voteReplies[p] <- voteResult{resp: &raftpb.RequestVoteResponse{Term: 1, VoteGranted: true}}
	}
	waitFor(t, 2*time.Second, "leader", func() bool {
		return h.n.Status().Role == raft.RoleLeader
	})
	// No-op round: one AppendEntries per peer carrying entry 1.
	calls := collectAppCalls(t, h.tr.appCalls, len(peers))
	for _, c := range calls {
		if c.req.PrevLogIndex != 0 || len(c.req.Entries) != 1 ||
			c.req.Entries[0].Index != 1 || len(c.req.Entries[0].Payload) != 0 {
			t.Fatalf("bad no-op round call: %+v", c.req)
		}
		h.tr.appReplies[c.to] <- appResult{resp: &raftpb.AppendEntriesResponse{Term: 1, Success: true}}
	}
	waitFor(t, 2*time.Second, "no-op committed", func() bool {
		return h.n.Status().CommitIndex == 1
	})
}

// collectAppCalls waits for n outbound AppendEntries and returns them.
func collectAppCalls(t *testing.T, ch chan appCall, n int) []appCall {
	t.Helper()
	out := make([]appCall, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, recvAppCall(t, ch))
	}
	return out
}

// asyncPropose runs Propose on its own goroutine and returns its result.
func asyncPropose(h *hnode, payload string) chan proposeResult {
	res := make(chan proposeResult, 1)
	go func() {
		idx, result, err := h.n.Propose(context.Background(), []byte(payload))
		res <- proposeResult{idx: idx, result: result, err: err}
	}()
	return res
}

type proposeResult struct {
	idx    uint64
	result any
	err    error
}

func awaitPropose(t *testing.T, res chan proposeResult) proposeResult {
	t.Helper()
	select {
	case r := <-res:
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Propose to commit")
		return proposeResult{}
	}
}

// A proposal reaches the leader's log, ships to followers, commits on one
// follower ack (majority of three), and the commit watermark rides the next
// heartbeat.
func TestLeaderReplicatesAndCommits(t *testing.T) {
	h := startNode(t, "n1", 1, "n1", "n2", "n3")
	electLeader(t, h, "n2", "n3")

	res := asyncPropose(h, "hello")

	// Both peers receive the entry with the correct prevLog anchor.
	calls := collectAppCalls(t, h.tr.appCalls, 2)
	for _, c := range calls {
		if c.req.PrevLogIndex != 1 || len(c.req.Entries) != 1 ||
			c.req.Entries[0].Index != 2 || string(c.req.Entries[0].Payload) != "hello" {
			t.Fatalf("bad replication call to %s: %+v", c.to, c.req)
		}
	}

	// One follower ack is a majority of 3 (leader's own synced copy + one).
	h.tr.appReplies["n2"] <- appResult{resp: &raftpb.AppendEntriesResponse{Term: 1, Success: true}}
	r := awaitPropose(t, res)
	if r.err != nil || r.idx != 2 {
		t.Fatalf("propose: idx=%d err=%v", r.idx, r.err)
	}
	waitFor(t, 2*time.Second, "commit index 2", func() bool {
		return h.n.Status().CommitIndex == 2
	})

	e, err := h.rlog.Get(2)
	if err != nil || string(e.Payload) != "hello" || e.Term != 1 {
		t.Fatalf("committed entry: %+v err=%v", e, err)
	}

	// The commit watermark propagates on the next heartbeat.
	h.tickN(3)
	hb := recvAppCall(t, h.tr.appCalls)
	if hb.req.LeaderCommit != 2 || hb.req.PrevLogIndex != 2 || len(hb.req.Entries) != 0 {
		t.Fatalf("heartbeat does not carry commit: %+v", hb.req)
	}
}

// A majority of acks is required: one ack of five nodes commits nothing;
// the second ack completes the majority.
func TestCommitRequiresMajority(t *testing.T) {
	peers := []transport.NodeID{"n1", "n2", "n3", "n4", "n5"}
	h := startNode(t, "n1", 7, peers...)
	electLeader(t, h, "n2", "n3", "n4", "n5") // majorityN = 3

	res := asyncPropose(h, "x")
	calls := collectAppCalls(t, h.tr.appCalls, 4)
	for _, c := range calls {
		if len(c.req.Entries) != 1 || c.req.Entries[0].Index != 2 {
			t.Fatalf("bad proposal call: %+v", c.req)
		}
	}

	// Single ack: leader + 1 follower = 2 of 5 — below majority. Nothing
	// may commit, no matter how long we wait.
	h.tr.appReplies["n2"] <- appResult{resp: &raftpb.AppendEntriesResponse{Term: 1, Success: true}}
	assertFor(t, 150*time.Millisecond, "committed without majority", func() bool {
		return h.n.Status().CommitIndex == 1
	})
	select {
	case r := <-res:
		t.Fatalf("propose returned without majority: %+v", r)
	default:
	}

	// Second ack completes 3 of 5.
	h.tr.appReplies["n3"] <- appResult{resp: &raftpb.AppendEntriesResponse{Term: 1, Success: true}}
	r := awaitPropose(t, res)
	if r.err != nil || r.idx != 2 {
		t.Fatalf("propose after majority: idx=%d err=%v", r.idx, r.err)
	}
	waitFor(t, 2*time.Second, "commit index 2", func() bool {
		return h.n.Status().CommitIndex == 2
	})
}

// Propose validation: followers reject with ErrNotLeader; empty payloads
// are reserved for internal no-ops.
func TestProposeValidation(t *testing.T) {
	h := startNode(t, "n1", 1, "n1", "n2", "n3")

	if _, _, err := h.n.Propose(context.Background(), nil); err != raft.ErrEmptyProposal {
		t.Fatalf("empty proposal: %v", err)
	}
	// Node is a follower (never ticked): not-leader rejection.
	if _, _, err := h.n.Propose(context.Background(), []byte("x")); err != raft.ErrNotLeader {
		t.Fatalf("follower proposal: %v", err)
	}
}

// A proposal in flight when the node loses leadership fails fast with
// ErrLeadershipLost — its fate is unknowable from here.
func TestPendingProposalFailsOnLeadershipLoss(t *testing.T) {
	h := startNode(t, "n1", 1, "n1", "n2", "n3")
	electLeader(t, h, "n2", "n3")

	res := asyncPropose(h, "doomed")
	// Both replication calls prove the entry was appended and the waiter
	// registered before we pull the rug.
	collectAppCalls(t, h.tr.appCalls, 2)

	// The in-flight RPC goroutines are parked on appReplies — have n2
	// answer with a higher term: step-down, waiters fail.
	h.tr.appReplies["n2"] <- appResult{resp: &raftpb.AppendEntriesResponse{Term: 9, Success: false}}

	r := awaitPropose(t, res)
	if r.err != raft.ErrLeadershipLost {
		t.Fatalf("expected ErrLeadershipLost, got %+v", r)
	}
	if s := h.n.Status(); s.Role != raft.RoleFollower || s.Term != 9 {
		t.Fatalf("not stepped down: %+v", s)
	}
}

// Rejected replication backs nextIndex down one index per rejection and
// retries — the resend must anchor at the follower's actual log, not guess.
func TestNextIndexBacksOffOnReject(t *testing.T) {
	h := startNode(t, "n1", 1, "n1", "n2", "n3")
	electLeader(t, h, "n2", "n3")

	res := asyncPropose(h, "y")
	collectAppCalls(t, h.tr.appCalls, 2) // both at prevLogIndex=1

	// n2 rejects: it doesn't have entry 2's anchor... leader walks next
	// from 2 back to 1 and resends the full range from prevLogIndex=0.
	h.tr.appReplies["n2"] <- appResult{resp: &raftpb.AppendEntriesResponse{Term: 1, Success: false}}
	retry := recvAppCall(t, h.tr.appCalls)
	if retry.to != "n2" {
		t.Fatalf("retry went to %s, want n2", retry.to)
	}
	if retry.req.PrevLogIndex != 0 || len(retry.req.Entries) != 2 ||
		retry.req.Entries[0].Index != 1 || retry.req.Entries[1].Index != 2 {
		t.Fatalf("backoff resend not anchored at follower's log: %+v", retry.req)
	}

	// Now n2 accepts: majority completes.
	h.tr.appReplies["n2"] <- appResult{resp: &raftpb.AppendEntriesResponse{Term: 1, Success: true}}
	r := awaitPropose(t, res)
	if r.err != nil || r.idx != 2 {
		t.Fatalf("propose: idx=%d err=%v", r.idx, r.err)
	}
}

// Follower-side log handling: divergent suffixes are replaced, rejected
// requests never mutate the log, extensions append and fsync, LeaderCommit
// advances commitIndex but never past what we hold — and malformed requests
// are refused untouched.
func TestFollowerLogReplication(t *testing.T) {
	h := startNode(t, "n1", 1, "n1", "n2", "n3") // follower (no ticks)
	ctx := context.Background()

	// Our history: three entries from term 1.
	if err := h.rlog.Append([]raftlog.Entry{
		{Index: 1, Term: 1, Payload: []byte("a")},
		{Index: 2, Term: 1, Payload: []byte("b")},
		{Index: 3, Term: 1, Payload: []byte("c")},
	}); err != nil {
		t.Fatalf("seed log: %v", err)
	}

	// (a) Divergent replacement: leader (term 5) rewrote 2 and 3.
	r, err := h.n.HandleAppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term: 5, LeaderId: "n2", PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []*raftpb.LogEntry{
			{Index: 2, Term: 5, Payload: []byte("X")},
			{Index: 3, Term: 5, Payload: []byte("Y")},
		},
		LeaderCommit: 3,
	})
	if err != nil || !r.Success {
		t.Fatalf("divergent replace: %+v err=%v", r, err)
	}
	for idx, want := range []struct {
		term    uint64
		payload string
	}{{1, "a"}, {5, "X"}, {5, "Y"}} {
		e, err := h.rlog.Get(uint64(idx + 1))
		if err != nil || e.Term != want.term || string(e.Payload) != want.payload {
			t.Fatalf("entry %d after replace: %+v err=%v (want %+v)", idx+1, e, err, want)
		}
	}
	if s := h.n.Status(); s.CommitIndex != 3 || s.LeaderID != "n2" {
		t.Fatalf("state after replace: %+v", s)
	}

	// (b) A rejected consistency check must NOT truncate: ask for prev
	// (2, term 1) — our entry 2 is now term 5, so it mismatches.
	r, err = h.n.HandleAppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term: 5, LeaderId: "n2", PrevLogIndex: 2, PrevLogTerm: 1,
		Entries: []*raftpb.LogEntry{{Index: 3, Term: 5, Payload: []byte("EVIL")}},
	})
	if err != nil || r.Success {
		t.Fatalf("mismatched prev accepted: %+v err=%v", r, err)
	}
	if e, err := h.rlog.Get(3); err != nil || string(e.Payload) != "Y" || h.rlog.LastIndex() != 3 {
		t.Fatalf("log mutated on reject: %+v err=%v", e, err)
	}

	// (c) Extension: new entry 4; LeaderCommit beyond our log clamps to
	// what we actually hold.
	r, err = h.n.HandleAppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term: 5, LeaderId: "n2", PrevLogIndex: 3, PrevLogTerm: 5,
		Entries:      []*raftpb.LogEntry{{Index: 4, Term: 5, Payload: []byte("Z")}},
		LeaderCommit: 99,
	})
	if err != nil || !r.Success {
		t.Fatalf("extension: %+v err=%v", r, err)
	}
	if h.rlog.LastIndex() != 4 {
		t.Fatalf("last index after extension: %d", h.rlog.LastIndex())
	}
	if s := h.n.Status(); s.CommitIndex != 4 {
		t.Fatalf("commit clamp: %d, want 4", s.CommitIndex)
	}

	// (d) Malformed entry indexes: refused without mutation.
	r, err = h.n.HandleAppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term: 5, LeaderId: "n2", PrevLogIndex: 4, PrevLogTerm: 5,
		Entries: []*raftpb.LogEntry{{Index: 9, Term: 5, Payload: []byte("JUMP")}},
	})
	if err != nil || r.Success {
		t.Fatalf("malformed entries accepted: %+v err=%v", r, err)
	}
	if h.rlog.LastIndex() != 4 {
		t.Fatalf("log mutated by malformed request: %d", h.rlog.LastIndex())
	}
}
