package raft

// White-box election tests: the event loop's handlers are driven directly
// and synchronously, so assertions on state are exact — no polling, no
// timing. The node's Run goroutine stays parked in select (no channel events
// are ever injected), so the test goroutine is the only writer.

import (
	"context"
	"math/rand"
	"testing"
	"time"

	raftpb "distrikv/gen/raft/v1"
	"distrikv/internal/raftlog"
	"distrikv/internal/transport"
)

// nopTransport never responds: outbound RPCs park until the node's RPC
// context is cancelled at cleanup, so no stray events reach the loop.
type nopTransport struct{ id transport.NodeID }

func (t *nopTransport) LocalID() transport.NodeID { return t.id }

func (t *nopTransport) Ping(context.Context, transport.NodeID) error { return nil }

func (t *nopTransport) RequestVote(ctx context.Context, _ transport.NodeID, _ *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (t *nopTransport) AppendEntries(ctx context.Context, _ transport.NodeID, _ *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (t *nopTransport) Close() error { return nil }

// newTestNode builds a node whose loop is running but idle: tests mutate
// state only through handle(...) on their own goroutine.
func newTestNode(t *testing.T, peers ...transport.NodeID) *Node {
	t.Helper()
	rlog, _, err := raftlog.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open raftlog: %v", err)
	}
	n, err := New(Config{
		ID:             peers[0],
		Peers:          peers,
		Transport:      &nopTransport{id: peers[0]},
		Log:            rlog,
		ElectionTicks:  10,
		HeartbeatTicks: 3,
		RNG:            rand.New(rand.NewSource(42)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- n.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runErr:
		case <-time.After(2 * time.Second):
			t.Error("Run did not exit")
		}
		if err := rlog.Close(); err != nil {
			t.Errorf("close raftlog: %v", err)
		}
	})
	return n
}

// asCandidate puts the node into a fresh candidacy without going through
// ticks (state is loop-owned; the test goroutine IS the loop here). It
// persists term+vote exactly like startCampaign does — the durable-before-
// vote-RPC contract holds for this hand-built state too.
func asCandidate(t *testing.T, n *Node, term uint64) {
	t.Helper()
	n.role = RoleCandidate
	n.term = term
	n.votedFor = n.id
	n.leaderID = ""
	n.votes = map[transport.NodeID]bool{n.id: true}
	if err := n.persistHardState(); err != nil {
		t.Fatalf("persist hard state: %v", err)
	}
}

func grant(from transport.NodeID, term uint64) voteRespEvent {
	return voteRespEvent{
		from: from,
		resp: &raftpb.RequestVoteResponse{Term: term, VoteGranted: true},
	}
}

// A response from an older term must never influence the current campaign.
func TestStaleVoteResponseIgnored(t *testing.T) {
	n := newTestNode(t, "n1", "n2", "n3")
	asCandidate(t, n, 5)

	if err := n.handle(grant("n2", 4)); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if n.role != RoleCandidate || n.term != 5 || len(n.votes) != 1 {
		t.Fatalf("stale grant counted: role=%s term=%d votes=%d", n.role, n.term, len(n.votes))
	}
}

// One grant per peer per term: replayed/duplicated responses don't inflate
// the count toward majority.
func TestDuplicateGrantCountsOnce(t *testing.T) {
	n := newTestNode(t, "n1", "n2", "n3", "n4", "n5") // majority = 3
	asCandidate(t, n, 5)

	for i := 0; i < 3; i++ {
		if err := n.handle(grant("n2", 5)); err != nil {
			t.Fatalf("handle dup grant: %v", err)
		}
	}
	if n.role != RoleCandidate || len(n.votes) != 2 {
		t.Fatalf("duplicate grant inflated count: role=%s votes=%d", n.role, len(n.votes))
	}

	// One distinct second peer completes 3 of 5.
	if err := n.handle(grant("n4", 5)); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if n.role != RoleLeader || n.leaderID != "n1" {
		t.Fatalf("majority grant did not elect: role=%s leader=%q", n.role, n.leaderID)
	}
	// Self-vote (term + vote) was durable from the campaign.
	hs := n.rlog.HardState()
	if hs.Term != 5 || hs.VotedFor != "n1" {
		t.Fatalf("campaign hard state: %+v", hs)
	}
}

// A response advertising a higher term forces a step-down with the new term
// persisted and the old vote cleared — even if it claims to grant.
func TestHigherTermResponseStepDownClearsVote(t *testing.T) {
	n := newTestNode(t, "n1", "n2", "n3")
	asCandidate(t, n, 5)

	if err := n.handle(grant("n2", 9)); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if n.role != RoleFollower || n.term != 9 || n.votedFor != "" || n.votes != nil {
		t.Fatalf("step-down state: role=%s term=%d votedFor=%q votes=%v",
			n.role, n.term, n.votedFor, n.votes)
	}
	hs := n.rlog.HardState()
	if hs.Term != 9 || hs.VotedFor != "" {
		t.Fatalf("step-down not persisted: %+v", hs)
	}
}

// Errors from unreachable peers are absorbed: the campaign survives and
// still wins if the remaining majority grants.
func TestUnreachablePeerDoesNotBlockElection(t *testing.T) {
	n := newTestNode(t, "n1", "n2", "n3") // majority = 2: self + one
	asCandidate(t, n, 5)

	if err := n.handle(voteRespEvent{from: "n2", err: context.DeadlineExceeded}); err != nil {
		t.Fatalf("handle error response: %v", err)
	}
	if n.role != RoleCandidate || len(n.votes) != 1 {
		t.Fatalf("error response counted: role=%s votes=%d", n.role, len(n.votes))
	}
	if err := n.handle(grant("n3", 5)); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if n.role != RoleLeader {
		t.Fatalf("election lost after RPC error: role=%s", n.role)
	}
}

// A higher-term vote REQUEST adopts the term, clears the old vote, and only
// then applies the vote rules — all in one synchronous, durable step.
func TestHigherTermVoteRequestAdoptsTermAndClearsVote(t *testing.T) {
	n := newTestNode(t, "n1", "n2", "n3")
	// Cast a vote in term 3 first.
	if _, err := n.onVoteRequest(&raftpb.RequestVoteRequest{
		Term: 3, CandidateId: "n2", LastLogIndex: 0, LastLogTerm: 0,
	}); err != nil {
		t.Fatalf("setup vote: %v", err)
	}
	if got := n.rlog.HardState(); got.Term != 3 || got.VotedFor != "n2" {
		t.Fatalf("setup hard state: %+v", got)
	}

	// Newer term from a different candidate: term adopted, previous vote
	// forgotten (the old term's vote has no meaning in the new term).
	resp, err := n.onVoteRequest(&raftpb.RequestVoteRequest{
		Term: 4, CandidateId: "n3", LastLogIndex: 0, LastLogTerm: 0,
	})
	if err != nil {
		t.Fatalf("onVoteRequest: %v", err)
	}
	if !resp.VoteGranted || resp.Term != 4 {
		t.Fatalf("expected grant at term 4: %+v", resp)
	}
	if hs := n.rlog.HardState(); hs.Term != 4 || hs.VotedFor != "n3" {
		t.Fatalf("term adoption not persisted: %+v", hs)
	}
}
