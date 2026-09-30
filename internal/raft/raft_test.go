package raft_test

// Phase 5 election tests that drive the real event loop from outside:
// manual Tick() calls for time, a scripted fake transport for the network.
// Deterministic assertions only rely on "after N ticks X must/must-not have
// happened" — no wall-clock races on the core state machine.

import (
	"context"
	"math/rand"
	"testing"
	"time"

	raftpb "distrikv/gen/raft/v1"
	"distrikv/internal/raft"
	"distrikv/internal/raftlog"
	"distrikv/internal/transport"
)

// --- fake transport ---

type voteCall struct {
	to  transport.NodeID
	req *raftpb.RequestVoteRequest
}

type appCall struct {
	to  transport.NodeID
	req *raftpb.AppendEntriesRequest
}

type voteResult struct {
	resp *raftpb.RequestVoteResponse
	err  error
}

type appResult struct {
	resp *raftpb.AppendEntriesResponse
	err  error
}

// fakeTransport records outbound RPCs and returns replies the test scripts
// per peer. A peer with no queued reply blocks until the node's RPC context
// is cancelled — i.e. it simulates an unreachable peer.
type fakeTransport struct {
	id          transport.NodeID
	voteCalls   chan voteCall
	appCalls    chan appCall
	voteReplies map[transport.NodeID]chan voteResult
	appReplies  map[transport.NodeID]chan appResult
}

func newFakeTransport(id transport.NodeID, peers []transport.NodeID) *fakeTransport {
	f := &fakeTransport{
		id:          id,
		voteCalls:   make(chan voteCall, 256),
		appCalls:    make(chan appCall, 256),
		voteReplies: make(map[transport.NodeID]chan voteResult),
		appReplies:  make(map[transport.NodeID]chan appResult),
	}
	for _, p := range peers {
		if p == id {
			continue
		}
		f.voteReplies[p] = make(chan voteResult, 8)
		f.appReplies[p] = make(chan appResult, 8)
	}
	return f
}

func (f *fakeTransport) LocalID() transport.NodeID { return f.id }

func (f *fakeTransport) Ping(context.Context, transport.NodeID) error { return nil }

func (f *fakeTransport) RequestVote(ctx context.Context, to transport.NodeID, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	select {
	case f.voteCalls <- voteCall{to: to, req: req}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	ch, ok := f.voteReplies[to]
	if !ok {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	select {
	case r := <-ch:
		return r.resp, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *fakeTransport) AppendEntries(ctx context.Context, to transport.NodeID, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	select {
	case f.appCalls <- appCall{to: to, req: req}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	ch, ok := f.appReplies[to]
	if !ok {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	select {
	case r := <-ch:
		return r.resp, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *fakeTransport) Close() error { return nil }

// --- harness ---

type hnode struct {
	n      *raft.Node
	tr     *fakeTransport
	rlog   *raftlog.Log
	runErr chan error
}

// startNode boots one Raft node with a scripted transport and manual time.
// ElectionTicks=10 → randomized timeout in [10, 19] ticks; HeartbeatTicks=3.
func startNode(t *testing.T, id transport.NodeID, seed int64, peers ...transport.NodeID) *hnode {
	t.Helper()
	return startNodeSM(t, id, seed, nil, peers...)
}

// startNodeSM is startNode with a state machine attached (Phase 7). A nil
// sm means apply is a no-op — the Phase 5/6 configuration.
func startNodeSM(t *testing.T, id transport.NodeID, seed int64, sm raft.StateMachine, peers ...transport.NodeID) *hnode {
	t.Helper()
	rlog, _, err := raftlog.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open raftlog: %v", err)
	}
	tr := newFakeTransport(id, peers)
	n, err := raft.New(raft.Config{
		ID:             id,
		Peers:          peers,
		Transport:      tr,
		Log:            rlog,
		ElectionTicks:  10,
		HeartbeatTicks: 3,
		RNG:            rand.New(rand.NewSource(seed)),
		StateMachine:   sm,
	})
	if err != nil {
		t.Fatalf("raft.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- n.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runErr:
		case <-time.After(2 * time.Second):
			t.Error("raft Run did not exit")
		}
		if err := rlog.Close(); err != nil {
			t.Errorf("close raftlog: %v", err)
		}
	})
	return &hnode{n: n, tr: tr, rlog: rlog, runErr: runErr}
}

func (h *hnode) tickN(k int) {
	for i := 0; i < k; i++ {
		h.n.Tick()
	}
}

// waitFor polls cond until true or fails the test.
func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout %v waiting for %s", d, what)
}

// assertFor requires cond to hold at every sample for the whole duration —
// used for "never happens" properties.
func assertFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if !cond() {
			t.Fatalf("assertion failed during %v window: %s", d, what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func recvVoteCall(t *testing.T, ch chan voteCall) voteCall {
	t.Helper()
	select {
	case c := <-ch:
		return c
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for outbound RequestVote")
		return voteCall{}
	}
}

func recvAppCall(t *testing.T, ch chan appCall) appCall {
	t.Helper()
	select {
	case c := <-ch:
		return c
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for outbound AppendEntries")
		return appCall{}
	}
}

// collectVoteCalls waits for n distinct vote RPCs and returns them by peer.
func collectVoteCalls(t *testing.T, ch chan voteCall, n int) map[transport.NodeID]*raftpb.RequestVoteRequest {
	t.Helper()
	got := make(map[transport.NodeID]*raftpb.RequestVoteRequest, n)
	for i := 0; i < n; i++ {
		c := recvVoteCall(t, ch)
		got[c.to] = c.req
	}
	if len(got) != n {
		t.Fatalf("expected %d distinct vote targets, got %v", n, got)
	}
	return got
}

func isLeader(n *raft.Node) bool { return n.Status().Role == raft.RoleLeader }

// --- tests ---

// A single-node cluster elects itself after its election timeout.
func TestSingleNodeElectsItself(t *testing.T) {
	h := startNode(t, "n1", 1, "n1")

	h.tickN(20) // timeout ∈ [10,19] → guaranteed at least one campaign
	waitFor(t, 2*time.Second, "single node leader", func() bool {
		s := h.n.Status()
		return s.Role == raft.RoleLeader && s.Term == 1 && s.LeaderID == "n1"
	})
	// No peers → no outbound vote RPCs ever.
	select {
	case c := <-h.tr.voteCalls:
		t.Fatalf("unexpected vote RPC to %s", c.to)
	default:
	}
}

// No campaign before the minimum timeout; campaign (term bump + vote RPCs
// with correct fields) by the maximum timeout.
func TestCampaignBoundsAndVoteRPCFields(t *testing.T) {
	h := startNode(t, "n1", 1, "n1", "n2", "n3")

	// Minimum randomized timeout is 10 ticks: 9 ticks can never campaign.
	h.tickN(9)
	assertFor(t, 50*time.Millisecond, "campaigned before minimum timeout", func() bool {
		s := h.n.Status()
		return s.Role == raft.RoleFollower && s.Term == 0
	})

	// Maximum is 19: by tick 20 the campaign must have happened.
	h.tickN(11)
	waitFor(t, 2*time.Second, "candidate after max timeout", func() bool {
		s := h.n.Status()
		return s.Role == raft.RoleCandidate && s.Term == 1
	})

	calls := collectVoteCalls(t, h.tr.voteCalls, 2)
	for _, peer := range []transport.NodeID{"n2", "n3"} {
		req, ok := calls[peer]
		if !ok {
			t.Fatalf("no vote RPC to %s", peer)
		}
		if req.Term != 1 || req.CandidateId != "n1" || req.LastLogIndex != 0 || req.LastLogTerm != 0 {
			t.Fatalf("bad vote request to %s: %+v", peer, req)
		}
	}

	// Self-vote and term are durable before any response could exist.
	hs := h.rlog.HardState()
	if hs.Term != 1 || hs.VotedFor != "n1" {
		t.Fatalf("hard state not persisted before vote RPCs: %+v", hs)
	}
}

// Majority of grants → leader; leadership asserted with an immediate
// empty heartbeat.
func TestMajorityGrantsElectLeader(t *testing.T) {
	h := startNode(t, "n1", 1, "n1", "n2", "n3")
	h.tickN(20)
	waitFor(t, 2*time.Second, "campaign", func() bool {
		return h.n.Status().Role == raft.RoleCandidate
	})
	collectVoteCalls(t, h.tr.voteCalls, 2)

	h.tr.voteReplies["n2"] <- voteResult{resp: &raftpb.RequestVoteResponse{Term: 1, VoteGranted: true}}
	h.tr.voteReplies["n3"] <- voteResult{resp: &raftpb.RequestVoteResponse{Term: 1, VoteGranted: true}}

	waitFor(t, 2*time.Second, "leader after majority grants", func() bool {
		s := h.n.Status()
		return s.Role == raft.RoleLeader && s.Term == 1 && s.LeaderID == "n1"
	})

	// Immediate replication asserts authority: the term-1 no-op entry (one
	// entry, empty payload) rides the first AppendEntries.
	hb := recvAppCall(t, h.tr.appCalls)
	if hb.to != "n2" && hb.to != "n3" {
		t.Fatalf("heartbeat to unknown peer %s", hb.to)
	}
	if hb.req.Term != 1 || hb.req.LeaderId != "n1" {
		t.Fatalf("bad heartbeat header: %+v", hb.req)
	}
	if len(hb.req.Entries) != 1 {
		t.Fatalf("expected the leader no-op entry, got %d entries", len(hb.req.Entries))
	}
	if e := hb.req.Entries[0]; e.Index != 1 || e.Term != 1 || len(e.Payload) != 0 {
		t.Fatalf("bad no-op entry: %+v", e)
	}
}

// A minority of grants never elects; the candidate re-campaigns at a higher
// term instead.
func TestMinorityNeverElects(t *testing.T) {
	peers := []transport.NodeID{"n1", "n2", "n3", "n4", "n5"}
	h := startNode(t, "n1", 7, peers...)

	h.tickN(20)
	calls := collectVoteCalls(t, h.tr.voteCalls, 4) // term 1 campaign
	_ = calls

	// Only one grant: 2 of 5 votes — below the 3-vote majority.
	h.tr.voteReplies["n2"] <- voteResult{resp: &raftpb.RequestVoteResponse{Term: 1, VoteGranted: true}}
	assertFor(t, 100*time.Millisecond, "minority elected a leader", func() bool {
		return !isLeader(h.n)
	})

	// Next timeout: new campaign at a strictly higher term.
	h.tickN(20)
	waitFor(t, 2*time.Second, "re-campaign at higher term", func() bool {
		s := h.n.Status()
		return s.Role == raft.RoleCandidate && s.Term >= 2
	})
	for i := 0; i < 4; i++ {
		c := recvVoteCall(t, h.tr.voteCalls)
		if c.req.Term < 2 {
			t.Fatalf("re-campaign reused stale term: %+v", c.req)
		}
	}
	assertFor(t, 50*time.Millisecond, "minority leader persisted", func() bool {
		return !isLeader(h.n)
	})
}

// A vote response advertising a higher term steps the candidate down and
// clears (durably) its vote.
func TestHigherTermResponseStepsDown(t *testing.T) {
	h := startNode(t, "n1", 1, "n1", "n2", "n3")
	h.tickN(20)
	waitFor(t, 2*time.Second, "campaign", func() bool {
		return h.n.Status().Role == raft.RoleCandidate
	})
	collectVoteCalls(t, h.tr.voteCalls, 2)

	h.tr.voteReplies["n2"] <- voteResult{resp: &raftpb.RequestVoteResponse{Term: 9, VoteGranted: true}}

	waitFor(t, 2*time.Second, "step down to term 9", func() bool {
		s := h.n.Status()
		return s.Role == raft.RoleFollower && s.Term == 9
	})
	hs := h.rlog.HardState()
	if hs.Term != 9 || hs.VotedFor != "" {
		t.Fatalf("step-down hard state not persisted: %+v", hs)
	}
}

// Split votes resolve: with grants for nobody, the candidate times out and
// campaigns again; when a majority finally grants, one leader emerges.
func TestSplitVoteResolvesOnRetry(t *testing.T) {
	peers := []transport.NodeID{"n1", "n2", "n3", "n4", "n5"}
	h := startNode(t, "n1", 7, peers...)

	// Round 1: term 1 — grants from nobody (a pure split).
	h.tickN(20)
	collectVoteCalls(t, h.tr.voteCalls, 4)
	assertFor(t, 100*time.Millisecond, "leader without grants", func() bool {
		return !isLeader(h.n)
	})

	// Round 2: term ≥ 2 — now a majority (self + 3) grants.
	h.tickN(20)
	waitFor(t, 2*time.Second, "second campaign", func() bool {
		return h.n.Status().Role == raft.RoleCandidate && h.n.Status().Term >= 2
	})
	secondTerm := h.n.Status().Term
	for i := 0; i < 4; i++ {
		c := recvVoteCall(t, h.tr.voteCalls)
		// An earlier round-2 campaign's RPCs may still be queued; all must
		// be from this election round or later — never round 1.
		if c.req.Term < 2 {
			t.Fatalf("vote RPC reused stale term: %+v", c.req)
		}
	}
	for _, p := range []transport.NodeID{"n2", "n3", "n4"} {
		h.tr.voteReplies[p] <- voteResult{resp: &raftpb.RequestVoteResponse{Term: secondTerm, VoteGranted: true}}
	}
	waitFor(t, 2*time.Second, "leader after retry", func() bool {
		s := h.n.Status()
		return s.Role == raft.RoleLeader && s.Term == secondTerm
	})
}

// A granted vote is durable the moment the handler returns (the RPC
// response cannot outrun the disk), and it blocks any other candidate in
// the same term.
func TestVotePersistedBeforeResponse(t *testing.T) {
	h := startNode(t, "n1", 1, "n1", "n2", "n3")
	ctx := context.Background()

	resp, err := h.n.HandleRequestVote(ctx, &raftpb.RequestVoteRequest{
		Term: 5, CandidateId: "n2", LastLogIndex: 0, LastLogTerm: 0,
	})
	if err != nil {
		t.Fatalf("HandleRequestVote: %v", err)
	}
	if !resp.VoteGranted || resp.Term != 5 {
		t.Fatalf("expected grant at term 5, got %+v", resp)
	}
	// The whole point: on-disk state matches the response we just sent.
	hs := h.rlog.HardState()
	if hs.Term != 5 || hs.VotedFor != "n2" {
		t.Fatalf("vote not durable when response was sent: %+v", hs)
	}

	// A different candidate in the same term must be refused.
	resp2, err := h.n.HandleRequestVote(ctx, &raftpb.RequestVoteRequest{
		Term: 5, CandidateId: "n3", LastLogIndex: 0, LastLogTerm: 0,
	})
	if err != nil {
		t.Fatalf("HandleRequestVote #2: %v", err)
	}
	if resp2.VoteGranted || resp2.Term != 5 {
		t.Fatalf("double vote allowed: %+v", resp2)
	}
	hs = h.rlog.HardState()
	if hs.VotedFor != "n2" {
		t.Fatalf("vote changed after refusal: %+v", hs)
	}

	// A stale-term candidate is refused and learns our term.
	resp3, err := h.n.HandleRequestVote(ctx, &raftpb.RequestVoteRequest{
		Term: 3, CandidateId: "n3", LastLogIndex: 0, LastLogTerm: 0,
	})
	if err != nil {
		t.Fatalf("HandleRequestVote #3: %v", err)
	}
	if resp3.VoteGranted || resp3.Term != 5 {
		t.Fatalf("stale candidate not refused: %+v", resp3)
	}
}

// The election restriction: candidates whose log is behind ours never win
// our vote, even at a newer term.
func TestVoteDeniedWhenCandidateLogBehind(t *testing.T) {
	h := startNode(t, "n1", 1, "n1", "n2", "n3")
	// Our log: entries 1..3, all from term 1.
	if err := h.rlog.Append([]raftlog.Entry{
		{Index: 1, Term: 1, Payload: []byte("a")},
		{Index: 2, Term: 1, Payload: []byte("b")},
		{Index: 3, Term: 1, Payload: []byte("c")},
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	ctx := context.Background()

	vote := func(cand string, idx, term uint64) (*raftpb.RequestVoteResponse, error) {
		t.Helper()
		return h.n.HandleRequestVote(ctx, &raftpb.RequestVoteRequest{
			Term: 1, CandidateId: cand, LastLogIndex: idx, LastLogTerm: term,
		})
	}

	// Behind on term (last term 0 < our 1): denied.
	if r, err := vote("n2", 0, 0); err != nil || r.VoteGranted {
		t.Fatalf("behind-on-term candidate granted: %+v err=%v", r, err)
	}
	// Same last term, fewer entries: denied.
	if r, err := vote("n3", 2, 1); err != nil || r.VoteGranted {
		t.Fatalf("shorter-log candidate granted: %+v err=%v", r, err)
	}
	// Equal log: granted.
	if r, err := vote("n4", 3, 1); err != nil || !r.VoteGranted {
		t.Fatalf("equal-log candidate denied: %+v err=%v", r, err)
	}
	// Vote already spent in this term.
	if r, err := vote("n5", 3, 1); err != nil || r.VoteGranted {
		t.Fatalf("second vote in same term granted: %+v err=%v", r, err)
	}
	hs := h.rlog.HardState()
	if hs.Term != 1 || hs.VotedFor != "n4" {
		t.Fatalf("hard state after votes: %+v", hs)
	}
}

// AppendEntries: stale leaders rejected (no timer reset), valid leaders
// recognized (timer reset, leader learned), prevLog consistency enforced.
func TestAppendEntriesHeartbeatAndConsistency(t *testing.T) {
	h := startNode(t, "n1", 1, "n1", "n2", "n3")
	ctx := context.Background()

	// Establish term 7 by granting n2 a vote first.
	if r, err := h.n.HandleRequestVote(ctx, &raftpb.RequestVoteRequest{
		Term: 7, CandidateId: "n2", LastLogIndex: 0, LastLogTerm: 0,
	}); err != nil || !r.VoteGranted {
		t.Fatalf("setup vote: %+v err=%v", r, err)
	}

	// Stale-term heartbeat: rejected, does not teach a leader, our term kept.
	r, err := h.n.HandleAppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term: 3, LeaderId: "n3", PrevLogIndex: 0, PrevLogTerm: 0,
	})
	if err != nil || r.Success || r.Term != 7 {
		t.Fatalf("stale heartbeat: %+v err=%v", r, err)
	}
	if s := h.n.Status(); s.LeaderID != "" {
		t.Fatalf("stale heartbeat taught leader %q", s.LeaderID)
	}

	// Valid heartbeat: accepted, leader learned, timer reset.
	r, err = h.n.HandleAppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term: 7, LeaderId: "n3", PrevLogIndex: 0, PrevLogTerm: 0,
	})
	if err != nil || !r.Success || r.Term != 7 {
		t.Fatalf("valid heartbeat: %+v err=%v", r, err)
	}
	if s := h.n.Status(); s.LeaderID != "n3" || s.Role != raft.RoleFollower {
		t.Fatalf("leader not adopted: %+v", s)
	}
	// Timer really was reset: 9 ticks (below the minimum timeout of 10)
	// must not produce a campaign.
	h.tickN(9)
	if s := h.n.Status(); s.Role != raft.RoleFollower || s.Term != 7 {
		t.Fatalf("campaigned too early after heartbeat: %+v", s)
	}

	// Entries now replicate (Phase 6) — see TestFollowerLogReplication for
	// the append/truncate/commit matrix. Here: prevLog consistency on an
	// empty log — index 5 can't match…
	r, err = h.n.HandleAppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term: 7, LeaderId: "n3", PrevLogIndex: 5, PrevLogTerm: 0,
	})
	if err != nil || r.Success {
		t.Fatalf("beyond-log prev accepted: %+v err=%v", r, err)
	}

	// …but a matching (index, term) does.
	if err := h.rlog.Append([]raftlog.Entry{{Index: 1, Term: 1, Payload: []byte("a")}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	r, err = h.n.HandleAppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term: 7, LeaderId: "n3", PrevLogIndex: 1, PrevLogTerm: 1,
	})
	if err != nil || !r.Success {
		t.Fatalf("matching prev rejected: %+v err=%v", r, err)
	}
	// Wrong term at the same index → rejected.
	r, err = h.n.HandleAppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term: 7, LeaderId: "n3", PrevLogIndex: 1, PrevLogTerm: 4,
	})
	if err != nil || r.Success {
		t.Fatalf("mismatched prev term accepted: %+v err=%v", r, err)
	}
}
