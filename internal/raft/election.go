package raft

import (
	"fmt"

	raftpb "distrikv/gen/raft/v1"
	"distrikv/internal/raftlog"
	"distrikv/internal/transport"
)

// onTick advances logical time. Leaders run on heartbeat time; everyone
// else — follower, pre-candidate, or candidate waiting out a failed round
// — runs on election time (randomized to [E, 2E) ticks per term so split
// votes are improbable). Election expiry starts the PRE-vote phase; only a
// pre-vote majority ever leads to a real campaign.
func (n *Group) onTick() error {
	switch n.role {
	case RoleLeader:
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.heartTO {
			n.heartbeatElapsed = 0
			// Heartbeats ride the replication path: entries when behind,
			// empty prevLog probe when caught up, LeaderCommit piggybacked.
			n.broadcastAppend()
		}
	default:
		n.electionElapsed++
		if n.electionElapsed >= n.electionTimeout {
			return n.startPreVote()
		}
	}
	return nil
}

// resetElectionTimer restarts the randomized election deadline.
func (n *Group) resetElectionTimer() {
	n.electionElapsed = 0
	// Randomized in [E, 2E): with a shared deadline split votes would loop
	// forever; randomized deadlines make one candidate win the next round.
	n.electionTimeout = n.electTO + n.rng.Intn(n.electTO)
}

// startPreVote begins (or retries) the pre-vote phase (dissertation §9.6):
// before bumping our term we ask a majority whether they WOULD vote for us
// at term+1. Nothing is persisted and no term changes on either side, so a
// node that cannot win — a partitioned node, a node with a stale log, or
// (the case that motivated this) a node whose timer merely fired while a
// majority still hears the leader — never inflates its term and never
// disrupts a cluster that is fine. A majority of pre-votes unlocks
// startCampaign (the one state change of the sequence: term++, persisted
// self-vote); a refusal just waits for the next randomized timeout and
// tries again.
func (n *Group) startPreVote() error {
	prevRole := n.role

	n.role = RolePreCandidate
	n.leaderID = ""
	n.preVotes = map[transport.NodeID]bool{n.id: true}
	n.resetElectionTimer()

	n.logf("pre_vote_start", "prev_role", prevRole, "term", n.term,
		"pre_term", n.term+1, "majority", n.majorityN)

	// A cluster that already has a majority in hand (e.g. single node)
	// proceeds straight to the real election.
	if len(n.preVotes) >= n.majorityN {
		return n.startCampaign()
	}

	req := &raftpb.PreVoteRequest{
		Term:         n.term + 1,
		CandidateId:  string(n.id),
		LastLogIndex: n.rlog.LastIndex(),
		LastLogTerm:  n.rlog.LastTerm(),
		GroupId:      uint64(n.groupID), // Phase 17: demux on the receiving host
	}
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		go n.sendPreVote(p, req)
	}
	return nil
}

// sendPreVote runs on its own goroutine: one outbound RPC, one response
// event back into the loop. The RPC lifetime is bounded by rpcCtx (cancelled
// when Run exits) — no response can outlive the node.
func (n *Group) sendPreVote(to transport.NodeID, req *raftpb.PreVoteRequest) {
	resp, err := n.transport.PreVote(n.rpcCtx, to, req)
	select {
	case n.events <- preVoteRespEvent{from: to, resp: resp, err: err}:
	case <-n.rpcCtx.Done():
	case <-n.done:
	}
}

// onPreVoteResponse handles one pre-vote reply while pre-candidate.
// No state changed on the responder's side, so a refusal is not a failure
// — only a majority of grants lets us campaign for real.
func (n *Group) onPreVoteResponse(e preVoteRespEvent) error {
	if n.role != RolePreCandidate {
		// Stale round: we already won it (campaigning), gave up, or
		// stepped down. Pre-vote replies are advisory — drop them.
		return nil
	}
	if e.err != nil {
		// Unreachable peer: not counted. Majority math makes this safe.
		n.logf("pre_vote_rpc_failed", "peer", e.from, "err", e.err.Error())
		return nil
	}
	if e.resp.Term > n.term {
		// A peer knows a newer term: we're stale. Adopting it is learning,
		// not inflation — that term already existed somewhere else (same
		// rule as vote responses).
		return n.stepDown(e.resp.Term, "higher_term_in_pre_vote_response")
	}
	if !e.resp.VoteGranted {
		n.logf("pre_vote_refused", "peer", e.from, "peer_term", e.resp.Term)
		return nil
	}
	if n.preVotes[e.from] {
		return nil // duplicate grant (at-most-once per peer per round)
	}
	n.preVotes[e.from] = true
	if len(n.preVotes) < n.majorityN {
		return nil
	}
	// Pre-vote majority in hand: the real election may begin.
	n.logfInfo("pre_vote_won", "term", n.term, "pre_term", n.term+1)
	return n.startCampaign()
}

// onPreVoteRequest decides one PreVote (loop goroutine, §9.6).
//
// Unlike onVoteRequest this mutates NOTHING: no term is adopted, no
// votedFor is recorded, nothing is fsynced — persist-before-respond has
// no write to wait for, so the response can never outrun the disk.
// Conditions to grant, in order:
//
//  1. Stale sender (req.Term < our term): refuse and let our term be the
//     answer — the sender adopts it instead of inflating its own. This is
//     how a partitioned old node heals without a term war.
//  2. A leader we still believe in must not be challenged: an incumbent
//     leader refuses outright, and a follower that has heard from its
//     leader within the minimum election timeout refuses too. This is the
//     whole point of pre-vote — a healthy majority keeps rejecting a
//     would-be disruptor's pre-votes while it still hears a leader, so
//     one node's stalled event loop can no longer force elections (and
//     term inflation) on a cluster that is fine.
//  3. §5.4.1 log check — the same up-to-date rule as RequestVote: only a
//     candidate whose log is at least as complete as ours may win, in the
//     pre-round as in the real one.
func (n *Group) onPreVoteRequest(req *raftpb.PreVoteRequest) (*raftpb.PreVoteResponse, error) {
	resp := &raftpb.PreVoteResponse{Term: n.term}

	if req.Term < n.term {
		return resp, nil // stale sender: our higher term is the answer
	}
	if n.role == RoleLeader || (n.leaderID != "" && n.electionElapsed < n.electTO) {
		return resp, nil // a live leader protects itself and its followers
	}
	lastIdx, lastTerm := n.rlog.LastIndex(), n.rlog.LastTerm()
	upToDate := req.LastLogTerm > lastTerm ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIdx)
	if !upToDate {
		return resp, nil // behind-log candidates never pass the §5.4.1 check
	}
	resp.VoteGranted = true
	return resp, nil
}

// startCampaign transitions pre-candidate → candidate: bump term, vote
// for self (persisted BEFORE any vote request leaves), and solicit votes.
// Callers have already won a pre-vote majority — this is the first (and
// only) state change of an election round.
func (n *Group) startCampaign() error {
	prevRole, prevTerm := n.role, n.term

	n.role = RoleCandidate
	n.term++
	n.votedFor = n.id
	n.leaderID = ""
	if err := n.persistHardState(); err != nil {
		return err
	}
	n.votes = map[transport.NodeID]bool{n.id: true}
	n.resetElectionTimer()

	// Elections are rare and consequential: keep them at INFO alongside
	// became_leader so a churn incident is auditable from the logs.
	n.logfInfo("campaign", "prev_role", prevRole, "prev_term", prevTerm,
		"peers", len(n.peers), "majority", n.majorityN)

	// A cluster that already has a majority in hand (e.g. single node)
	// wins immediately without waiting for responses.
	if len(n.votes) >= n.majorityN {
		return n.becomeLeader()
	}

	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		req := &raftpb.RequestVoteRequest{
			Term:         n.term,
			CandidateId:  string(n.id),
			LastLogIndex: n.rlog.LastIndex(),
			LastLogTerm:  n.rlog.LastTerm(),
			GroupId:      uint64(n.groupID), // Phase 17: demux on the receiving host
		}
		go n.sendRequestVote(p, req)
	}
	return nil
}

// sendRequestVote runs on its own goroutine: one outbound RPC, one response
// event back into the loop. The RPC lifetime is bounded by rpcCtx (cancelled
// when Run exits) — no response can outlive the node.
func (n *Group) sendRequestVote(to transport.NodeID, req *raftpb.RequestVoteRequest) {
	resp, err := n.transport.RequestVote(n.rpcCtx, to, req)
	select {
	case n.events <- voteRespEvent{from: to, resp: resp, err: err}:
	case <-n.rpcCtx.Done():
	case <-n.done:
	}
}

// onVoteResponse handles one vote reply while campaigning.
func (n *Group) onVoteResponse(e voteRespEvent) error {
	if e.err != nil {
		// Unreachable peer: not counted. Majority math makes this safe.
		n.logf("vote_rpc_failed", "peer", e.from, "err", e.err.Error())
		return nil
	}
	if e.resp.Term > n.term {
		// A peer knows a newer term: we're stale. Step down (Raft §5.1).
		return n.stepDown(e.resp.Term, "higher_term_in_vote_response")
	}
	if n.role != RoleCandidate || e.resp.Term < n.term {
		// Stale response from an older campaign — ignore.
		return nil
	}
	if !e.resp.VoteGranted {
		return nil
	}
	if n.votes[e.from] {
		return nil // duplicate grant (at-most-once per peer per term)
	}
	n.votes[e.from] = true
	if len(n.votes) >= n.majorityN {
		return n.becomeLeader()
	}
	return nil
}

// becomeLeader installs leader state, per-peer replication progress, and a
// no-op entry for this term — then replicates immediately.
//
// The no-op (empty payload, current term) does two jobs: it asserts
// leadership with current-term content, and it lets the commit rule see a
// current-term entry so entries carried over from previous terms can commit
// indirectly (Figure 8). Without it, a new leader's commitIndex would stall
// until some client happened to write.
func (n *Group) becomeLeader() error {
	n.role = RoleLeader
	n.leaderID = n.id
	n.votes = nil
	n.heartbeatElapsed = 0

	// Progress starts optimistically at last+1; rejections walk it back to
	// the true match point. match starts at 0 — only acknowledgments count.
	next := n.rlog.LastIndex() + 1
	n.progress = make(map[transport.NodeID]*peerProgress, len(n.peers))
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		n.progress[p] = &peerProgress{next: next, match: 0}
	}

	n.logfInfo("became_leader")

	// Remember the no-op's index: ReadIndex gates on this term committing
	// it (Figure 8 — the read point must cover prior-term commits).
	n.noopIndex = next
	if err := n.rlog.Append([]raftlog.Entry{{Index: next, Term: n.term, Payload: nil}}); err != nil {
		return fmt.Errorf("append leader no-op: %w", err)
	}
	if err := n.rlog.Sync(); err != nil {
		return fmt.Errorf("sync leader no-op: %w", err)
	}

	n.broadcastAppend()
	// Single-node cluster: majority is ourselves; the no-op commits now.
	return n.maybeAdvanceCommit()
}

// leaveLeadership transitions any role → follower, failing proposals this
// node can no longer commit on its own authority, and reads whose
// leadership proof died with the role. Callers log the transition.
func (n *Group) leaveLeadership() {
	if n.role == RoleLeader {
		n.failWaiters(ErrLeadershipLost)
	}
	n.failReads(ErrNotLeader)
	n.noopIndex = 0
	n.role = RoleFollower
	n.votes = nil
	n.progress = nil
}

// stepDown abandons candidacy/leadership because a higher term was observed.
// The new term and cleared vote are persisted BEFORE anything else happens.
func (n *Group) stepDown(newTerm uint64, reason string) error {
	prev := n.term
	if newTerm > n.term {
		n.term = newTerm
		n.votedFor = ""
		if err := n.persistHardState(); err != nil {
			return err
		}
	}
	if n.role != RoleFollower {
		n.logf("step_down", "reason", reason, "prev_term", prev, "was", string(n.role))
	}
	n.leaveLeadership()
	n.leaderID = ""
	n.resetElectionTimer()
	return nil
}

// persistHardState fsyncs term+votedFor. MUST complete before the response
// or outbound request that assumes it — the loop runs it synchronously, so
// every ordering falls out of single-threading (no locks to get wrong).
func (n *Group) persistHardState() error {
	if err := n.rlog.SetHardState(raftlog.HardState{
		Term:     n.term,
		VotedFor: string(n.votedFor),
	}); err != nil {
		return fmt.Errorf("persist hard state: %w", err)
	}
	return nil
}

// logf emits a structured transition log with term/role context (debug:
// campaigns and step-downs can be frequent during instability).
func (n *Group) logf(msg string, kv ...any) {
	if n.log_ == nil {
		return
	}
	args := append([]any{"node", string(n.id), "role", string(n.role), "term", n.term}, kv...)
	n.log_.Debug(msg, args...)
}

// logfInfo is logf at INFO: acquiring leadership is the operational event
// operators watch for (it happens at most once per term, so it never spams).
func (n *Group) logfInfo(msg string, kv ...any) {
	if n.log_ == nil {
		return
	}
	args := append([]any{"node", string(n.id), "role", string(n.role), "term", n.term}, kv...)
	n.log_.Info(msg, args...)
}
