package raft

import (
	"fmt"

	raftpb "distrikv/gen/raft/v1"
	"distrikv/internal/raftlog"
	"distrikv/internal/transport"
)

// onTick advances logical time. Leaders run on heartbeat time; followers and
// candidates run on election time (randomized to [E, 2E) ticks per term so
// split votes are improbable).
func (n *Node) onTick() error {
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
			return n.startCampaign()
		}
	}
	return nil
}

// resetElectionTimer restarts the randomized election deadline.
func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	// Randomized in [E, 2E): with a shared deadline split votes would loop
	// forever; randomized deadlines make one candidate win the next round.
	n.electionTimeout = n.electTO + n.rng.Intn(n.electTO)
}

// startCampaign transitions follower/candidate → candidate: bump term, vote
// for self (persisted BEFORE any vote request leaves), and solicit votes.
func (n *Node) startCampaign() error {
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

	n.logf("campaign", "prev_role", prevRole, "prev_term", prevTerm,
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
		}
		go n.sendRequestVote(p, req)
	}
	return nil
}

// sendRequestVote runs on its own goroutine: one outbound RPC, one response
// event back into the loop. The RPC lifetime is bounded by rpcCtx (cancelled
// when Run exits) — no response can outlive the node.
func (n *Node) sendRequestVote(to transport.NodeID, req *raftpb.RequestVoteRequest) {
	resp, err := n.transport.RequestVote(n.rpcCtx, to, req)
	select {
	case n.events <- voteRespEvent{from: to, resp: resp, err: err}:
	case <-n.rpcCtx.Done():
	case <-n.done:
	}
}

// onVoteResponse handles one vote reply while campaigning.
func (n *Node) onVoteResponse(e voteRespEvent) error {
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
func (n *Node) becomeLeader() error {
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
func (n *Node) leaveLeadership() {
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
func (n *Node) stepDown(newTerm uint64, reason string) error {
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
func (n *Node) persistHardState() error {
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
func (n *Node) logf(msg string, kv ...any) {
	if n.log_ == nil {
		return
	}
	args := append([]any{"node", string(n.id), "role", string(n.role), "term", n.term}, kv...)
	n.log_.Debug(msg, args...)
}

// logfInfo is logf at INFO: acquiring leadership is the operational event
// operators watch for (it happens at most once per term, so it never spams).
func (n *Node) logfInfo(msg string, kv ...any) {
	if n.log_ == nil {
		return
	}
	args := append([]any{"node", string(n.id), "role", string(n.role), "term", n.term}, kv...)
	n.log_.Info(msg, args...)
}
