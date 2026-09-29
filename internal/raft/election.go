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
			if err := n.broadcastHeartbeat(); err != nil {
				return err
			}
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

// becomeLeader installs leader state and asserts authority immediately with
// an empty AppendEntries heartbeat (followers reset their timers on it).
func (n *Node) becomeLeader() error {
	n.role = RoleLeader
	n.leaderID = n.id
	n.votes = nil
	n.heartbeatElapsed = 0
	// nextIndex/matchIndex: Phase 6 (replication).
	n.logfInfo("became_leader")
	return n.broadcastHeartbeat()
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
	n.role = RoleFollower
	n.votes = nil
	n.leaderID = ""
	n.resetElectionTimer()
	return nil
}

// broadcastHeartbeat sends an empty AppendEntries to every peer. In Phase 5
// this is pure liveness (timer reset + leadership assertion); Phase 6 puts
// entries in it.
func (n *Node) broadcastHeartbeat() error {
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		to := p
		// Fresh message per peer — protobuf messages are never copied.
		req := &raftpb.AppendEntriesRequest{
			Term:         n.term,
			LeaderId:     string(n.id),
			PrevLogIndex: n.rlog.LastIndex(),
			PrevLogTerm:  n.rlog.LastTerm(),
			LeaderCommit: n.commitIndex,
		}
		go func() {
			// Phase 6 consumes the response for replication bookkeeping;
			// Phase 5 needs only the sending side (liveness).
			_, _ = n.transport.AppendEntries(n.rpcCtx, to, req)
		}()
	}
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
