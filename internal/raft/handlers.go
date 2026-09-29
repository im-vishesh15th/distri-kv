package raft

import (
	raftpb "distrikv/gen/raft/v1"
	"distrikv/internal/transport"
)

// onVoteRequest decides one RequestVote (loop goroutine).
//
// Raft §5.4.1 — vote safety rests on three checks, in this order:
//
//  1. Term: a stale request is rejected outright; a newer term is adopted
//     (and persisted) before anything else, which also demotes us — a node
//     that just learned of a newer term can no longer act on its old one.
//  2. votedFor: at most one vote per term per node, durably recorded.
//  3. Up-to-date log: only a candidate whose log is at least as complete as
//     ours may win — this is what makes elected leaders contain all committed
//     entries (§5.4.2).
//
// The response is composed only after the checks above have had their say,
// and any persistence they require completes inside this call — the RPC
// response therefore never outruns the disk (persist-before-respond).
func (n *Node) onVoteRequest(req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	resp := &raftpb.RequestVoteResponse{Term: n.term, VoteGranted: false}

	if req.Term < n.term {
		// Stale candidate from an older term: reject, change nothing.
		return resp, nil
	}

	if req.Term > n.term {
		// Newer term: adopt it, clear the old term's vote, step down. All
		// persisted synchronously before this function can return.
		n.term = req.Term
		n.votedFor = ""
		if err := n.persistHardState(); err != nil {
			return nil, err
		}
		if n.role != RoleFollower {
			n.logf("step_down", "reason", "higher_term_in_vote_request", "was", string(n.role))
		}
		n.role = RoleFollower
		n.votes = nil
		n.leaderID = ""
		n.resetElectionTimer()
	}

	cand := transport.NodeID(req.CandidateId)
	if (n.votedFor == "" || n.votedFor == cand) && n.logUpToDate(req.LastLogIndex, req.LastLogTerm) {
		n.votedFor = cand
		if err := n.persistHardState(); err != nil {
			return nil, err
		}
		// Granting a vote defers our own election: the grantee may well win
		// and start heartbeating before our deadline fires, avoiding a
		// pointless spurious campaign.
		n.resetElectionTimer()
		resp.VoteGranted = true
	}
	resp.Term = n.term
	return resp, nil
}

// logUpToDate implements the Raft §5.4.1 election restriction: the candidate
// must have an entry at least as recent as ours — higher last term wins; on
// a tie, at least as many entries.
func (n *Node) logUpToDate(candIndex, candTerm uint64) bool {
	myIndex, myTerm := n.rlog.LastIndex(), n.rlog.LastTerm()
	return candTerm > myTerm || (candTerm == myTerm && candIndex >= myIndex)
}

// onAppendEntries decides one AppendEntries (loop goroutine).
//
// Phase 5 uses this for leader liveness: a follower that hears a valid-term
// leader resets its election timer (the leader keeps the cluster alive by
// voice alone). The log-consistency half of the reply is already honest —
// the prevLog check below is the same one replication will rely on in Phase 6
// — but entry transfer itself is not implemented yet, so a request carrying
// entries is refused rather than silently ignored.
func (n *Node) onAppendEntries(req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	resp := &raftpb.AppendEntriesResponse{Term: n.term, Success: false}

	if req.Term < n.term {
		// Stale leader (or partitioned old leader): reject, change nothing —
		// in particular we do NOT reset our election timer for it.
		return resp, nil
	}

	if req.Term > n.term {
		n.term = req.Term
		n.votedFor = ""
		if err := n.persistHardState(); err != nil {
			return nil, err
		}
	}

	// Valid-term leader message: accept its authority, defer our election.
	// This covers candidate→follower (another candidate won) and
	// leader→leader (split brain at the same term — shouldn't happen, but
	// stepping down keeps us safe if it ever does).
	if n.role != RoleFollower {
		n.logf("step_down", "reason", "valid_leader_heartbeat",
			"was", string(n.role), "leader", req.LeaderId)
	}
	n.role = RoleFollower
	n.votes = nil
	n.leaderID = transport.NodeID(req.LeaderId)
	n.resetElectionTimer()

	if len(req.Entries) > 0 {
		// Entry transfer lands in Phase 6 (replication). Our Phase-5 leader
		// only ever sends empty heartbeats, so this cannot fire yet; refuse
		// loudly instead of pretending to accept.
		resp.Term = n.term
		return resp, nil
	}

	resp.Success = n.prevLogMatches(req.PrevLogIndex, req.PrevLogTerm)
	resp.Term = n.term
	return resp, nil
}

// prevLogMatches is the Raft §5.3 consistency check: does our log actually
// contain an entry matching (prevLogIndex, prevLogTerm)? index 0 is the
// base case (Term(0) = 0 by convention — an empty log matches).
func (n *Node) prevLogMatches(index, term uint64) bool {
	if index > n.rlog.LastIndex() {
		return false // we don't have that far yet
	}
	t, err := n.rlog.Term(index)
	if err != nil {
		return false // below the compaction point (Phase 12) or missing
	}
	return t == term
}
