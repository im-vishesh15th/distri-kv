package raft

import (
	"fmt"

	raftpb "distrikv/gen/raft/v1"
	"distrikv/internal/raftlog"
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
		n.leaveLeadership()
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

// onAppendEntries processes one AppendEntries (loop goroutine) — heartbeat
// liveness AND log replication (Phase 6).
//
// Order matters and mirrors Raft §5.3:
//
//  1. Term handling (stale → reject untouched; newer → adopt + persist).
//  2. Leadership acceptance: step down, learn leaderID, RESET the election
//     timer — for any valid-term message, even one we will reject below:
//     an alive leader is what the timer measures.
//  3. Consistency check on (prevLogIndex, prevLogTerm) — BEFORE any
//     mutation: a rejected request must never truncate our log.
//  4. Append: find the first divergent index, truncate the tail, append
//     the rest, fsync — durable before we acknowledge.
//  5. Commit propagation: commitIndex = max(current, min(leaderCommit,
//     last index we now know exists)).
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
	// Covers candidate→follower (another candidate won) and leader→leader
	// (split brain at the same term — shouldn't happen, but stepping down
	// keeps us safe if it ever does).
	if n.role != RoleFollower {
		n.logf("step_down", "reason", "valid_leader_append", "was", string(n.role), "leader", req.LeaderId)
	}
	n.leaveLeadership()
	n.leaderID = transport.NodeID(req.LeaderId)
	n.resetElectionTimer()

	// Consistency check first — nothing below may run on a failed match.
	if !n.prevLogMatches(req.PrevLogIndex, req.PrevLogTerm) {
		resp.Term = n.term
		return resp, nil
	}

	// Defensive: entries must form the exact continuation of prevLog.
	// A violation is a protocol error at the sender; reject without mutation.
	for i, e := range req.Entries {
		if e.Index != req.PrevLogIndex+1+uint64(i) {
			n.logf("append_entries_malformed", "leader", req.LeaderId, "position", i,
				"got_index", e.Index, "want_index", req.PrevLogIndex+1+uint64(i))
			resp.Term = n.term
			return resp, nil
		}
	}

	lastNew := req.PrevLogIndex + uint64(len(req.Entries))
	if len(req.Entries) > 0 {
		if err := n.applyEntries(req); err != nil {
			return nil, err // disk failure: halt (cannot uphold safety)
		}
	}

	// Commit propagation (never moves backward; never past what we have).
	if req.LeaderCommit > n.commitIndex {
		c := req.LeaderCommit
		if c > lastNew {
			c = lastNew
		}
		if c > n.commitIndex {
			n.commitIndex = c
			n.logf("commit_advanced", "commit_index", n.commitIndex, "via", "leader_commit")
			if err := n.applyCommitted(); err != nil {
				return nil, err
			}
		}
	}

	resp.Success = true
	resp.Term = n.term
	return resp, nil
}

// onInstallSnapshot handles a leader's InstallSnapshot RPC (Phase 13):
// our log ends below the leader's compaction point, so entries alone can
// no longer catch us up — the leader ships the snapshot instead (Raft §7).
//
// Mutation order, chosen so every crash/retry window is safe:
//
//  1. Restore first: it validates the payload and is all-or-nothing
//     (kv.SM.Restore builds the new state before swapping). A failure
//     changes NOTHING durable — we stay behind and the leader retries
//     with the same bytes (debug-level log each attempt; this is a
//     capacity/decode issue, not a safety breach).
//  2. Position adopted with the restore: the state machine is already at
//     lastIncludedIndex, so applying our old tail on top of it would
//     double-apply.
//  3. SaveSnapshot makes the intent durable — a crash after this is
//     reconciled at startup by restoreSnapshot's catch-up.
//  4. The log is rebased: a compatible tail (matching term at the
//     boundary) is kept via TruncatePrefix; a short or divergent tail is
//     discarded wholesale via Reset. Structural disk failure halts, as
//     anywhere else.
//
// If step 3 fails after the in-memory install, we still respond failure —
// the leader retries, and the "already at that position" guard below
// answers success so its match advances. A crash before the retry restarts
// us from the OLD durable state (old log, old sidecar), which is
// self-consistent: the adopted position was memory-only.
func (n *Node) onInstallSnapshot(req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	resp := &raftpb.InstallSnapshotResponse{Term: n.term, Success: false}

	if req.Term < n.term {
		// Stale leader from a dead epoch: reject, change nothing — in
		// particular we do NOT reset our election timer for it.
		return resp, nil
	}
	if req.Term > n.term {
		n.term = req.Term
		n.votedFor = ""
		if err := n.persistHardState(); err != nil {
			return nil, err // disk failure: halt
		}
		resp.Term = n.term
	}

	// Same authority acceptance as AppendEntries: a valid-term leader
	// message defers our election (and steps us down if we thought we
	// were leader — split brain at the same term shouldn't happen, but
	// stepping down keeps us safe if it ever does).
	if n.role != RoleFollower {
		n.logf("step_down", "reason", "valid_leader_snapshot", "was", string(n.role), "leader", req.LeaderId)
	}
	n.leaveLeadership()
	n.leaderID = transport.NodeID(req.LeaderId)
	n.resetElectionTimer()

	// Protocol validity: position 0 is meaningless (Raft terms start at
	// index 0/term 0 only as the genesis base), and a term-0 boundary can
	// never match a real entry — reject without mutation.
	if req.LastIncludedIndex == 0 || req.LastIncludedTerm == 0 {
		n.logf("install_snapshot_malformed", "leader", req.LeaderId,
			"last_included_index", req.LastIncludedIndex, "last_included_term", req.LastIncludedTerm)
		return resp, nil
	}

	// Never regress: we have already applied this position (or past it),
	// so the snapshot is redundant — acknowledge so the leader can advance
	// its match, but change nothing. This also answers the retry after a
	// failed persistence attempt (position adopted in memory, sidecar not
	// yet durable).
	if req.LastIncludedIndex <= n.lastApplied {
		resp.Success = true
		return resp, nil
	}

	if n.snapshottable == nil {
		// Our state machine cannot restore snapshots: an install can never
		// be verified or applied, so refuse it. The cluster's majority does
		// not depend on us; we simply keep falling behind.
		n.logf("install_snapshot_unsupported", "leader", req.LeaderId, "index", req.LastIncludedIndex)
		return resp, nil
	}
	meta := raftlog.SnapshotMeta{
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
	}

	// 1. Restore: all-or-nothing validation of the leader's payload.
	if err := n.snapshottable.Restore(req.Data); err != nil {
		n.logf("install_snapshot_failed", "leader", req.LeaderId,
			"index", req.LastIncludedIndex, "err", err.Error())
		return resp, nil // state untouched: retry-safe
	}

	// 2. The state machine now contains lastIncludedIndex's state.
	n.commitIndex = req.LastIncludedIndex
	n.lastApplied = req.LastIncludedIndex

	// 3. Durable intent (before the log rewrite it justifies).
	if err := n.rlog.SaveSnapshot(meta, req.Data); err != nil {
		n.logf("install_snapshot_persist_failed", "leader", req.LeaderId,
			"index", req.LastIncludedIndex, "err", err.Error())
		return resp, nil // capacity failure: position kept in memory only
	}
	n.lastSnapIndex = req.LastIncludedIndex

	// 4. Rebase the log onto the snapshot boundary.
	if err := n.adoptLogPosition(meta); err != nil {
		return nil, err // structural disk failure: halt
	}

	resp.Success = true
	n.logfInfo("snapshot_installed",
		"leader", req.LeaderId,
		"last_included_index", meta.LastIncludedIndex,
		"last_included_term", meta.LastIncludedTerm,
		"payload_bytes", len(req.Data),
		"log_first_index", n.rlog.FirstIndex(),
	)
	return resp, nil
}

// adoptLogPosition rebases our log onto the snapshot's boundary after an
// install (Phase 13). Loop goroutine only.
//
// A tail we still hold with the boundary entry (same term at
// lastIncludedIndex) is compatible — keep everything after it: those are
// uncommitted entries a new leader may still want. Anything else — tail
// shorter than the snapshot, or a divergent term at the boundary — means
// nothing we hold is trusted: discard every entry and adopt the metadata
// (Reset). Errors are structural (disk) and halt the node.
func (n *Node) adoptLogPosition(meta raftlog.SnapshotMeta) error {
	idx := meta.LastIncludedIndex
	if idx <= n.rlog.LastIndex() {
		t, err := n.rlog.Term(idx)
		if err == nil && t == meta.LastIncludedTerm {
			if err := n.rlog.TruncatePrefix(idx); err != nil {
				return fmt.Errorf("raft: install snapshot compact to %d: %w", idx, err)
			}
			return nil
		}
	}
	if err := n.rlog.Reset(meta); err != nil {
		return fmt.Errorf("raft: install snapshot rebase to %d: %w", idx, err)
	}
	return nil
}

// applyEntries merges the request's entries into our log: matching prefixes
// are skipped, the first divergent index truncates our tail, and everything
// from there on is appended and fsynced. Loop goroutine only.
func (n *Node) applyEntries(req *raftpb.AppendEntriesRequest) error {
	followerLast := n.rlog.LastIndex()
	for i, e := range req.Entries {
		idx := req.PrevLogIndex + 1 + uint64(i)
		if idx > followerLast {
			// Past our end: append the remainder.
			return n.appendTail(req.Entries[i:])
		}
		t, err := n.rlog.Term(idx)
		if err != nil {
			// idx is within [firstIndex, lastIndex] (prevLog matched), so
			// this is a structural failure — refuse to guess.
			return fmt.Errorf("term at %d: %w", idx, err)
		}
		if t == e.Term {
			continue // identical entry already durable
		}
		// Divergence: our suffix was written by a different history.
		if err := n.rlog.TruncateSuffix(idx); err != nil {
			return fmt.Errorf("truncate divergent suffix at %d: %w", idx, err)
		}
		n.logf("log_truncated", "at", idx, "our_term", t, "leader_term", e.Term)
		return n.appendTail(req.Entries[i:])
	}
	return nil // every entry already present and durable
}

// appendTail writes entries to disk and syncs before the caller
// acknowledges them (fsync-before-ack).
func (n *Node) appendTail(wire []*raftpb.LogEntry) error {
	batch := make([]raftlog.Entry, 0, len(wire))
	for _, e := range wire {
		batch = append(batch, raftlog.Entry{Index: e.Index, Term: e.Term, Payload: e.Payload})
	}
	if err := n.rlog.Append(batch); err != nil {
		return fmt.Errorf("append entries: %w", err)
	}
	if err := n.rlog.Sync(); err != nil {
		return fmt.Errorf("sync entries: %w", err)
	}
	return nil
}

// prevLogMatches is the Raft §5.3 consistency check: does our log actually
// contain an entry matching (prevLogIndex, prevLogTerm)? index 0 is the
// base case (Term(0) = 0 by convention — an empty log matches). The
// compaction boundary itself (index == firstIndex-1) is answerable since
// Phase 12 — Term serves firstTerm (lastIncludedTerm); only BELOW it does
// the check fail, which is Phase 13's InstallSnapshot territory.
func (n *Node) prevLogMatches(index, term uint64) bool {
	if index > n.rlog.LastIndex() {
		return false // we don't have that far yet
	}
	t, err := n.rlog.Term(index)
	if err != nil {
		return false // below the compaction point or missing
	}
	return t == term
}
