package raft

// Log replication (Phase 6): how a leader ships entries to followers, how
// majority acknowledgment advances commitIndex, and how Propose callers are
// released. All state here is loop-owned; outbound RPCs run on short-lived
// goroutines that post responses back as events.

import (
	"fmt"
	"sort"

	raftpb "distrikv/gen/raft/v1"
	"distrikv/internal/raftlog"
	"distrikv/internal/transport"
)

// maxEntriesPerAppend caps one AppendEntries RPC; a leader farther ahead
// resends on the response (the in-flight/pending machinery drives the
// continuation), so catch-up flows in bounded batches.
const maxEntriesPerAppend = 64

// peerProgress is the leader's view of one follower. Rebuilt at every
// becomeLeader — no state crosses terms.
type peerProgress struct {
	next     uint64 // next index to send to this follower
	match    uint64 // highest index confirmed durable on this follower
	inflight bool   // an AppendEntries RPC is outstanding
	pending  bool   // a trigger arrived while inflight — resend on response
	sentLast uint64 // last index carried by the in-flight request
	gen      uint64 // send generation: bumps per request (ReadIndex freshness)
}

// handleProposeBatch appends a run of leader proposals to the leader's own
// log as ONE write + ONE fsync (group commit), registers a commit waiter
// per entry, and replicates once. Replies are written later — on commit
// advance, or by failWaiters if leadership is lost.
//
// Batching is a performance change only: durability is one F_FULLFSYNC per
// Sync on darwin (~2.7 ms measured), and paying it per proposal serialized
// a group at ~365 writes/s at any offered concurrency (docs/benchmarks.md).
// The batch is a run of consecutive proposeEvents taken from the channel
// head, so indices (first..first+n-1), terms, and channel order are exactly
// what per-proposal handling would have produced, and fsync-before-ack is
// preserved — nothing in the batch is acknowledged or counted toward a
// majority before the single Sync returns.
func (n *Group) handleProposeBatch(batch []proposeEvent) error {
	if len(batch) == 0 {
		return nil
	}
	if n.role != RoleLeader {
		for _, e := range batch {
			e.reply <- proposeReply{err: ErrNotLeader}
		}
		return nil
	}
	first := n.rlog.LastIndex() + 1
	entries := make([]raftlog.Entry, len(batch))
	payload := 0
	for i, e := range batch {
		entries[i] = raftlog.Entry{Index: first + uint64(i), Term: n.term, Payload: e.payload}
		payload += len(e.payload)
	}
	if err := n.rlog.Append(entries); err != nil {
		for _, e := range batch {
			e.reply <- proposeReply{err: err}
		}
		return fmt.Errorf("append proposal: %w", err) // disk/structure failure: halt
	}
	// fsync-before-ack: the whole batch must be durable before any of it
	// may count itself toward the majority.
	if err := n.rlog.Sync(); err != nil {
		for _, e := range batch {
			e.reply <- proposeReply{err: err}
		}
		return fmt.Errorf("sync proposal: %w", err)
	}
	for i, e := range batch {
		n.waiters[entries[i].Index] = append(n.waiters[entries[i].Index], e.reply)
	}
	n.logf("proposals_appended", "first_index", first, "count", len(batch),
		"term", n.term, "bytes", payload)

	n.broadcastAppend()
	// A single-node cluster (or a majority already satisfied by earlier
	// matches) commits right here.
	return n.maybeAdvanceCommit()
}

// broadcastAppend triggers a send to every peer; busy peers are marked
// pending and resend when their in-flight RPC returns.
func (n *Group) broadcastAppend() {
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		n.sendAppendTo(p)
	}
}

// sendAppendTo ships entries starting at progress.next (empty = heartbeat
// when caught up). prevLog is the consistency anchor; leaderCommit
// piggybacks the commit watermark for followers.
func (n *Group) sendAppendTo(to transport.NodeID) {
	prog, ok := n.progress[to]
	if !ok {
		return // stale trigger from a previous term
	}
	if prog.inflight {
		prog.pending = true
		return
	}

	last := n.rlog.LastIndex()
	if prog.next > last+1 {
		prog.next = last + 1 // defensive clamp
	}
	prev := prog.next - 1

	// Confirmed position below our compaction point (Phase 13): nothing
	// can be appended above the boundary for a follower that far behind —
	// and walking down one reject per RTT would burn gap-many round trips.
	// Install directly. (match == 0 is a fresh term with no evidence of
	// where the follower is; fall through to normal probing so a caught-up
	// follower isn't shipped a needless snapshot.)
	if prog.match > 0 && prog.match+1 < n.rlog.FirstIndex() {
		n.sendInstallSnapshotTo(to)
		return
	}
	// Same condition via the anchor: during a reject walk-down prev crosses
	// below the boundary — prev == firstIndex-1 is still answerable from
	// the boundary term, only below it do entries run out.
	if prev+1 < n.rlog.FirstIndex() {
		n.sendInstallSnapshotTo(to)
		return
	}
	prevTerm, err := n.rlog.Term(prev)
	if err != nil {
		// Defensive: prev >= firstIndex-1 must be answerable (boundary
		// term at firstIndex-1, entries above). Anything else is
		// structural — log and let the next heartbeat retry.
		n.logf("append_blocked", "peer", to, "prev", prev, "err", err.Error())
		return
	}

	var entries []*raftpb.LogEntry
	for i := prog.next; i <= last && len(entries) < maxEntriesPerAppend; i++ {
		e, err := n.rlog.Get(i)
		if err != nil {
			n.logf("append_read_failed", "peer", to, "index", i, "err", err.Error())
			return
		}
		entries = append(entries, &raftpb.LogEntry{Index: e.Index, Term: e.Term, Payload: e.Payload})
	}

	prog.inflight = true
	prog.gen++
	gen := prog.gen
	prog.sentLast = prev + uint64(len(entries))
	req := &raftpb.AppendEntriesRequest{
		Term:         n.term,
		LeaderId:     string(n.id),
		PrevLogIndex: prev,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
		GroupId:      uint64(n.groupID), // Phase 17: demux on the receiving host
	}
	go func() {
		resp, err := n.transport.AppendEntries(n.rpcCtx, to, req)
		select {
		case n.events <- appRespEvent{from: to, term: req.Term, gen: gen, resp: resp, err: err}:
		case <-n.rpcCtx.Done():
		case <-n.done:
		}
	}()
}

// sendInstallSnapshotTo ships the whole snapshot to a peer below our
// compaction point (Phase 13): its log can no longer be anchored there, so
// instead of entries the follower gets lastIncludedIndex/Term + the state
// machine payload, then catches up on the tail from match+1. It shares the
// per-peer in-flight slot with sendAppendTo (which has already checked it
// when calling, so this guard is belt-and-suspenders for future callers).
func (n *Group) sendInstallSnapshotTo(to transport.NodeID) {
	prog, ok := n.progress[to]
	if !ok {
		return // stale trigger from a previous term
	}
	if prog.inflight {
		prog.pending = true
		return
	}
	meta, payload, err := n.rlog.LoadSnapshot()
	if err != nil || meta.Zero() {
		// No usable snapshot (missing/corrupt sidecar — impossible if
		// compaction ran, since it saves first, but refuse to guess):
		// log and let the next heartbeat retry.
		reason := "no snapshot on disk"
		if err != nil {
			reason = err.Error()
		}
		n.logf("snapshot_send_blocked", "peer", to, "reason", reason)
		return
	}

	prog.inflight = true
	prog.gen++
	gen := prog.gen
	req := &raftpb.InstallSnapshotRequest{
		Term:              n.term,
		LeaderId:          string(n.id),
		LastIncludedIndex: meta.LastIncludedIndex,
		LastIncludedTerm:  meta.LastIncludedTerm,
		Data:              payload,
		GroupId:           uint64(n.groupID), // Phase 17: demux on the receiving host
	}
	go func() {
		resp, err := n.transport.InstallSnapshot(n.rpcCtx, to, req)
		select {
		case n.events <- snapRespEvent{from: to, term: req.Term, gen: gen, included: meta.LastIncludedIndex, resp: resp, err: err}:
		case <-n.rpcCtx.Done():
		case <-n.done:
		}
	}()
}

// onInstallSnapshotResponse folds one InstallSnapshot reply into
// replication state (Phase 13). Success advances match to the snapshot's
// lastIncludedIndex (monotonic: a redundant snapshot must not regress it)
// so the follower then catches up on the tail through normal appends.
func (n *Group) onInstallSnapshotResponse(e snapRespEvent) error {
	if e.err != nil {
		// Errors carry no term, so gate on ours: only the CURRENT
		// leadership epoch may touch progress (same reasoning as
		// onAppendResponse — a stale error must not clear a newer slot).
		if n.role != RoleLeader || e.term != n.term {
			return nil
		}
		if prog, ok := n.progress[e.from]; ok {
			prog.inflight = false
			prog.pending = false
		}
		n.logf("snapshot_rpc_failed", "peer", e.from, "err", e.err.Error())
		return nil
	}
	if e.resp.Term > n.term {
		return n.stepDown(e.resp.Term, "higher_term_in_snapshot_response")
	}
	if n.role != RoleLeader || e.resp.Term < n.term {
		return nil // stale response from an older term
	}
	prog, ok := n.progress[e.from]
	if !ok {
		return nil
	}
	prog.inflight = false
	triggered := prog.pending
	prog.pending = false

	if e.resp.Success {
		if e.included > prog.match {
			prog.match = e.included
		}
		prog.next = prog.match + 1
		// ReadIndex: same accounting as an append success — this follower
		// accepted our CURRENT term as of this response (readindex.go
		// gate 1).
		n.countReadAck(e.from, e.term, e.gen)
		if err := n.maybeAdvanceCommit(); err != nil {
			return err
		}
		// The tail beyond the snapshot still has to flow.
		if triggered || prog.next <= n.rlog.LastIndex() {
			n.sendAppendTo(e.from)
		}
		return nil
	}

	// Refused (persistence capacity or a payload the follower cannot
	// restore — logged there). Installs have no back-off to walk: retry on
	// the heartbeat cadence; pending triggers are dropped deliberately so
	// a repeated refusal cannot spin a retry loop.
	n.logf("install_rejected", "peer", e.from, "index", e.included)
	return nil
}

// onAppendResponse folds one AppendEntries reply into replication state.
func (n *Group) onAppendResponse(e appRespEvent) error {
	if e.err != nil {
		// Errors carry no term, so gate on ours: only the CURRENT
		// leadership epoch may touch progress. A stale error from a
		// previous epoch must not clear a newer in-flight slot —
		// overlapping RPCs would corrupt sentLast/match accounting, and
		// with it the commit rule.
		if n.role != RoleLeader || e.term != n.term {
			return nil
		}
		// Unreachable peer: clear in-flight and let the next heartbeat
		// (~30 ms) retry — never spin on a dead connection.
		if prog, ok := n.progress[e.from]; ok {
			prog.inflight = false
			prog.pending = false
		}
		n.logf("append_rpc_failed", "peer", e.from, "err", e.err.Error())
		return nil
	}
	if e.resp.Term > n.term {
		return n.stepDown(e.resp.Term, "higher_term_in_append_response")
	}
	if n.role != RoleLeader || e.resp.Term < n.term {
		return nil // stale response from an older term
	}
	prog, ok := n.progress[e.from]
	if !ok {
		return nil
	}
	prog.inflight = false
	triggered := prog.pending
	prog.pending = false

	if e.resp.Success {
		if prog.sentLast > prog.match {
			prog.match = prog.sentLast
		}
		prog.next = prog.match + 1
		// ReadIndex: this follower accepted our CURRENT term as of this
		// response — fold it into every pending read whose quorum proof
		// this send post-dates (readindex.go gate 1).
		n.countReadAck(e.from, e.term, e.gen)
		if err := n.maybeAdvanceCommit(); err != nil {
			return err
		}
		// Remainders (batch cap), or a trigger that arrived mid-flight.
		if triggered || prog.next <= n.rlog.LastIndex() {
			n.sendAppendTo(e.from)
		}
		return nil
	}

	// Rejected: the follower doesn't have prevLog. Back off ONE index and
	// retry immediately — next strictly decreases per rejection, so the
	// loop terminates at the match point. (The dissertation's conflict-term
	// hints jump whole terms instead; deferred until measurement shows
	// catch-up gaps actually cost — no optimization before correctness.)
	if prog.next <= 1 {
		// prev=0 matches by construction; a reject here is not actionable.
		// Log loudly and fall back to the heartbeat cadence.
		n.logf("append_reject_at_base", "peer", e.from)
		return nil
	}
	prog.next--
	n.sendAppendTo(e.from)
	return nil
}

// maybeAdvanceCommit applies the Raft commit rule (§5.4.2, Figure 8):
// index N commits when a majority of the cluster has N durable on disk AND
// term(N) equals the CURRENT term. The term condition is what stops a new
// leader from committing entries from previous terms merely by counting
// replicas — those commit indirectly, once a current-term entry above them
// commits (new leaders append a no-op for exactly this reason).
func (n *Group) maybeAdvanceCommit() error {
	matches := make([]uint64, 0, len(n.peers))
	for _, p := range n.peers {
		if p == n.id {
			matches = append(matches, n.rlog.LastIndex()) // our own log is synced
			continue
		}
		if prog, ok := n.progress[p]; ok {
			matches = append(matches, prog.match)
		} else {
			matches = append(matches, 0)
		}
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	if len(matches) < n.majorityN {
		return nil
	}
	nIdx := matches[n.majorityN-1]
	if nIdx <= n.commitIndex {
		return nil
	}
	term, err := n.rlog.Term(nIdx)
	if err != nil || term != n.term {
		// Prior-term entry (Figure 8) or unreadable — do not commit.
		return nil
	}
	n.commitIndex = nIdx
	n.logf("commit_advanced", "commit_index", nIdx)
	// Commit and apply are one loop turn: no observer can see a committed
	// entry that hasn't been applied on this node, and Propose waiters are
	// released with the state machine's result.
	return n.applyCommitted()
}

// applyCommitted executes every committed-but-unapplied entry in log order
// (Raft §5.3 / section 12 of the spec): lastApplied trails commitIndex by
// exactly the entries still owed to the state machine. Waiters on each
// index receive that entry's result — or its domain error, which is an
// outcome of the entry, not a failure of the node.
func (n *Group) applyCommitted() error {
	for n.lastApplied < n.commitIndex {
		idx := n.lastApplied + 1
		e, err := n.rlog.Get(idx)
		if err != nil {
			// A committed entry we cannot read: structural failure, and
			// continuing would apply a suffix — a different state machine
			// than every other replica. Halt.
			return fmt.Errorf("apply entry %d: %w", idx, err)
		}
		var (
			result any
			aerr   error
		)
		// Empty payload = leader no-op (or pre-command log): internal
		// raft traffic, not state-machine input.
		if len(e.Payload) > 0 && n.sm != nil {
			result, aerr = n.sm.Apply(e.Payload)
			if aerr != nil {
				n.logf("apply_error", "index", idx, "term", e.Term, "err", aerr.Error())
			}
		}
		n.lastApplied = idx
		for _, ch := range n.waiters[idx] {
			ch <- proposeReply{index: idx, result: result, err: aerr}
		}
		delete(n.waiters, idx)
	}
	// Snapshot window check (Phase 12): runs after the batch, on the loop
	// goroutine, so lastApplied is the exact position captured.
	n.maybeSnapshot()
	return nil
}

// failWaiters fails every pending Propose (leadership loss). Reply channels
// are buffered, so this never blocks the loop.
func (n *Group) failWaiters(err error) {
	for idx, chans := range n.waiters {
		for _, ch := range chans {
			ch <- proposeReply{err: err}
		}
		delete(n.waiters, idx)
	}
}
