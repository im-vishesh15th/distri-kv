package raft

import (
	"context"

	"distrikv/internal/transport"
)

// ReadIndex (Phase 11, spec §14): the mechanism that makes reads
// linearizable WITHOUT writing to the log. "Reads go to the leader" is
// not a proof — a deposed or partitioned leader can still believe it
// leads while a newer leader commits writes it has never seen. The proof
// has three gates, and ReadIndex enforces all three before returning.
//
// Gate 1 — fresh leadership proof. Raft elects at most one leader per
// term, so a majority acknowledging OUR term, for requests sent AFTER
// the read began, rules out any other leader having been elected as of
// that quorum. Freshness is what makes delayed responses harmless: every
// outbound send carries a generation number (peerProgress.gen), each
// waiter records the generation it needs, and responses to pre-read
// requests are excluded. Stale acks from before a partition could
// otherwise confirm a leader that is no longer authoritative.
//
// Gate 2 — current-term no-op committed (Figure 8). Leader completeness
// already put every previously committed entry in our LOG at election;
// the term's no-op committing is what pulls commitIndex up over them, so
// commitIndex — the read point — genuinely covers everything committed
// by any earlier leader.
//
// Gate 3 — apply has caught up. lastApplied >= readIndex is guaranteed
// BEFORE ReadIndex returns, so the state machine itself reflects the
// read point (not merely the commit cursor).
//
// The service reads the engine only after all three: a returned index
// means "a read now reflects every write acknowledged before this call".
// A read has no side effects, so any failure (ErrNotLeader on
// follower/candidate or lost leadership, ErrStopped, ctx) is safe for the
// caller to route elsewhere and retry.

// readIndexEvent asks the loop to run the ReadIndex gates. The reply is
// written when all three pass — or with an error if leadership is lost
// while waiting (see ReadIndex).
type readIndexEvent struct {
	reply chan readIndexReply
}

func (readIndexEvent) isEvent() {}

type readIndexReply struct {
	index uint64
	err   error
}

// readWait is one in-flight read. Stage 1 (ready=false): the term's
// no-op has not committed yet — activation waits for it. Stage 2
// (ready=true): ridx/term/minGen are fixed, acks collects the fresh
// quorum, and release needs len(acks) >= majority AND lastApplied >= ridx.
type readWait struct {
	reply chan readIndexReply

	ready bool
	ridx  uint64
	term  uint64
	acks  map[transport.NodeID]bool
	// minGen[p] is the FIRST send generation that post-dates this read:
	// only AppendEntries responses at or above it count (delayed
	// responses to pre-read requests are excluded — gate 1).
	minGen map[transport.NodeID]uint64
}

// ReadIndex blocks until it is SAFE to read the local state machine, and
// returns the read point. See the file comment for the three gates; in
// short: when ReadIndex returns, the state machine reflects every write
// acknowledged to any client before this call began.
//
// Errors: ErrNotLeader (this node is a follower/candidate, or lost
// leadership while the read waited — redirect and retry; reads have no
// side effects), ErrStopped (Run exited), ctx.Err() (the caller gave up;
// the loop may still complete the read later and discard the reply).
func (n *Group) ReadIndex(ctx context.Context) (uint64, error) {
	reply := make(chan readIndexReply, 1)
	select {
	case n.events <- readIndexEvent{reply: reply}:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.done:
		return 0, ErrStopped
	}
	select {
	case r := <-reply:
		return r.index, r.err
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.done:
		return 0, ErrStopped
	}
}

// onReadIndex registers a read; activation/release happen in checkReads,
// which runs at the end of every loop turn — including this one, so a
// read that is already past all gates answers without another event.
func (n *Group) onReadIndex(e readIndexEvent) error {
	if n.role != RoleLeader {
		e.reply <- readIndexReply{err: ErrNotLeader}
		return nil
	}
	n.readWaiters = append(n.readWaiters, &readWait{reply: e.reply})
	return nil
}

// checkReads drives every pending read: activates stage-1 waiters once
// the term's no-op has committed, releases those whose fresh quorum is
// counted AND whose apply cursor has reached the read point. Loop
// goroutine only; called once after each handled event.
func (n *Group) checkReads() {
	if len(n.readWaiters) == 0 {
		return
	}
	if n.role != RoleLeader {
		// leaveLeadership fails reads on every leader exit path; this is
		// a defensive belt so no read can hang on a non-leader.
		n.failReads(ErrNotLeader)
		return
	}

	gate := n.noopIndex > 0 && n.commitIndex >= n.noopIndex
	kept := n.readWaiters[:0]
	for _, w := range n.readWaiters {
		if !w.ready {
			if !gate {
				kept = append(kept, w) // stage 1: waiting for the no-op
				continue
			}
			n.activateRead(w)
		}
		if len(w.acks) >= n.majorityN && n.lastApplied >= w.ridx {
			w.reply <- readIndexReply{index: w.ridx} // buffered: never blocks
			continue                                 // released — drop it
		}
		kept = append(kept, w)
	}
	n.readWaiters = kept
}

// activateRead moves a read to stage 2: the read point is commitIndex
// NOW (gate 2 passed), the quorum proof restarts from this moment, and
// a send is forced to every peer so a fresh acknowledgment exists to
// wait for.
func (n *Group) activateRead(w *readWait) {
	w.ready = true
	w.ridx = n.commitIndex
	w.term = n.term
	w.acks = map[transport.NodeID]bool{n.id: true} // self counts
	w.minGen = make(map[transport.NodeID]uint64, len(n.progress))

	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		prog, ok := n.progress[p]
		if !ok {
			continue
		}
		// The next send (forced now, or the resend of an in-flight
		// request) will be gen+1 — strictly after this read began.
		w.minGen[p] = prog.gen + 1
		if prog.inflight {
			prog.pending = true // resend after the current response lands
		} else {
			n.sendAppendTo(p)
		}
	}
}

// countReadAck folds one fresh, successful AppendEntries acknowledgment
// into the pending reads it post-dates. Called from onAppendResponse
// with the response's send generation (gate 1 freshness).
func (n *Group) countReadAck(from transport.NodeID, term, gen uint64) {
	for _, w := range n.readWaiters {
		if !w.ready || w.term != term {
			continue
		}
		if min, ok := w.minGen[from]; ok && gen >= min {
			w.acks[from] = true
		}
	}
}

// failReads fails every pending read (leadership loss / step-down). The
// reply channels are buffered, so this never blocks the loop.
func (n *Group) failReads(err error) {
	for _, w := range n.readWaiters {
		w.reply <- readIndexReply{err: err}
	}
	n.readWaiters = nil
}
