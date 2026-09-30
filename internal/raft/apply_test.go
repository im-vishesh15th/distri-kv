package raft_test

// Phase 7: committed entries reach the state machine in log order,
// uncommitted entries never do, and Propose hands the apply outcome back to
// the proposer (result or domain error — an outcome, not a node failure).

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	raftpb "distrikv/gen/raft/v1"
)

// recordingSM is a deterministic test state machine: it records every
// payload it is asked to apply and echoes a value derived from it, so tests
// can assert both that Apply ran and what Propose delivered.
type recordingSM struct {
	mu       sync.Mutex
	payloads []string
	err      error // if set, every Apply reports it (domain outcome)
}

func (s *recordingSM) Apply(payload []byte) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.payloads = append(s.payloads, string(payload))
	if s.err != nil {
		return nil, s.err
	}
	return "ok:" + string(payload), nil
}

func (s *recordingSM) applied() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.payloads...)
}

// TestProposeDeliversApplyResult: Propose completes only after the entry has
// been applied, returning the entry's index and the state machine's result.
// The election no-op (empty payload) is raft-internal and must not reach the
// state machine.
func TestProposeDeliversApplyResult(t *testing.T) {
	sm := &recordingSM{}
	h := startNodeSM(t, "n1", 1, sm, "n1", "n2", "n3")
	electLeader(t, h, "n2", "n3")

	if got := sm.applied(); len(got) != 0 {
		t.Fatalf("state machine saw %q during election; want no entries (no-op skipped)", got)
	}

	res := asyncPropose(h, "hello")
	calls := collectAppCalls(t, h.tr.appCalls, 2)
	for _, c := range calls {
		h.tr.appReplies[c.to] <- appResult{resp: &raftpb.AppendEntriesResponse{Term: 1, Success: true}}
	}

	r := awaitPropose(t, res)
	if r.err != nil {
		t.Fatalf("Propose: %v", r.err)
	}
	if r.idx != 2 {
		t.Fatalf("index = %d, want 2 (no-op holds 1)", r.idx)
	}
	if r.result != "ok:hello" {
		t.Fatalf("result = %v, want %q", r.result, "ok:hello")
	}
	// Apply finishes before the proposer is released: lastApplied has
	// caught up to commitIndex by the time Propose returns.
	if st := h.n.Status(); st.CommitIndex != 2 || st.LastApplied != 2 {
		t.Fatalf("status = commit %d applied %d, want 2/2", st.CommitIndex, st.LastApplied)
	}
	if got := sm.applied(); !reflect.DeepEqual(got, []string{"hello"}) {
		t.Fatalf("applied payloads = %q, want [hello]", got)
	}
}

// TestApplyErrorIsAnOutcomeNotAFailure: a state-machine domain error is
// delivered to the proposer as the entry's outcome — lastApplied still
// advances (the entry was processed), and the node keeps running.
func TestApplyErrorIsAnOutcomeNotAFailure(t *testing.T) {
	sentinel := errors.New("domain boom")
	sm := &recordingSM{err: sentinel}
	h := startNodeSM(t, "n1", 1, sm, "n1", "n2", "n3")
	electLeader(t, h, "n2", "n3")

	res := asyncPropose(h, "x")
	calls := collectAppCalls(t, h.tr.appCalls, 2)
	for _, c := range calls {
		h.tr.appReplies[c.to] <- appResult{resp: &raftpb.AppendEntriesResponse{Term: 1, Success: true}}
	}

	r := awaitPropose(t, res)
	if !errors.Is(r.err, sentinel) {
		t.Fatalf("Propose err = %v, want the state machine's domain error", r.err)
	}
	if r.idx != 2 {
		t.Fatalf("index = %d, want 2 (entry applied despite its outcome)", r.idx)
	}
	waitFor(t, 2*time.Second, "lastApplied advances past a domain error", func() bool {
		return h.n.Status().LastApplied == 2
	})
	// The loop is still alive: a second proposal still works.
	res2 := asyncPropose(h, "y")
	calls = collectAppCalls(t, h.tr.appCalls, 2)
	for _, c := range calls {
		h.tr.appReplies[c.to] <- appResult{resp: &raftpb.AppendEntriesResponse{Term: 1, Success: true}}
	}
	if r2 := awaitPropose(t, res2); r2.idx != 3 || !errors.Is(r2.err, sentinel) {
		t.Fatalf("second Propose = (%d, %v), want (3, domain boom)", r2.idx, r2.err)
	}
}

// TestFollowerAppliesOnlyCommitted is section 12 of the spec made executable:
// entries in the log but not committed must not become visible as user
// state; once LeaderCommit advances, apply happens exactly once, in order.
func TestFollowerAppliesOnlyCommitted(t *testing.T) {
	sm := &recordingSM{}
	h := startNodeSM(t, "n1", 1, sm, "n1", "n2", "n3") // follower: no ticks
	ctx := context.Background()

	// Three entries appended, commit still at 0: nothing may apply.
	r, err := h.n.HandleAppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term: 5, LeaderId: "n2", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []*raftpb.LogEntry{
			{Index: 1, Term: 5, Payload: []byte("A")},
			{Index: 2, Term: 5, Payload: []byte("B")},
			{Index: 3, Term: 5, Payload: []byte("C")},
		},
		LeaderCommit: 0,
	})
	if err != nil || !r.Success {
		t.Fatalf("append: %+v err=%v", r, err)
	}
	if got := sm.applied(); len(got) != 0 {
		t.Fatalf("uncommitted entries applied: %q, want none", got)
	}
	if st := h.n.Status(); st.CommitIndex != 0 || st.LastApplied != 0 {
		t.Fatalf("status after uncommitted append = commit %d applied %d, want 0/0",
			st.CommitIndex, st.LastApplied)
	}

	// Commit to 2: exactly A and B apply, in log order — C is still owed.
	if _, err := h.n.HandleAppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term: 5, LeaderId: "n2", PrevLogIndex: 3, PrevLogTerm: 5,
		LeaderCommit: 2,
	}); err != nil {
		t.Fatalf("commit advance: %v", err)
	}
	if got := sm.applied(); !reflect.DeepEqual(got, []string{"A", "B"}) {
		t.Fatalf("applied after commit=2: %q, want [A B]", got)
	}
	if st := h.n.Status(); st.CommitIndex != 2 || st.LastApplied != 2 {
		t.Fatalf("status = commit %d applied %d, want 2/2", st.CommitIndex, st.LastApplied)
	}

	// Commit to 3: the remainder applies once; no re-application of A/B.
	if _, err := h.n.HandleAppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term: 5, LeaderId: "n2", PrevLogIndex: 3, PrevLogTerm: 5,
		LeaderCommit: 3,
	}); err != nil {
		t.Fatalf("commit advance: %v", err)
	}
	if got := sm.applied(); !reflect.DeepEqual(got, []string{"A", "B", "C"}) {
		t.Fatalf("applied after commit=3: %q, want [A B C]", got)
	}
	if st := h.n.Status(); st.LastApplied != 3 {
		t.Fatalf("lastApplied = %d, want 3", st.LastApplied)
	}
}

// TestLastAppliedSurvivesNoSm pins the no-state-machine configuration: the
// raft layer alone still tracks lastApplied = commitIndex (Phase 5/6 setup,
// and a node wired without an engine must not stall the apply cursor).
func TestLastAppliedTracksCommitWithoutSM(t *testing.T) {
	h := startNode(t, "n1", 1, "n1", "n2", "n3") // no SM
	electLeader(t, h, "n2", "n3")

	res := asyncPropose(h, "hello")
	calls := collectAppCalls(t, h.tr.appCalls, 2)
	for _, c := range calls {
		h.tr.appReplies[c.to] <- appResult{resp: &raftpb.AppendEntriesResponse{Term: 1, Success: true}}
	}
	r := awaitPropose(t, res)
	if r.err != nil || r.idx != 2 {
		t.Fatalf("Propose = (%d, %v), want (2, nil)", r.idx, r.err)
	}
	if r.result != nil {
		t.Fatalf("result = %v, want nil without a state machine", r.result)
	}
	if st := h.n.Status(); st.LastApplied != 2 {
		t.Fatalf("lastApplied = %d, want 2", st.LastApplied)
	}
}
