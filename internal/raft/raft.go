// Package raft implements DistriKV's custom Raft consensus core:
// leader election (Phase 5), log replication (Phase 6), and beyond.
//
// # Concurrency model: single event loop
//
// All Raft state (term, votedFor, role, timers, votes) is owned by ONE
// goroutine running Node.Run. Everything that could touch that state —
// clock ticks, inbound RPCs, outbound RPC responses, status queries — is an
// event on a channel. There are no locks around Raft state because there is
// only one writer, ever. This makes every state transition sequential and
// reason-able (and testable by injecting events directly).
//
// # Time: tick-driven, injectable
//
// The node never reads a clock. An external driver calls Node.Tick() at a
// fixed period (StartTicker in production; manually in tests), and all
// timeouts are counted in ticks (ElectionTicks, randomized to [E, 2E) per
// term; HeartbeatTicks). Deterministic tests therefore control time exactly.
//
// # Durability: persist-before-respond
//
// Any state transition that changes term or votedFor is fsynced to the
// persistent Raft log's hard state BEFORE the RPC response that depends on
// it is sent, and before any outbound message assuming it. This is what
// prevents a crashed node from voting twice in the same term.
package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync/atomic"
	"time"

	raftpb "distrikv/gen/raft/v1"
	"distrikv/internal/raftlog"
	"distrikv/internal/transport"
)

// Role is a node's Raft role.
type Role string

const (
	RoleFollower  Role = "follower"
	RoleCandidate Role = "candidate"
	RoleLeader    Role = "leader"
)

// ErrStopped is returned by node entry points after Run has exited.
var ErrStopped = errors.New("raft: node stopped")

// Proposal errors (Phase 6).
var (
	// ErrNotLeader: Propose was called on a follower/candidate. Check
	// Status().LeaderID for the redirect target (Phase 8 does this for
	// clients).
	ErrNotLeader = errors.New("raft: not leader")

	// ErrLeadershipLost: the proposal was appended locally but the node
	// stopped being leader before it committed. The entry MAY still commit
	// under the new leader — the outcome is unknowable here, which is
	// exactly why client retries wait for Phase 9 dedup.
	ErrLeadershipLost = errors.New("raft: leadership lost before commit")

	// ErrEmptyProposal: empty payloads are reserved for internal no-op
	// entries (appended by new leaders), so a client empty proposal would
	// be indistinguishable from one.
	ErrEmptyProposal = errors.New("raft: empty proposal")
)

// Status is a snapshot of node state for observability and tests.
// It is safe to call from any goroutine.
type Status struct {
	ID       transport.NodeID
	Role     Role
	Term     uint64
	LeaderID transport.NodeID // "" = unknown
	// CommitIndex/LastApplied: written by Phase 6/7; present so Status
	// never changes shape.
	CommitIndex uint64
	LastApplied uint64
	// Log high-water marks (for election-safety reasoning in tests).
	LastLogIndex uint64
	LastLogTerm  uint64
	// Peers is the static cluster membership (including self).
	Peers []transport.NodeID
}

// Config constructs a Node.
type Config struct {
	// ID is this node's identity.
	ID transport.NodeID

	// Peers is the static cluster membership INCLUDING self.
	// nil means [ID] (standalone node — elects itself immediately).
	Peers []transport.NodeID

	// Transport is the outbound message channel (gRPC in production,
	// simulated network in Phase 14 tests).
	Transport transport.Transport

	// Log is the persistent Raft log: source of truth for hard state
	// (currentTerm/votedFor) and log high-water marks.
	Log *raftlog.Log

	// ElectionTicks is the BASE election timeout in ticks; the effective
	// timeout is randomized to [ElectionTicks, 2*ElectionTicks) to make
	// split votes unlikely.
	ElectionTicks int

	// HeartbeatTicks is the leader heartbeat period in ticks; must be
	// smaller than ElectionTicks (typically 3-5x smaller).
	HeartbeatTicks int

	// RNG randomizes election timeouts. Nil seeds from the clock; tests
	// pass a fixed seed for reproducibility. Only touched inside Run.
	RNG *rand.Rand

	// Logger receives election/leadership transitions (debug/info).
	// Nil disables logging.
	Logger *slog.Logger

	// StateMachine receives committed log entries in order (Phase 7).
	// Nil is legal: entries still replicate and commit, apply is a no-op
	// (the raft layer alone, as in the election/replication tests).
	StateMachine StateMachine

	// SnapshotEvery is how many APPLIED entries trigger a snapshot +
	// log compaction (Phase 12). 0 (default) disables snapshots: the log
	// then grows forever, which is fine for tests and wrong for a running
	// server (cmd/server sets a real value).
	//
	// The state machine must implement Snapshotter for this to take
	// effect; machines that don't simply never compact. Snapshots are a
	// capacity concern, not a safety one: a failure to snapshot is logged
	// and retried on the next window, never a node halt.
	SnapshotEvery uint64
}

// StateMachine is the apply hook the Raft layer drives: payload in, result
// out. It is called strictly in log order, exactly once per non-empty
// committed entry, on the event-loop goroutine. The returned result is
// delivered to the proposer waiting on that index; the returned error is a
// domain outcome for that entry (the node keeps running — every replica
// makes the same decision on the same bytes).
//
// Empty payloads (leader no-op entries) never reach Apply.
type StateMachine interface {
	Apply(payload []byte) (any, error)
}

// Snapshotter is an OPTIONAL capability of a StateMachine (Phase 12): the
// ability to serialize its full state and reload it. When a machine
// implements it and Config.SnapshotEvery > 0, the node periodically
// captures the state at lastApplied, persists it via the log's snapshot
// sidecar, and compacts the log entries the snapshot covers — then restores
// from the snapshot at startup before replaying the remaining log tail.
//
// A machine that does not implement it keeps working: snapshots are simply
// never taken (the log grows). Raft itself never interprets the payload —
// only the same state machine reads it back.
type Snapshotter interface {
	// Snapshot returns the machine's complete state at the moment of the
	// call (made on the event-loop goroutine, between applies, so it sees
	// a consistent prefix of the log).
	Snapshot() ([]byte, error)
	// Restore replaces the machine's state wholesale with a payload
	// Snapshot produced. Called from New (before Run), exactly once per
	// node startup that finds a persisted snapshot.
	Restore(data []byte) error
}

func (c Config) validate() error {
	if c.ID == "" {
		return errors.New("raft: empty node ID")
	}
	if c.Transport == nil {
		return errors.New("raft: nil transport")
	}
	if c.Log == nil {
		return errors.New("raft: nil log")
	}
	if c.ElectionTicks < 1 {
		return fmt.Errorf("raft: ElectionTicks must be >= 1, got %d", c.ElectionTicks)
	}
	if c.HeartbeatTicks < 1 || c.HeartbeatTicks >= c.ElectionTicks {
		return fmt.Errorf("raft: HeartbeatTicks must be in [1, ElectionTicks), got %d/%d", c.HeartbeatTicks, c.ElectionTicks)
	}
	return nil
}

// event is anything the loop processes.
type event interface{ isEvent() }

type tickEvent struct{}

func (tickEvent) isEvent() {}

type voteReqEvent struct {
	req   *raftpb.RequestVoteRequest
	reply chan voteReply
}

func (voteReqEvent) isEvent() {}

type voteReply struct {
	resp *raftpb.RequestVoteResponse
	err  error
}

type voteRespEvent struct {
	from transport.NodeID
	resp *raftpb.RequestVoteResponse
	err  error
}

func (voteRespEvent) isEvent() {}

type appReqEvent struct {
	req   *raftpb.AppendEntriesRequest
	reply chan appReply
}

func (appReqEvent) isEvent() {}

type appReply struct {
	resp *raftpb.AppendEntriesResponse
	err  error
}

// appRespEvent is an outbound AppendEntries reply (Phase 6). term is the
// REQUEST's term — errors carry no response, so the event must remember
// which leadership epoch it belongs to. gen is the send generation
// number: ReadIndex uses it to tell fresh acknowledgments (sent after a
// read began) from delayed responses to older requests (Phase 11).
type appRespEvent struct {
	from transport.NodeID
	term uint64
	gen  uint64
	resp *raftpb.AppendEntriesResponse
	err  error
}

func (appRespEvent) isEvent() {}

// snapReqEvent is an inbound InstallSnapshot RPC (Phase 13), routed
// through the loop like every other Raft decision.
type snapReqEvent struct {
	req   *raftpb.InstallSnapshotRequest
	reply chan snapReply
}

func (snapReqEvent) isEvent() {}

type snapReply struct {
	resp *raftpb.InstallSnapshotResponse
	err  error
}

// snapRespEvent is an outbound InstallSnapshot reply. included is the
// request's lastIncludedIndex — success advances the follower's match to
// it (the response doesn't echo it back). term/gen mirror appRespEvent:
// which leadership epoch the send belongs to (stale-response fencing) and
// the ReadIndex send generation.
type snapRespEvent struct {
	from     transport.NodeID
	term     uint64
	gen      uint64
	included uint64
	resp     *raftpb.InstallSnapshotResponse
	err      error
}

func (snapRespEvent) isEvent() {}

type statusEvent struct{ reply chan Status }

func (statusEvent) isEvent() {}

// proposeEvent asks the leader to replicate a payload. The reply is NOT
// written here: it is released when the entry has been applied (or fails
// fast on leadership loss), see Propose.
type proposeEvent struct {
	payload []byte
	reply   chan proposeReply
}

func (proposeEvent) isEvent() {}

type proposeReply struct {
	index  uint64
	result any // state machine's outcome for this entry (nil if none)
	err    error
}

// Node is a single Raft replica. Create with New, then run exactly one
// goroutine via Run; drive time with Tick (or StartTicker).
type Node struct {
	id        transport.NodeID
	peers     []transport.NodeID
	majorityN int
	transport transport.Transport
	rlog      *raftlog.Log
	electTO   int // base election timeout (ticks)
	heartTO   int // heartbeat period (ticks)
	rng       *rand.Rand
	log_      *slog.Logger
	sm        StateMachine // nil = apply is a no-op (see StateMachine)

	// events is the loop's mailbox. Buffered so ticks sent slightly before
	// Run starts queue instead of blocking; capacity is generous because
	// elections with many peers fan out quickly.
	events chan event
	// done closes when Run exits; entry points select on it to avoid
	// blocking forever on a stopped node.
	done chan struct{}
	// rpcCtx cancels all in-flight outbound RPC goroutines when Run exits.
	rpcCtx    context.Context
	rpcCancel context.CancelFunc
	running   atomic.Bool

	// --- state owned exclusively by the Run goroutine ---

	role     Role
	term     uint64
	votedFor transport.NodeID
	leaderID transport.NodeID

	// commitIndex advances by majority ack (Phase 6, leader) or LeaderCommit
	// (follower); lastApplied advances when the state machine consumes
	// committed entries (Phase 7). Status reports both.
	commitIndex uint64
	lastApplied uint64

	// Snapshot/compaction state (Phase 12): snapshottable is the
	// type-asserted Snapshotter (nil = snapshots disabled for this
	// machine); snapEvery mirrors Config.SnapshotEvery; lastSnapIndex is
	// the lastIncludedIndex of the newest snapshot this node has taken OR
	// restored (0 = none). Compaction discards entries <= lastSnapIndex,
	// so lastApplied >= lastSnapIndex always holds.
	snapshottable Snapshotter
	snapEvery     uint64
	lastSnapIndex uint64

	// votes dedupes grants for the current candidacy; len() is the
	// distinct-vote count.
	votes map[transport.NodeID]bool

	// progress is the leader's per-peer replication state (next/match/
	// in-flight), rebuilt fresh at every becomeLeader. Nil while follower.
	progress map[transport.NodeID]*peerProgress

	// waiters holds Propose callers blocked until their index commits;
	// released on commit advance, failed on leadership loss.
	waiters map[uint64][]chan proposeReply

	// ReadIndex (Phase 11): noopIndex is this term's leader no-op (0 =
	// not leader); readWaiters are reads awaiting the gates in readindex.go.
	noopIndex   uint64
	readWaiters []*readWait

	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int
}

// New validates cfg and loads persisted hard state from the log.
// It does not start anything: call Run.
func New(cfg Config) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	peers := cfg.Peers
	if len(peers) == 0 {
		peers = []transport.NodeID{cfg.ID}
	}
	seen := make(map[transport.NodeID]bool, len(peers))
	self := false
	peersCopy := make([]transport.NodeID, len(peers))
	copy(peersCopy, peers)
	for _, p := range peersCopy {
		if seen[p] {
			return nil, fmt.Errorf("raft: duplicate peer %q", p)
		}
		seen[p] = true
		if p == cfg.ID {
			self = true
		}
	}
	if !self {
		return nil, fmt.Errorf("raft: local ID %q missing from peers %v", cfg.ID, peersCopy)
	}

	rng := cfg.RNG
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		id:        cfg.ID,
		peers:     peersCopy,
		majorityN: len(peersCopy)/2 + 1,
		transport: cfg.Transport,
		rlog:      cfg.Log,
		electTO:   cfg.ElectionTicks,
		heartTO:   cfg.HeartbeatTicks,
		rng:       rng,
		log_:      cfg.Logger,
		sm:        cfg.StateMachine,

		events:    make(chan event, 128),
		done:      make(chan struct{}),
		rpcCtx:    ctx,
		rpcCancel: cancel,

		role:            RoleFollower,
		votes:           make(map[transport.NodeID]bool),
		waiters:         make(map[uint64][]chan proposeReply),
		electionTimeout: cfg.ElectionTicks,
	}

	// Recover persisted hard state: a restarted node must remember its
	// term and vote BEFORE it can safely participate.
	hs := cfg.Log.HardState()
	n.term = hs.Term
	n.votedFor = transport.NodeID(hs.VotedFor)
	n.resetElectionTimer()

	// Snapshot recovery (Phase 12): restore the captured state machine,
	// then fast-forward commitIndex/lastApplied to the snapshot's log
	// position so apply resumes at the tail instead of replaying entries
	// the snapshot already subsumes. Both are legal at exactly this value:
	// a snapshot is only ever taken from APPLIED state, and applied
	// implies committed.
	if n.sm != nil {
		if s, ok := n.sm.(Snapshotter); ok {
			n.snapshottable = s
		}
	}
	n.snapEvery = cfg.SnapshotEvery
	if err := n.restoreSnapshot(); err != nil {
		cancel()
		return nil, err
	}

	return n, nil
}

// restoreSnapshot loads the persisted snapshot (if any) into the state
// machine and aligns the node's apply position with it. Called from New,
// before Run: nothing mutates state concurrently yet.
//
// The three durable states it reconciles:
//
//  1. No snapshot: fresh or never-compacted node — nothing to do.
//  2. Snapshot + fully compacted log: firstIndex already re-established
//     from snapshot metadata by raftlog.Open — restore payload, adopt
//     position.
//  3. Snapshot saved but compaction not finished (crash between
//     SaveSnapshot and TruncatePrefix): the log still holds covered
//     entries — truncate them now (idempotent), then adopt the position.
//  4. Snapshot saved but the log ends before it (crash between the
//     follower's SaveSnapshot and Reset during InstallSnapshot): every
//     retained entry is covered — discard them all and rebase the
//     boundary on the metadata (Reset).
//
// A snapshot whose state machine cannot restore (or whose payload sits
// beyond the log's retained start — entries lost) is a hard error: running
// anyway would fabricate a state machine that diverges from the cluster.
func (n *Node) restoreSnapshot() error {
	meta, payload, err := n.rlog.LoadSnapshot()
	if err != nil {
		return fmt.Errorf("raft: load snapshot: %w", err)
	}
	if meta.Zero() {
		return nil
	}
	if n.snapshottable == nil {
		return fmt.Errorf("raft: snapshot at index %d exists but state machine cannot restore snapshots", meta.LastIncludedIndex)
	}
	// The log must retain everything after the snapshot position; a gap
	// means the tail entries the snapshot does not cover are gone.
	first := n.rlog.FirstIndex()
	if meta.LastIncludedIndex+1 < first {
		return fmt.Errorf("raft: snapshot at index %d but log starts at %d: entries %d..%d lost",
			meta.LastIncludedIndex, first, meta.LastIncludedIndex+1, first-1)
	}
	// Crash-window catch-up: discard entries the snapshot already covers
	// (TruncatePrefix is a no-op when already compacted past them). If the
	// log ends BEFORE the snapshot position — the InstallSnapshot crash
	// window (sidecar saved, log not yet rebased) — every retained entry is
	// covered, so discard all and rebase the boundary instead.
	if idx := meta.LastIncludedIndex; idx > first-1 {
		if idx > n.rlog.LastIndex() {
			if err := n.rlog.Reset(meta); err != nil {
				return fmt.Errorf("raft: rebase log on snapshot index %d: %w", idx, err)
			}
		} else if err := n.rlog.TruncatePrefix(idx); err != nil {
			return fmt.Errorf("raft: compact to snapshot index %d: %w", idx, err)
		}
	}

	if err := n.snapshottable.Restore(payload); err != nil {
		return fmt.Errorf("raft: restore snapshot payload at index %d: %w", meta.LastIncludedIndex, err)
	}
	n.lastApplied = meta.LastIncludedIndex
	n.commitIndex = meta.LastIncludedIndex
	n.lastSnapIndex = meta.LastIncludedIndex
	n.logfInfo("snapshot_restored",
		"last_included_index", meta.LastIncludedIndex,
		"last_included_term", meta.LastIncludedTerm,
		"payload_bytes", len(payload),
		"log_first_index", n.rlog.FirstIndex(),
	)
	return nil
}

// Run processes events until ctx is cancelled, then stops all event sources
// (RPC goroutines, ticker) via their own contexts. It must be called exactly
// once, from its own goroutine; it returns nil on clean shutdown or the
// persistence error that halted the node (a disk failure stops Raft — the
// node cannot uphold safety without its durable state).
func (n *Node) Run(ctx context.Context) error {
	if !n.running.CompareAndSwap(false, true) {
		return errors.New("raft: Run called twice")
	}
	defer close(n.done)
	defer n.rpcCancel()

	for {
		select {
		case <-ctx.Done():
			return nil
		case ev := <-n.events:
			if err := n.handle(ev); err != nil {
				return fmt.Errorf("raft: %s halted: %w", n.id, err)
			}
		}
	}
}

// Tick advances logical time by one unit. Drive it from exactly one driver
// goroutine (StartTicker does this in production).
func (n *Node) Tick() {
	select {
	case n.events <- tickEvent{}:
	case <-n.done:
	}
}

// Status returns a snapshot of the node's state (any goroutine).
func (n *Node) Status() Status {
	reply := make(chan Status, 1)
	select {
	case n.events <- statusEvent{reply: reply}:
	case <-n.done:
		return Status{ID: n.id}
	}
	select {
	case s := <-reply:
		return s
	case <-n.done:
		return Status{ID: n.id}
	}
}

// Propose replicates payload through the Raft log and blocks until the entry
// is APPLIED — committed on a majority (with the current-term rule) AND
// executed against the local state machine in log order. Returns the entry's
// log index and the state machine's result for it (what the client response
// is built from). The caller's own read-your-writes follows: apply finishes
// before this returns.
//
// Errors: ErrNotLeader (follower/candidate — see Status().LeaderID),
// ErrLeadershipLost (appended locally but the node lost leadership before
// commit; the entry may still commit elsewhere), ErrEmptyProposal,
// ErrStopped, ctx.Err(), or the state machine's domain error for this entry
// (the entry applied; only its outcome failed).
//
// Cancelling ctx does NOT retract an appended entry: it may still commit
// and apply. Distinguishing "applied" from "never happened" on retry is
// Phase 9's dedup job — until then, callers must not blindly retry writes.
func (n *Node) Propose(ctx context.Context, payload []byte) (uint64, any, error) {
	if len(payload) == 0 {
		return 0, nil, ErrEmptyProposal
	}
	reply := make(chan proposeReply, 1)
	ev := proposeEvent{payload: append([]byte(nil), payload...), reply: reply}
	select {
	case n.events <- ev:
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	case <-n.done:
		return 0, nil, ErrStopped
	}
	select {
	case r := <-reply:
		return r.index, r.result, r.err
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	case <-n.done:
		return 0, nil, ErrStopped
	}
}

// HandleRequestVote implements transport.Handler — called on the inbound
// RPC's goroutine; the actual decision happens inside the event loop so the
// response is ordered after any term/vote persistence.
func (n *Node) HandleRequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	reply := make(chan voteReply, 1)
	select {
	case n.events <- voteReqEvent{req: req, reply: reply}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.done:
		return nil, ErrStopped
	}
	select {
	case r := <-reply:
		return r.resp, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.done:
		return nil, ErrStopped
	}
}

// HandleAppendEntries implements transport.Handler (see HandleRequestVote).
func (n *Node) HandleAppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	reply := make(chan appReply, 1)
	select {
	case n.events <- appReqEvent{req: req, reply: reply}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.done:
		return nil, ErrStopped
	}
	select {
	case r := <-reply:
		return r.resp, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.done:
		return nil, ErrStopped
	}
}

// HandleInstallSnapshot implements transport.Handler (see HandleRequestVote).
func (n *Node) HandleInstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	reply := make(chan snapReply, 1)
	select {
	case n.events <- snapReqEvent{req: req, reply: reply}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.done:
		return nil, ErrStopped
	}
	select {
	case r := <-reply:
		return r.resp, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.done:
		return nil, ErrStopped
	}
}

// StartTicker drives n.Tick at period until ctx is cancelled. This is the
// production time source; tests call Tick manually instead.
func StartTicker(ctx context.Context, n *Node, period time.Duration) {
	go func() {
		t := time.NewTicker(period)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				n.Tick()
			}
		}
	}()
}

// handle dispatches one event, then runs the ReadIndex hook: commit
// advance, apply advance, and fresh quorum acks all arrive as ordinary
// events, so one post-event point is exactly where pending reads
// activate and release. A returned error halts the node.
func (n *Node) handle(ev event) error {
	if err := n.dispatch(ev); err != nil {
		return err
	}
	n.checkReads()
	return nil
}

// dispatch runs the event itself (see handle for the ReadIndex hook).
func (n *Node) dispatch(ev event) error {
	switch e := ev.(type) {
	case tickEvent:
		return n.onTick()

	case voteReqEvent:
		resp, err := n.onVoteRequest(e.req)
		e.reply <- voteReply{resp: resp, err: err}
		return err

	case voteRespEvent:
		return n.onVoteResponse(e)

	case appRespEvent:
		return n.onAppendResponse(e)

	case appReqEvent:
		resp, err := n.onAppendEntries(e.req)
		e.reply <- appReply{resp: resp, err: err}
		return err

	case snapReqEvent:
		resp, err := n.onInstallSnapshot(e.req)
		e.reply <- snapReply{resp: resp, err: err}
		return err

	case snapRespEvent:
		return n.onInstallSnapshotResponse(e)

	case statusEvent:
		e.reply <- n.snapshot()
		return nil

	case proposeEvent:
		return n.onPropose(e)

	case readIndexEvent:
		return n.onReadIndex(e)

	default:
		return fmt.Errorf("raft: unknown event %T", ev)
	}
}

// snapshot builds Status (loop goroutine only).
func (n *Node) snapshot() Status {
	return Status{
		ID:           n.id,
		Role:         n.role,
		Term:         n.term,
		LeaderID:     n.leaderID,
		CommitIndex:  n.commitIndex,
		LastApplied:  n.lastApplied,
		LastLogIndex: n.rlog.LastIndex(),
		LastLogTerm:  n.rlog.LastTerm(),
		Peers:        append([]transport.NodeID(nil), n.peers...),
	}
}
