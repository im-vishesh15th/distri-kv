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

type statusEvent struct{ reply chan Status }

func (statusEvent) isEvent() {}

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

	// commitIndex/lastApplied are advanced by Phase 6/7; declared now so
	// Status is stable.
	commitIndex uint64
	lastApplied uint64

	// votes dedupes grants for the current candidacy; len() is the
	// distinct-vote count.
	votes map[transport.NodeID]bool

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

		events:    make(chan event, 128),
		done:      make(chan struct{}),
		rpcCtx:    ctx,
		rpcCancel: cancel,

		role:            RoleFollower,
		votes:           make(map[transport.NodeID]bool),
		electionTimeout: cfg.ElectionTicks,
	}

	// Recover persisted hard state: a restarted node must remember its
	// term and vote BEFORE it can safely participate.
	hs := cfg.Log.HardState()
	n.term = hs.Term
	n.votedFor = transport.NodeID(hs.VotedFor)
	n.resetElectionTimer()

	return n, nil
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

// handle dispatches one event. A returned error halts the node.
func (n *Node) handle(ev event) error {
	switch e := ev.(type) {
	case tickEvent:
		return n.onTick()

	case voteReqEvent:
		resp, err := n.onVoteRequest(e.req)
		e.reply <- voteReply{resp: resp, err: err}
		return err

	case voteRespEvent:
		return n.onVoteResponse(e)

	case appReqEvent:
		resp, err := n.onAppendEntries(e.req)
		e.reply <- appReply{resp: resp, err: err}
		return err

	case statusEvent:
		e.reply <- n.snapshot()
		return nil

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
