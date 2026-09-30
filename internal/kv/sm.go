package kv

// The replicated state machine seam (Phase 7) + client-session
// deduplication (Phase 9) + snapshots (Phase 12): log payload -> Command ->
// dedup check -> Engine.Apply, with the full state (engine AND session
// table) serializable so Raft can compact its log.
//
// Why dedup lives here: the session table is REPLICATED STATE. Every
// replica must reach the same duplicate/not-duplicate decision for the
// same entry, which only works if the decision is made from the logged
// command (client_id + sequence_number ride the entry, codec.go) at apply
// time, in log order — the same determinism that makes the engine state
// converge (D1) makes the session table converge. Snapshots preserve the
// same property: serializing this table (spec §13: session state must be
// handled correctly with replication and snapshots) keeps dedup decisions
// identical on a node that resumed from a snapshot and one that replayed
// the full log.

import (
	"fmt"
	"sort"

	kv1 "distrikv/gen/kv/v1"
	"google.golang.org/protobuf/proto"
)

// SM binds a kv.Engine to the Raft apply path. Every replica runs the same
// decode-then-apply sequence in log order, which is what makes their states
// identical (D1).
//
// Phase 9 adds a per-client session table consulted before every apply:
//
//	seq >  last  (or first request) -> apply, record (seq, result, err)
//	seq == last                    -> DUPLICATE: replay the recorded
//	                                  result/error without touching the engine
//	seq <  last                    -> REJECTED as stale: never applied
//	                                  (re-applying would clobber
//	                                  state written by newer requests)
//
// Gaps (seq > last+1) are legal: an attempt the client abandoned before
// sending consumes its sequence number. client_id == "" opts out entirely
// (internal/test commands, always applied) — the gRPC edge rejects
// mutations without session fields, so this path never serves clients.
//
// The precise guarantee (S1 in docs/consistency.md): a retried mutation
// applies AT MOST ONCE per session, and its original response is replayed
// when it did apply. This is not "exactly once": a request the client
// gives up on without retrying is simply lost, and dedup state is scoped
// to client_id with a bounded table (sessionCap below), so the at-most-once
// window ends when a session is evicted — documented, never silent.
//
// Apply is called serially by the Raft event loop (never concurrently);
// the session map needs no lock. Phase 12 adds Snapshot/Restore (the
// raft.Snapshotter capability): both serialize the engine dump AND this
// session table, and Restore replaces wholesale (startup only).
const (
	// sessionCap bounds the session table: at sessionCap live sessions, the
	// arrival of a NEW client_id evicts the earliest-created session (birth
	// order — deterministic replicated state, so every replica evicts the
	// same one).
	//
	// Why bounded: sessions accumulate one per distinct client_id ever
	// seen; unbounded growth is a durability leak. What the cap costs: a
	// retry whose original application predates its session's eviction is
	// no longer recognized and applies again (the dedup horizon shrinks to
	// "the last 65536 sessions"). Eviction requires 65536 other sessions to
	// appear first, while SDK retries finish within seconds — so the
	// weakening never fires for a live retry in practice. See S1.
	sessionCap = 65536
)

type SM struct {
	engine   Engine
	sessions map[string]session
	// nextBirth numbers sessions in creation order; it advances only in
	// Apply (replicated determinism) and is snapshotted so post-restore
	// eviction order matches every replica that never restarted.
	nextBirth uint64
}

// session records the last request applied for one client_id: enough to
// recognize its retry (same sequence_number) and replay the original
// response, including a domain error (a retry of a failed INCR must fail
// identically rather than succeed against state that has since changed).
// birth is the creation counter used for deterministic cap eviction.
type session struct {
	birth     uint64
	lastSeq   uint64
	result    Result
	resultErr error
}

// NewSM returns the state-machine adapter for engine.
func NewSM(engine Engine) *SM {
	return &SM{engine: engine, sessions: make(map[string]session)}
}

// Apply decodes payload, dedup-checks it against the session table, and —
// unless it is a duplicate — executes it against the engine, returning the
// kv.Result the proposer receives. It satisfies
// `interface { Apply([]byte) (any, error) }` — raft never imports kv.
func (s *SM) Apply(payload []byte) (any, error) {
	cmd, err := DecodeCommand(payload)
	if err != nil {
		return nil, err
	}

	if cmd.ClientID != "" {
		if sess, seen := s.sessions[cmd.ClientID]; seen {
			switch {
			case cmd.Sequence == sess.lastSeq:
				// Retry of the last applied request: replay the recorded
				// outcome. The engine is NOT touched.
				return sess.result, sess.resultErr
			case cmd.Sequence < sess.lastSeq:
				// Superseded: a request older than one already applied
				// from this session. Applying it would rewind state.
				return Result{}, fmt.Errorf("%w: client %s seq %d, last applied %d",
					ErrStaleSequence, cmd.ClientID, cmd.Sequence, sess.lastSeq)
			}
		}
	}

	result, aerr := s.engine.Apply(cmd)

	if cmd.ClientID != "" {
		sess, seen := s.sessions[cmd.ClientID]
		if !seen {
			// A new session may need to evict the oldest to stay under
			// the cap. Deterministic: same orders everywhere (D1).
			s.evictOldestForRoom()
			s.nextBirth++
			sess.birth = s.nextBirth
		}
		sess.lastSeq = cmd.Sequence
		sess.result = result
		sess.resultErr = aerr
		s.sessions[cmd.ClientID] = sess
	}
	return result, aerr
}

// evictOldestForRoom drops the earliest-created session when the table is
// full, making room for the one about to be inserted. Called only when a
// NEW session arrives (an existing session never grows the table).
func (s *SM) evictOldestForRoom() {
	if len(s.sessions) < sessionCap {
		return
	}
	var (
		victim   string
		minBirth = ^uint64(0)
	)
	for id, sess := range s.sessions {
		if sess.birth < minBirth {
			minBirth = sess.birth
			victim = id
		}
	}
	if victim != "" {
		delete(s.sessions, victim)
	}
}

// Snapshot serializes the complete SM state — engine key space, session
// table, and eviction counter — for raft.Snapshotter (Phase 12). Raft
// persists the bytes with lastIncludedIndex/lastIncludedTerm and compacts
// the log prefix they cover.
//
// Sessions are emitted sorted by client_id so identical state yields
// identical bytes.
func (s *SM) Snapshot() ([]byte, error) {
	engineState, err := s.engine.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("kv: sm snapshot: %w", err)
	}

	ids := make([]string, 0, len(s.sessions))
	for id := range s.sessions {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	sessions := make([]*kv1.SessionState, 0, len(ids))
	for _, id := range ids {
		sess := s.sessions[id]
		ss := &kv1.SessionState{
			ClientId:     id,
			LastSequence: sess.lastSeq,
			Applied:      sess.result.Applied,
			ResultValue:  clone(sess.result.Value),
			Birth:        sess.birth,
		}
		if sess.resultErr != nil {
			ss.ResultError = sess.resultErr.Error()
		}
		sessions = append(sessions, ss)
	}

	msg := &kv1.StateMachineSnapshot{
		EngineState:      engineState,
		Sessions:         sessions,
		NextSessionBirth: s.nextBirth,
	}
	b, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("kv: sm snapshot marshal: %w", err)
	}
	return b, nil
}

// Restore replaces the entire SM state with a Snapshot payload (startup
// only, before the node joins the cluster). It satisfies
// raft.Snapshotter. Anything the live state held that the snapshot does
// not is discarded — the restored state must equal exactly what the
// snapshotting replica saw at lastIncludedIndex, or apply decisions would
// diverge from the rest of the cluster.
func (s *SM) Restore(data []byte) error {
	msg := &kv1.StateMachineSnapshot{}
	if err := proto.Unmarshal(data, msg); err != nil {
		return fmt.Errorf("kv: sm restore: %w", err)
	}
	if err := s.engine.Restore(msg.EngineState); err != nil {
		return fmt.Errorf("kv: sm restore engine: %w", err)
	}

	sessions := make(map[string]session, len(msg.Sessions))
	for _, ss := range msg.Sessions {
		sessions[ss.ClientId] = session{
			birth:     ss.Birth,
			lastSeq:   ss.LastSequence,
			result:    Result{Applied: ss.Applied, Value: clone(ss.ResultValue)},
			resultErr: sessionError(ss.ResultError),
		}
	}
	s.sessions = sessions
	s.nextBirth = msg.NextSessionBirth
	return nil
}

// sessionError rebuilds a cached domain error from its Error() text. The
// known sentinels map back to the SAME error values, so a retry replayed
// after a snapshot restore still satisfies errors.Is and the SDK/server
// classify it exactly as the original application did. An unrecognized
// text becomes a plain error: identifiable by message, not by identity
// (only sentinel errors are part of the contract).
func sessionError(s string) error {
	switch s {
	case "":
		return nil
	case ErrKeyNotFound.Error():
		return ErrKeyNotFound
	case ErrNotInteger.Error():
		return ErrNotInteger
	case ErrOverflow.Error():
		return ErrOverflow
	case ErrUnknownOp.Error():
		return ErrUnknownOp
	default:
		return fmt.Errorf("kv: restored session error: %s", s)
	}
}
