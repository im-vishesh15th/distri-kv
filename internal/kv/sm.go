package kv

// The replicated state machine seam (Phase 7) + client-session
// deduplication (Phase 9): log payload -> Command -> dedup check ->
// Engine.Apply.
//
// Why dedup lives here: the session table is REPLICATED STATE. Every
// replica must reach the same duplicate/not-duplicate decision for the
// same entry, which only works if the decision is made from the logged
// command (client_id + sequence_number ride the entry, codec.go) at apply
// time, in log order — the same determinism that makes the engine state
// converge (D1) makes the session table converge.

import "fmt"

// SM binds a kv.Engine to the Raft apply path. Every replica runs the same
// decode-then-apply sequence in log order, which is what makes their states
// identical (D1).
//
// Phase 9 adds a per-client session table consulted before every apply:
//
//	seq >  last  (or first request) -> apply, record (seq, result, err)
//	seq == last                    -> DUPLICATE: replay the recorded
//	                                  result/error without touching the engine
//	seq <  last                    -> superseded: ErrStaleSequence, NOT
//	                                  applied (re-applying would clobber
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
// to client_id (a client that loses its identity starts a fresh session).
//
// Apply is called serially by the Raft event loop (never concurrently);
// the session map needs no lock. Session entries accumulate one per
// client — bounding them is snapshot/GC work (Phase 12), which must also
// serialize this table into snapshots (spec §13: session state must be
// handled correctly with replication and snapshots).
type SM struct {
	engine   Engine
	sessions map[string]session
}

// session records the last request applied for one client_id: enough to
// recognize its retry (same sequence_number) and replay the original
// response, including a domain error (a retry of a failed INCR must fail
// identically rather than succeed against state that has since changed).
type session struct {
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
		s.sessions[cmd.ClientID] = session{
			lastSeq:   cmd.Sequence,
			result:    result,
			resultErr: aerr,
		}
	}
	return result, aerr
}
