package kv

// The replicated state machine seam (Phase 7): log payload -> Command ->
// Engine.Apply. SM adapts an Engine to the raft.StateMachine hook.

// SM binds a kv.Engine to the Raft apply path. Every replica runs the same
// decode-then-apply sequence in log order, which is what makes their states
// identical (D1).
//
// Domain outcomes (ErrNotInteger, overflow, ...) are per-entry results
// delivered to the proposer — the entry was processed, lastApplied advances
// regardless. Decode failures on a COMMITTED entry are refused-but-skipped
// identically on every replica (same bytes => same decision), with the
// error reported loudly: they signal corruption or version skew, never a
// routine outcome.
type SM struct {
	engine Engine
}

// NewSM returns the state-machine adapter for engine.
func NewSM(engine Engine) *SM { return &SM{engine: engine} }

// Apply decodes payload and executes it against the engine, returning the
// kv.Result the proposer receives. It satisfies
// `interface { Apply([]byte) (any, error) }` — raft never imports kv.
func (s *SM) Apply(payload []byte) (any, error) {
	cmd, err := DecodeCommand(payload)
	if err != nil {
		return nil, err
	}
	return s.engine.Apply(cmd)
}
