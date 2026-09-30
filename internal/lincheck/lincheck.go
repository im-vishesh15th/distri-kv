// Package lincheck decides whether a recorded operation history is
// linearizable (spec §20/§21: "these histories can then be analyzed for
// consistency properties").
//
// A history is linearizable when there exists a total order of its
// operations — consistent with real time (an operation that finished
// before another started must precede it) — such that every successful
// operation's recorded output matches the KV model's state at its
// position in the order. The model is internal/kv's exact semantics:
// Set upserts, Get reports the current value or absence, IncrBy starts a
// missing key at 0 and reports the new value, CAS reports whether its
// precondition held.
//
// Failure semantics (the subtle part): a failed write's outcome is
// unknown — the entry may or may not have committed — so the checker
// branches and tries both. A failed read returns no answer and imposes
// no constraint, so it is dropped. This is what makes the check exact
// for histories recorded through retrying clients (the SDK retries with
// the same session sequence; dedup makes that safe).
//
// The spec suggests "Porcupine or another suitable checker"; this is a
// custom backtracking search with memoization (the same family of
// algorithm), kept dependency-free and unit-tested against known-good
// and known-bad histories in lincheck_test.go.
package lincheck

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"distrikv/internal/history"
)

// Absent is the recorded output for a Get of a missing key. It doubles
// as the CAS "expect absent" sentinel inside a recorded CAS input.
const Absent = "<absent>"

// casInputSep separates the expected and new values in a recorded CAS
// input ("old\x00new").
const casInputSep = "\x00"

// nodeBudget bounds the backtracking search so a pathological history
// fails loudly instead of hanging the test suite.
const nodeBudget = 5_000_000

// node is one op in the search universe: a successful op (must be
// explained) or a failed write (optional effect). Failed reads never
// enter the universe — they impose no constraint.
type node struct {
	op       history.Op
	must     bool // successful: output must match the model
	optional bool // failed write: may or may not take effect
}

// Check decides whether ops is linearizable. On success it also returns
// the linearization — the op IDs in explained order (failed reads
// excluded) — which is useful for debugging a failure.
//
// Two phases, because the failed-write branching is the expensive part:
//
//  1. Linearize the successful ops alone — a failed write placed without
//     effect is a valid outcome for an unknown-result op, so if the
//     successful ops are linearizable on their own, the history is too.
//     No branching: one plain backtracking search.
//  2. Only if that fails, branch on the failed writes' effects (each may
//     or may not have committed) and search again.
//
// The model is internal/kv's exact semantics: Set upserts, Get reports
// the current value or absence, IncrBy starts a missing key at 0 and
// reports the new value, CAS reports whether its precondition held
// (old == "<absent>" means "key must be absent").
func Check(ops []history.Op) (linearization []int64, err error) {
	if len(ops) == 0 {
		return nil, nil
	}

	// Partition: successful ops must be explained; failed writes are
	// optional (unknown outcome); failed reads are dropped (no output,
	// no effect — a read imposes no constraint).
	var must, optional []node
	for _, op := range ops {
		switch {
		case op.Success:
			must = append(must, node{op: op, must: true})
		case op.Type == history.OpSet || op.Type == history.OpIncrBy || op.Type == history.OpCAS:
			optional = append(optional, node{op: op, optional: true})
		}
	}

	// Phase 1: successful ops alone (failed writes as no-ops).
	if lin, _ := runSearch(must); lin != nil {
		return lin, nil
	}

	// Phase 2: branch on the failed writes' effects.
	lin2, hit := runSearch(append(append([]node{}, must...), optional...))
	if lin2 != nil {
		return lin2, nil
	} else if hit {
		return nil, fmt.Errorf("linearizability check exceeded its %d-node budget on a %d-op history", nodeBudget, len(ops))
	}

	return nil, fmt.Errorf("history of %d ops is not linearizable", len(ops))
}

// runSearch runs the backtracking linearization search over nodes
// (successful + optional failed writes) and returns the linearization
// (op IDs of successful ops, in explained order). The second result is
// true if the node budget was exceeded (the answer is then unknown, not
// "non-linearizable").
func runSearch(nodes []node) (lin []int64, budgetHit bool) {
	n := len(nodes)
	if n == 0 {
		return nil, false
	}

	// Real-time predecessors: preds[i] holds every j whose op finished
	// strictly before i started — j must precede i in the linearization.
	preds := make([][]int, n)
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j && nodes[j].op.Finish.Before(nodes[i].op.Start) {
				preds[i] = append(preds[i], j)
			}
		}
	}

	// Candidate iteration prefers real-time order (prunes faster).
	byStart := make([]int, n)
	for i := range byStart {
		byStart[i] = i
	}
	sort.SliceStable(byStart, func(a, b int) bool {
		return nodes[byStart[a]].op.Start.Before(nodes[byStart[b]].op.Start)
	})

	s := &searcher{
		nodes:   nodes,
		preds:   preds,
		byStart: byStart,
		memo:    make(map[string]bool),
		order:   make([]int, 0, n),
	}
	if !s.search(make([]byte, (n+7)/8), &model{state: make(map[string]string)}) {
		return nil, s.budgetExceeded
	}
	for _, ni := range s.order {
		if s.nodes[ni].op.Success {
			lin = append(lin, s.nodes[ni].op.ID)
		}
	}
	return lin, false
}

// model is the checked state: the KV key space.
type model struct {
	state map[string]string
}

func (m *model) clone() *model {
	nm := &model{state: make(map[string]string, len(m.state))}
	for k, v := range m.state {
		nm.state[k] = v
	}
	return nm
}

// hash identifies the state for memoization.
func (m *model) hash() string {
	keys := make([]string, 0, len(m.state))
	for k := range m.state {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(m.state[k])
		b.WriteByte('\n')
	}
	return b.String()
}

// applyMust applies a successful op and checks its recorded output
// against the model. It returns the new state and whether the op is
// consistent at this position.
func applyMust(m *model, op history.Op) (*model, bool) {
	switch op.Type {
	case history.OpSet:
		if op.Output != "" {
			return nil, false // our recording format: Set carries no output
		}
		nm := m.clone()
		nm.state[op.Key] = op.Input
		return nm, true

	case history.OpGet:
		v, ok := m.state[op.Key]
		if !ok {
			return m, op.Output == Absent // reads don't change state
		}
		return m, op.Output == v

	case history.OpIncrBy:
		d, err := strconv.ParseInt(op.Input, 10, 64)
		if err != nil {
			return nil, false
		}
		n := int64(0)
		if cur, ok := m.state[op.Key]; ok {
			parsed, perr := strconv.ParseInt(cur, 10, 64)
			if perr != nil {
				return nil, false // non-integer value: the real system would have failed the op
			}
			n = parsed
		}
		n += d
		want := strconv.FormatInt(n, 10)
		if op.Output != want {
			return nil, false
		}
		nm := m.clone()
		nm.state[op.Key] = want
		return nm, true

	case history.OpCAS:
		old, new, ok := strings.Cut(op.Input, casInputSep)
		if !ok {
			return nil, false
		}
		if op.Output != "true" && op.Output != "false" {
			return nil, false
		}
		match := false
		if old == Absent {
			_, present := m.state[op.Key]
			match = !present
		} else {
			match = m.state[op.Key] == old
		}
		if op.Output == "true" && !match {
			return nil, false
		}
		if op.Output == "false" && match {
			return nil, false
		}
		nm := m.clone()
		if match {
			nm.state[op.Key] = new
		}
		return nm, true
	}
	return nil, false
}

// applyOptional applies a failed write's state transition with no output
// check — the op's outcome is unknown, only its effect (if any) matters.
func applyOptional(m *model, op history.Op) *model {
	nm := m.clone()
	switch op.Type {
	case history.OpSet:
		nm.state[op.Key] = op.Input

	case history.OpIncrBy:
		d, err := strconv.ParseInt(op.Input, 10, 64)
		if err != nil {
			return nm
		}
		n := int64(0)
		if cur, ok := nm.state[op.Key]; ok {
			parsed, perr := strconv.ParseInt(cur, 10, 64)
			if perr != nil {
				return nm
			}
			n = parsed
		}
		nm.state[op.Key] = strconv.FormatInt(n+d, 10)

	case history.OpCAS:
		old, new, ok := strings.Cut(op.Input, casInputSep)
		if !ok {
			return nm
		}
		match := false
		if old == Absent {
			_, present := nm.state[op.Key]
			match = !present
		} else {
			match = nm.state[op.Key] == old
		}
		if match {
			nm.state[op.Key] = new
		}
	}
	return nm
}

// searcher is the backtracking linearization search. State: which ops are
// placed (bitmask) and the current model. A placement is extendable by
// any unplaced op whose real-time predecessors are all placed.
type searcher struct {
	nodes          []node
	preds          [][]int
	byStart        []int
	memo           map[string]bool // (placed, state) -> known dead end
	order          []int           // placement order (node indices)
	nodesExplored  int
	budgetExceeded bool
}

func (s *searcher) set(placed []byte, i int, v bool) {
	if v {
		placed[i>>3] |= 1 << (i & 7)
	} else {
		placed[i>>3] &^= 1 << (i & 7)
	}
}

func (s *searcher) placedBit(placed []byte, i int) bool {
	return placed[i>>3]&(1<<(i&7)) != 0
}

func (s *searcher) search(placed []byte, m *model) bool {
	if len(s.order) == len(s.nodes) {
		return true
	}
	if s.nodesExplored > nodeBudget {
		s.budgetExceeded = true
		return false
	}
	key := string(placed) + "|" + m.hash()
	if s.memo[key] {
		return false
	}

	for _, i := range s.byStart {
		if s.placedBit(placed, i) {
			continue
		}
		ready := true
		for _, p := range s.preds[i] {
			if !s.placedBit(placed, p) {
				ready = false
				break
			}
		}
		if !ready {
			continue
		}
		s.nodesExplored++

		if s.nodes[i].must {
			if nm, ok := applyMust(m, s.nodes[i].op); ok {
				s.set(placed, i, true)
				s.order = append(s.order, i)
				if s.search(placed, nm) {
					return true
				}
				s.order = s.order[:len(s.order)-1]
				s.set(placed, i, false)
			}
			continue
		}

		// Optional (failed write): branch 1 — it took effect.
		nm := applyOptional(m, s.nodes[i].op)
		s.set(placed, i, true)
		s.order = append(s.order, i)
		if s.search(placed, nm) {
			return true
		}
		s.order = s.order[:len(s.order)-1]
		// Branch 2 — no effect (still marked placed so it isn't retried).
		s.order = append(s.order, i)
		if s.search(placed, m) {
			return true
		}
		s.order = s.order[:len(s.order)-1]
		s.set(placed, i, false)
	}

	s.memo[key] = false
	return false
}
