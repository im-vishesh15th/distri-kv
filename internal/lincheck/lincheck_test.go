package lincheck

import (
	"testing"
	"time"

	"distrikv/internal/history"
)

// op builds a history.Op with the given times. startMs/finishMs are
// milliseconds on an arbitrary axis — only their order matters.
func op(id int64, client string, typ history.OpType, key, input, output string, startMs, finishMs int64, success bool) history.Op {
	base := time.Unix(0, 0)
	return history.Op{
		ID: id, ClientID: client, Key: key, Type: typ,
		Input: input, Output: output,
		Start:   base.Add(time.Duration(startMs) * time.Millisecond),
		Finish:  base.Add(time.Duration(finishMs) * time.Millisecond),
		Success: success,
	}
}

func TestEmptyHistory(t *testing.T) {
	if _, err := Check(nil); err != nil {
		t.Fatalf("empty history: %v", err)
	}
}

func TestSequentialHistoryIsLinearizable(t *testing.T) {
	ops := []history.Op{
		op(1, "a", history.OpSet, "x", "1", "", 0, 10, true),
		op(2, "a", history.OpGet, "x", "", "1", 11, 20, true),
		op(3, "a", history.OpIncrBy, "n", "5", "5", 21, 30, true),
		op(4, "a", history.OpGet, "n", "", "5", 31, 40, true),
		op(5, "a", history.OpCAS, "x", "1"+casInputSep+"2", "true", 41, 50, true),
		op(6, "a", history.OpGet, "x", "", "2", 51, 60, true),
	}
	lin, err := Check(ops)
	if err != nil {
		t.Fatalf("sequential history must be linearizable: %v", err)
	}
	if len(lin) != 6 {
		t.Fatalf("linearization has %d ops, want 6", len(lin))
	}
}

func TestReadOfUnwrittenValueIsNotLinearizable(t *testing.T) {
	// Get returns "1" but no Set of "1" ever preceded it in any
	// real-time-consistent order.
	ops := []history.Op{
		op(1, "a", history.OpGet, "x", "", "1", 0, 10, true),
		op(2, "a", history.OpSet, "x", "1", "", 20, 30, true), // starts after the get finished
	}
	if _, err := Check(ops); err == nil {
		t.Fatal("read of a value written only later must not be linearizable")
	}
}

func TestConcurrentWritesAnyOrderIsLinearizable(t *testing.T) {
	// Two concurrent Sets to the same key: either order explains the
	// final read.
	ops := []history.Op{
		op(1, "a", history.OpSet, "x", "1", "", 0, 10, true),
		op(2, "b", history.OpSet, "x", "2", "", 0, 10, true), // concurrent with 1
		op(3, "a", history.OpGet, "x", "", "2", 11, 20, true),
	}
	if _, err := Check(ops); err != nil {
		t.Fatalf("concurrent writes with a later read must be linearizable: %v", err)
	}
}

func TestOverlappingReadsDifferentValuesAreNotLinearizable(t *testing.T) {
	// Two overlapping reads of the same key returning different values,
	// with no write in between: no real-time-consistent order explains
	// both — whichever order you pick, the second read contradicts the
	// first.
	ops := []history.Op{
		op(1, "a", history.OpSet, "x", "1", "", 0, 5, true),
		op(2, "a", history.OpGet, "x", "", "1", 6, 10, true), // after the set: sees "1"
		op(3, "b", history.OpGet, "x", "", "2", 7, 15, true), // overlaps op2, reads "2" — impossible
	}
	if _, err := Check(ops); err == nil {
		t.Fatal("overlapping reads with different values and no write must not be linearizable")
	}
}

func TestFailedWriteOptionalEffect(t *testing.T) {
	// The failed Set may or may not have committed. The read of "1" is
	// explained only if it did — the checker must take that branch.
	ops := []history.Op{
		op(1, "a", history.OpSet, "x", "1", "", 0, 10, false), // failed
		op(2, "a", history.OpGet, "x", "", "1", 11, 20, true),
	}
	if _, err := Check(ops); err != nil {
		t.Fatalf("failed write with visible effect must be linearizable: %v", err)
	}
}

func TestFailedWriteNoEffect(t *testing.T) {
	// The failed Set did not commit; the read of "absent" is explained
	// without it.
	ops := []history.Op{
		op(1, "a", history.OpSet, "x", "1", "", 0, 10, false),
		op(2, "a", history.OpGet, "x", "", Absent, 11, 20, true),
	}
	if _, err := Check(ops); err != nil {
		t.Fatalf("failed write with no effect must be linearizable: %v", err)
	}
}

func TestFailedReadDropped(t *testing.T) {
	// A failed read imposes no constraint.
	ops := []history.Op{
		op(1, "a", history.OpGet, "x", "", "", 0, 10, false),
		op(2, "a", history.OpSet, "x", "1", "", 11, 20, true),
	}
	if _, err := Check(ops); err != nil {
		t.Fatalf("failed read must be droppable: %v", err)
	}
}

func TestIncrByStartsMissingKeyAtZero(t *testing.T) {
	ops := []history.Op{
		op(1, "a", history.OpIncrBy, "n", "5", "5", 0, 10, true),
		op(2, "a", history.OpIncrBy, "n", "-2", "3", 11, 20, true),
		op(3, "a", history.OpGet, "n", "", "3", 21, 30, true),
	}
	if _, err := Check(ops); err != nil {
		t.Fatalf("incr from absent key must start at 0: %v", err)
	}
}

func TestCASPreconditionOutcomes(t *testing.T) {
	ops := []history.Op{
		op(1, "a", history.OpSet, "x", "1", "", 0, 10, true),
		op(2, "a", history.OpCAS, "x", "2"+casInputSep+"3", "false", 11, 20, true), // wrong expected
		op(3, "a", history.OpCAS, "x", "1"+casInputSep+"3", "true", 21, 30, true),  // right expected
		op(4, "a", history.OpGet, "x", "", "3", 31, 40, true),
	}
	if _, err := Check(ops); err != nil {
		t.Fatalf("CAS outcomes must match the model: %v", err)
	}
}

func TestCASExpectAbsent(t *testing.T) {
	ops := []history.Op{
		op(1, "a", history.OpCAS, "x", Absent+casInputSep+"1", "true", 0, 10, true),
		op(2, "a", history.OpGet, "x", "", "1", 11, 20, true),
	}
	if _, err := Check(ops); err != nil {
		t.Fatalf("CAS expect-absent on a missing key must apply: %v", err)
	}

	// Same call on a present key must fail.
	ops = []history.Op{
		op(1, "a", history.OpSet, "x", "9", "", 0, 10, true),
		op(2, "a", history.OpCAS, "x", Absent+casInputSep+"1", "true", 11, 20, true),
	}
	if _, err := Check(ops); err == nil {
		t.Fatal("CAS expect-absent on a present key must not apply")
	}
}

func TestRealTimeOrderEnforced(t *testing.T) {
	// A finishes before B starts; B's output depends on A NOT having
	// happened — no valid linearization.
	ops := []history.Op{
		op(1, "a", history.OpSet, "x", "1", "", 0, 10, true),
		op(2, "a", history.OpGet, "x", "", Absent, 11, 20, true), // after the set: must see it
	}
	if _, err := Check(ops); err == nil {
		t.Fatal("a read after a write must observe it")
	}
}

func TestLinearizationReturned(t *testing.T) {
	ops := []history.Op{
		op(1, "a", history.OpSet, "x", "1", "", 0, 10, true),
		op(2, "b", history.OpSet, "y", "2", "", 0, 10, true), // concurrent
		op(3, "a", history.OpGet, "x", "", "1", 11, 20, true),
	}
	lin, err := Check(ops)
	if err != nil {
		t.Fatalf("must be linearizable: %v", err)
	}
	// The read (3) must come after the Set of x (1); the Set of y (2) is
	// concurrent and may sit anywhere.
	if len(lin) != 3 {
		t.Fatalf("linearization has %d ops, want 3", len(lin))
	}
	pos := map[int64]int{}
	for i, id := range lin {
		pos[id] = i
	}
	if pos[3] < pos[1] {
		t.Fatalf("linearization %v places the read before the write it observed", lin)
	}
}
