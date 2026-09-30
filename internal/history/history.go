// Package history records operation histories for consistency analysis
// (spec §20): every client operation with its id, client, key, type,
// input, output, start/finish times, and success flag. The analysis
// half — deciding whether a recorded history is linearizable — lives in
// internal/lincheck.
//
// The recorder is the single write path for history: clients (real SDKs
// or test drivers) record one Op per logical operation, in completion
// order. Times are wall-clock at the caller, which is what the
// real-time order in a linearizability check is built from.
package history

import (
	"sync"
	"time"
)

// OpType is the operation type recorded in a history.
type OpType string

const (
	OpSet    OpType = "set"
	OpGet    OpType = "get"
	OpIncrBy OpType = "incrby"
	OpCAS    OpType = "cas"
)

// Op is one recorded client operation (spec §20's fields, verbatim):
//
//	operation_id  ID        — unique within the history
//	client_id     ClientID  — who issued it
//	key           Key
//	operation type          — OpSet / OpGet / OpIncrBy / OpCAS
//	input         Input     — set: value; incrby: delta; cas: old + "\x00" + new
//	output        Output    — get: value or "<absent>"; incrby: new value;
//	                        cas: "true"/"false"; set: ""
//	start time    Start     — when the client issued it
//	finish time   Finish    — when the client got the final answer
//	success       Success   — false if the client never got a definitive
//	                        answer (timeout, unreachable, leadership lost)
//
// A failed op's Input still records what the client tried; its Output is
// "". How the checker treats failures (optional effect for writes, no
// constraint for reads) is lincheck's business.
type Op struct {
	ID       int64
	ClientID string
	Key      string
	Type     OpType
	Input    string
	Output   string
	Start    time.Time
	Finish   time.Time
	Success  bool
}

// Recorder is a thread-safe operation history. Clients record into it
// from their own goroutines; the analysis reads a snapshot.
type Recorder struct {
	mu     sync.Mutex
	ops    []Op
	nextID int64
}

// NewRecorder returns an empty history.
func NewRecorder() *Recorder { return &Recorder{} }

// Record appends one operation. IDs are assigned in record order.
func (r *Recorder) Record(clientID string, opType OpType, key, input, output string, start, finish time.Time, success bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	r.ops = append(r.ops, Op{
		ID:       r.nextID,
		ClientID: clientID,
		Key:      key,
		Type:     opType,
		Input:    input,
		Output:   output,
		Start:    start,
		Finish:   finish,
		Success:  success,
	})
}

// Ops returns a copy of the history in record order.
func (r *Recorder) Ops() []Op {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Op(nil), r.ops...)
}

// Len is the number of recorded operations.
func (r *Recorder) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ops)
}
