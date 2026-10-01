package kv

import (
	"encoding/json"
	"errors"
	"strconv"
)

// Op identifies a state-machine command.
//
// Commands are the unit of replication: they are serialized into persistent
// Raft log entries (since Phase 7) and applied by every replica via
// Engine.Apply. The op set is deliberately small — GET and EXISTS are reads
// and never enter the log; writes and atomic mutations do.
type Op uint8

const (
	// OpSet overwrites cmd.Value at cmd.Key.
	OpSet Op = iota + 1
	// OpDelete removes cmd.Key (idempotent).
	OpDelete
	// OpCAS conditionally swaps: see Command.Expected.
	OpCAS
	// OpIncr adds cmd.Delta (signed; negative = decrement) to an int64 value.
	OpIncr
	// OpMoveSlots moves a slot range between groups (metadata command).
	OpMoveSlots
)

// Command is a deterministic mutation to be applied to the KV state machine.
//
// Wire encoding of Command into Raft log entries is Protocol Buffers, fixed
// in Phase 7 and implemented in codec.go (EncodeCommand/DecodeCommand); the
// struct below is the in-process form.
type Command struct {
	Op    Op
	Key   string
	Value []byte // OpSet: new value. OpCAS: new value to store on success.

	// Expected is the OpCAS precondition:
	//   nil  => the key must be absent;
	//   non-nil => the key must be present and byte-equal to Expected.
	// (A CAS against an existing empty value must therefore send a non-nil
	// empty []byte — the gRPC layer carries an explicit presence flag in
	// Phase 2 to keep this distinction intact across the wire.)
	Expected []byte

	// Delta is the OpIncr amount (signed; negative = decrement).
	Delta int64

	// ClientID + Sequence identify the logical client request for retry
	// deduplication (Phase 9). The SM consults them on every apply:
	// Sequence > last (or first request) applies, == replays the cached
	// result, < is rejected as superseded (see ErrStaleSequence).
	//
	// ClientID == "" opts out (internal/test commands: always applied).
	// Set them via the constructors' results — the service assigns one
	// sequence per logical operation, reused across retries.
	ClientID string
	Sequence uint64
}

// Result is the outcome of applying a Command. It is what gets recorded for
// client responses and cached in the session table for retry deduplication
// (Phase 9): a retry with the same (client_id, sequence_number) replays this
// Result instead of re-applying. Identical commands must yield identical
// Results on all replicas.
type Result struct {
	// Applied reports whether the mutation took effect.
	// OpCAS returning Applied=false with error==nil is a normal
	// precondition failure, not an error.
	Applied bool

	// Value carries op-specific output. Currently: the new value after OpIncr.
	Value []byte
}

// Command constructors. These are the only supported ways to build Commands
// so invalid combinations cannot be constructed accidentally.

// Set returns a command that stores value at key.
func Set(key string, value []byte) Command {
	return Command{Op: OpSet, Key: key, Value: value}
}

// Delete returns a command that removes key.
func Delete(key string) Command {
	return Command{Op: OpDelete, Key: key}
}

// CAS returns a command that stores newValue at key only if the current state
// matches expected (nil expected = key must be absent).
func CAS(key string, expected, newValue []byte) Command {
	return Command{Op: OpCAS, Key: key, Expected: expected, Value: newValue}
}

// IncrBy returns a command that adds delta to the int64 stored at key.
// Delta may be negative.
func IncrBy(key string, delta int64) Command {
	return Command{Op: OpIncr, Key: key, Delta: delta}
}

// Incr returns a command that increments the int64 stored at key by 1.
func Incr(key string) Command { return IncrBy(key, 1) }

// Decr returns a command that decrements the int64 stored at key by 1.
func Decr(key string) Command { return IncrBy(key, -1) }

// Command-layer errors. Like ErrKeyNotFound, these are sentinels so callers
// can match them with errors.Is.
var (
	// ErrNotInteger is returned by OpIncr when the stored value is not an
	// int64 in base-10 form.
	ErrNotInteger = errors.New("kv: value is not an integer")

	// ErrOverflow is returned by OpIncr when the result would exceed int64.
	ErrOverflow = errors.New("kv: integer overflow")

	// ErrUnknownOp is returned by Apply for an unrecognized Op — a signal of
	// log corruption or version skew, never a routine outcome.
	ErrUnknownOp = errors.New("kv: unknown command op")

	// ErrStaleSequence is returned when a session request arrives with a
	// sequence_number older than the last one already applied for that
	// client_id (Phase 9). The request is NOT applied: its result is no
	// longer retained, and re-applying it would clobber state produced by
	// newer requests. Only misbehaving or long-abandoned clients can see
	// it — an SDK's in-flight retries always carry the newest sequence.
	ErrStaleSequence = errors.New("kv: sequence_number superseded by a newer request from this session")
)

// MoveSlots returns a command that proposes a slot range move between groups.
// This is a metadata command: the Value field carries a JSON-encoded
// MoveSlotsRequest, and ClientID is empty (bypasses dedup).
func MoveSlots(req MoveSlotsRequest) Command {
	b, _ := json.Marshal(req)
	return Command{Op: OpMoveSlots, Key: "_meta/move-slots", Value: b, ClientID: ""}
}

// parseInt64 parses a canonical base-10 int64 (the form formatInt64 writes).
func parseInt64(b []byte) (int64, error) {
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, ErrNotInteger
	}
	return n, nil
}

// formatInt64 renders n in the canonical stored form.
func formatInt64(n int64) []byte {
	return []byte(strconv.FormatInt(n, 10))
}
