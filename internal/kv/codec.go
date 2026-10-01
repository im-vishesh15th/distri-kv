package kv

// Command wire encoding (Phase 7, session fields since Phase 9): the bytes
// that ride in Raft log entries.
//
// One Command per entry, protobuf-encoded (proto/kv.proto). Determinism is
// the contract: every replica decodes identical bytes and applies them in
// identical order, so Encode/Decode must be lossless for every legal
// Command — the CAS presence flag included (proto3 bytes cannot tell nil
// from empty, so the encoding carries the distinction explicitly) and the
// client session fields included (deduplication reads them from the logged
// command itself).

import (
	"fmt"

	kv1 "distrikv/gen/kv/v1"
	"google.golang.org/protobuf/proto"
)

// EncodeCommand serializes cmd for a Raft log entry.
//
// Errors indicate a programming mistake (unrecognized op), never a routine
// outcome: commands are built via the constructors above.
func EncodeCommand(cmd Command) ([]byte, error) {
	m := &kv1.Command{
		Op:             kv1.CommandOp(cmd.Op),
		Key:            cmd.Key,
		Value:          cmd.Value,
		Delta:          cmd.Delta,
		ClientId:       cmd.ClientID,
		SequenceNumber: cmd.Sequence,
	}
	// Expected nil = "key must be absent"; non-nil (even empty) = "must be
	// present and byte-equal". Presence is explicit on the wire.
	if cmd.Expected != nil {
		m.ExpectedExists = true
		m.ExpectedValue = cmd.Expected
	}
	if m.Op == kv1.CommandOp_COMMAND_OP_UNSPECIFIED {
		return nil, fmt.Errorf("kv: encode command: op 0 is not a command")
	}
	if cmd.Key == "" {
		return nil, fmt.Errorf("kv: encode command: empty key")
	}
	b, err := proto.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("kv: encode command: %w", err)
	}
	return b, nil
}

// DecodeCommand parses a log entry payload back into a Command. It is the
// mirror of EncodeCommand and the gate in front of Engine.Apply: anything
// that does not round-trip (unknown/missing op, empty key, malformed bytes)
// is refused here, so replicas agree on what is and isn't a command.
//
// Callers distinguish outcomes with errors.Is: malformed bytes wrap a plain
// error; an unrecognized op reports ErrUnknownOp (log corruption or version
// skew — loud, never routine).
func DecodeCommand(payload []byte) (Command, error) {
	var m kv1.Command
	if err := proto.Unmarshal(payload, &m); err != nil {
		return Command{}, fmt.Errorf("kv: decode command: %w", err)
	}
	op := Op(m.Op)
	switch op {
	case OpSet, OpDelete, OpCAS, OpIncr, OpMoveSlots:
	default:
		return Command{}, fmt.Errorf("kv: decode command: op %d: %w", m.Op, ErrUnknownOp)
	}
	if m.Key == "" {
		return Command{}, fmt.Errorf("kv: decode command: empty key")
	}
	cmd := Command{
		Op:       op,
		Key:      m.Key,
		Value:    m.Value,
		Delta:    m.Delta,
		ClientID: m.ClientId,
		Sequence: m.SequenceNumber,
	}
	if m.ExpectedExists {
		// Empty-but-present must stay non-nil (see Command.Expected).
		cmd.Expected = m.ExpectedValue
		if cmd.Expected == nil {
			cmd.Expected = []byte{}
		}
	}
	return cmd, nil
}
