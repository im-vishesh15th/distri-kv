package kv

// Phase 7 wire encoding tests: every legal Command must round-trip through
// the log-entry bytes losslessly — the CAS presence flag in particular, since
// proto3 bytes cannot tell nil from empty and replicas must not disagree
// about a precondition.

import (
	"bytes"
	"errors"
	"testing"

	kv1 "distrikv/gen/kv/v1"
	"google.golang.org/protobuf/proto"
)

func TestCommandCodecRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		cmd  Command
	}{
		{"set", Set("k", []byte("v"))},
		{"set empty value", Set("k", []byte{})},
		{"set nil value", Set("k", nil)},
		{"delete", Delete("k")},
		{"cas expected absent", CAS("k", nil, []byte("new"))},
		{"cas expected empty-present", CAS("k", []byte{}, []byte("new"))},
		{"cas expected value", CAS("k", []byte("old"), []byte("new"))},
		{"incr positive", IncrBy("n", 7)},
		{"incr negative", IncrBy("n", -7)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := EncodeCommand(tc.cmd)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			got, err := DecodeCommand(b)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got.Op != tc.cmd.Op || got.Key != tc.cmd.Key || got.Delta != tc.cmd.Delta {
				t.Fatalf("decoded %+v, want %+v", got, tc.cmd)
			}
			if !bytes.Equal(got.Value, tc.cmd.Value) && !(len(got.Value) == 0 && len(tc.cmd.Value) == 0) {
				t.Fatalf("value = %q, want %q", got.Value, tc.cmd.Value)
			}
			// Expected: presence AND emptiness are both semantic.
			if (got.Expected == nil) != (tc.cmd.Expected == nil) {
				t.Fatalf("expected presence: got nil=%v, want nil=%v",
					got.Expected == nil, tc.cmd.Expected == nil)
			}
			if !bytes.Equal(got.Expected, tc.cmd.Expected) {
				t.Fatalf("expected = %q, want %q", got.Expected, tc.cmd.Expected)
			}
		})
	}
}

func TestDecodeRejectsMalformed(t *testing.T) {
	if _, err := DecodeCommand([]byte("not a protobuf command")); err == nil {
		t.Fatal("garbage payload decoded successfully")
	}

	// Op zero: a message that is valid protobuf but not a command.
	zero, err := proto.Marshal(&kv1.Command{Key: "k"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := DecodeCommand(zero); !errors.Is(err, ErrUnknownOp) {
		t.Fatalf("op 0: err = %v, want ErrUnknownOp", err)
	}

	// Unknown future op: same class — version skew must be loud.
	future, err := proto.Marshal(&kv1.Command{
		Op:  kv1.CommandOp(99),
		Key: "k",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := DecodeCommand(future); !errors.Is(err, ErrUnknownOp) {
		t.Fatalf("op 99: err = %v, want ErrUnknownOp", err)
	}

	// Empty key: refused (callers validate before proposing; a logged
	// empty key can only be corruption or a caller bug).
	noKey, err := proto.Marshal(&kv1.Command{Op: kv1.CommandOp_COMMAND_OP_SET})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := DecodeCommand(noKey); err == nil {
		t.Fatal("empty key decoded successfully")
	}
}

func TestEncodeRejectsInvalid(t *testing.T) {
	if _, err := EncodeCommand(Command{Op: 0, Key: "k"}); err == nil {
		t.Fatal("encoded op 0")
	}
	if _, err := EncodeCommand(Command{Op: OpSet, Key: ""}); err == nil {
		t.Fatal("encoded empty key")
	}
}
