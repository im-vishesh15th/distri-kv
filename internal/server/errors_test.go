package server

// The retry-safety contract (Phase 8) in one table: which replication
// failures become the redirectable class (codes.Aborted — provably rejected
// before append) and which stay ambiguous (codes.Unavailable — the entry may
// still commit, so clients must not auto-retry mutations until Phase 9).

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"distrikv/internal/kv"
	"distrikv/internal/raft"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMapRaftErrorSafetyClasses(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"not leader is redirectable", raft.ErrNotLeader, codes.Aborted},
		{"not leader wrapped", fmt.Errorf("propose: %w", raft.ErrNotLeader), codes.Aborted},
		{"leadership lost is ambiguous", raft.ErrLeadershipLost, codes.Unavailable},
		{"stopped is ambiguous", raft.ErrStopped, codes.Unavailable},
		{"not integer passes through", kv.ErrNotInteger, codes.FailedPrecondition},
		{"overflow passes through", kv.ErrOverflow, codes.OutOfRange},
		{"key not found passes through", kv.ErrKeyNotFound, codes.NotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mapRaftError(tc.err)
			if code := status.Code(got); code != tc.want {
				t.Fatalf("mapRaftError(%v) code = %v, want %v", tc.err, code, tc.want)
			}
		})
	}

	// Context errors pass through untouched (cancellation semantics).
	if got := mapRaftError(context.Canceled); !errors.Is(got, context.Canceled) {
		t.Fatalf("context.Canceled = %v, want passthrough", got)
	}
	if got := mapRaftError(nil); got != nil {
		t.Fatalf("nil = %v, want nil", got)
	}
}
