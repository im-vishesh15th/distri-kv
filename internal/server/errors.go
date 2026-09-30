package server

import (
	"context"
	"errors"

	"distrikv/internal/kv"
	"distrikv/internal/raft"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mapError converts a domain error from the engine into a gRPC status.
// Context errors pass through so cancellation/deadline semantics are preserved.
//
// The status code IS the contract — the client (pkg/client) maps codes back
// to sentinels without parsing messages:
//
//	kv.ErrKeyNotFound -> codes.NotFound
//	kv.ErrNotInteger  -> codes.FailedPrecondition
//	kv.ErrOverflow    -> codes.OutOfRange
//	anything else     -> codes.Internal
func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}

	switch {
	case errors.Is(err, kv.ErrKeyNotFound):
		return status.Error(codes.NotFound, kv.ErrKeyNotFound.Error())
	case errors.Is(err, kv.ErrNotInteger):
		return status.Error(codes.FailedPrecondition, kv.ErrNotInteger.Error())
	case errors.Is(err, kv.ErrOverflow):
		return status.Error(codes.OutOfRange, kv.ErrOverflow.Error())
	default:
		// ErrUnknownOp or unexpected engine failure: internal, never routine.
		return status.Error(codes.Internal, err.Error())
	}
}

func statusInvalidKey() error {
	return status.Error(codes.InvalidArgument, "key must not be empty")
}

func statusErrorInternal(err error) error {
	return status.Error(codes.Internal, err.Error())
}

// mapRaftError maps replication outcomes (the Propose seam) before falling
// through to the kv domain mapping. The split is load-bearing for Phase 8:
//
//	ErrNotLeader -> codes.Aborted: the proposal was rejected BEFORE append,
//	        so nothing happened — the SDK may redirect to the discovered
//	        leader and retry the same write safely.
//	ErrLeadershipLost / ErrStopped -> codes.Unavailable: ambiguous — the
//	        entry may still commit under the new leader. Whether retrying is
//	        SAFE is Phase 9's question: until dedup exists, a lost-response
//	        retry can duplicate a committed write, so the SDK never
//	        auto-retries mutations in this class.
func mapRaftError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, raft.ErrLeadershipLost),
		errors.Is(err, raft.ErrStopped):
		return status.Error(codes.Unavailable, err.Error())
	default:
		// kv domain outcomes from apply (ErrNotInteger &co) and context.
		return mapError(err)
	}
}
