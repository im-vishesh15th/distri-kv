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
//	kv.ErrKeyNotFound    -> codes.NotFound
//	kv.ErrNotInteger     -> codes.FailedPrecondition
//	kv.ErrOverflow       -> codes.OutOfRange
//	kv.ErrStaleSequence  -> codes.InvalidArgument (raw message: no sentinel
//	                         collides with this code in the SDK table)
//	anything else        -> codes.Internal
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
	case errors.Is(err, kv.ErrStaleSequence):
		// Superseded session request (Phase 9): never applied, and only a
		// misbehaving client can provoke it — pass the full message so the
		// caller sees which sequences are involved.
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		// ErrUnknownOp or unexpected engine failure: internal, never routine.
		return status.Error(codes.Internal, err.Error())
	}
}

func statusInvalidKey() error {
	return status.Error(codes.InvalidArgument, "key must not be empty")
}

func statusInvalidSession() error {
	return status.Error(codes.InvalidArgument,
		"mutation requires client_id and sequence_number >= 1 (client sessions)")
}

func statusErrorInternal(err error) error {
	return status.Error(codes.Internal, err.Error())
}

// mapRaftError maps replication outcomes (the Propose seam) before falling
// through to the kv domain mapping. The split is load-bearing for client
// routing (Phase 8) and retry safety (Phase 9):
//
//	ErrNotLeader -> codes.Aborted: the proposal was rejected BEFORE append,
//	        so nothing happened — the SDK redirects to the discovered
//	        leader and retries the same write (same session sequence)
//	        without consulting the session table at all.
//	ErrLeadershipLost / ErrStopped -> codes.Unavailable: ambiguous — the
//	        entry may still commit under the new leader. The SDK retries
//	        this class (Phase 9) with the SAME (client_id, sequence_number):
//	        whether or not the original committed, the session table lets
//	        it apply at most once (S1).
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
