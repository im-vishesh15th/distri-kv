package server

import (
	"context"
	"errors"

	"distrikv/internal/kv"

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
