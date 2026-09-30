// Package server implements the client-facing gRPC KVService over a kv.Engine.
//
// Phase 7: every mutation is encoded as a kv.Command and replicated through
// Raft (the Proposer seam) — the engine is mutated only by the apply path,
// in log order, so all replicas see identical state. Reads (Get/Exists) are
// served straight from the local engine; making them linearizable is
// Phase 11's job.
//
// Error mapping (server -> wire -> SDK): the status code is the contract.
//
//	kv.ErrKeyNotFound          -> codes.NotFound         -> ErrKeyNotFound
//	kv.ErrNotInteger           -> codes.FailedPrecondition -> ErrNotInteger
//	kv.ErrOverflow             -> codes.OutOfRange       -> ErrOverflow
//	kv.ErrUnknownOp            -> codes.Internal         -> passes through
//	empty key                  -> codes.InvalidArgument
//	raft.ErrNotLeader          -> codes.Aborted          -> ErrNotLeader
//	  (provably rejected before append: safe for the SDK to redirect and retry)
//	raft.ErrLeadershipLost /
//	raft.ErrStopped            -> codes.Unavailable      -> ambiguous
//	  (entry may still commit elsewhere: the SDK must NOT auto-retry
//	   mutations until Phase 9's dedup makes retries safe)
//	context cancellation       -> passes through untouched
//
// CAS precondition failure is NOT an error: it returns applied=false.
package server

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	kv1 "distrikv/gen/kv/v1"
	"distrikv/internal/kv"
	"distrikv/internal/raft"
	"distrikv/internal/transport"
)

// StatusFunc reports this node's Raft status (identity, role, leader) for
// leader routing. *raft.Node.Status satisfies it.
type StatusFunc func() raft.Status

// Proposer replicates one encoded Command through Raft and waits for it to
// be applied, returning the state machine's result for that entry.
// *raft.Node implements it; the seam exists so the service can be tested
// against any implementation without importing raft's internals.
type Proposer interface {
	Propose(ctx context.Context, payload []byte) (index uint64, result any, err error)
}

// Service adapts a kv.Engine to the gRPC KVService.
type Service struct {
	kv1.UnimplementedKVServiceServer

	engine   kv.Engine // reads (Phase 11 revisits); apply-path writes
	proposer Proposer
	status   StatusFunc                  // routing: who am I / who leads
	addrs    map[transport.NodeID]string // routing: leader ID -> dialable addr (-peers)
	log      *slog.Logger
}

// NewService returns a KVService reading from engine and replicating
// mutations through proposer. status/addrs drive leader routing (Phase 8):
// status may be nil (GetStatus then reports Internal), addrs may be nil
// (leader_addr stays empty — standalone deployments route by leader_id ==
// node_id instead). log may be nil (logging then disabled).
func NewService(engine kv.Engine, proposer Proposer, status StatusFunc,
	addrs map[transport.NodeID]string, log *slog.Logger) *Service {
	return &Service{
		engine: engine, proposer: proposer, status: status, addrs: addrs, log: log,
	}
}

// GetStatus answers "who am I, who leads, where is the leader" for client
// routing (Phase 8). It never fails for role reasons: a follower, a
// candidate, and a leader all answer truthfully about their own view.
func (s *Service) GetStatus(ctx context.Context, _ *kv1.GetStatusRequest) (*kv1.GetStatusResponse, error) {
	if s.status == nil {
		return nil, statusErrorInternal(fmt.Errorf("service has no status source (wire a StatusFunc)"))
	}
	st := s.status()
	resp := &kv1.GetStatusResponse{
		NodeId:   string(st.ID),
		Role:     string(st.Role),
		LeaderId: string(st.LeaderID),
	}
	if st.LeaderID != "" {
		resp.LeaderAddr = s.addrs[st.LeaderID] // "" when unknown — clients compare leader_id instead
	}
	return resp, nil
}

// Get returns the value for key or codes.NotFound.
func (s *Service) Get(ctx context.Context, req *kv1.GetRequest) (*kv1.GetResponse, error) {
	if err := validateKey(req.Key); err != nil {
		return nil, err
	}
	v, gerr := s.engine.Get(req.Key)
	if gerr != nil {
		return nil, mapError(gerr)
	}
	return &kv1.GetResponse{Value: v}, nil
}

// Put stores value at key (upsert), replicated through Raft.
// client_id/sequence_number are accepted but ignored until Phase 9.
func (s *Service) Put(ctx context.Context, req *kv1.PutRequest) (*kv1.PutResponse, error) {
	if err := validateKey(req.Key); err != nil {
		return nil, err
	}
	if err := s.propose(ctx, kv.Set(req.Key, req.Value)); err != nil {
		return nil, err
	}
	s.logOp(ctx, "put", req.Key, req.ClientId, req.SequenceNumber)
	return &kv1.PutResponse{}, nil
}

// Delete removes key (idempotent), replicated through Raft.
func (s *Service) Delete(ctx context.Context, req *kv1.DeleteRequest) (*kv1.DeleteResponse, error) {
	if err := validateKey(req.Key); err != nil {
		return nil, err
	}
	if err := s.propose(ctx, kv.Delete(req.Key)); err != nil {
		return nil, err
	}
	s.logOp(ctx, "delete", req.Key, req.ClientId, req.SequenceNumber)
	return &kv1.DeleteResponse{}, nil
}

// Exists reports key presence.
func (s *Service) Exists(ctx context.Context, req *kv1.ExistsRequest) (*kv1.ExistsResponse, error) {
	if err := validateKey(req.Key); err != nil {
		return nil, err
	}
	return &kv1.ExistsResponse{Exists: s.engine.Exists(req.Key)}, nil
}

// CAS conditionally stores new_value. applied=false is a normal outcome,
// decided by the replicated apply — all replicas agree on it because they
// all evaluated the same precondition in the same log position.
func (s *Service) CAS(ctx context.Context, req *kv1.CASRequest) (*kv1.CASResponse, error) {
	if err := validateKey(req.Key); err != nil {
		return nil, err
	}
	// Reconstruct the kv.Command precondition: proto3 bytes cannot express
	// nil, so expected_exists carries presence and an empty-but-present
	// expected value becomes a non-nil empty slice (codec keeps it intact).
	var expected []byte
	if req.ExpectedExists {
		expected = req.ExpectedValue
		if expected == nil {
			expected = []byte{}
		}
	}
	res, rerr := s.proposeResult(ctx, kv.CAS(req.Key, expected, req.NewValue))
	if rerr != nil {
		return nil, rerr
	}
	return &kv1.CASResponse{Applied: res.Applied}, nil
}

// Incr atomically adds delta (negative = decrement) to the int64 at key.
func (s *Service) Incr(ctx context.Context, req *kv1.IncrRequest) (*kv1.IncrResponse, error) {
	if err := validateKey(req.Key); err != nil {
		return nil, err
	}
	res, rerr := s.proposeResult(ctx, kv.IncrBy(req.Key, req.Delta))
	if rerr != nil {
		return nil, rerr
	}
	n, perr := strconv.ParseInt(string(res.Value), 10, 64)
	if perr != nil {
		// Unreachable: Engine.Apply returns canonical ints.
		return nil, statusErrorInternal(kv.ErrUnknownOp)
	}
	return &kv1.IncrResponse{Value: n}, nil
}

// propose encodes cmd, replicates it, and maps any failure to a status.
// Used by mutations whose engine result the response ignores.
func (s *Service) propose(ctx context.Context, cmd kv.Command) error {
	_, rerr := s.proposeResult(ctx, cmd)
	return rerr
}

// proposeResult is the write path: encode -> Propose (replicate + apply)
// -> extract the kv.Result the state machine produced for this entry.
func (s *Service) proposeResult(ctx context.Context, cmd kv.Command) (kv.Result, error) {
	payload, err := kv.EncodeCommand(cmd)
	if err != nil {
		// Constructors + key validation make this unreachable; if it
		// happens it is a bug, not a client error.
		return kv.Result{}, statusErrorInternal(err)
	}
	_, result, perr := s.proposer.Propose(ctx, payload)
	if perr != nil {
		return kv.Result{}, mapRaftError(perr)
	}
	res, ok := result.(kv.Result)
	if !ok {
		// Propose succeeded but carried no kv.Result — only possible if
		// the node was wired without the KV state machine.
		return kv.Result{}, statusErrorInternal(
			fmt.Errorf("state machine returned %T, want kv.Result (node wired with kv.NewSM?)", result))
	}
	return res, nil
}

// validateKey rejects empty keys with codes.InvalidArgument.
// Key/value size limits belong to the product gateway (Phase P2), not the core.
func validateKey(key string) error {
	if key == "" {
		return statusInvalidKey()
	}
	return nil
}

// logOp emits one debug line per mutation. Debug level keeps normal operation
// quiet while staying available for distributed-failure investigation.
func (s *Service) logOp(ctx context.Context, op, key, clientID string, seq uint64) {
	if s.log == nil {
		return
	}
	s.log.DebugContext(ctx, "kv_op",
		slog.String("op", op),
		slog.String("key", key),
		slog.String("client_id", clientID),
		slog.Uint64("sequence_number", seq),
	)
}
