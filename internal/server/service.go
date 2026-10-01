// Package server implements the client-facing gRPC KVService over a kv.Engine.
//
// Phase 7: every mutation is encoded as a kv.Command and replicated through
// Raft (the Proposer seam) — the engine is mutated only by the apply path,
// in log order, so all replicas see identical state.
//
// Phase 11: reads (Get/Exists) pass through Node.ReadIndex before touching
// the engine — current-term leadership proven by a fresh heartbeat quorum,
// then wait for apply to reach the read point. Followers and candidates
// answer codes.Aborted (ErrNotLeader) instead of a possibly stale value;
// the SDK redirects to the leader. GetStatus stays deliberately
// non-linearizable: it is a routing hint, not data.
//
// Error mapping (server -> wire -> SDK): the status code is the contract.
//
//	kv.ErrKeyNotFound          -> codes.NotFound         -> ErrKeyNotFound
//	kv.ErrNotInteger           -> codes.FailedPrecondition -> ErrNotInteger
//	kv.ErrOverflow             -> codes.OutOfRange       -> ErrOverflow
//	kv.ErrUnknownOp            -> codes.Internal         -> passes through
//	kv.ErrStaleSequence        -> codes.InvalidArgument  -> raw message
//	  (superseded session request: never applied, no sentinel — only
//	   misbehaving or long-abandoned clients can see it)
//	empty key                  -> codes.InvalidArgument
//	missing session fields     -> codes.InvalidArgument
//	  (Phase 9: mutations require client_id + sequence_number >= 1)
//	raft.ErrNotLeader          -> codes.Aborted          -> ErrNotLeader
//	  (mutations: rejected before append; reads: not the leader — either
//	   way provably nothing happened: the SDK redirects and retries)
//	raft.ErrLeadershipLost /
//	raft.ErrStopped            -> codes.Unavailable      -> retried (Phase 9)
//	  (outcome ambiguous, but the retry reuses the same session sequence,
//	   so the session table guarantees at-most-once application)
//	context cancellation       -> passes through untouched
//
// CAS precondition failure is NOT an error: it returns applied=false.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strconv"

	kv1 "distrikv/gen/kv/v1"
	"distrikv/internal/kv"
	"distrikv/internal/raft"
	"distrikv/internal/shard"
	"distrikv/internal/transport"
)

// metadataGroupID is the reserved GroupID for the metadata Raft group.
const metadataGroupID = raft.GroupID(math.MaxUint64)

// StatusFunc reports this node's Raft status (identity, role, leader) for
// leader routing. *raft.Group.Status satisfies it.
type StatusFunc func() raft.Status

// Proposer replicates one encoded Command through Raft and waits for it to
// be applied, returning the state machine's result for that entry; it also
// exposes ReadIndex, the linearizable-read barrier (Phase 11). *raft.Group
// implements it; the seam exists so the service can be tested against any
// implementation without importing raft's internals.
type Proposer interface {
	Propose(ctx context.Context, payload []byte) (index uint64, result any, err error)

	// ReadIndex blocks until it is safe to read the local state machine:
	// current-term leadership proven by a fresh quorum acknowledgment, the
	// term's no-op committed (so commitIndex covers prior-term commits),
	// and lastApplied >= the returned index — i.e. a subsequent engine read
	// reflects every write acknowledged before this call began. Errors:
	// raft.ErrNotLeader (follower/candidate or leadership lost while
	// waiting), raft.ErrStopped, ctx.Err().
	ReadIndex(ctx context.Context) (uint64, error)
}

// GroupProposer is the multi-group analog of Proposer: every operation is
// routed to a specific Raft group by its GroupID. *multiraft.Host implements
// it.
type GroupProposer interface {
	Propose(ctx context.Context, gid raft.GroupID, payload []byte) (uint64, any, error)
	ReadIndex(ctx context.Context, gid raft.GroupID) (uint64, error)
}

// GroupStatusFunc reports the Raft status for a specific group.
type GroupStatusFunc func(gid raft.GroupID) (raft.Status, error)

// Service adapts a kv.Engine to the gRPC KVService.
type Service struct {
	kv1.UnimplementedKVServiceServer

	engine   kv.Engine // reads after the ReadIndex barrier; apply-path writes
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

// GroupedService adapts multiple KV engines (one per Raft group) to the
// gRPC KVService, using a shard map to route keys to groups.
// The shard map is provided by a MetadataSM, allowing dynamic config updates
// when the metadata Raft group commits a new version.
type GroupedService struct {
	kv1.UnimplementedKVServiceServer

	metadataSM *kv.MetadataSM // provides dynamic config via Config()
	proposer   GroupProposer
	status     GroupStatusFunc
	addrs      map[transport.NodeID]string
	log        *slog.Logger

	// engines maps groupID -> kv.Engine for reads. Writes go through
	// proposer which routes to the correct group's SM.
	engines map[raft.GroupID]kv.Engine
}

// NewGroupedService returns a KVService that routes every operation to the
// Raft group that owns its key (per the shard map from metadataSM).
// metadataSM provides the current config via Config(); if nil, all keys map to group 0.
// proposer/status must be able to serve all groups in the config; an unknown group returns codes.Internal.
// addrs maps node ID -> dialable address for leader hints. log may be nil.
// engines is the per-group engine map (required for reads).
func NewGroupedService(metadataSM *kv.MetadataSM, proposer GroupProposer, status GroupStatusFunc,
	addrs map[transport.NodeID]string, log *slog.Logger, engines map[raft.GroupID]kv.Engine) *GroupedService {
	return &GroupedService{
		metadataSM: metadataSM, proposer: proposer, status: status, addrs: addrs, log: log, engines: engines,
	}
}

// currentConfig returns the current shard config from metadataSM.
// Returns nil if metadataSM is nil (falls back to group 0).
func (s *GroupedService) currentConfig() *shard.Config {
	if s.metadataSM == nil {
		return nil
	}
	return s.metadataSM.Config()
}

// groupFor returns the group that owns key. Nil config -> group 0.
// Empty key maps to group 0 (backward compatibility for GetStatus with
// no key specified).
func (s *GroupedService) groupFor(key string) raft.GroupID {
	cfg := s.currentConfig()
	if cfg == nil || key == "" {
		return 0
	}
	return cfg.Group(key)
}

// propose routes a mutation to the group that owns the key.
func (s *GroupedService) propose(ctx context.Context, key string, cmd kv.Command, clientID string, seq uint64) error {
	_, rerr := s.proposeResult(ctx, key, cmd, clientID, seq)
	return rerr
}

// proposeResult is the write path: stamp session -> encode -> Propose (with gid)
// -> extract the kv.Result the state machine produced for this entry.
func (s *GroupedService) proposeResult(ctx context.Context, key string, cmd kv.Command, clientID string, seq uint64) (kv.Result, error) {
	gid := s.groupFor(key)
	cmd.ClientID, cmd.Sequence = clientID, seq
	payload, err := kv.EncodeCommand(cmd)
	if err != nil {
		return kv.Result{}, statusErrorInternal(err)
	}
	_, result, perr := s.proposer.Propose(ctx, gid, payload)
	if perr != nil {
		return kv.Result{}, mapRaftError(perr)
	}
	res, ok := result.(kv.Result)
	if !ok {
		return kv.Result{}, statusErrorInternal(
			fmt.Errorf("state machine returned %T, want kv.Result (node wired with kv.NewSM?)", result))
	}
	return res, nil
}

// linearizableRead runs the ReadIndex barrier on the group that owns the key.
func (s *GroupedService) linearizableRead(ctx context.Context, key string) error {
	gid := s.groupFor(key)
	if _, err := s.proposer.ReadIndex(ctx, gid); err != nil {
		return mapRaftError(err)
	}
	return nil
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

// Get returns the value for key or codes.NotFound. The read is
// linearizable (spec §14): the ReadIndex barrier gates it.
func (s *Service) Get(ctx context.Context, req *kv1.GetRequest) (*kv1.GetResponse, error) {
	if err := validateKey(req.Key); err != nil {
		return nil, err
	}
	if err := s.linearizableRead(ctx); err != nil {
		return nil, err
	}
	v, gerr := s.engine.Get(req.Key)
	if gerr != nil {
		return nil, mapError(gerr)
	}
	return &kv1.GetResponse{Value: v}, nil
}

// linearizableRead is the ReadIndex barrier every read passes through:
// confirm this node still leads the current term (fresh heartbeat
// quorum), wait until the state machine has applied everything committed
// when the read began, then let the caller read the engine. A follower
// or candidate returns ErrNotLeader -> codes.Aborted — the SDK redirects
// rather than risk a stale answer (a read has no side effects, so the
// redirect is trivially safe).
func (s *Service) linearizableRead(ctx context.Context) error {
	if _, err := s.proposer.ReadIndex(ctx); err != nil {
		return mapRaftError(err)
	}
	return nil
}

// Put stores value at key (upsert), replicated through Raft.
func (s *Service) Put(ctx context.Context, req *kv1.PutRequest) (*kv1.PutResponse, error) {
	if err := validateKey(req.Key); err != nil {
		return nil, err
	}
	if err := validateSession(req.ClientId, req.SequenceNumber); err != nil {
		return nil, err
	}
	if err := s.propose(ctx, kv.Set(req.Key, req.Value), req.ClientId, req.SequenceNumber); err != nil {
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
	if err := validateSession(req.ClientId, req.SequenceNumber); err != nil {
		return nil, err
	}
	if err := s.propose(ctx, kv.Delete(req.Key), req.ClientId, req.SequenceNumber); err != nil {
		return nil, err
	}
	s.logOp(ctx, "delete", req.Key, req.ClientId, req.SequenceNumber)
	return &kv1.DeleteResponse{}, nil
}

// Exists reports key presence (same ReadIndex barrier as Get).
func (s *Service) Exists(ctx context.Context, req *kv1.ExistsRequest) (*kv1.ExistsResponse, error) {
	if err := validateKey(req.Key); err != nil {
		return nil, err
	}
	if err := s.linearizableRead(ctx); err != nil {
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
	if err := validateSession(req.ClientId, req.SequenceNumber); err != nil {
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
	res, rerr := s.proposeResult(ctx, kv.CAS(req.Key, expected, req.NewValue),
		req.ClientId, req.SequenceNumber)
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
	if err := validateSession(req.ClientId, req.SequenceNumber); err != nil {
		return nil, err
	}
	res, rerr := s.proposeResult(ctx, kv.IncrBy(req.Key, req.Delta),
		req.ClientId, req.SequenceNumber)
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

// propose encodes cmd (stamped with the client session), replicates it,
// and maps any failure to a status. Used by mutations whose engine result
// the response ignores.
func (s *Service) propose(ctx context.Context, cmd kv.Command, clientID string, seq uint64) error {
	_, rerr := s.proposeResult(ctx, cmd, clientID, seq)
	return rerr
}

// proposeResult is the write path: stamp session -> encode -> Propose
// (replicate + apply) -> extract the kv.Result the state machine produced
// for this entry. The session fields ride the LOGGED command: dedup is
// decided at apply time by the replicated session table, identical on
// every replica.
func (s *Service) proposeResult(ctx context.Context, cmd kv.Command, clientID string, seq uint64) (kv.Result, error) {
	cmd.ClientID, cmd.Sequence = clientID, seq
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

// validateSession enforces the Phase-9 session contract at the edge: every
// mutation must carry a client_id and sequence_number >= 1, or retry
// deduplication has nothing to key on. The SDK always sends both; the
// check exists so raw stubs cannot silently bypass dedup.
func validateSession(clientID string, seq uint64) error {
	if clientID == "" || seq == 0 {
		return statusInvalidSession()
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

// --- GroupedService KVService methods ---

// GetStatus answers "who am I, who leads, where is the leader" for the
// Raft group that owns the requested key (Phase 20). Empty key -> group 0.
// Also returns the current shard config version (Phase 21).
func (s *GroupedService) GetStatus(ctx context.Context, req *kv1.GetStatusRequest) (*kv1.GetStatusResponse, error) {
	gid := s.groupFor(req.Key)
	if s.status == nil {
		return nil, statusErrorInternal(fmt.Errorf("service has no status source (wire a GroupStatusFunc)"))
	}
	st, err := s.status(gid)
	if err != nil {
		return nil, statusErrorInternal(fmt.Errorf("group %d status: %w", gid, err))
	}
	resp := &kv1.GetStatusResponse{
		NodeId:        string(st.ID),
		Role:          string(st.Role),
		LeaderId:      string(st.LeaderID),
		ConfigVersion: s.currentConfigVersion(),
	}
	if st.LeaderID != "" {
		resp.LeaderAddr = s.addrs[st.LeaderID]
	}
	return resp, nil
}

// currentConfigVersion returns the current shard config version (0 if none).
func (s *GroupedService) currentConfigVersion() uint64 {
	cfg := s.currentConfig()
	if cfg == nil {
		return 0
	}
	return cfg.Version
}

// GetShardConfig returns the current versioned shard configuration.
func (s *GroupedService) GetShardConfig(ctx context.Context, req *kv1.GetShardConfigRequest) (*kv1.GetShardConfigResponse, error) {
	cfg := s.currentConfig()
	if cfg == nil {
		return &kv1.GetShardConfigResponse{Version: 0, ConfigJson: ""}, nil
	}
	// Check if_version_gt to avoid sending unchanged config.
	if req.IfVersionGt > 0 && cfg.Version <= req.IfVersionGt {
		return &kv1.GetShardConfigResponse{Version: cfg.Version, ConfigJson: ""}, nil
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		return nil, statusErrorInternal(fmt.Errorf("marshal config: %w", err))
	}
	return &kv1.GetShardConfigResponse{Version: cfg.Version, ConfigJson: string(b)}, nil
}

// MoveSlots proposes a slot range move via the metadata Raft group.
// The request is encoded as a metadata command and proposed to the metadata group.
func (s *GroupedService) MoveSlots(ctx context.Context, req *kv1.MoveSlotsRequest) (*kv1.MoveSlotsResponse, error) {
	// Build the metadata command payload.
	moveReq := kv.MoveSlotsRequest{
		StartSlot:  req.StartSlot,
		EndSlot:    req.EndSlot,
		FromGroup:  req.FromGroup,
		ToGroup:    req.ToGroup,
		NewVersion: req.NewVersion,
	}
	cmd := kv.MoveSlots(moveReq)
	payload, err := kv.EncodeCommand(cmd)
	if err != nil {
		return nil, statusErrorInternal(fmt.Errorf("encode move-slots: %w", err))
	}
	// Propose to the metadata group.
	_, result, err := s.proposer.Propose(ctx, metadataGroupID, payload)
	if err != nil {
		return &kv1.MoveSlotsResponse{Error: err.Error()}, nil
	}
	// Result contains the new version.
	res, ok := result.(kv.Result)
	if !ok {
		return &kv1.MoveSlotsResponse{Error: "metadata group returned unexpected result"}, nil
	}
	versionStr := string(res.Value)
	var newVersion uint64
	fmt.Sscanf(versionStr, "%d", &newVersion)
	return &kv1.MoveSlotsResponse{NewVersion: newVersion}, nil
}

// Get returns the value for key or codes.NotFound. The read is
// linearizable: the ReadIndex barrier gates it on the group that owns key.
func (s *GroupedService) Get(ctx context.Context, req *kv1.GetRequest) (*kv1.GetResponse, error) {
	if err := validateKey(req.Key); err != nil {
		return nil, err
	}
	if err := s.linearizableRead(ctx, req.Key); err != nil {
		return nil, err
	}
	v, gerr := s.engineFor(req.Key).Get(req.Key)
	if gerr != nil {
		return nil, mapError(gerr)
	}
	return &kv1.GetResponse{Value: v}, nil
}

// Exists reports key presence (same ReadIndex barrier as Get).
func (s *GroupedService) Exists(ctx context.Context, req *kv1.ExistsRequest) (*kv1.ExistsResponse, error) {
	if err := validateKey(req.Key); err != nil {
		return nil, err
	}
	if err := s.linearizableRead(ctx, req.Key); err != nil {
		return nil, err
	}
	return &kv1.ExistsResponse{Exists: s.engineFor(req.Key).Exists(req.Key)}, nil
}

// Put stores value at key (upsert), replicated through Raft.
func (s *GroupedService) Put(ctx context.Context, req *kv1.PutRequest) (*kv1.PutResponse, error) {
	if err := validateKey(req.Key); err != nil {
		return nil, err
	}
	if err := validateSession(req.ClientId, req.SequenceNumber); err != nil {
		return nil, err
	}
	if err := s.propose(ctx, req.Key, kv.Set(req.Key, req.Value), req.ClientId, req.SequenceNumber); err != nil {
		return nil, err
	}
	s.logOp(ctx, "put", req.Key, req.ClientId, req.SequenceNumber)
	return &kv1.PutResponse{}, nil
}

// Delete removes key (idempotent), replicated through Raft.
func (s *GroupedService) Delete(ctx context.Context, req *kv1.DeleteRequest) (*kv1.DeleteResponse, error) {
	if err := validateKey(req.Key); err != nil {
		return nil, err
	}
	if err := validateSession(req.ClientId, req.SequenceNumber); err != nil {
		return nil, err
	}
	if err := s.propose(ctx, req.Key, kv.Delete(req.Key), req.ClientId, req.SequenceNumber); err != nil {
		return nil, err
	}
	s.logOp(ctx, "delete", req.Key, req.ClientId, req.SequenceNumber)
	return &kv1.DeleteResponse{}, nil
}

// CAS conditionally stores new_value. applied=false is a normal outcome,
// decided by the replicated apply — all replicas agree on it because they
// all evaluated the same precondition in the same log position.
func (s *GroupedService) CAS(ctx context.Context, req *kv1.CASRequest) (*kv1.CASResponse, error) {
	if err := validateKey(req.Key); err != nil {
		return nil, err
	}
	if err := validateSession(req.ClientId, req.SequenceNumber); err != nil {
		return nil, err
	}
	var expected []byte
	if req.ExpectedExists {
		expected = req.ExpectedValue
		if expected == nil {
			expected = []byte{}
		}
	}
	res, rerr := s.proposeResult(ctx, req.Key, kv.CAS(req.Key, expected, req.NewValue),
		req.ClientId, req.SequenceNumber)
	if rerr != nil {
		return nil, rerr
	}
	return &kv1.CASResponse{Applied: res.Applied}, nil
}

// Incr atomically adds delta (negative = decrement) to the int64 at key.
func (s *GroupedService) Incr(ctx context.Context, req *kv1.IncrRequest) (*kv1.IncrResponse, error) {
	if err := validateKey(req.Key); err != nil {
		return nil, err
	}
	if err := validateSession(req.ClientId, req.SequenceNumber); err != nil {
		return nil, err
	}
	res, rerr := s.proposeResult(ctx, req.Key, kv.IncrBy(req.Key, req.Delta),
		req.ClientId, req.SequenceNumber)
	if rerr != nil {
		return nil, rerr
	}
	n, perr := strconv.ParseInt(string(res.Value), 10, 64)
	if perr != nil {
		return nil, statusErrorInternal(kv.ErrUnknownOp)
	}
	return &kv1.IncrResponse{Value: n}, nil
}

// engineFor returns the engine for the group that owns key.
func (s *GroupedService) engineFor(key string) kv.Engine {
	gid := s.groupFor(key)
	eng, ok := s.engines[gid]
	if !ok {
		// This is a misconfiguration — the group should have been registered.
		// Return a nil engine to trigger a proper error.
		return nil
	}
	return eng
}

// logOp emits one debug line per mutation. Debug level keeps normal operation
// quiet while staying available for distributed-failure investigation.
func (s *GroupedService) logOp(ctx context.Context, op, key, clientID string, seq uint64) {
	if s.log == nil {
		return
	}
	s.log.DebugContext(ctx, "kv_op",
		slog.String("op", op),
		slog.String("key", key),
		slog.String("client_id", clientID),
		slog.Uint64("sequence_number", seq),
		slog.Uint64("group", uint64(s.groupFor(key))),
	)
}
