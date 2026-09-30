package sim

import (
	"context"

	raftpb "distrikv/gen/raft/v1"
	"distrikv/internal/transport"
)

// Transport is SimulatedTransport: a transport.Transport that routes RPCs
// through a shared Network instead of sockets. Each node gets its own
// Transport (its identity); the Network is shared across the whole test
// cluster.
//
// It implements the exact interface the Raft core depends on, so a full
// cluster runs against it unchanged — same events, same handlers, same
// state machine — with no gRPC, no ports, and no kernel in the loop.
type Transport struct {
	net *Network
	id  transport.NodeID
}

var _ transport.Transport = (*Transport)(nil)

// NewTransport binds a transport identity to the shared network.
func NewTransport(n *Network, id transport.NodeID) *Transport {
	return &Transport{net: n, id: id}
}

// LocalID is this endpoint's node ID.
func (t *Transport) LocalID() transport.NodeID { return t.id }

// Close releases this endpoint. The shared Network is owned by the test
// (closed separately); per-endpoint close is a no-op.
func (t *Transport) Close() error { return nil }

// Ping is connectivity: it succeeds only if the target has a registered,
// live handler and no partition blocks the path.
func (t *Transport) Ping(ctx context.Context, to transport.NodeID) error {
	_, err := t.net.Call(ctx, t.id, to, func(h transport.Handler) (any, error) {
		return nil, nil // handler present == reachable; identity is structural
	})
	return err
}

func (t *Transport) RequestVote(ctx context.Context, to transport.NodeID, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	v, err := t.net.Call(ctx, t.id, to, func(h transport.Handler) (any, error) {
		return h.HandleRequestVote(ctx, req)
	})
	if err != nil {
		return nil, err
	}
	return v.(*raftpb.RequestVoteResponse), nil
}

func (t *Transport) AppendEntries(ctx context.Context, to transport.NodeID, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	v, err := t.net.Call(ctx, t.id, to, func(h transport.Handler) (any, error) {
		return h.HandleAppendEntries(ctx, req)
	})
	if err != nil {
		return nil, err
	}
	return v.(*raftpb.AppendEntriesResponse), nil
}

func (t *Transport) InstallSnapshot(ctx context.Context, to transport.NodeID, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	v, err := t.net.Call(ctx, t.id, to, func(h transport.Handler) (any, error) {
		return h.HandleInstallSnapshot(ctx, req)
	})
	if err != nil {
		return nil, err
	}
	return v.(*raftpb.InstallSnapshotResponse), nil
}
