// Package grpctransport implements transport.Transport over gRPC — the
// production ("real") transport.
//
// One node runs ONE gRPC server (shared with the client-facing KVService);
// this package registers the RaftService onto it and maintains one lazy
// client connection per peer.
//
// Connection behavior: grpc.NewClient never fails synchronously for an
// unreachable peer — it reconnects in the background with exponential
// backoff. So a peer that is down makes RPCs fail fast with Unavailable, and
// the connection recovers automatically when the peer returns; no manual
// retry loop is needed (and none is written, by design).
package grpctransport

import (
	"context"
	"fmt"
	"sync"

	raftpb "distrikv/gen/raft/v1"
	"distrikv/internal/transport"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// RealTransport is the gRPC-backed Transport (outbound side).
//
// The inbound side (raftpb.RaftServiceServer) is served by a private adapter
// registered via RegisterRaftService — the outbound methods and the gRPC
// service methods share names (Ping, RequestVote, AppendEntries) and cannot
// live on one Go type.
//
// Construction must happen before the shared gRPC server starts serving.
type RealTransport struct {
	localID transport.NodeID
	handler transport.Handler // nil until the Raft core is attached

	mu     sync.Mutex
	conns  map[transport.NodeID]*grpc.ClientConn
	peers  map[transport.NodeID]string
	closed bool
}

// New builds the transport for localID with the given static peers.
//
// Requirements: if peers is non-empty it must contain localID (a node that
// cannot find itself in the membership list is a configuration error).
// handler may be nil (Raft RPCs then return Unimplemented).
func New(localID transport.NodeID, peers []transport.Peer, handler transport.Handler) (*RealTransport, error) {
	if localID == "" {
		return nil, fmt.Errorf("grpctransport: empty node ID")
	}
	peerMap := transport.PeerMap(peers)
	if len(peers) > 0 {
		if _, ok := peerMap[localID]; !ok {
			return nil, fmt.Errorf("grpctransport: local ID %q not in peer list %v", localID, peers)
		}
	}

	t := &RealTransport{
		localID: localID,
		handler: handler,
		peers:   peerMap,
		conns:   make(map[transport.NodeID]*grpc.ClientConn),
	}

	// Lazily-established client connections to every OTHER peer.
	for _, p := range peers {
		if p.ID == localID {
			continue
		}
		conn, err := grpc.NewClient(p.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Close()
			return nil, fmt.Errorf("grpctransport: client for %s (%s): %w", p.ID, p.Addr, err)
		}
		t.conns[p.ID] = conn
	}
	return t, nil
}

// SetHandler attaches the Raft core (Phase 5). Call before serving.
func (t *RealTransport) SetHandler(h transport.Handler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handler = h
}

// LocalID implements transport.Transport.
func (t *RealTransport) LocalID() transport.NodeID { return t.localID }

// Close closes all peer connections (the shared gRPC server is owned by the
// caller and left running).
func (t *RealTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true
	var firstErr error
	for id, conn := range t.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("grpctransport: close conn to %s: %w", id, err)
		}
	}
	return firstErr
}

// Ping implements transport.Transport.
func (t *RealTransport) Ping(ctx context.Context, to transport.NodeID) error {
	conn, err := t.conn(to)
	if err != nil {
		return err
	}
	resp, err := raftpb.NewRaftServiceClient(conn).Ping(ctx, &raftpb.PingRequest{
		FromNodeId: string(t.localID),
	})
	if err != nil {
		return fmt.Errorf("grpctransport: ping %s: %w", to, err)
	}
	// Identity check: the responder must be who we meant to reach.
	if resp.NodeId != string(to) {
		return fmt.Errorf("%w: reached %q when addressing %q", transport.ErrPeerMismatch, resp.NodeId, to)
	}
	return nil
}

// RequestVote implements transport.Transport.
func (t *RealTransport) RequestVote(ctx context.Context, to transport.NodeID, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	conn, err := t.conn(to)
	if err != nil {
		return nil, err
	}
	resp, err := raftpb.NewRaftServiceClient(conn).RequestVote(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("grpctransport: request vote from %s: %w", to, err)
	}
	return resp, nil
}

// PreVote implements transport.Transport.
func (t *RealTransport) PreVote(ctx context.Context, to transport.NodeID, req *raftpb.PreVoteRequest) (*raftpb.PreVoteResponse, error) {
	conn, err := t.conn(to)
	if err != nil {
		return nil, err
	}
	resp, err := raftpb.NewRaftServiceClient(conn).PreVote(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("grpctransport: pre-vote from %s: %w", to, err)
	}
	return resp, nil
}

// AppendEntries implements transport.Transport.
func (t *RealTransport) AppendEntries(ctx context.Context, to transport.NodeID, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	conn, err := t.conn(to)
	if err != nil {
		return nil, err
	}
	resp, err := raftpb.NewRaftServiceClient(conn).AppendEntries(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("grpctransport: append entries to %s: %w", to, err)
	}
	return resp, nil
}

// InstallSnapshot implements transport.Transport (Phase 13).
func (t *RealTransport) InstallSnapshot(ctx context.Context, to transport.NodeID, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	conn, err := t.conn(to)
	if err != nil {
		return nil, err
	}
	resp, err := raftpb.NewRaftServiceClient(conn).InstallSnapshot(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("grpctransport: install snapshot to %s: %w", to, err)
	}
	return resp, nil
}

// conn returns the peer's client connection.
func (t *RealTransport) conn(to transport.NodeID) (*grpc.ClientConn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, transport.ErrClosed
	}
	conn, ok := t.conns[to]
	if !ok {
		if _, peerKnown := t.peers[to]; !peerKnown {
			return nil, fmt.Errorf("%w: %s", transport.ErrUnknownPeer, to)
		}
		// Peer known but not in conns: it is the local node. Dialing
		// ourselves is a configuration error too.
		return nil, fmt.Errorf("%w: %s (peers must not target the local node through its own transport)", transport.ErrUnknownPeer, to)
	}
	return conn, nil
}

// --- inbound (raftpb.RaftServiceServer) ---

// RegisterRaftService registers the inbound Raft service for t on the shared
// gRPC server. Ping is served by the transport itself; RequestVote and
// AppendEntries forward to t's Handler (the Raft core), or report
// Unimplemented before one is attached.
func RegisterRaftService(s *grpc.Server, t *RealTransport) {
	raftpb.RegisterRaftServiceServer(s, &server{t: t})
}

// server adapts the inbound gRPC service to RealTransport's handler.
// It exists because the outbound Transport methods and the gRPC service
// methods would otherwise collide by name on a single type.
type server struct {
	raftpb.UnimplementedRaftServiceServer
	t *RealTransport
}

// Ping serves inbound pings with this node's identity.
func (s *server) Ping(ctx context.Context, req *raftpb.PingRequest) (*raftpb.PingResponse, error) {
	return &raftpb.PingResponse{NodeId: string(s.t.localID)}, nil
}

// RequestVote forwards to the Raft core.
func (s *server) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	s.t.mu.Lock()
	h := s.t.handler
	s.t.mu.Unlock()
	if h == nil {
		return nil, status.Error(codes.Unimplemented, "raft core not attached (Phase 5)")
	}
	return h.HandleRequestVote(ctx, req)
}

// PreVote forwards to the Raft core.
func (s *server) PreVote(ctx context.Context, req *raftpb.PreVoteRequest) (*raftpb.PreVoteResponse, error) {
	s.t.mu.Lock()
	h := s.t.handler
	s.t.mu.Unlock()
	if h == nil {
		return nil, status.Error(codes.Unimplemented, "raft core not attached")
	}
	return h.HandlePreVote(ctx, req)
}

// AppendEntries forwards to the Raft core.
func (s *server) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	s.t.mu.Lock()
	h := s.t.handler
	s.t.mu.Unlock()
	if h == nil {
		return nil, status.Error(codes.Unimplemented, "raft core not attached (Phase 5)")
	}
	return h.HandleAppendEntries(ctx, req)
}

// InstallSnapshot forwards to the Raft core (Phase 13).
func (s *server) InstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	s.t.mu.Lock()
	h := s.t.handler
	s.t.mu.Unlock()
	if h == nil {
		return nil, status.Error(codes.Unimplemented, "raft core not attached")
	}
	return h.HandleInstallSnapshot(ctx, req)
}
