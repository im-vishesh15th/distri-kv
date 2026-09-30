// Package transport defines how DistriKV nodes talk to each other.
//
// The Transport interface is the architectural seam of the whole system:
//
//   - Production uses RealTransport (internal/transport/grpc): Raft RPCs over
//     gRPC.
//   - Testing uses SimulatedTransport (Phase 14): a deterministic in-process
//     network with seeded delay/drop/reorder/duplicate/partition faults and a
//     virtual clock.
//
// The Raft core (Phase 5+) depends ONLY on this interface — never on gRPC,
// sockets, or real time — so the same Raft code runs against both. That is
// what makes reproducible fault testing possible.
//
// Design note: the interface is synchronous RPC-style (call → response or
// error). Real networks' reordering emerges naturally from concurrent
// callers with variable latencies; the simulated transport (Phase 14)
// implements delay/drop/partition/slow-node as error and latency behavior,
// and exercises duplicate *delivery* internally at its dispatch layer.
package transport

import (
	"context"
	"errors"
	"fmt"
	"strings"

	raftpb "distrikv/gen/raft/v1"
)

// NodeID identifies a node in the cluster (e.g. "node1").
type NodeID string

// Peer is one statically-configured cluster member.
type Peer struct {
	ID   NodeID
	Addr string
}

// Transport is the outbound side of node-to-node communication, as used by
// the Raft core.
type Transport interface {
	// Ping verifies connectivity to `to`. Returns nil only if the responder
	// is the node we meant to reach (ID mismatches are errors).
	Ping(ctx context.Context, to NodeID) error

	// RequestVote sends a RequestVote RPC to `to`.
	RequestVote(ctx context.Context, to NodeID, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error)

	// AppendEntries sends an AppendEntries RPC (or heartbeat when entries
	// is empty) to `to`.
	AppendEntries(ctx context.Context, to NodeID, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error)

	// InstallSnapshot sends an InstallSnapshot RPC to `to` (Phase 13):
	// used when the peer is behind the sender's compaction point and
	// entries alone can no longer catch it up.
	InstallSnapshot(ctx context.Context, to NodeID, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error)

	// LocalID is this transport's node identity.
	LocalID() NodeID

	// Close releases outbound resources. It does not stop a shared server.
	Close() error
}

// Handler is the inbound side, implemented by the Raft core (from Phase 5)
// and by test doubles before then. The transport routes inbound RPCs here.
//
// Handlers receive the caller's context (gRPC deadlines propagate) and must
// be safe for concurrent use — one call per inbound RPC.
type Handler interface {
	HandleRequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error)
	HandleAppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error)
	HandleInstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error)
}

// Transport-level errors.
var (
	// ErrUnknownPeer: sending to a node ID that is not in the peer
	// configuration — a config error, not a transient network failure.
	ErrUnknownPeer = errors.New("transport: unknown peer")

	// ErrPeerMismatch: the responder's self-reported node ID differs from
	// the peer we intended to reach — two nodes disagree about cluster
	// membership/identity. Loud by design: proceeding on a misconfigured
	// cluster would corrupt logs in ways that look like consensus bugs.
	ErrPeerMismatch = errors.New("transport: peer identity mismatch")

	// ErrClosed: use after Close.
	ErrClosed = errors.New("transport: closed")
)

// ParsePeers parses a static peer list of the form:
//
//	"node1@127.0.0.1:8080,node2@127.0.0.1:8081,node3@127.0.0.1:8082"
//
// The local node must list itself (catches "forgot your own ID" typos).
// An empty string yields an empty list (standalone development node).
func ParsePeers(spec string) ([]Peer, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, nil
	}

	seen := make(map[NodeID]bool)
	var peers []Peer
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, errors.New("transport: empty entry in peer list")
		}
		id, addr, ok := strings.Cut(part, "@")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("transport: bad peer %q (want id@host:port)", part)
		}
		if seen[NodeID(id)] {
			return nil, fmt.Errorf("transport: duplicate peer ID %q", id)
		}
		seen[NodeID(id)] = true
		peers = append(peers, Peer{ID: NodeID(id), Addr: addr})
	}
	return peers, nil
}

// PeerMap converts a peer slice to the map form transports use.
func PeerMap(peers []Peer) map[NodeID]string {
	m := make(map[NodeID]string, len(peers))
	for _, p := range peers {
		m[p.ID] = p.Addr
	}
	return m
}
