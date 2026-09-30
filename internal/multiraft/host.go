// Package multiraft hosts multiple independent Raft groups on one node
// (Phase 17, spec §"Multi-Raft"). A Host owns one raft.Group per group
// ID; the transport is shared across all groups on the node (one
// connection per peer pair), and each inbound RPC is demultiplexed to
// the right group by its group_id field.
//
// Per-group isolation: every group has its own raft.Group, raftlog.Log,
// kv.SM, hard state, and snapshots, all under data/g<id>/. The only
// component shared between groups is the transport. Each group runs its
// own event loop (the single-goroutine actor model), and each group's
// election RNG is seeded from (node ID, group ID) so groups on the same
// node do not campaign in lockstep.
package multiraft

import (
	"context"
	"fmt"

	raftpb "distrikv/gen/raft/v1"
	"distrikv/internal/raft"
	"distrikv/internal/transport"
)

// Host implements transport.Handler by demultiplexing on group_id.
var _ transport.Handler = (*Host)(nil)

// Host is one node's set of Raft groups. It implements
// transport.Handler by demuxing each inbound RPC to the raft.Group
// named by the request's group_id. A message for a group the node does
// not host returns an error — it never panics and never touches any other
// group's state.
type Host struct {
	id     transport.NodeID
	groups map[raft.GroupID]*raft.Group
}

// NewHost returns a host for one node with no groups yet. Register
// groups with AddGroup before the host serves traffic.
func NewHost(id transport.NodeID) *Host {
	return &Host{id: id, groups: make(map[raft.GroupID]*raft.Group)}
}

// ID is this host's node identity.
func (h *Host) ID() transport.NodeID { return h.id }

// AddGroup registers one Raft group served by this host.
func (h *Host) AddGroup(g *raft.Group) {
	h.groups[g.GroupID()] = g
}

// GroupIDs lists the groups this host serves.
func (h *Host) GroupIDs() []raft.GroupID {
	ids := make([]raft.GroupID, 0, len(h.groups))
	for id := range h.groups {
		ids = append(ids, id)
	}
	return ids
}

// group returns the group for id, or nil if this host does not host it.
func (h *Host) group(id raft.GroupID) *raft.Group {
	return h.groups[id]
}

// Propose routes a write to the named group and blocks until it is
// applied (or the group rejects/fails it). Returns the entry's log
// index and the state machine's result.
func (h *Host) Propose(ctx context.Context, gid raft.GroupID, payload []byte) (uint64, any, error) {
	g := h.group(gid)
	if g == nil {
		return 0, nil, fmt.Errorf("multiraft: node %s does not host group %d", h.id, gid)
	}
	return g.Propose(ctx, payload)
}

// Status returns the named group's status (any goroutine).
func (h *Host) Status(gid raft.GroupID) (raft.Status, error) {
	g := h.group(gid)
	if g == nil {
		return raft.Status{}, fmt.Errorf("multiraft: node %s does not host group %d", h.id, gid)
	}
	return g.Status(), nil
}

// HandleRequestVote implements transport.Handler — demuxed by group_id.
func (h *Host) HandleRequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	g := h.group(raft.GroupID(req.GroupId))
	if g == nil {
		return nil, fmt.Errorf("multiraft: node %s does not host group %d", h.id, req.GroupId)
	}
	return g.HandleRequestVote(ctx, req)
}

// HandleAppendEntries implements transport.Handler — demuxed by group_id.
func (h *Host) HandleAppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	g := h.group(raft.GroupID(req.GroupId))
	if g == nil {
		return nil, fmt.Errorf("multiraft: node %s does not host group %d", h.id, req.GroupId)
	}
	return g.HandleAppendEntries(ctx, req)
}

// HandleInstallSnapshot implements transport.Handler — demuxed by group_id.
func (h *Host) HandleInstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	g := h.group(raft.GroupID(req.GroupId))
	if g == nil {
		return nil, fmt.Errorf("multiraft: node %s does not host group %d", h.id, req.GroupId)
	}
	return g.HandleInstallSnapshot(ctx, req)
}

// ReadIndex runs the linearizable-read barrier on the named group.
func (h *Host) ReadIndex(ctx context.Context, gid raft.GroupID) (uint64, error) {
	g := h.group(gid)
	if g == nil {
		return 0, fmt.Errorf("multiraft: node %s does not host group %d", h.id, gid)
	}
	return g.ReadIndex(ctx)
}

// Group returns the group for id, or nil if this host does not host it.
// Exported so cmd/server can start each group's run loop/ticker.
func (h *Host) Group(gid raft.GroupID) *raft.Group {
	return h.group(gid)
}
