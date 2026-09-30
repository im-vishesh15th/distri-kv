package grpctransport_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	raftpb "distrikv/gen/raft/v1"
	"distrikv/internal/transport"
	grpctransport "distrikv/internal/transport/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// testHandler is a stand-in for the future Raft core: it records what it
// received and returns scripted responses, proving inbound routing works
// before any Raft logic exists.
type testHandler struct {
	mu       sync.Mutex
	lastVote *raftpb.RequestVoteRequest
	lastApp  *raftpb.AppendEntriesRequest
	lastSnap *raftpb.InstallSnapshotRequest

	voteResp *raftpb.RequestVoteResponse
	appResp  *raftpb.AppendEntriesResponse
}

func (h *testHandler) HandleRequestVote(_ context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastVote = req
	if h.voteResp != nil {
		return h.voteResp, nil
	}
	return &raftpb.RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
}

func (h *testHandler) HandleAppendEntries(_ context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastApp = req
	if h.appResp != nil {
		return h.appResp, nil
	}
	return &raftpb.AppendEntriesResponse{Term: req.Term, Success: true}, nil
}

func (h *testHandler) HandleInstallSnapshot(_ context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastSnap = req
	return &raftpb.InstallSnapshotResponse{Term: req.Term, Success: true}, nil
}

// testNode is one in-process node: a real TCP listener, a real gRPC server
// hosting KV-less Raft service, and a RealTransport.
type testNode struct {
	id      transport.NodeID
	addr    string
	lis     net.Listener
	server  *grpc.Server
	tpt     *grpctransport.RealTransport
	handler *testHandler
}

// startCluster brings up n nodes on real localhost TCP ports.
//
// Ordering matters and mirrors production: listen first (to learn the
// ephemeral ports), then build membership, then create transports, then
// serve. handlerFor may return nil to run a node without a Raft handler.
func startCluster(t *testing.T, n int, handlerFor func(id transport.NodeID) transport.Handler) []*testNode {
	t.Helper()

	ids := make([]transport.NodeID, n)
	lis := make([]net.Listener, n)
	for i := 0; i < n; i++ {
		ids[i] = transport.NodeID(fmt.Sprintf("node%d", i+1))
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		lis[i] = l
	}

	peers := make([]transport.Peer, n)
	for i := range peers {
		peers[i] = transport.Peer{ID: ids[i], Addr: lis[i].Addr().String()}
	}

	nodes := make([]*testNode, n)
	for i := 0; i < n; i++ {
		var h transport.Handler
		if handlerFor != nil {
			h = handlerFor(ids[i])
		}
		tpt, err := grpctransport.New(ids[i], peers, h)
		if err != nil {
			t.Fatalf("transport %s: %v", ids[i], err)
		}

		srv := grpc.NewServer()
		grpctransport.RegisterRaftService(srv, tpt)

		node := &testNode{
			id:     ids[i],
			addr:   lis[i].Addr().String(),
			lis:    lis[i],
			server: srv,
			tpt:    tpt,
		}
		if th, ok := h.(*testHandler); ok {
			node.handler = th
		}
		nodes[i] = node

		go func(s *grpc.Server, l net.Listener) {
			_ = s.Serve(l)
		}(srv, lis[i])

		t.Cleanup(func() {
			srv.Stop()
			_ = tpt.Close()
		})
	}
	return nodes
}

func TestPingMesh(t *testing.T) {
	nodes := startCluster(t, 3, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, from := range nodes {
		for _, to := range nodes {
			if from.id == to.id {
				continue
			}
			if err := from.tpt.Ping(ctx, to.id); err != nil {
				t.Fatalf("%s -> %s ping: %v", from.id, to.id, err)
			}
		}
	}
}

func TestPingUnknownPeer(t *testing.T) {
	nodes := startCluster(t, 3, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := nodes[0].tpt.Ping(ctx, "ghost"); !errors.Is(err, transport.ErrUnknownPeer) {
		t.Fatalf("Ping(ghost) = %v, want ErrUnknownPeer", err)
	}
	// Dialing yourself through your own transport is also refused.
	if err := nodes[0].tpt.Ping(ctx, nodes[0].id); !errors.Is(err, transport.ErrUnknownPeer) {
		t.Fatalf("Ping(self) = %v, want ErrUnknownPeer", err)
	}
}

// TestPeerIdentityMismatch: the responder reports who it actually is. If the
// peer table lies about which node lives at an address, Ping fails loudly
// instead of letting two nodes disagree about the cluster.
func TestPeerIdentityMismatch(t *testing.T) {
	nodes := startCluster(t, 3, nil)

	// Build a transport for node1 whose peer "imposter" points at node2's
	// real address (node1 itself is included, as required).
	tpt, err := grpctransport.New("node1", []transport.Peer{
		{ID: "node1", Addr: nodes[0].addr},
		{ID: "imposter", Addr: nodes[1].addr},
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tpt.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = tpt.Ping(ctx, "imposter")
	if !errors.Is(err, transport.ErrPeerMismatch) {
		t.Fatalf("Ping(imposter) = %v, want ErrPeerMismatch", err)
	}
}

func TestNewRequiresSelfInPeers(t *testing.T) {
	_, err := grpctransport.New("node1", []transport.Peer{
		{ID: "node2", Addr: "127.0.0.1:1"},
		{ID: "node3", Addr: "127.0.0.1:2"},
	}, nil)
	if err == nil {
		t.Fatal("New without local ID in peers = nil error, want failure")
	}
	if _, err := grpctransport.New("", nil, nil); err == nil {
		t.Fatal("New with empty ID = nil error, want failure")
	}
}

// TestRequestVoteRoundTrip proves end-to-end typed RPC plumbing: outbound
// call → gRPC → inbound adapter → handler → response back. (The Raft logic
// inside the handler arrives in Phase 5.)
func TestRequestVoteRoundTrip(t *testing.T) {
	voteHandler := &testHandler{voteResp: &raftpb.RequestVoteResponse{Term: 42, VoteGranted: true}}
	nodes := startCluster(t, 3, func(id transport.NodeID) transport.Handler {
		if id == "node2" {
			return voteHandler
		}
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := nodes[0].tpt.RequestVote(ctx, "node2", &raftpb.RequestVoteRequest{
		Term:         7,
		CandidateId:  "node1",
		LastLogIndex: 5,
		LastLogTerm:  3,
	})
	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if resp.Term != 42 || !resp.VoteGranted {
		t.Fatalf("response = %+v, want {42 true}", resp)
	}

	voteHandler.mu.Lock()
	got := voteHandler.lastVote
	voteHandler.mu.Unlock()
	if got == nil || got.CandidateId != "node1" || got.Term != 7 || got.LastLogIndex != 5 || got.LastLogTerm != 3 {
		t.Fatalf("handler received %+v, want full request forwarded", got)
	}

	// A node without a handler reports Unimplemented — until Phase 5.
	_, err = nodes[0].tpt.RequestVote(ctx, "node3", &raftpb.RequestVoteRequest{Term: 1})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("RequestVote to handler-less node = %v (code %v), want Unimplemented", err, status.Code(err))
	}
}

// TestAppendEntriesRoundTrip verifies entry payloads survive serialization —
// the payload bytes Phase 6 will replicate.
func TestAppendEntriesRoundTrip(t *testing.T) {
	appHandler := &testHandler{}
	nodes := startCluster(t, 3, func(id transport.NodeID) transport.Handler {
		if id == "node3" {
			return appHandler
		}
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x42}
	resp, err := nodes[0].tpt.AppendEntries(ctx, "node3", &raftpb.AppendEntriesRequest{
		Term:         9,
		LeaderId:     "node1",
		PrevLogIndex: 4,
		PrevLogTerm:  2,
		Entries: []*raftpb.LogEntry{
			{Index: 5, Term: 3, Payload: payload},
			{Index: 6, Term: 3, Payload: []byte("second")},
		},
		LeaderCommit: 4,
	})
	if err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !resp.Success || resp.Term != 9 {
		t.Fatalf("response = %+v, want {term 9 success}", resp)
	}

	appHandler.mu.Lock()
	got := appHandler.lastApp
	appHandler.mu.Unlock()
	if got == nil || len(got.Entries) != 2 {
		t.Fatalf("handler received %+v, want 2 entries", got)
	}
	if got.Entries[0].Index != 5 || got.Entries[0].Term != 3 || string(got.Entries[0].Payload) != string(payload) {
		t.Fatalf("entry[0] = %+v", got.Entries[0])
	}
	if got.PrevLogIndex != 4 || got.PrevLogTerm != 2 || got.LeaderCommit != 4 || got.LeaderId != "node1" {
		t.Fatalf("request fields not forwarded: %+v", got)
	}
}

// TestInstallSnapshotRoundTrip proves the Phase 13 RPC's client and server
// plumbing: the transport's InstallSnapshot reaches the handler with every
// field (including the opaque snapshot payload) intact.
func TestInstallSnapshotRoundTrip(t *testing.T) {
	snapHandler := &testHandler{}
	nodes := startCluster(t, 3, func(id transport.NodeID) transport.Handler {
		if id == "node3" {
			return snapHandler
		}
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	payload := []byte{0xFE, 0xED, 0xFA, 0xCE, 0x00, 0x99}
	resp, err := nodes[0].tpt.InstallSnapshot(ctx, "node3", &raftpb.InstallSnapshotRequest{
		Term:              7,
		LeaderId:          "node1",
		LastIncludedIndex: 9000,
		LastIncludedTerm:  5,
		Data:              payload,
	})
	if err != nil {
		t.Fatalf("InstallSnapshot: %v", err)
	}
	if !resp.Success || resp.Term != 7 {
		t.Fatalf("response = %+v, want {term 7 success}", resp)
	}

	snapHandler.mu.Lock()
	got := snapHandler.lastSnap
	snapHandler.mu.Unlock()
	if got == nil {
		t.Fatal("handler received nothing")
	}
	if got.LastIncludedIndex != 9000 || got.LastIncludedTerm != 5 || got.Term != 7 || got.LeaderId != "node1" {
		t.Fatalf("request fields not forwarded: %+v", got)
	}
	if string(got.Data) != string(payload) {
		t.Fatalf("payload = %x, want %x", got.Data, payload)
	}
}

// TestPingRecoversAfterPeerRestart is the connection-loss test: killing a
// node's server makes pings fail; bringing it back on the same address makes
// them succeed again, because grpc-go reconnects with backoff on its own.
func TestPingRecoversAfterPeerRestart(t *testing.T) {
	nodes := startCluster(t, 3, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	victim := nodes[1]
	addr := victim.addr

	// Baseline.
	if err := nodes[0].tpt.Ping(ctx, victim.id); err != nil {
		t.Fatalf("baseline ping: %v", err)
	}

	// Kill node2 (Stop closes its listener).
	victim.server.Stop()
	if err := nodes[0].tpt.Ping(ctx, victim.id); err == nil {
		t.Fatal("ping to killed node succeeded, want error")
	}

	// Restart node2 on the SAME address: new listener, new transport
	// (standing in for process restart), same membership.
	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("re-listen %s: %v", addr, err)
	}
	peers := []transport.Peer{
		{ID: nodes[0].id, Addr: nodes[0].addr},
		{ID: nodes[1].id, Addr: addr},
		{ID: nodes[2].id, Addr: nodes[2].addr},
	}
	tpt, err := grpctransport.New(victim.id, peers, nil)
	if err != nil {
		t.Fatalf("replacement transport: %v", err)
	}
	srv := grpc.NewServer()
	grpctransport.RegisterRaftService(srv, tpt)
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = tpt.Close()
	})

	// The original client connection must reconnect on its own (grpc-go
	// backoff) without any code on our side.
	deadline := time.Now().Add(15 * time.Second)
	for {
		err = nodes[0].tpt.Ping(ctx, victim.id)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ping never recovered after restart: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
