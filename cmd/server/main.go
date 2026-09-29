// Command server runs a DistriKV node.
//
// Phase 4: single node still (no Raft core yet), in-memory engine, gRPC API,
// persistent Raft log at -data-dir, static cluster membership via -peers, and
// the gRPC transport serving Ping/RequestVote/AppendEntries (inbound Raft RPCs
// report Unimplemented until the Raft core attaches in Phase 5).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	kv1 "distrikv/gen/kv/v1"
	"distrikv/internal/kv"
	"distrikv/internal/raftlog"
	"distrikv/internal/server"
	"distrikv/internal/transport"
	grpctransport "distrikv/internal/transport/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	var (
		id        = flag.String("id", "node1", "node identity")
		addr      = flag.String("addr", ":8080", "gRPC listen address")
		dataDir   = flag.String("data-dir", "data", "directory for persistent state (persistent Raft log)")
		peersSpec = flag.String("peers", "", "static cluster membership: id@host:port,id@host:port,... (must include this node; empty = standalone)")
		debug     = flag.Bool("debug", false, "enable debug-level structured logging")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	if err := run(*id, *addr, *dataDir, *peersSpec, log); err != nil {
		log.Error("server_exit", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(id, addr, dataDir, peersSpec string, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	engine := kv.NewMemEngine()

	// Open the persistent Raft log (Phase 3). Recovery is reported, never
	// silent: a torn tail is discarded loudly, mid-file corruption refuses
	// to start the node.
	rlog, rec, err := raftlog.Open(filepath.Join(dataDir, "raft"))
	if err != nil {
		return fmt.Errorf("open raft log: %w", err)
	}
	defer func() {
		if cerr := rlog.Close(); cerr != nil {
			log.Error("raft_log_close", slog.String("error", cerr.Error()))
		}
	}()
	log.Info("raft_log_opened",
		slog.String("node_id", id),
		slog.Int64("records", rec.Records),
		slog.Bool("torn_tail_discarded", rec.TornTail),
		slog.Int64("truncated_bytes", rec.TruncatedBytes),
	)

	// Static cluster membership + transport (Phase 4). The Raft core is not
	// attached yet (Phase 5): outbound Raft RPCs work, inbound ones report
	// Unimplemented until SetHandler is called.
	peers, err := transport.ParsePeers(peersSpec)
	if err != nil {
		return fmt.Errorf("parse peers: %w", err)
	}
	rt, err := grpctransport.New(transport.NodeID(id), peers, nil)
	if err != nil {
		return fmt.Errorf("transport: %w", err)
	}
	defer func() {
		if cerr := rt.Close(); cerr != nil {
			log.Error("transport_close", slog.String("error", cerr.Error()))
		}
	}()
	peerIDs := make([]string, 0, len(peers))
	for _, p := range peers {
		peerIDs = append(peerIDs, string(p.ID))
	}
	log.Info("cluster_configured",
		slog.String("node_id", id),
		slog.String("addr", addr),
		slog.Any("members", peerIDs),
	)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	grpcServer := grpc.NewServer()
	kv1.RegisterKVServiceServer(grpcServer, server.NewService(engine, log))
	grpctransport.RegisterRaftService(grpcServer, rt)
	// Reflection lets grpcurl/gRPC tooling discover the API without stubs.
	reflection.Register(grpcServer)

	errCh := make(chan error, 1)
	go func() {
		log.Info("server_started",
			slog.String("node_id", id),
			slog.String("addr", lis.Addr().String()),
		)
		if serveErr := grpcServer.Serve(lis); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			errCh <- serveErr
		}
	}()

	select {
	case serveErr := <-errCh:
		return fmt.Errorf("serve: %w", serveErr)
	case <-ctx.Done():
		log.Info("server_shutting_down", slog.String("node_id", id))
	}

	// Graceful stop with a hard deadline: in-flight RPCs get a chance to
	// finish, but shutdown can never hang.
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		grpcServer.Stop()
	}
	log.Info("server_stopped", slog.String("node_id", id))
	return nil
}
