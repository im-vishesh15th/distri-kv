// Command server runs a DistriKV node.
//
// Phase 5: single node plus the Raft core — leader election over the gRPC
// transport with persistent term/vote in the Raft log, in-memory engine, gRPC
// API, static cluster membership via -peers. Log replication arrives in
// Phase 6.
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
	"distrikv/internal/raft"
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

	// Static cluster membership + transport (Phase 4).
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

	// Raft core (Phase 5): election over the transport above, hard state
	// (term/vote) in the persistent log. Empty peers → standalone cluster
	// of one (the node elects itself).
	raftPeers := make([]transport.NodeID, len(peers))
	for i, p := range peers {
		raftPeers[i] = p.ID
	}
	rn, err := raft.New(raft.Config{
		ID:             transport.NodeID(id),
		Peers:          raftPeers,
		Transport:      rt,
		Log:            rlog,
		ElectionTicks:  10, // randomized timeout: 100–190 ms at 10 ms/tick
		HeartbeatTicks: 3,  // 30 ms heartbeats
		Logger:         log,
	})
	if err != nil {
		return fmt.Errorf("raft core: %w", err)
	}
	// Inbound Raft RPCs now reach the core instead of returning
	// Unimplemented. Must happen before the server starts serving.
	rt.SetHandler(rn)

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

	// Start the Raft event loop and its time source. A halted loop (disk
	// failure persisting term/vote) takes the node down: without durable
	// hard state it cannot uphold the one-vote-per-term guarantee.
	go func() {
		if raftErr := rn.Run(ctx); raftErr != nil {
			log.Error("raft_halted",
				slog.String("node_id", id),
				slog.String("error", raftErr.Error()),
			)
			select {
			case errCh <- raftErr:
			default:
			}
		}
	}()
	raft.StartTicker(ctx, rn, 10*time.Millisecond)
	if s := rn.Status(); s.Role != "" {
		log.Info("raft_started",
			slog.String("node_id", id),
			slog.String("role", string(s.Role)),
			slog.Uint64("term", s.Term),
		)
	}

	select {
	case serveErr := <-errCh:
		return fmt.Errorf("server component stopped: %w", serveErr)
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
