// Command server runs a DistriKV node.
//
// Phase 3: single node, in-memory engine, gRPC API, persistent Raft log
// opened at -data-dir (recovery reported at startup). Phase 4 adds peers
// (cluster membership); Phase 5+ puts the Raft core on top of the log.
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

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	var (
		id      = flag.String("id", "node1", "node identity (used from Phase 4)")
		addr    = flag.String("addr", ":8080", "gRPC listen address")
		dataDir = flag.String("data-dir", "data", "directory for persistent state (persistent Raft log)")
		debug   = flag.Bool("debug", false, "enable debug-level structured logging")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	if err := run(*id, *addr, *dataDir, log); err != nil {
		log.Error("server_exit", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(id, addr, dataDir string, log *slog.Logger) error {
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

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	grpcServer := grpc.NewServer()
	kv1.RegisterKVServiceServer(grpcServer, server.NewService(engine, log))
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
