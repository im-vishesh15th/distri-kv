// Command server runs a DistriKV node.
//
// Phase 8: the Raft core replicates commands, the committed log feeds a
// KV state machine (kv.NewSM), mutations proposed by the gRPC service are
// applied in log order on every replica, and GetStatus serves leader
// routing (this node's status + the -peers addresses as the hint map).
// Phase 9: mutations require a client session (client_id +
// sequence_number), replicated inside the command so the SM's session
// table deduplicates retries. Phase 11: reads pass Node.ReadIndex —
// leader-only and linearizable; followers answer codes.Aborted so
// clients redirect. Leader election over the gRPC transport,
// persistent term/vote in the Raft log, in-memory engine, static cluster
// membership via -peers.
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

// snapshotEveryDefault is the -snapshot-every default: how many applied
// entries trigger a state-machine snapshot and log-prefix compaction
// (Phase 12). It bounds log growth in production: the log retains only the
// entries after the newest snapshot plus what has applied since. 0 disables
// compaction (legal for tests, never what a server wants).
const snapshotEveryDefault = 1024

func main() {
	var (
		id        = flag.String("id", "node1", "node identity")
		addr      = flag.String("addr", ":8080", "gRPC listen address")
		dataDir   = flag.String("data-dir", "data", "directory for persistent state (persistent Raft log)")
		peersSpec = flag.String("peers", "", "static cluster membership: id@host:port,id@host:port,... (must include this node; empty = standalone)")
		debug     = flag.Bool("debug", false, "enable debug-level structured logging")
		snapEvery = flag.Uint64("snapshot-every", snapshotEveryDefault, "applied entries per state-machine snapshot + log compaction (0 disables; the log then grows without bound)")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	if err := run(*id, *addr, *dataDir, *peersSpec, *snapEvery, log); err != nil {
		log.Error("server_exit", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(id, addr, dataDir, peersSpec string, snapEvery uint64, log *slog.Logger) error {
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
	// Leader-routing addresses for GetStatus (Phase 8): ID -> dialable
	// addr as given by -peers, which must be client-reachable for hints
	// to work. Empty for standalone (leader_id == node_id routes instead).
	leaderAddrs := make(map[transport.NodeID]string, len(peers))
	for _, p := range peers {
		peerIDs = append(peerIDs, string(p.ID))
		leaderAddrs[p.ID] = p.Addr
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
		// The KV engine IS the state machine: committed entries decode to
		// Commands and apply in log order (Phase 7). kv.SM also snapshots
		// (Phase 12): every -snapshot-every applied entries the node
		// captures engine + session table and compacts the log prefix they
		// cover, so the log cannot grow forever in production.
		StateMachine:  kv.NewSM(engine),
		SnapshotEvery: snapEvery,
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
	// The service mutates through Raft (rn.Propose) and reads through
	// rn.ReadIndex (Phase 11: linearizable; followers refuse with
	// codes.Aborted and clients redirect). rn.Status + the -peers
	// addresses power GetStatus leader routing.
	kv1.RegisterKVServiceServer(grpcServer, server.NewService(engine, rn, rn.Status, leaderAddrs, log))
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
