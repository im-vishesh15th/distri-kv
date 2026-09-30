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
	"distrikv/internal/multiraft"
	"distrikv/internal/raft"
	"distrikv/internal/raftlog"
	"distrikv/internal/server"
	"distrikv/internal/shard"
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
		id          = flag.String("id", "node1", "node identity")
		addr        = flag.String("addr", ":8080", "gRPC listen address")
		dataDir     = flag.String("data-dir", "data", "directory for persistent state (persistent Raft log)")
		peersSpec   = flag.String("peers", "", "static cluster membership: id@host:port,id@host:port,... (must include this node; empty = standalone)")
		shardConfig = flag.String("shard-config", "", "path to shard configuration JSON (optional; enables multi-group mode)")
		debug       = flag.Bool("debug", false, "enable debug-level structured logging")
		snapEvery   = flag.Uint64("snapshot-every", snapshotEveryDefault, "applied entries per state-machine snapshot + log compaction (0 disables; the log then grows without bound)")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	if err := run(*id, *addr, *dataDir, *peersSpec, *shardConfig, *snapEvery, log); err != nil {
		log.Error("server_exit", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(id, addr, dataDir, peersSpec, shardConfigPath string, snapEvery uint64, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Load shard config if provided.
	var shardCfg *shard.Config
	if shardConfigPath != "" {
		cfg, err := shard.LoadConfig(shardConfigPath)
		if err != nil {
			return fmt.Errorf("load shard config: %w", err)
		}
		shardCfg = cfg
		log.Info("shard_config_loaded",
			slog.String("path", shardConfigPath),
			slog.Uint64("version", cfg.Version),
			slog.Uint64("groups", cfg.Groups),
		)
	}

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

	raftPeers := make([]transport.NodeID, len(peers))
	for i, p := range peers {
		raftPeers[i] = p.ID
	}

	// Determine groups to host.
	var groups []raft.GroupID
	if shardCfg != nil {
		for g := uint64(0); g < shardCfg.Groups; g++ {
			groups = append(groups, raft.GroupID(g))
		}
	} else {
		groups = []raft.GroupID{0}
	}

	// Build per-group engines, logs, and raft groups.
	engines := make(map[raft.GroupID]kv.Engine)
	var host *multiraft.Host
	var singleRaft *raft.Group // for backward compat when no shard config

	if shardCfg != nil {
		// Multi-group mode: create one Host with all groups.
		host = multiraft.NewHost(transport.NodeID(id))
		for _, gid := range groups {
			// Per-group directory: data/g<g>/<node>/raft
			dir := filepath.Join(dataDir, fmt.Sprintf("g%d", gid), id, "raft")
			rlog, rec, err := raftlog.Open(dir)
			if err != nil {
				return fmt.Errorf("group %d: open raft log: %w", gid, err)
			}
			log.Info("raft_log_opened",
				slog.String("node_id", id),
				slog.Uint64("group", uint64(gid)),
				slog.Int64("records", rec.Records),
				slog.Bool("torn_tail_discarded", rec.TornTail),
				slog.Int64("truncated_bytes", rec.TruncatedBytes),
			)
			engine := kv.NewMemEngine()
			engines[gid] = engine
			group, err := raft.New(raft.Config{
				ID:             transport.NodeID(id),
				GroupID:        gid,
				Peers:          raftPeers,
				Transport:      rt,
				Log:            rlog,
				ElectionTicks:  10,
				HeartbeatTicks: 3,
				Logger:         log,
				StateMachine:   kv.NewSM(engine),
				SnapshotEvery:  snapEvery,
			})
			if err != nil {
				return fmt.Errorf("group %d: raft.New: %w", gid, err)
			}
			host.AddGroup(group)
		}
		rt.SetHandler(host)
	} else {
		// Single-group mode (backward compatible).
		engine := kv.NewMemEngine()
		engines[0] = engine
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
		rn, err := raft.New(raft.Config{
			ID:             transport.NodeID(id),
			GroupID:        0,
			Peers:          raftPeers,
			Transport:      rt,
			Log:            rlog,
			ElectionTicks:  10,
			HeartbeatTicks: 3,
			Logger:         log,
			StateMachine:   kv.NewSM(engine),
			SnapshotEvery:  snapEvery,
		})
		if err != nil {
			return fmt.Errorf("raft core: %w", err)
		}
		singleRaft = rn
		rt.SetHandler(rn)
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	grpcServer := grpc.NewServer()

	// Create the appropriate KV service.
	var kvService kv1.KVServiceServer
	if shardCfg != nil {
		// Multi-group: use GroupedService with per-group engines.
		kvService = server.NewGroupedService(
			shardCfg,
			host,        // implements GroupProposer
			host.Status, // implements GroupStatusFunc
			leaderAddrs,
			log,
			engines,
		)
	} else {
		// Single-group: use legacy Service.
		kvService = server.NewService(engines[0], singleRaft, singleRaft.Status, leaderAddrs, log)
	}

	kv1.RegisterKVServiceServer(grpcServer, kvService)
	grpctransport.RegisterRaftService(grpcServer, rt)
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

	// Start Raft event loops.
	if shardCfg != nil {
		// Multi-group: start each group's loop + ticker.
		for _, gid := range groups {
			g := host.Group(gid)
			go func(group *raft.Group) {
				if raftErr := group.Run(ctx); raftErr != nil {
					log.Error("raft_halted",
						slog.String("node_id", id),
						slog.Uint64("group", uint64(group.GroupID())),
						slog.String("error", raftErr.Error()),
					)
					select {
					case errCh <- raftErr:
					default:
					}
				}
			}(g)
			raft.StartTicker(ctx, g, 10*time.Millisecond)
		}
		if s := host.Group(0).Status(); s.Role != "" {
			log.Info("raft_started",
				slog.String("node_id", id),
				slog.String("role", string(s.Role)),
				slog.Uint64("term", s.Term),
				slog.Uint64("group", 0),
			)
		}
	} else {
		// Single-group.
		go func() {
			if raftErr := singleRaft.Run(ctx); raftErr != nil {
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
		raft.StartTicker(ctx, singleRaft, 10*time.Millisecond)
		if s := singleRaft.Status(); s.Role != "" {
			log.Info("raft_started",
				slog.String("node_id", id),
				slog.String("role", string(s.Role)),
				slog.Uint64("term", s.Term),
			)
		}
	}

	select {
	case serveErr := <-errCh:
		return fmt.Errorf("server component stopped: %w", serveErr)
	case <-ctx.Done():
		log.Info("server_shutting_down", slog.String("node_id", id))
	}

	// Graceful stop with a hard deadline.
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
