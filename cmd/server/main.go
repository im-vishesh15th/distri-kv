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
// Phase 21: metadata Raft group (GroupID = MaxUint64) owns the versioned
// shard config and replicates MoveSlots commands; its commits trigger
// config hot-swaps in the data-plane services and clients.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	_ "net/http/pprof"
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

// metadataGroupID is the reserved GroupID for the metadata Raft group.
// It uses MaxUint64 to avoid collision with user data groups [0, 16383].
const metadataGroupID = raft.GroupID(math.MaxUint64)

func main() {
	var (
		id          = flag.String("id", "node1", "node identity")
		addr        = flag.String("addr", ":8080", "gRPC listen address")
		pprofAddr   = flag.String("pprof-addr", "", "pprof HTTP listen address (e.g. :6060); empty disables")
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

	if err := run(*id, *addr, *pprofAddr, *dataDir, *peersSpec, *shardConfig, *snapEvery, log); err != nil {
		log.Error("server_exit", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(id, addr, pprofAddr, dataDir, peersSpec, shardConfigPath string, snapEvery uint64, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Start pprof HTTP server if requested.
	if pprofAddr != "" {
		go func() {
			log.Info("pprof_http_started", slog.String("addr", pprofAddr))
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				log.Error("pprof_http_error", slog.String("error", err.Error()))
			}
		}()
	}

	// Load initial shard config if provided.
	var initialShardCfg *shard.Config
	if shardConfigPath != "" {
		cfg, err := shard.LoadConfig(shardConfigPath)
		if err != nil {
			return fmt.Errorf("load shard config: %w", err)
		}
		initialShardCfg = cfg
		log.Info("shard_config_loaded",
			slog.String("path", shardConfigPath),
			slog.Uint64("version", cfg.Version),
			slog.Uint64("groups", cfg.Groups),
		)
	} else {
		// Default single-group config.
		initialShardCfg = shard.NewMap(1)
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

	// Determine data groups to host (non-metadata groups).
	var dataGroups []raft.GroupID
	if initialShardCfg != nil {
		for g := uint64(0); g < initialShardCfg.Groups; g++ {
			dataGroups = append(dataGroups, raft.GroupID(g))
		}
	} else {
		dataGroups = []raft.GroupID{0}
	}

	// Build per-group engines, logs, and raft groups.
	engines := make(map[raft.GroupID]kv.Engine)
	host := multiraft.NewHost(transport.NodeID(id))
	var singleRaft *raft.Group // for backward compat when no shard config

	// 1) Create metadata state machine and group (always).
	metaDir := filepath.Join(dataDir, "metadata", id, "raft")
	metaRlog, metaRec, err := raftlog.Open(metaDir)
	if err != nil {
		return fmt.Errorf("metadata group: open raft log: %w", err)
	}
	log.Info("raft_log_opened",
		slog.String("node_id", id),
		slog.String("group", "metadata"),
		slog.Int64("records", metaRec.Records),
		slog.Bool("torn_tail_discarded", metaRec.TornTail),
		slog.Int64("truncated_bytes", metaRec.TruncatedBytes),
	)

	// Create metadata SM with initial config and a callback to hot-swap.
	// We'll create the GroupedService after this and pass the metadataSM to it.
	metadataSM := kv.NewMetadataSM(kv.MetadataSMConfig{
		InitialConfig: initialShardCfg,
		OnConfigChange: func(version uint64) {
			log.Info("metadata_config_changed", slog.Uint64("version", version))
			// GroupedService will pick up the new config on next request via metadataSM.Config()
		},
	})
	metaEngine := kv.NewMemEngine()
	engines[metadataGroupID] = metaEngine
	metaGroup, err := raft.New(raft.Config{
		ID:             transport.NodeID(id),
		GroupID:        metadataGroupID,
		Peers:          raftPeers,
		Transport:      rt,
		Log:            metaRlog,
		ElectionTicks:  50,
		HeartbeatTicks: 5,
		Logger:         log,
		StateMachine:   metadataSM,
		SnapshotEvery:  snapEvery,
	})
	if err != nil {
		return fmt.Errorf("metadata group: raft.New: %w", err)
	}
	host.AddGroup(metaGroup)

	// 2) Create data groups.
	if len(dataGroups) > 1 || (len(dataGroups) == 1 && dataGroups[0] != 0) {
		// Multi-group mode: create data groups in the same host.
		for _, gid := range dataGroups {
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
				ElectionTicks:  50,
				HeartbeatTicks: 5,
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
		// Single-group mode (backward compatible): group 0 only, no metadata group in host.
		// But we still have the metadata group in the host above.
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
			ElectionTicks:  50,
			HeartbeatTicks: 5,
			Logger:         log,
			StateMachine:   kv.NewSM(engine),
			SnapshotEvery:  snapEvery,
		})
		if err != nil {
			return fmt.Errorf("raft core: %w", err)
		}
		singleRaft = rn
		host.AddGroup(singleRaft) // Add group 0 to host for status checks
		rt.SetHandler(host)
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	grpcServer := grpc.NewServer()

	// Create the appropriate KV service.
	var kvService kv1.KVServiceServer
	if len(dataGroups) > 1 || (len(dataGroups) == 1 && dataGroups[0] != 0) {
		// Multi-group: use GroupedService with metadataSM for dynamic config.
		kvService = server.NewGroupedService(
			metadataSM,  // provides dynamic config via Config()
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

	// Start Raft event loops for all groups in the host.
	for _, gid := range host.GroupIDs() {
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

	// Wait for groups to initialize by polling Status() until it succeeds.
	// Group.Run() initializes the events channel; we poll until Status() succeeds.
	for _, gid := range host.GroupIDs() {
		for i := 0; i < 50; i++ {
			if s := host.Group(gid).Status(); s.Role != "" || s.ID != "" {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	if s := host.Group(0).Status(); s.Role != "" {
		log.Info("raft_started",
			slog.String("node_id", id),
			slog.String("role", string(s.Role)),
			slog.Uint64("term", s.Term),
			slog.Uint64("group", 0),
		)
	}
	if s := host.Group(metadataGroupID).Status(); s.Role != "" {
		log.Info("raft_started",
			slog.String("node_id", id),
			slog.String("role", string(s.Role)),
			slog.Uint64("term", s.Term),
			slog.String("group", "metadata"),
		)
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
