package server_test

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	kv1 "distrikv/gen/kv/v1"
	"distrikv/internal/kv"
	"distrikv/internal/raft"
	"distrikv/internal/raftlog"
	"distrikv/internal/server"
	"distrikv/internal/transport"
	grpctransport "distrikv/internal/transport/grpc"
	"distrikv/pkg/client"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// startSingleNode wires the full single-node stack — engine, persistent
// Raft log, transport, event loop, ticker — and waits for self-election.
// The engine is only ever mutated by this node's apply path, exactly as in
// a cluster: mutations go Service -> Propose -> log -> apply -> engine.
func startSingleNode(t *testing.T) (kv.Engine, *raft.Node) {
	t.Helper()

	engine := kv.NewMemEngine()
	rlog, _, err := raftlog.Open(filepath.Join(t.TempDir(), "raft"))
	if err != nil {
		t.Fatalf("raftlog: %v", err)
	}
	tpt, err := grpctransport.New("n1", nil, nil)
	if err != nil {
		t.Fatalf("transport: %v", err)
	}
	node, err := raft.New(raft.Config{
		ID:             "n1",
		Peers:          []transport.NodeID{"n1"}, // standalone majority of one
		Transport:      tpt,
		Log:            rlog,
		ElectionTicks:  10,
		HeartbeatTicks: 3,
		StateMachine:   kv.NewSM(engine),
	})
	if err != nil {
		t.Fatalf("raft.New: %v", err)
	}
	tpt.SetHandler(node)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- node.Run(ctx) }()
	raft.StartTicker(ctx, node, 10*time.Millisecond)
	t.Cleanup(func() {
		cancel()
		<-runDone // loop stops before the log it reads is closed
		_ = tpt.Close()
		if cerr := rlog.Close(); cerr != nil {
			t.Errorf("raftlog close: %v", cerr)
		}
	})

	// Writes need a leader; self-election takes 100–190 ms.
	deadline := time.Now().Add(5 * time.Second)
	for node.Status().Role != raft.RoleLeader {
		if time.Now().After(deadline) {
			t.Fatal("standalone node never became leader")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return engine, node
}

// newTestClient wires a real gRPC server (bufconn) + the pkg/client SDK
// entirely in-process, so these tests exercise the full wire stack:
// SDK -> protobuf -> gRPC -> service -> Raft propose -> apply -> engine,
// and the reverse.
func newTestClient(t *testing.T) *client.Client {
	t.Helper()

	engine, node := startSingleNode(t)

	lis := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	kv1.RegisterKVServiceServer(grpcServer, server.NewService(engine, node, node.Status, nil, nil))

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	t.Cleanup(grpcServer.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := client.Dial(ctx, "passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestRoundTrip(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	// Missing key.
	if _, err := c.Get(ctx, "nope"); !errors.Is(err, client.ErrKeyNotFound) {
		t.Fatalf("Get(missing) = %v, want ErrKeyNotFound", err)
	}
	if ok, err := c.Exists(ctx, "nope"); err != nil || ok {
		t.Fatalf("Exists(missing) = %v, %v; want false, nil", ok, err)
	}

	// Put → Get → Exists → overwrite → Delete → Get.
	if err := c.Put(ctx, "k", []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	v, err := c.Get(ctx, "k")
	if err != nil || string(v) != "v1" {
		t.Fatalf("Get = %q, %v; want v1, nil", v, err)
	}
	if ok, _ := c.Exists(ctx, "k"); !ok {
		t.Fatal("Exists = false after Put")
	}
	if err := c.Put(ctx, "k", []byte("v2")); err != nil {
		t.Fatalf("Put overwrite: %v", err)
	}
	if v, _ := c.Get(ctx, "k"); string(v) != "v2" {
		t.Fatalf("after overwrite Get = %q, want v2", v)
	}
	if err := c.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := c.Get(ctx, "k"); !errors.Is(err, client.ErrKeyNotFound) {
		t.Fatalf("Get after Delete = %v, want ErrKeyNotFound", err)
	}
	// Idempotent delete.
	if err := c.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete of missing key = %v, want nil", err)
	}
}

func TestCASOverWire(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	// Absent key, nil expected (must-be-absent) → applied.
	ok, err := c.CAS(ctx, "k", nil, []byte("created"))
	if err != nil || !ok {
		t.Fatalf("CAS absent: ok=%v err=%v; want true, nil", ok, err)
	}

	// Present key, wrong expected → applied=false, NOT an error.
	ok, err = c.CAS(ctx, "k", []byte("wrong"), []byte("x"))
	if err != nil || ok {
		t.Fatalf("CAS mismatch: ok=%v err=%v; want false, nil", ok, err)
	}
	if v, _ := c.Get(ctx, "k"); string(v) != "created" {
		t.Fatalf("failed CAS mutated state: %q", v)
	}

	// Present key, correct expected → applied.
	ok, err = c.CAS(ctx, "k", []byte("created"), []byte("v2"))
	if err != nil || !ok {
		t.Fatalf("CAS match: ok=%v err=%v; want true, nil", ok, err)
	}

	// Present key, nil expected (must-be-absent) → applied=false.
	ok, err = c.CAS(ctx, "k", nil, []byte("x"))
	if err != nil || ok {
		t.Fatalf("CAS nil-vs-present: ok=%v err=%v; want false, nil", ok, err)
	}

	// Absent key, non-nil expected → applied=false.
	ok, err = c.CAS(ctx, "absent", []byte("v"), []byte("x"))
	if err != nil || ok {
		t.Fatalf("CAS expected-vs-absent: ok=%v err=%v; want false, nil", ok, err)
	}
}

func TestIncrDecrOverWire(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	if n, err := c.IncrBy(ctx, "n"); err != nil || n != 1 {
		t.Fatalf("Incr on absent = %d, %v; want 1, nil", n, err)
	}
	if n, err := c.Incr(ctx, "n", 41); err != nil || n != 42 {
		t.Fatalf("IncrBy 41 = %d, %v; want 42, nil", n, err)
	}
	if n, err := c.Decr(ctx, "n"); err != nil || n != 41 {
		t.Fatalf("Decr = %d, %v; want 41, nil", n, err)
	}
	if n, err := c.Incr(ctx, "n", -10); err != nil || n != 31 {
		t.Fatalf("IncrBy -10 = %d, %v; want 31, nil", n, err)
	}

	// Non-integer → ErrNotInteger (FailedPrecondition on the wire).
	if err := c.Put(ctx, "s", []byte("hello")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := c.IncrBy(ctx, "s"); !errors.Is(err, client.ErrNotInteger) {
		t.Fatalf("Incr on non-integer = %v, want ErrNotInteger", err)
	}

	// Overflow → ErrOverflow.
	if err := c.Put(ctx, "max", []byte("9223372036854775807")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := c.IncrBy(ctx, "max"); !errors.Is(err, client.ErrOverflow) {
		t.Fatalf("Incr at max = %v, want ErrOverflow", err)
	}
}

func TestEmptyKeyRejected(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	if _, err := c.Get(ctx, ""); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Get(\"\") = %v, want InvalidArgument", err)
	}
	if err := c.Put(ctx, "", []byte("v")); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Put(\"\") = %v, want InvalidArgument", err)
	}
}

// TestSessionFieldsOnWire asserts the Phase-9 fields (client_id,
// sequence_number) are populated by the SDK on every mutation: client_id is
// stable and sequence_number increases monotonically. The server ignores them
// until Phase 9 — this test pins the wire behavior so Phase 9 adds no client
// changes.
func TestSessionFieldsOnWire(t *testing.T) {
	lis := bufconn.Listen(1 << 20)

	var (
		mu        sync.Mutex
		clientIDs = map[string]int{}
		seqs      []uint64
	)
	capture := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		mu.Lock()
		switch r := req.(type) {
		case *kv1.PutRequest:
			clientIDs[r.ClientId]++
			seqs = append(seqs, r.SequenceNumber)
		case *kv1.DeleteRequest:
			clientIDs[r.ClientId]++
			seqs = append(seqs, r.SequenceNumber)
		}
		mu.Unlock()
		return handler(ctx, req)
	}

	grpcServer := grpc.NewServer(grpc.ChainUnaryInterceptor(capture))
	engine, node := startSingleNode(t)
	kv1.RegisterKVServiceServer(grpcServer, server.NewService(engine, node, node.Status, nil, nil))
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, "passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	for i := 0; i < 5; i++ {
		if err := c.Put(ctx, "k"+strconv.Itoa(i), []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := c.Delete(ctx, "k0"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(clientIDs) != 1 {
		t.Fatalf("client IDs = %v, want exactly one stable ID", clientIDs)
	}
	for id, n := range clientIDs {
		if id == "" {
			t.Fatal("client_id is empty on the wire")
		}
		if n != 6 {
			t.Fatalf("requests with this client_id = %d, want 6", n)
		}
	}
	if len(seqs) != 6 {
		t.Fatalf("captured %d sequences, want 6", len(seqs))
	}
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Fatalf("sequence numbers not monotonic: %v", seqs)
		}
	}
}

// TestConcurrentIncrThroughWire: N goroutines × M increments over real gRPC
// must produce exactly N*M — the atomic-op guarantee end-to-end.
func TestConcurrentIncrThroughWire(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	const (
		goroutines = 10
		perG       = 50
	)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				if _, err := c.IncrBy(ctx, "counter"); err != nil {
					t.Errorf("Incr: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	v, err := c.Get(ctx, "counter")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(v) != strconv.Itoa(goroutines*perG) {
		t.Fatalf("counter = %s, want %d (lost updates)", v, goroutines*perG)
	}
}

// TestStatusSingleLeaderSelf pins the standalone routing view (Phase 8):
// a lone node reports itself as the leader, and with no -peers membership
// there is no address hint — the SDK's rule is leader_id == node_id means
// "you are already on the leader".
func TestStatusSingleLeaderSelf(t *testing.T) {
	c := newTestClient(t)
	st, err := c.Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.NodeID != "n1" || st.Role != "leader" || st.LeaderID != "n1" {
		t.Fatalf("status = %+v, want node=n1 role=leader leader=n1", st)
	}
	if st.LeaderAddr != "" {
		t.Fatalf("leader_addr = %q, want empty (standalone has no peer map)", st.LeaderAddr)
	}
}
