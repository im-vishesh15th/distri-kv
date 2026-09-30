// Package client is the DistriKV Go SDK.
//
// Status (Phase 9): leader-aware routing + client sessions. The client
// dials any node it is given, follows leader redirects for mutations,
// retries transient failures under the session's sequence number, and
// fails reads over between endpoints it has seen.
//
// Session model: one Client == one session. A stable random client_id and
// one sequence number per LOGICAL operation — retries of that operation
// reuse it (allocated inside mutate's critical section, so issue order
// always equals sequence order), the counter starts at 1, and mutations
// are serialized per Client. That ordering is what lets the server-side
// session table adjudicate duplicates precisely (S1: a retried mutation
// applies at most once and its original response is replayed; see
// docs/consistency.md for the exact bounds — this is deliberately NOT
// called "exactly once").
//
// Routing and retry policy:
//
//   - codes.Aborted (raft.ErrNotLeader): rejected BEFORE append — nothing
//     happened. The client calls GetStatus, re-dials the leader, retries
//     immediately (bounded by maxLeaderHops). If no leader is known yet
//     (election in progress), it backs off and retries like any transient
//     failure — still provably nothing applied.
//   - codes.Unavailable (leadership lost, node down, transport break):
//     outcome ambiguous — the original entry may still commit. Retried
//     since Phase 9 with the SAME session sequence: whether or not the
//     original landed, the session table caps application at once. Backoff
//     (50ms doubling to 800ms, maxTransientRetries tries) rides out
//     elections; the caller's context bounds the whole call.
//   - Domain outcomes (ErrKeyNotFound, ErrNotInteger, ...) and context
//     errors return untouched — never retried by the SDK.
//   - Reads never mutate: they fail over between known endpoints freely
//     and may be served by any node (linearizability is server-side,
//     Phase 11).
package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	kv1 "distrikv/gen/kv/v1"
	"distrikv/internal/kv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Domain errors re-exported so external consumers of this SDK can match them
// with errors.Is without importing DistriKV internals.
var (
	ErrKeyNotFound = kv.ErrKeyNotFound
	ErrNotInteger  = kv.ErrNotInteger
	ErrOverflow    = kv.ErrOverflow

	// ErrNotLeader (codes.Aborted): this node rejected the mutation before
	// replicating it — nothing was applied. The SDK redirects and retries
	// automatically; callers see it only when no leader could be located
	// (election in progress) or the redirect budget was exhausted.
	ErrNotLeader = fmt.Errorf("client: not leader and no leader could be located")
)

// maxLeaderHops bounds leader redirects within one call.
const maxLeaderHops = 3

// Phase 9 transient-retry budget: dedup makes retrying a mutation safe, so
// the SDK rides out elections and failovers instead of surfacing them.
// Backoff doubles from initial to max between tries; the caller's context
// deadline bounds the total.
const (
	maxTransientRetries = 5
	initialRetryBackoff = 50 * time.Millisecond
	maxRetryBackoff     = 800 * time.Millisecond
)

// NodeStatus is the routing view returned by Status/GetStatus.
type NodeStatus struct {
	NodeID     string
	Role       string // follower | candidate | leader
	LeaderID   string // "" = no leader known
	LeaderAddr string // dialable leader address; "" = unknown (standalone)
}

// Client is a DistriKV connection. It is safe for concurrent use:
// mutations serialize on mutateMu (one in-flight mutation per session —
// sequence numbers must be issued in order for dedup to adjudicate them),
// while reads and status calls run freely.
//
// One connection is maintained per address ever needed (memoized, closed
// only by Close): yanking a connection out from under in-flight RPCs would
// turn their outcomes ambiguous, and addresses are few (cluster-sized).
type Client struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn // address -> connection (memoized)
	opts  []grpc.DialOption           // applied to every dial (tests, future TLS)
	orig  string                      // the address the caller dialed (fallback endpoint)
	// current is the endpoint reads and writes prefer: initially the dial
	// target, then the discovered leader. Guarded by mu.
	current string

	mutateMu sync.Mutex // serializes mutations AND sequence allocation

	clientID string
	seq      atomic.Uint64
}

// Dial connects to a DistriKV node at addr (host:port) — any node: leader
// routing happens on the first mutation (Phase 8).
//
// Transport is currently plaintext (dev/single-node). TLS lands with the
// product gateway (Phase P2). opts are appended after the defaults — they
// exist for tests (e.g. bufconn dialers) and future transport options.
//
// A bare host:port is dialed passthrough-style: grpc.NewClient's default
// DNS resolver performs a service-config TXT lookup before publishing any
// address, which can stall every RPC on networks that filter DNS (observed
// live in the Phase 7 smoke: the first call hung to its deadline with zero
// sockets open). The system resolver at connect time — /etc/hosts, search
// domains — is what "host:port" implies anyway. Explicit schemes are left
// untouched.
func Dial(ctx context.Context, addr string, opts ...grpc.DialOption) (*Client, error) {
	c := &Client{
		conns:   make(map[string]*grpc.ClientConn),
		opts:    opts,
		orig:    addr,
		current: addr,
	}
	if _, err := c.connLocked(addr); err != nil {
		return nil, err
	}
	id, err := newClientID()
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	c.clientID = id
	c.seq.Store(0) // first nextSeq() returns 1 (session sequences start at 1)
	return c, nil
}

// Close releases every connection the client has opened.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var first error
	for addr, conn := range c.conns {
		if err := conn.Close(); err != nil && first == nil {
			first = fmt.Errorf("client: close %s: %w", addr, err)
		}
	}
	return first
}

// Status returns the routing view of the endpoint the client currently
// prefers (the leader once discovered, else the dial target).
func (c *Client) Status(ctx context.Context) (NodeStatus, error) {
	stub, _ := c.endpoint()
	resp, err := stub.GetStatus(ctx, &kv1.GetStatusRequest{})
	if err != nil {
		return NodeStatus{}, unwrap(err)
	}
	return NodeStatus{
		NodeID:     resp.NodeId,
		Role:       resp.Role,
		LeaderID:   resp.LeaderId,
		LeaderAddr: resp.LeaderAddr,
	}, nil
}

// Get returns the value for key, or ErrKeyNotFound.
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	var out []byte
	err := c.read(ctx, func(stub kv1.KVServiceClient) error {
		resp, err := stub.Get(ctx, &kv1.GetRequest{Key: key})
		if err != nil {
			return err
		}
		out = resp.Value
		return nil
	})
	if err != nil {
		return nil, unwrap(err)
	}
	return out, nil
}

// Put stores value at key (upsert), routed to the leader.
func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	return unwrap(c.mutate(ctx, func(stub kv1.KVServiceClient, seq uint64) error {
		_, err := stub.Put(ctx, &kv1.PutRequest{
			Key:            key,
			Value:          value,
			ClientId:       c.clientID,
			SequenceNumber: seq,
		})
		return err
	}))
}

// Delete removes key (idempotent), routed to the leader.
func (c *Client) Delete(ctx context.Context, key string) error {
	return unwrap(c.mutate(ctx, func(stub kv1.KVServiceClient, seq uint64) error {
		_, err := stub.Delete(ctx, &kv1.DeleteRequest{
			Key:            key,
			ClientId:       c.clientID,
			SequenceNumber: seq,
		})
		return err
	}))
}

// Exists reports whether key is present.
func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	var exists bool
	err := c.read(ctx, func(stub kv1.KVServiceClient) error {
		resp, err := stub.Exists(ctx, &kv1.ExistsRequest{Key: key})
		if err != nil {
			return err
		}
		exists = resp.Exists
		return nil
	})
	if err != nil {
		return false, unwrap(err)
	}
	return exists, nil
}

// CAS stores newValue at key iff the precondition holds; the bool is false
// when the precondition failed (a normal outcome, not an error).
//
// expected == nil means "key must be absent"; a non-nil (even empty) slice
// means "key must equal expected".
func (c *Client) CAS(ctx context.Context, key string, expected, newValue []byte) (bool, error) {
	var applied bool
	err := c.mutate(ctx, func(stub kv1.KVServiceClient, seq uint64) error {
		resp, err := stub.CAS(ctx, &kv1.CASRequest{
			Key:            key,
			ExpectedExists: expected != nil,
			ExpectedValue:  expected,
			NewValue:       newValue,
			ClientId:       c.clientID,
			SequenceNumber: seq,
		})
		if err != nil {
			return err
		}
		applied = resp.Applied
		return nil
	})
	if err != nil {
		return false, unwrap(err)
	}
	return applied, nil
}

// Incr atomically adds delta to the int64 stored at key (negative = decrement)
// and returns the new value. An absent key is treated as 0.
func (c *Client) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	var out int64
	err := c.mutate(ctx, func(stub kv1.KVServiceClient, seq uint64) error {
		resp, err := stub.Incr(ctx, &kv1.IncrRequest{
			Key:            key,
			Delta:          delta,
			ClientId:       c.clientID,
			SequenceNumber: seq,
		})
		if err != nil {
			return err
		}
		out = resp.Value
		return nil
	})
	if err != nil {
		return 0, unwrap(err)
	}
	return out, nil
}

// IncrBy is Incr with delta +1.
func (c *Client) IncrBy(ctx context.Context, key string) (int64, error) {
	return c.Incr(ctx, key, 1)
}

// Decr atomically decrements the int64 stored at key by 1.
func (c *Client) Decr(ctx context.Context, key string) (int64, error) {
	return c.Incr(ctx, key, -1)
}

// --- routing + retries (Phases 8/9) ---------------------------------------
//
// mutate follows leader redirects (codes.Aborted) and retries transient
// failures with backoff (dedup makes mutation retries safe since Phase 9);
// read fails over between endpoints (side-effect-free).

type readOp func(stub kv1.KVServiceClient) error              // reads: no session
type writeOp func(stub kv1.KVServiceClient, seq uint64) error // mutations: session seq

// mutate runs a write against the current endpoint. It holds mutateMu for
// the whole call and allocates the sequence number INSIDE that critical
// section: one in-flight mutation per session, issued order == sequence
// order (the server's dedup rule needs both).
//
// Failure classes:
//   - codes.Aborted + a routable leader: redirect immediately (nothing was
//     appended on this node).
//   - codes.Aborted + no leader known yet, codes.Unavailable: transient —
//     repair routing, back off, retry within maxTransientRetries. Every
//     attempt reuses this call's sequence number, so the server's session
//     table guarantees at-most-once application (S1).
//   - anything else (domain outcomes, context errors): returned untouched.
func (c *Client) mutate(ctx context.Context, op writeOp) error {
	c.mutateMu.Lock()
	defer c.mutateMu.Unlock()

	seq := c.nextSeq() // under the lock: sequence order == issue order
	var err error
	redirects, retries := 0, 0
	backoff := initialRetryBackoff
	for {
		stub, addr := c.endpoint()
		err = op(stub, seq)
		if err == nil {
			return nil
		}
		switch status.Code(err) {
		case codes.Aborted:
			if redirects >= maxLeaderHops {
				return err // redirect budget exhausted
			}
			if next, ok := c.discover(ctx, stub, addr); ok && c.switchTo(next) {
				redirects++
				continue // immediate: the redirect itself moved us
			}
			// No routable leader (election in progress): transient.
		case codes.Unavailable:
			// Ambiguous outcome — retried since Phase 9 under the same
			// session sequence. Discovery is a read, always safe.
			c.repair(ctx, addr)
		default:
			return err // domain outcome or context error
		}
		if retries >= maxTransientRetries {
			return err
		}
		retries++
		if !sleepBackoff(ctx, backoff) {
			return err // caller's deadline is our stop signal
		}
		if backoff < maxRetryBackoff {
			backoff *= 2
		}
	}
}

// sleepBackoff waits for d or the caller's context; false means the
// context is done (the caller returns the last transport error).
func sleepBackoff(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// read runs a read against the current endpoint and fails over to any
// other known endpoint on transport failure.
func (c *Client) read(ctx context.Context, op readOp) error {
	stub, addr := c.endpoint()
	err := op(stub)
	if err == nil || status.Code(err) != codes.Unavailable {
		return err
	}
	alt, altStub, ok := c.otherConn(addr)
	if !ok {
		return err
	}
	err2 := op(altStub)
	if status.Code(err2) != codes.Unavailable {
		c.switchTo(alt) // it answered (even with a domain error) — adopt it
	}
	return err2
}

// discover asks the node that just redirected us where the leader lives.
// Returns false when routing is impossible (no leader known, or the
// address is unusable — checked by switchTo).
func (c *Client) discover(ctx context.Context, stub kv1.KVServiceClient, askedAddr string) (string, bool) {
	resp, err := stub.GetStatus(ctx, &kv1.GetStatusRequest{})
	if err != nil {
		return "", false
	}
	switch {
	case resp.LeaderAddr != "":
		return resp.LeaderAddr, true
	case resp.LeaderId == resp.NodeId:
		// The node we asked won leadership between rejecting the write and
		// this lookup: stay put and retry.
		return askedAddr, true
	default:
		return "", false // election in progress — caller surfaces ErrNotLeader
	}
}

// repair re-points routing at a live endpoint after a transport failure on
// failedAddr — discovery is a read, always safe, and never affects the
// error the current call returns.
func (c *Client) repair(ctx context.Context, failedAddr string) {
	alt, altStub, ok := c.otherConn(failedAddr)
	if !ok {
		return
	}
	next := alt
	if resp, err := altStub.GetStatus(ctx, &kv1.GetStatusRequest{}); err == nil {
		// Adopt the discovered leader unless the hint points back at the
		// endpoint that just failed (stale view of a dead leader).
		if resp.LeaderAddr != "" && resp.LeaderAddr != failedAddr {
			next = resp.LeaderAddr
		}
	}
	c.switchTo(next)
}

// endpoint returns the stub and address mutations/reads currently prefer.
func (c *Client) endpoint() (kv1.KVServiceClient, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	conn, err := c.connLocked(c.current)
	if err != nil {
		// connLocked only fails on unparseable targets; current came from
		// dial or a validated hint. Fall back to the dial target.
		conn, _ = c.connLocked(c.orig)
	}
	return kv1.NewKVServiceClient(conn), c.current
}

// otherConn returns a live-candidate connection to some endpoint other
// than addr, preferring the original dial target.
func (c *Client) otherConn(addr string) (string, kv1.KVServiceClient, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pick := func(a string) (kv1.KVServiceClient, bool) {
		conn, ok := c.conns[a]
		if !ok {
			return nil, false
		}
		return kv1.NewKVServiceClient(conn), true
	}
	if c.orig != addr && c.orig != "" {
		if stub, ok := pick(c.orig); ok {
			return c.orig, stub, true
		}
	}
	for a := range c.conns {
		if a == addr {
			continue
		}
		if stub, ok := pick(a); ok {
			return a, stub, true
		}
	}
	return "", nil, false
}

// switchTo moves the preferred endpoint to addr (dialed lazily, memoized).
// Returns false if the address cannot even form a client — callers then
// keep their current routing.
func (c *Client) switchTo(addr string) bool {
	if addr == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.connLocked(addr); err != nil {
		return false
	}
	c.current = addr
	return true
}

// connLocked returns the memoized connection for addr, dialing on first
// use. Caller must hold c.mu.
func (c *Client) connLocked(addr string) (*grpc.ClientConn, error) {
	if conn, ok := c.conns[addr]; ok {
		return conn, nil
	}
	target := addr
	if !strings.Contains(target, "://") {
		target = "passthrough:///" + target // see the package doc
	}
	conn, err := grpc.NewClient(target, append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, c.opts...)...)
	if err != nil {
		return nil, fmt.Errorf("client: dial %s: %w", addr, err)
	}
	c.conns[addr] = conn
	return conn, nil
}

// nextSeq returns the next session sequence number for this client.
func (c *Client) nextSeq() uint64 { return c.seq.Add(1) }

// newClientID generates a random 128-bit client identity.
func newClientID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("client: generate client_id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// unwrap maps wire errors back to SDK-visible domain errors. The gRPC status
// code is the contract (see internal/server): NotFound -> ErrKeyNotFound,
// FailedPrecondition -> ErrNotInteger, OutOfRange -> ErrOverflow,
// Aborted -> ErrNotLeader (rejected before replication; the SDK already
// attempted routing). The server message is preserved in the wrapped error;
// everything else passes through.
func unwrap(err error) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.NotFound:
		return wrapSentinel(ErrKeyNotFound, err)
	case codes.FailedPrecondition:
		return wrapSentinel(ErrNotInteger, err)
	case codes.OutOfRange:
		return wrapSentinel(ErrOverflow, err)
	case codes.Aborted:
		return wrapSentinel(ErrNotLeader, err)
	default:
		return err
	}
}

// wrapSentinel returns sentinel wrapped with the wire message so errors.Is
// matches sentinel while the original server text survives.
func wrapSentinel(sentinel, err error) error {
	msg := status.Convert(err).Message()
	if msg == "" || msg == sentinel.Error() {
		return sentinel
	}
	return fmt.Errorf("%w: %s", sentinel, msg)
}
