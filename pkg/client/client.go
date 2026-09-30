// Package client is the DistriKV Go SDK.
//
// Phase 2 status: single-node client. Leader-aware retries arrive in Phase 8;
// client sessions (client_id/sequence_number enforcement) in Phase 9 — but
// this client already generates and sends both fields on every mutation so
// the wire behavior never changes when the server starts honoring them.
//
// Retry safety: mutating calls are NOT auto-retried. Before Phase 9's
// deduplication, a retry after a lost response could double-apply a write.
// Callers control retries explicitly via their own context policy.
package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync/atomic"

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
)

// Client is a DistriKV connection. It is safe for concurrent use.
type Client struct {
	conn *grpc.ClientConn
	stub kv1.KVServiceClient

	// Session identity, sent on every mutation (enforced server-side from
	// Phase 9). Per-client: id is stable for the Client's lifetime and
	// sequence_number increments monotonically.
	clientID string
	seq      atomic.Uint64
}

// Dial connects to a DistriKV node at addr (host:port).
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
	if !strings.Contains(addr, "://") {
		addr = "passthrough:///" + addr
	}
	conn, err := grpc.NewClient(addr,
		append([]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("client: dial %s: %w", addr, err)
	}
	id, err := newClientID()
	if err != nil {
		conn.Close()
		return nil, err
	}
	c := &Client{conn: conn, stub: kv1.NewKVServiceClient(conn), clientID: id}
	c.seq.Store(1)
	return c, nil
}

// Close releases the underlying connection.
func (c *Client) Close() error { return c.conn.Close() }

// Get returns the value for key, or ErrKeyNotFound.
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := c.stub.Get(ctx, &kv1.GetRequest{Key: key})
	if err != nil {
		return nil, unwrap(err)
	}
	return resp.Value, nil
}

// Put stores value at key (upsert).
func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	_, err := c.stub.Put(ctx, &kv1.PutRequest{
		Key:            key,
		Value:          value,
		ClientId:       c.clientID,
		SequenceNumber: c.nextSeq(),
	})
	return unwrap(err)
}

// Delete removes key (idempotent).
func (c *Client) Delete(ctx context.Context, key string) error {
	_, err := c.stub.Delete(ctx, &kv1.DeleteRequest{
		Key:            key,
		ClientId:       c.clientID,
		SequenceNumber: c.nextSeq(),
	})
	return unwrap(err)
}

// Exists reports whether key is present.
func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	resp, err := c.stub.Exists(ctx, &kv1.ExistsRequest{Key: key})
	if err != nil {
		return false, unwrap(err)
	}
	return resp.Exists, nil
}

// CAS stores newValue at key iff the precondition holds; the bool is false
// when the precondition failed (a normal outcome, not an error).
//
// expected == nil means "key must be absent"; a non-nil (even empty) slice
// means "key must equal expected".
func (c *Client) CAS(ctx context.Context, key string, expected, newValue []byte) (bool, error) {
	resp, err := c.stub.CAS(ctx, &kv1.CASRequest{
		Key:            key,
		ExpectedExists: expected != nil,
		ExpectedValue:  expected,
		NewValue:       newValue,
		ClientId:       c.clientID,
		SequenceNumber: c.nextSeq(),
	})
	if err != nil {
		return false, unwrap(err)
	}
	return resp.Applied, nil
}

// Incr atomically adds delta to the int64 stored at key (negative = decrement)
// and returns the new value. An absent key is treated as 0.
func (c *Client) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	resp, err := c.stub.Incr(ctx, &kv1.IncrRequest{
		Key:            key,
		Delta:          delta,
		ClientId:       c.clientID,
		SequenceNumber: c.nextSeq(),
	})
	if err != nil {
		return 0, unwrap(err)
	}
	return resp.Value, nil
}

// IncrBy is Incr with delta +1.
func (c *Client) IncrBy(ctx context.Context, key string) (int64, error) {
	return c.Incr(ctx, key, 1)
}

// Decr atomically decrements the int64 stored at key by 1.
func (c *Client) Decr(ctx context.Context, key string) (int64, error) {
	return c.Incr(ctx, key, -1)
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
// FailedPrecondition -> ErrNotInteger, OutOfRange -> ErrOverflow. The server
// message is preserved in the wrapped error; everything else passes through.
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
