package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"distrikv/pkg/client"
)

// Pool is the production KV: several cluster SDK clients used round-robin.
//
// Why a pool: the SDK serializes mutations per client (one session, one
// sequence counter), so a single shared client would queue every tenant's
// writes behind each other. Each pooled client has its own session, so
// retries inside one client stay deduplicated by the cluster.
//
// Every client knows every cluster address, so losing one node does not
// strand the gateway (the SDK follows leader redirects across endpoints).
type Pool struct {
	clients []*client.Client
	next    atomic.Uint64
}

var _ KV = (*Pool)(nil)

// NewPool dials size clients, spreading their first connection across addrs.
func NewPool(ctx context.Context, addrs []string, size int) (*Pool, error) {
	if len(addrs) == 0 {
		return nil, errors.New("gateway: no KV addresses given")
	}
	if size < 1 {
		size = 1
	}
	p := &Pool{}
	for i := 0; i < size; i++ {
		c, err := client.Dial(ctx, addrs[i%len(addrs)])
		if err != nil {
			_ = p.Close()
			return nil, fmt.Errorf("gateway: dial %s: %w", addrs[i%len(addrs)], err)
		}
		for _, a := range addrs {
			if err := c.AddEndpoint(a); err != nil {
				_ = c.Close()
				_ = p.Close()
				return nil, fmt.Errorf("gateway: add endpoint %s: %w", a, err)
			}
		}
		p.clients = append(p.clients, c)
	}
	return p, nil
}

func (p *Pool) pick() *client.Client {
	return p.clients[(p.next.Add(1)-1)%uint64(len(p.clients))]
}

// translate maps SDK/gRPC errors onto the gateway's own sentinels.
func translate(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, client.ErrKeyNotFound):
		return ErrKeyNotFound
	case errors.Is(err, client.ErrNotInteger):
		return ErrNotInteger
	case errors.Is(err, client.ErrOverflow):
		return ErrOverflow
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return err
	case errors.Is(err, client.ErrNotLeader):
		return ErrUnavailable
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.Aborted:
		return ErrUnavailable
	case codes.DeadlineExceeded:
		return context.DeadlineExceeded
	}
	return err
}

func (p *Pool) Get(ctx context.Context, key string) ([]byte, error) {
	v, err := p.pick().Get(ctx, key)
	return v, translate(err)
}

func (p *Pool) Put(ctx context.Context, key string, value []byte) error {
	return translate(p.pick().Put(ctx, key, value))
}

func (p *Pool) Delete(ctx context.Context, key string) error {
	return translate(p.pick().Delete(ctx, key))
}

func (p *Pool) CAS(ctx context.Context, key string, expected, newValue []byte) (bool, error) {
	ok, err := p.pick().CAS(ctx, key, expected, newValue)
	return ok, translate(err)
}

func (p *Pool) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	v, err := p.pick().Incr(ctx, key, delta)
	return v, translate(err)
}

func (p *Pool) Close() error {
	var first error
	for _, c := range p.clients {
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
