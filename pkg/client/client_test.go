package client

// Dial-target normalization is a load-bearing detail: grpc.NewClient's
// default DNS resolver performs a service-config TXT lookup before
// publishing addresses, which hung every RPC to its deadline on a
// DNS-filtered network (caught by the Phase 7 smoke test, with zero
// sockets ever opened). Bare host:port targets must resolve passthrough.

import (
	"context"
	"testing"
)

// currentTarget reports the gRPC target of the endpoint the client prefers
// for group 0.
func currentTarget(c *Client) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	addr := c.leaders[0]
	if addr == "" {
		addr = c.orig
	}
	return c.conns[addr].Target()
}

func TestDialBareTargetUsesPassthrough(t *testing.T) {
	c, err := Dial(context.Background(), "127.0.0.1:1")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()
	if got, want := currentTarget(c), "passthrough:///127.0.0.1:1"; got != want {
		t.Fatalf("target = %q, want %q", got, want)
	}
}

func TestDialKeepsExplicitScheme(t *testing.T) {
	c, err := Dial(context.Background(), "passthrough:///bufnet")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()
	if got, want := currentTarget(c), "passthrough:///bufnet"; got != want {
		t.Fatalf("target = %q, want %q", got, want)
	}
}
