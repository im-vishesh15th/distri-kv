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

func TestDialBareTargetUsesPassthrough(t *testing.T) {
	c, err := Dial(context.Background(), "127.0.0.1:1")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()
	if got, want := c.conn.Target(), "passthrough:///127.0.0.1:1"; got != want {
		t.Fatalf("target = %q, want %q", got, want)
	}
}

func TestDialKeepsExplicitScheme(t *testing.T) {
	c, err := Dial(context.Background(), "passthrough:///bufnet")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()
	if got, want := c.conn.Target(), "passthrough:///bufnet"; got != want {
		t.Fatalf("target = %q, want %q", got, want)
	}
}
