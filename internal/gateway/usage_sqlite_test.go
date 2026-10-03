package gateway

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestSQLiteUsageSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "gw.db")
	now := time.Now().UTC()

	s1, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateTenant(ctx, s1, "alpha", "Alpha", now); err != nil {
		t.Fatal(err)
	}
	key, _, err := CreateAPIKey(ctx, s1, "alpha", "live", 0, now)
	if err != nil {
		t.Fatal(err)
	}

	gw1 := New(Config{RateLimitRPS: 1e6, RateLimitBurst: 1e6}, s1, newFakeKV())
	e1 := &env{gw: gw1}
	e1.do("PUT", "/kv/a", key, `{"value":"1"}`)
	e1.do("GET", "/kv/a", key, "")
	e1.do("GET", "/kv/missing", key, "")
	if err := gw1.Metrics().Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := gw1.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	gw2 := New(Config{RateLimitRPS: 1e6, RateLimitBurst: 1e6}, s2, newFakeKV())
	t.Cleanup(func() { _ = gw2.Close() })
	ser, err := gw2.Metrics().Series("alpha", "24h")
	if err != nil {
		t.Fatal(err)
	}
	if len(ser.Points) != 24 {
		t.Fatalf("24h points = %d", len(ser.Points))
	}
	var sum uint64
	for _, p := range ser.Points {
		sum += p.Requests
	}
	if sum != 3 {
		t.Fatalf("series sum = %d, want 3", sum)
	}
	e2 := &env{gw: gw2}
	u := usageOf(t, e2, key)
	if u.ByOp["put"] != 1 || u.ByOp["get"] != 2 {
		t.Fatalf("after restart by_op = %v", u.ByOp)
	}
	if u.Requests != 3 {
		t.Fatalf("after restart requests = %d, want 3", u.Requests)
	}
	if u.Since.IsZero() {
		t.Fatal("since should be the first stored bucket")
	}
}

func TestSQLiteUsageIsPerTenant(t *testing.T) {
	ctx := context.Background()
	s, _ := openTemp(t)
	t.Cleanup(func() { _ = s.Close() })
	now := time.Now()
	if _, err := CreateTenant(ctx, s, "alpha", "A", now); err != nil {
		t.Fatal(err)
	}
	if _, err := CreateTenant(ctx, s, "beta", "B", now); err != nil {
		t.Fatal(err)
	}
	a, _, err := CreateAPIKey(ctx, s, "alpha", "a", 0, now)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := CreateAPIKey(ctx, s, "beta", "b", 0, now)
	if err != nil {
		t.Fatal(err)
	}
	gw := New(Config{RateLimitRPS: 1e6, RateLimitBurst: 1e6}, s, newFakeKV())
	t.Cleanup(func() { _ = gw.Close() })
	e := &env{gw: gw}
	e.do("PUT", "/kv/k", a, `{"value":"x"}`)
	if err := gw.Metrics().Flush(ctx); err != nil {
		t.Fatal(err)
	}

	ser, err := gw.Metrics().Series("beta", "24h")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range ser.Points {
		if p.Requests != 0 {
			t.Fatalf("beta series leaked %d", p.Requests)
		}
	}

	if got := usageOf(t, e, b).Requests; got != 0 {
		t.Fatalf("beta requests = %d", got)
	}
}
