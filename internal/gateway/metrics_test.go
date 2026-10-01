package gateway

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func usageOf(t *testing.T, e *env, key string) Usage {
	t.Helper()
	rec := e.do("GET", "/v1/usage", key, "")
	if rec.Code != 200 {
		t.Fatalf("usage: %d %s", rec.Code, rec.Body.String())
	}
	var u Usage
	if err := json.Unmarshal(rec.Body.Bytes(), &u); err != nil {
		t.Fatalf("usage JSON: %v", err)
	}
	return u
}

func TestUsageCountsPerOpAndStatus(t *testing.T) {
	e := newEnv(t, Config{})
	key := e.tenantKey(t, "alpha")

	e.do("PUT", "/kv/a", key, `{"value":"1"}`)
	e.do("GET", "/kv/a", key, "")
	e.do("GET", "/kv/missing", key, "") // 404
	e.do("POST", "/kv", key, `{"op":"incr","key":"n","delta":2}`)

	u := usageOf(t, e, key)
	if u.ByOp["put"] != 1 || u.ByOp["get"] != 2 || u.ByOp["incr"] != 1 {
		t.Fatalf("by_op = %v", u.ByOp)
	}
	if u.ByStatus["200"] != 3 || u.ByStatus["404"] != 1 {
		t.Fatalf("by_status = %v", u.ByStatus)
	}
	// The usage call itself is counted only after it returns, so it is not
	// in its own response.
	if u.Requests != 4 {
		t.Fatalf("requests = %d, want 4", u.Requests)
	}
}

func TestUsageIsPerTenant(t *testing.T) {
	e := newEnv(t, Config{})
	a, b := e.tenantKey(t, "alpha"), e.tenantKey(t, "beta")
	for i := 0; i < 3; i++ {
		e.do("PUT", "/kv/k", a, `{"value":"x"}`)
	}
	if got := usageOf(t, e, b).Requests; got != 0 {
		t.Fatalf("beta sees %d requests from alpha", got)
	}
	if got := usageOf(t, e, a).Requests; got != 3 {
		t.Fatalf("alpha requests = %d, want 3", got)
	}
}

func TestUsageNeedsAuth(t *testing.T) {
	e := newEnv(t, Config{})
	if rec := e.do("GET", "/v1/usage", "", ""); rec.Code != 401 {
		t.Fatalf("want 401, got %d", rec.Code)
	}
}

func TestPrometheusOutput(t *testing.T) {
	e := newEnv(t, Config{RateLimitRPS: 1, RateLimitBurst: 2})
	key := e.tenantKey(t, "alpha")
	e.do("PUT", "/kv/a", key, `{"value":"1"}`)
	e.do("GET", "/kv/a", key, "")
	if rec := e.do("GET", "/kv/a", key, ""); rec.Code != 429 { // burst of 2 spent
		t.Fatalf("want 429, got %d", rec.Code)
	}
	e.do("GET", "/kv/a", "junk", "") // unauthenticated

	rec := httptest.NewRecorder()
	e.gw.Metrics().Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	out := rec.Body.String()
	for _, want := range []string{
		`distrikv_gateway_requests_total{tenant="alpha",op="put",status="200"} 1`,
		`distrikv_gateway_requests_total{tenant="alpha",op="get",status="429"} 1`,
		`distrikv_gateway_requests_total{tenant="none",op="get",status="401"} 1`,
		`distrikv_gateway_rate_limited_total{tenant="alpha"} 1`,
		`distrikv_gateway_request_duration_seconds_count{op="put"} 1`,
		`distrikv_gateway_request_duration_seconds_bucket{op="put",le="+Inf"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics output missing %q\n%s", want, out)
		}
	}
	if strings.Contains(out, "dkv_live_") {
		t.Fatal("metrics output leaked an API key")
	}
}

func TestMetricsNotServedOnPublicHandler(t *testing.T) {
	e := newEnv(t, Config{})
	key := e.tenantKey(t, "alpha")
	if rec := e.do("GET", "/metrics", "", ""); rec.Code != 401 {
		t.Fatalf("/metrics without key: %d, want 401", rec.Code)
	}
	if rec := e.do("GET", "/metrics", key, ""); rec.Code != 404 {
		t.Fatalf("/metrics with key: %d, want 404", rec.Code)
	}
}

// blockingKV holds Get calls until released, to keep requests in flight.
type blockingKV struct {
	*fakeKV
	entered chan struct{}
	release chan struct{}
}

func (b *blockingKV) Get(ctx context.Context, k string) ([]byte, error) {
	b.entered <- struct{}{}
	select {
	case <-b.release:
	case <-ctx.Done():
	}
	return b.fakeKV.Get(ctx, k)
}

func TestConcurrencyCapPerTenant(t *testing.T) {
	bk := &blockingKV{fakeKV: newFakeKV(), entered: make(chan struct{}, 4), release: make(chan struct{})}
	e := newEnv(t, Config{MaxConcurrentPerTenant: 1})
	e.gw.kv = bk
	a, b := e.tenantKey(t, "alpha"), e.tenantKey(t, "beta")
	bk.fakeKV.m["t:alpha:k"] = []byte("v")
	bk.fakeKV.m["t:beta:k"] = []byte("v")

	done := make(chan int, 1)
	go func() { done <- e.do("GET", "/kv/k", a, "").Code }()
	select {
	case <-bk.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first request never reached the KV")
	}

	// alpha is at its cap: the second request is refused at once.
	rec := e.do("GET", "/kv/k", a, "")
	if rec.Code != 429 || errCode(t, rec) != "too_many_concurrent_requests" {
		t.Fatalf("second alpha request: %d %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("missing Retry-After")
	}

	// beta has its own slots and is unaffected.
	betaDone := make(chan int, 1)
	go func() { betaDone <- e.do("GET", "/kv/k", b, "").Code }()
	select {
	case <-bk.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("beta was blocked by alpha's cap")
	}

	close(bk.release)
	if c := <-done; c != 200 {
		t.Fatalf("held alpha request: %d", c)
	}
	if c := <-betaDone; c != 200 {
		t.Fatalf("beta request: %d", c)
	}

	// The slot is free again.
	if rec := e.do("GET", "/kv/k", a, ""); rec.Code != 200 {
		t.Fatalf("after release: %d", rec.Code)
	}
	if got := usageOf(t, e, a).ConcurrencyLimited; got != 1 {
		t.Fatalf("concurrency_limited = %d, want 1", got)
	}
}

func TestRunRejectsHalfConfiguredTLS(t *testing.T) {
	for _, cfg := range []Config{{TLSCertFile: "c.pem"}, {TLSKeyFile: "k.pem"}} {
		e := newEnv(t, cfg)
		if err := e.gw.Run(context.Background()); err == nil {
			t.Fatalf("Run accepted half TLS config %+v", cfg)
		}
	}
}
