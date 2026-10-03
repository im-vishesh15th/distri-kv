package gateway

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func fakeNode(t *testing.T, doc string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tenant-storage" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(doc))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// Per group the replica with the highest applied index wins (a lagging
// follower must not lower the number); groups are then summed.
func TestStorageSourceAggregates(t *testing.T) {
	n1 := fakeNode(t, `{"node":"node1","groups":{
	  "0":{"applied":50,"tenants":{"acme":{"bytes":1000,"keys":10},"beta":{"bytes":7,"keys":1}}},
	  "1":{"applied":40,"tenants":{"acme":{"bytes":500,"keys":5}}}}}`)
	n2 := fakeNode(t, `{"node":"node2","groups":{
	  "0":{"applied":30,"tenants":{"acme":{"bytes":9999,"keys":99}}},
	  "1":{"applied":45,"tenants":{"acme":{"bytes":600,"keys":6}}}}}`)
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()

	s := NewStorageSource([]string{n1.URL, n2.URL, dead.URL})
	u, err := s.Usage(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	// group 0 from node1 (applied 50): 1000/10; group 1 from node2 (applied 45): 600/6.
	if u.Bytes != 1600 || u.Keys != 16 || u.Groups != 2 || u.AsOf.IsZero() {
		t.Fatalf("acme: %+v", u)
	}
	if b, _ := s.Usage(context.Background(), "beta"); b.Bytes != 7 || b.Keys != 1 {
		t.Fatalf("beta: %+v", b)
	}
	if z, err := s.Usage(context.Background(), "nobody"); err != nil || z.Bytes != 0 || z.Keys != 0 {
		t.Fatalf("tenant without data should be zero, got %+v %v", z, err)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// Fresh window: no fetches. Soft window: serve the old snapshot at once and
// refresh ONCE in the background. Hard window: callers share one blocking refresh.
func TestStorageSourceFreshnessPolicy(t *testing.T) {
	var mu sync.Mutex
	calls, bytes := 0, 5
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		calls++
		b := bytes
		mu.Unlock()
		fmt.Fprintf(w, `{"groups":{"0":{"applied":1,"tenants":{"acme":{"bytes":%d,"keys":1}}}}}`, b)
	}))
	defer srv.Close()
	get := func() (int, int) { mu.Lock(); defer mu.Unlock(); return calls, bytes }
	set := func(b int) { mu.Lock(); bytes = b; mu.Unlock() }

	var cmu sync.Mutex
	now := time.Now()
	clock := func() time.Time { cmu.Lock(); defer cmu.Unlock(); return now }
	advance := func(d time.Duration) { cmu.Lock(); now = now.Add(d); cmu.Unlock() }
	s := NewStorageSource([]string{srv.URL})
	s.now = clock
	ctx := context.Background()

	u, _ := s.Usage(ctx, "acme") // cold: blocking refresh
	if u.Bytes != 5 {
		t.Fatalf("cold: %+v", u)
	}
	for i := 0; i < 5; i++ {
		_, _ = s.Usage(ctx, "acme")
	}
	if c, _ := get(); c != 1 {
		t.Fatalf("fresh window must not fetch, calls=%d", c)
	}

	set(9)
	advance(15 * time.Second) // soft window
	u, _ = s.Usage(ctx, "acme")
	if u.Bytes != 5 {
		t.Fatalf("soft window must answer from cache immediately, got %d", u.Bytes)
	}
	waitFor(t, "background refresh", func() bool { c, _ := get(); return c == 2 })
	waitFor(t, "fresh value", func() bool { u, _ := s.Usage(ctx, "acme"); return u.Bytes == 9 })

	set(12)
	advance(31 * time.Second) // hard window: blocks, returns the NEW value
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if u, err := s.Usage(ctx, "acme"); err != nil || u.Bytes != 12 {
				t.Errorf("hard window: %+v %v", u, err)
			}
		}()
	}
	wg.Wait()
	if c, _ := get(); c != 3 {
		t.Fatalf("20 concurrent callers must share ONE refresh, calls=%d", c)
	}
}

// If the nodes fail, keep serving the last good snapshot (with its real age in
// as_of) for a while, do not hammer the dead nodes, then give up.
func TestStorageSourceServesStaleOnFailure(t *testing.T) {
	var mu sync.Mutex
	up, calls := true, 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		ok := up
		calls++
		mu.Unlock()
		if !ok {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"groups":{"0":{"applied":1,"tenants":{"acme":{"bytes":5,"keys":1}}}}}`))
	}))
	defer srv.Close()
	var cmu sync.Mutex
	now := time.Now()
	s := NewStorageSource([]string{srv.URL})
	s.now = func() time.Time { cmu.Lock(); defer cmu.Unlock(); return now }
	advance := func(d time.Duration) { cmu.Lock(); now = now.Add(d); cmu.Unlock() }
	ctx := context.Background()

	first, err := s.Usage(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	up = false
	mu.Unlock()

	advance(40 * time.Second) // past the hard TTL, nodes down
	u, err := s.Usage(ctx, "acme")
	if err != nil || u.Bytes != 5 || !u.AsOf.Equal(first.AsOf) {
		t.Fatalf("expected stale snapshot with original as_of, got %+v %v", u, err)
	}
	mu.Lock()
	c := calls
	mu.Unlock()
	for i := 0; i < 5; i++ {
		_, _ = s.Usage(ctx, "acme")
	}
	mu.Lock()
	if calls != c {
		t.Fatalf("retry gap violated: %d extra calls", calls-c)
	}
	mu.Unlock()

	advance(6 * time.Minute) // older than the stale limit
	advance(0)
	if _, err := s.Usage(ctx, "acme"); err != ErrStorageUnavailable {
		t.Fatalf("beyond the stale limit want ErrStorageUnavailable, got %v", err)
	}
}

func TestStorageSourceUnavailable(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()
	if _, err := NewStorageSource([]string{dead.URL}).Usage(context.Background(), "acme"); err != ErrStorageUnavailable {
		t.Fatalf("want ErrStorageUnavailable, got %v", err)
	}
	if _, err := NewStorageSource(nil).Usage(context.Background(), "acme"); err != ErrStorageUnavailable {
		t.Fatalf("unconfigured: %v", err)
	}
}

func TestControlTenantStorageEndpoint(t *testing.T) {
	e := newCtl(t, func(c *ControlConfig) {
		c.Storage = func(_ context.Context, tenant string) (StorageUsage, error) {
			return StorageUsage{Bytes: 3 << 30, Keys: 42, Groups: 3}, nil
		}
	})
	a := e.signup("a@x.io", "tenant-a")
	r := e.do("GET", "/api/v1/tenant/storage", nil, withCookie(a.cookie))
	if r.Code != 200 {
		t.Fatalf("storage: %d %s", r.Code, r.Body.String())
	}
	st := r.json()["storage"].(map[string]any)
	if st["bytes"] != float64(3<<30) || st["keys"] != float64(42) || r.json()["tenant_id"] != "tenant-a" {
		t.Fatalf("body: %s", r.Body.String())
	}
	if r := e.do("GET", "/api/v1/tenant/storage", nil); r.Code != 401 {
		t.Fatalf("unauthenticated: %d", r.Code)
	}

	// Not configured / failing source => 503 with a stable code, never a fake zero.
	e2 := newCtl(t, nil)
	a2 := e2.signup("b@x.io", "tenant-b")
	if r := e2.do("GET", "/api/v1/tenant/storage", nil, withCookie(a2.cookie)); r.Code != 503 || r.code() != "storage_not_configured" {
		t.Fatalf("not configured: %d %s", r.Code, r.Body.String())
	}
	e3 := newCtl(t, func(c *ControlConfig) {
		c.Storage = func(context.Context, string) (StorageUsage, error) { return StorageUsage{}, ErrStorageUnavailable }
	})
	a3 := e3.signup("c@x.io", "tenant-c")
	if r := e3.do("GET", "/api/v1/tenant/storage", nil, withCookie(a3.cookie)); r.Code != 503 || r.code() != "storage_unavailable" {
		t.Fatalf("source down: %d %s", r.Code, r.Body.String())
	}
}
