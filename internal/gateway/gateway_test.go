package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ------------------------------------------------------------------ fake KV

type fakeKV struct {
	mu   sync.Mutex
	m    map[string][]byte
	keys []string // every key ever written, in order
}

func newFakeKV() *fakeKV { return &fakeKV{m: map[string][]byte{}} }

func (f *fakeKV) Get(_ context.Context, k string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.m[k]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return append([]byte(nil), v...), nil
}

func (f *fakeKV) Put(_ context.Context, k string, v []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[k] = append([]byte{}, v...)
	f.keys = append(f.keys, k)
	return nil
}

func (f *fakeKV) Delete(_ context.Context, k string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.m, k)
	return nil
}

func (f *fakeKV) CAS(_ context.Context, k string, expected, nv []byte) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cur, ok := f.m[k]
	if expected == nil { // must not exist
		if ok {
			return false, nil
		}
	} else if !ok || !bytes.Equal(cur, expected) {
		return false, nil
	}
	f.m[k] = append([]byte{}, nv...)
	f.keys = append(f.keys, k)
	return true, nil
}

func (f *fakeKV) Incr(_ context.Context, k string, d int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var cur int64
	if v, ok := f.m[k]; ok {
		n, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return 0, ErrNotInteger
		}
		cur = n
	}
	cur += d
	f.m[k] = []byte(strconv.FormatInt(cur, 10))
	f.keys = append(f.keys, k)
	return cur, nil
}

func (f *fakeKV) Close() error { return nil }

func (f *fakeKV) snapshotKeys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.keys...)
}

// ------------------------------------------------------------------ helpers

type env struct {
	gw    *Gateway
	kv    *fakeKV
	store *MemStore
	clock *time.Time
}

func newEnv(t *testing.T, cfg Config) *env {
	t.Helper()
	e := &env{kv: newFakeKV(), store: NewMemStore()}
	start := time.Unix(1_700_000_000, 0)
	e.clock = &start
	e.gw = New(cfg, e.store, e.kv)
	e.gw.now = func() time.Time { return *e.clock }
	return e
}

func (e *env) tenantKey(t *testing.T, tenant string) string {
	t.Helper()
	ctx := context.Background()
	if _, err := e.store.GetTenant(ctx, tenant); err != nil {
		if _, err := CreateTenant(ctx, e.store, tenant, tenant, *e.clock); err != nil {
			t.Fatal(err)
		}
	}
	key, _, err := CreateAPIKey(ctx, e.store, tenant, "test", 0, *e.clock)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func (e *env) do(method, path, key, body string) *httptest.ResponseRecorder {
	var rd *strings.Reader
	if body != "" {
		rd = strings.NewReader(body)
	} else {
		rd = strings.NewReader("")
	}
	req := httptest.NewRequest(method, path, rd)
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	rec := httptest.NewRecorder()
	e.gw.ServeHTTP(rec, req)
	return rec
}

func errCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var b errBody
	if err := json.Unmarshal(rec.Body.Bytes(), &b); err != nil {
		t.Fatalf("error body is not JSON: %q", rec.Body.String())
	}
	return b.Error.Code
}

func jsonField(t *testing.T, rec *httptest.ResponseRecorder, field string) any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("bad JSON %q: %v", rec.Body.String(), err)
	}
	return m[field]
}

// -------------------------------------------------------------------- tests

func TestHealthzNeedsNoAuth(t *testing.T) {
	e := newEnv(t, Config{})
	if rec := e.do("GET", "/healthz", "", ""); rec.Code != 200 {
		t.Fatalf("healthz: %d", rec.Code)
	}
}

func TestAuthFailures(t *testing.T) {
	e := newEnv(t, Config{})
	good := e.tenantKey(t, "alpha")
	unknown := KeyPrefix + strings.Repeat("0", keyRandHex)

	cases := []struct {
		name, header string
	}{
		{"missing", ""},
		{"basic scheme", "Basic abc"},
		{"empty bearer", "Bearer "},
		{"junk", "Bearer junk"},
		{"wrong prefix, right length", "Bearer " + strings.Repeat("x", keyLen)},
		{"unknown but well-formed", "Bearer " + unknown},
		{"truncated real key", "Bearer " + good[:len(good)-1]},
	}
	for _, c := range cases {
		req := httptest.NewRequest("GET", "/kv/x", nil)
		if c.header != "" {
			req.Header.Set("Authorization", c.header)
		}
		rec := httptest.NewRecorder()
		e.gw.ServeHTTP(rec, req)
		if rec.Code != 401 {
			t.Errorf("%s: want 401, got %d", c.name, rec.Code)
			continue
		}
		if errCode(t, rec) != "unauthorized" {
			t.Errorf("%s: error body %q", c.name, rec.Body.String())
		}
	}
	if rec := e.do("GET", "/kv/x", good, ""); rec.Code != 404 { // authed, key absent
		t.Fatalf("valid key rejected: %d %s", rec.Code, rec.Body.String())
	}
}

func TestRevokedKeyRejectedImmediately(t *testing.T) {
	e := newEnv(t, Config{})
	key := e.tenantKey(t, "alpha")
	if rec := e.do("PUT", "/kv/k", key, `{"value":"v"}`); rec.Code != 200 {
		t.Fatalf("put: %d", rec.Code)
	}
	keys, _ := e.store.ListKeys(context.Background(), "alpha")
	if err := e.store.RevokeKey(context.Background(), "alpha", keys[0].Prefix, *e.clock); err != nil {
		t.Fatal(err)
	}
	if rec := e.do("GET", "/kv/k", key, ""); rec.Code != 401 {
		t.Fatalf("revoked key still works: %d", rec.Code)
	}
}

func TestExpiredKeyRejected(t *testing.T) {
	e := newEnv(t, Config{})
	ctx := context.Background()
	if _, err := CreateTenant(ctx, e.store, "alpha", "A", *e.clock); err != nil {
		t.Fatal(err)
	}
	key, _, err := CreateAPIKey(ctx, e.store, "alpha", "short", time.Hour, *e.clock)
	if err != nil {
		t.Fatal(err)
	}
	if rec := e.do("GET", "/kv/x", key, ""); rec.Code != 404 {
		t.Fatalf("fresh key: %d", rec.Code)
	}
	*e.clock = e.clock.Add(time.Hour + time.Second)
	if rec := e.do("GET", "/kv/x", key, ""); rec.Code != 401 {
		t.Fatalf("expired key accepted: %d", rec.Code)
	}
	if _, _, err := CreateAPIKey(ctx, e.store, "alpha", "neg", -time.Second, *e.clock); err == nil {
		t.Fatal("negative ttl must be rejected")
	}
}

// The security property: tenants share a key NAME space only in appearance.
func TestCrossTenantIsolation(t *testing.T) {
	e := newEnv(t, Config{})
	alpha := e.tenantKey(t, "alpha")
	beta := e.tenantKey(t, "beta")
	// "a" + "b:c" vs "a-b" + "c": the pair that collided under the old encoding.
	a := e.tenantKey(t, "a")
	ab := e.tenantKey(t, "a-b")

	// Same key name, different tenants, different values.
	e.do("PUT", "/kv/stock", alpha, `{"value":"alpha-10"}`)
	e.do("PUT", "/kv/stock", beta, `{"value":"beta-99"}`)
	if v := jsonField(t, e.do("GET", "/kv/stock", alpha, ""), "value"); v != "alpha-10" {
		t.Fatalf("alpha sees %v", v)
	}
	if v := jsonField(t, e.do("GET", "/kv/stock", beta, ""), "value"); v != "beta-99" {
		t.Fatalf("beta sees %v", v)
	}

	// Alpha tries crafted keys aimed at beta's data.
	crafted := []string{
		"tenant:beta:stock", "t:beta:stock", "../beta/stock", "beta:stock",
		"/t:beta:stock", "%2e%2e/beta/stock", "stock%00", ":beta:stock",
	}
	for _, k := range crafted {
		rec := e.do("GET", "/kv/"+k, alpha, "")
		if rec.Code == 200 {
			t.Fatalf("crafted key %q returned data: %s", k, rec.Body.String())
		}
		e.do("PUT", "/kv/"+k, alpha, `{"value":"pwned"}`)
		e.do("DELETE", "/kv/"+k, alpha, "")
	}
	if v := jsonField(t, e.do("GET", "/kv/stock", beta, ""), "value"); v != "beta-99" {
		t.Fatalf("beta's data was modified by alpha: %v", v)
	}

	// The old collision pair stays separate.
	e.do("PUT", "/kv/b:c", a, `{"value":"from-a"}`)
	e.do("PUT", "/kv/c", ab, `{"value":"from-a-b"}`)
	if v := jsonField(t, e.do("GET", "/kv/b:c", a, ""), "value"); v != "from-a" {
		t.Fatalf("tenant a sees %v", v)
	}
	if v := jsonField(t, e.do("GET", "/kv/c", ab, ""), "value"); v != "from-a-b" {
		t.Fatalf("tenant a-b sees %v", v)
	}

	// Every key alpha ever caused to be written is under alpha's prefix.
	for _, k := range e.kv.snapshotKeys() {
		tn, _, err := splitNamespaced(k)
		if err != nil {
			t.Fatalf("stored key %q is not namespaced", k)
		}
		if strings.HasPrefix(k, "t:alpha:") && tn != "alpha" {
			t.Fatalf("key %q escaped its tenant", k)
		}
	}
}

func TestInvalidKeysRejected(t *testing.T) {
	e := newEnv(t, Config{})
	key := e.tenantKey(t, "alpha")
	if rec := e.do("GET", "/kv/", key, ""); rec.Code != 400 || errCode(t, rec) != "invalid_key" {
		t.Fatalf("empty key: %d %s", rec.Code, rec.Body.String())
	}
	long := strings.Repeat("k", maxKeyLen+1)
	if rec := e.do("GET", "/kv/"+long, key, ""); rec.Code != 400 {
		t.Fatalf("long key: %d", rec.Code)
	}
	if rec := e.do("POST", "/kv", key, `{"op":"set","key":"","value":"v"}`); rec.Code != 400 {
		t.Fatalf("empty key in POST: %d", rec.Code)
	}
}

func TestRateLimitReturns429WithRetryAfter(t *testing.T) {
	e := newEnv(t, Config{RateLimitRPS: 1, RateLimitBurst: 3})
	alpha := e.tenantKey(t, "alpha")
	beta := e.tenantKey(t, "beta")

	for i := 0; i < 3; i++ {
		if rec := e.do("GET", "/kv/x", alpha, ""); rec.Code == 429 {
			t.Fatalf("request %d limited too early", i)
		}
	}
	rec := e.do("GET", "/kv/x", alpha, "")
	if rec.Code != 429 {
		t.Fatalf("burst exhausted: want 429, got %d", rec.Code)
	}
	if errCode(t, rec) != "rate_limited" {
		t.Fatalf("body: %s", rec.Body.String())
	}
	if ra := rec.Header().Get("Retry-After"); ra != "1" {
		t.Fatalf("Retry-After = %q, want 1", ra)
	}
	// Another tenant is unaffected.
	if rec := e.do("GET", "/kv/x", beta, ""); rec.Code == 429 {
		t.Fatal("beta throttled because of alpha")
	}
	// After a second, one token has refilled.
	*e.clock = e.clock.Add(1100 * time.Millisecond)
	if rec := e.do("GET", "/kv/x", alpha, ""); rec.Code == 429 {
		t.Fatal("no refill after 1s")
	}
}

func TestPerTenantQuotaOverrideTakesEffect(t *testing.T) {
	e := newEnv(t, Config{RateLimitRPS: 1000, RateLimitBurst: 1000})
	key := e.tenantKey(t, "alpha")
	e.do("GET", "/kv/x", key, "") // creates the default limiter
	if err := e.store.SetTenantQuota(context.Background(), "alpha", 1, 2); err != nil {
		t.Fatal(err)
	}
	limited := false
	for i := 0; i < 5; i++ {
		if e.do("GET", "/kv/x", key, "").Code == 429 {
			limited = true
		}
	}
	if !limited {
		t.Fatal("new quota (burst 2) was not applied")
	}
}

func TestBodyAndValueLimits(t *testing.T) {
	e := newEnv(t, Config{MaxBodyBytes: 256, MaxValueBytes: 64})
	key := e.tenantKey(t, "alpha")

	big := `{"value":"` + strings.Repeat("v", 1000) + `"}`
	if rec := e.do("PUT", "/kv/k", key, big); rec.Code != 413 || errCode(t, rec) != "body_too_large" {
		t.Fatalf("big body: %d %s", rec.Code, rec.Body.String())
	}
	mid := `{"value":"` + strings.Repeat("v", 100) + `"}` // under body limit, over value limit
	if rec := e.do("PUT", "/kv/k", key, mid); rec.Code != 413 || errCode(t, rec) != "value_too_large" {
		t.Fatalf("big value: %d %s", rec.Code, rec.Body.String())
	}
	if rec := e.do("PUT", "/kv/k", key, `not json`); rec.Code != 400 {
		t.Fatalf("bad json: %d", rec.Code)
	}
	if rec := e.do("PUT", "/kv/k", key, `{"value":"x","extra":1}`); rec.Code != 400 {
		t.Fatalf("unknown field accepted: %d", rec.Code)
	}
	if rec := e.do("PUT", "/kv/k", key, `{}`); rec.Code != 400 || errCode(t, rec) != "value_required" {
		t.Fatalf("missing value: %d %s", rec.Code, rec.Body.String())
	}
}

func TestOperations(t *testing.T) {
	e := newEnv(t, Config{})
	key := e.tenantKey(t, "alpha")

	if rec := e.do("PUT", "/kv/name", key, `{"value":"ravi"}`); rec.Code != 200 {
		t.Fatalf("put: %d", rec.Code)
	}
	if v := jsonField(t, e.do("GET", "/kv/name", key, ""), "value"); v != "ravi" {
		t.Fatalf("get: %v", v)
	}
	if rec := e.do("DELETE", "/kv/name", key, ""); rec.Code != 204 {
		t.Fatalf("delete: %d", rec.Code)
	}
	if rec := e.do("GET", "/kv/name", key, ""); rec.Code != 404 {
		t.Fatalf("get after delete: %d", rec.Code)
	}

	// CAS: omitted "expected" means create-if-absent.
	if v := jsonField(t, e.do("POST", "/kv", key, `{"op":"cas","key":"seat","value":"taken"}`), "applied"); v != true {
		t.Fatalf("create-if-absent: %v", v)
	}
	if v := jsonField(t, e.do("POST", "/kv", key, `{"op":"cas","key":"seat","value":"other"}`), "applied"); v != false {
		t.Fatalf("create-if-absent on existing key: %v", v)
	}
	if v := jsonField(t, e.do("POST", "/kv", key, `{"op":"cas","key":"seat","expected":"taken","value":"free"}`), "applied"); v != true {
		t.Fatalf("cas with matching expected: %v", v)
	}
	if v := jsonField(t, e.do("POST", "/kv", key, `{"op":"cas","key":"seat","expected":"taken","value":"x"}`), "applied"); v != false {
		t.Fatalf("cas with stale expected: %v", v)
	}
	// present-but-empty expected is NOT "absent".
	e.do("PUT", "/kv/empty", key, `{"value":""}`)
	if v := jsonField(t, e.do("POST", "/kv", key, `{"op":"cas","key":"empty","expected":"","value":"filled"}`), "applied"); v != true {
		t.Fatalf("cas expected empty string: %v", v)
	}

	// incr / decr with deltas
	if v := jsonField(t, e.do("POST", "/kv", key, `{"op":"incr","key":"n"}`), "value"); v != float64(1) {
		t.Fatalf("incr default: %v", v)
	}
	if v := jsonField(t, e.do("POST", "/kv", key, `{"op":"incr","key":"n","delta":10}`), "value"); v != float64(11) {
		t.Fatalf("incr 10: %v", v)
	}
	if v := jsonField(t, e.do("POST", "/kv", key, `{"op":"decr","key":"n","delta":4}`), "value"); v != float64(7) {
		t.Fatalf("decr 4: %v", v)
	}
	for _, bad := range []string{`{"op":"incr","key":"n","delta":0}`, `{"op":"decr","key":"n","delta":-3}`} {
		if rec := e.do("POST", "/kv", key, bad); rec.Code != 400 {
			t.Fatalf("%s: want 400, got %d", bad, rec.Code)
		}
	}
	e.do("PUT", "/kv/text", key, `{"value":"abc"}`)
	if rec := e.do("POST", "/kv", key, `{"op":"incr","key":"text"}`); rec.Code != 409 {
		t.Fatalf("incr on non-integer: %d", rec.Code)
	}
	if rec := e.do("POST", "/kv", key, `{"op":"bogus","key":"n"}`); rec.Code != 400 {
		t.Fatalf("unknown op: %d", rec.Code)
	}
	if rec := e.do("PATCH", "/kv/n", key, ""); rec.Code != 405 {
		t.Fatalf("PATCH: %d", rec.Code)
	}
	if rec := e.do("GET", "/nope", key, ""); rec.Code != 404 {
		t.Fatalf("unknown route: %d", rec.Code)
	}
}

func TestConcurrentIncrIsExact(t *testing.T) {
	e := newEnv(t, Config{RateLimitRPS: 1e6, RateLimitBurst: 1e6})
	key := e.tenantKey(t, "alpha")
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if rec := e.do("POST", "/kv", key, `{"op":"incr","key":"hits"}`); rec.Code != 200 {
				t.Errorf("incr: %d", rec.Code)
			}
		}()
	}
	wg.Wait()
	if v := jsonField(t, e.do("GET", "/kv/hits", key, ""), "value"); v != "100" {
		t.Fatalf("hits = %v, want 100", v)
	}
}

func TestRotateAndList(t *testing.T) {
	e := newEnv(t, Config{})
	ctx := context.Background()
	if _, err := CreateTenant(ctx, e.store, "alpha", "A", *e.clock); err != nil {
		t.Fatal(err)
	}
	old, oldRec, err := CreateAPIKey(ctx, e.store, "alpha", "prod", 0, *e.clock)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(old, KeyPrefix) || len(old) != keyLen {
		t.Fatalf("bad key format %q", old)
	}
	if strings.Contains(oldRec.Hash, old) || oldRec.Hash == old {
		t.Fatal("hash must not contain the plaintext key")
	}
	if !strings.HasPrefix(old, oldRec.Prefix) {
		t.Fatal("prefix must be the start of the key")
	}
	newKey, _, err := RotateAPIKey(ctx, e.store, "alpha", oldRec.Prefix, 0, *e.clock)
	if err != nil {
		t.Fatal(err)
	}
	if rec := e.do("GET", "/kv/x", old, ""); rec.Code != 401 {
		t.Fatalf("old key still valid after rotation: %d", rec.Code)
	}
	if rec := e.do("GET", "/kv/x", newKey, ""); rec.Code != 404 {
		t.Fatalf("new key rejected: %d", rec.Code)
	}
	if _, _, err := RotateAPIKey(ctx, e.store, "alpha", "dkv_live_unknown0", 0, *e.clock); err == nil {
		t.Fatal("rotating an unknown key must fail")
	}
	if _, err := CreateTenant(ctx, e.store, "Bad:ID", "x", *e.clock); err == nil {
		t.Fatal("invalid tenant id accepted")
	}
}

var _ = http.StatusOK
