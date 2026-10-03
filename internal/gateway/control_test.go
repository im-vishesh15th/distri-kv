package gateway

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// ------------------------------------------------------------ password

func TestPBKDF2KnownVectors(t *testing.T) {
	// RFC 7914 section 11: PBKDF2-HMAC-SHA256.
	got := hex.EncodeToString(pbkdf2SHA256([]byte("passwd"), []byte("salt"), 1, 64))
	want := "55ac046e56e3089fec1691c22544b605f94185216dde0465e68b9d57c20dacbc49ca9cccf179b645991664b39d77ef317c71b845b1e30bd509112041d3a19783"
	if got != want {
		t.Fatalf("c=1: got %s", got)
	}
	got = hex.EncodeToString(pbkdf2SHA256([]byte("Password"), []byte("NaCl"), 80000, 64))
	want = "4ddcd8f60b98be21830cee5ef22701f9641a4418d04c0414aeff08876b34ab56a1d425a1225833549adb841b51c9b3176a272bdebba1d078478f62b397f33c8d"
	if got != want {
		t.Fatalf("c=80000: got %s", got)
	}
}

func TestPasswordHashVerify(t *testing.T) {
	h, err := HashPassword("correct horse battery", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(h, "correct horse") {
		t.Fatal("hash contains the password")
	}
	if ok, err := VerifyPassword("correct horse battery", h); !ok || err != nil {
		t.Fatalf("verify good: %v %v", ok, err)
	}
	if ok, _ := VerifyPassword("wrong horse battery", h); ok {
		t.Fatal("wrong password verified")
	}
	h2, _ := HashPassword("correct horse battery", 1000)
	if h == h2 {
		t.Fatal("two hashes of the same password are identical (no salt?)")
	}
	for _, bad := range []string{"", "x", "pbkdf2-sha256$0$aa$bb", "pbkdf2-sha256$99999999$aa$bb", "md5$1$aa$bb", "pbkdf2-sha256$1$!!$!!"} {
		if ok, err := VerifyPassword("p", bad); ok || err == nil {
			t.Errorf("malformed %q: ok=%v err=%v", bad, ok, err)
		}
	}
	if ValidatePassword("short") == nil || ValidatePassword(strings.Repeat("x", 200)) == nil || ValidatePassword("long enough pw") != nil {
		t.Fatal("password policy wrong")
	}
}

// -------------------------------------------------------------- harness

type ctlEnv struct {
	t     *testing.T
	store *MemStore
	api   *ControlAPI
	h     http.Handler
	clock *time.Time
	gw    *Gateway
}

func newCtl(t *testing.T, mod func(*ControlConfig)) *ctlEnv {
	t.Helper()
	e := &ctlEnv{t: t, store: NewMemStore()}
	start := time.Now()
	e.clock = &start
	cfg := ControlConfig{
		AllowSignup:        true,
		PasswordIterations: 1000,
		AllowedOrigins:     []string{"http://app.example"},
		now:                func() time.Time { return *e.clock },
	}
	e.gw = New(Config{RateLimitRPS: 1e6, RateLimitBurst: 1e6}, e.store, newFakeKV())
	cfg.Usage = e.gw.Metrics().Usage
	cfg.UsageSeries = e.gw.Metrics().Series
	if mod != nil {
		mod(&cfg)
	}
	api, err := NewControlAPI(cfg, e.store)
	if err != nil {
		t.Fatal(err)
	}
	e.api, e.h = api, api.Handler()
	return e
}

type resp struct {
	*httptest.ResponseRecorder
}

func (r resp) json() map[string]any {
	var m map[string]any
	if err := json.Unmarshal(r.Body.Bytes(), &m); err != nil {
		panic("bad json: " + r.Body.String())
	}
	return m
}

func (r resp) code() string {
	e, _ := r.json()["error"].(map[string]any)
	c, _ := e["code"].(string)
	return c
}

func (r resp) cookie() *http.Cookie {
	for _, c := range r.Result().Cookies() {
		if c.Name == sessionCookie {
			return c
		}
	}
	return nil
}

type opt func(*http.Request)

func withCookie(c *http.Cookie) opt {
	return func(r *http.Request) { r.AddCookie(&http.Cookie{Name: c.Name, Value: c.Value}) }
}
func withHeader(k, v string) opt { return func(r *http.Request) { r.Header.Set(k, v) } }
func withIP(ip string) opt       { return func(r *http.Request) { r.RemoteAddr = ip + ":1234" } }

func (e *ctlEnv) do(method, path string, body any, opts ...opt) resp {
	e.t.Helper()
	var rd *bytes.Reader
	if body == nil {
		rd = bytes.NewReader(nil)
	} else if s, ok := body.(string); ok {
		rd = bytes.NewReader([]byte(s))
	} else {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, rd)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.RemoteAddr = "10.0.0.1:1234"
	for _, o := range opts {
		o(req)
	}
	rec := httptest.NewRecorder()
	e.h.ServeHTTP(rec, req)
	return resp{rec}
}

type acct struct {
	cookie *http.Cookie
	key    string
	tenant string
}

func (e *ctlEnv) signup(email, tenant string) acct {
	e.t.Helper()
	body := map[string]string{"email": email, "password": "a-good-password", "tenant_id": tenant}
	r := e.do("POST", "/api/v1/auth/signup", body, withIP("10.1."+tenant[len(tenant)-1:]+".1"))
	if r.Code != 201 {
		e.t.Fatalf("signup %s: %d %s", email, r.Code, r.Body.String())
	}
	ak := r.json()["api_key"].(map[string]any)
	return acct{cookie: r.cookie(), key: ak["key"].(string), tenant: tenant}
}

func (e *ctlEnv) makeAdmin(email string) *http.Cookie {
	e.t.Helper()
	h, _ := HashPassword("admin-password-1", 1000)
	if err := e.store.CreateAccount(context.Background(), User{ID: "adm1", Email: email, PasswordHash: h, IsAdmin: true, CreatedAt: *e.clock}, nil); err != nil {
		e.t.Fatal(err)
	}
	r := e.do("POST", "/api/v1/auth/login", map[string]string{"email": email, "password": "admin-password-1"}, withIP("10.9.9.9"))
	if r.Code != 200 {
		e.t.Fatalf("admin login: %d %s", r.Code, r.Body.String())
	}
	return r.cookie()
}

// gwStatus calls the DATA-PLANE gateway with an API key.
func (e *ctlEnv) gwStatus(key string) int {
	req := httptest.NewRequest("GET", "/kv/probe", nil)
	req.Header.Set("Authorization", "Bearer "+key)
	rec := httptest.NewRecorder()
	e.gw.ServeHTTP(rec, req)
	return rec.Code
}

// ---------------------------------------------------------------- tests

func TestSignupCreatesWorkingAccount(t *testing.T) {
	e := newCtl(t, nil)
	r := e.do("POST", "/api/v1/auth/signup", map[string]string{
		"email": "  Ann@Example.COM ", "password": "a-good-password", "tenant_id": "ann-co", "name": "Ann Co",
	})
	if r.Code != 201 {
		t.Fatalf("signup: %d %s", r.Code, r.Body.String())
	}
	j := r.json()
	if u := j["user"].(map[string]any); u["email"] != "ann@example.com" || u["tenant_id"] != "ann-co" || u["is_admin"] != false {
		t.Fatalf("user: %v", u)
	}
	if tn := j["tenant"].(map[string]any); tn["name"] != "Ann Co" || tn["effective_rps"] != float64(1000) {
		t.Fatalf("tenant: %v", tn)
	}
	key := j["api_key"].(map[string]any)["key"].(string)
	if !strings.HasPrefix(key, KeyPrefix) || len(key) != keyLen {
		t.Fatalf("api key shape: %q", key)
	}
	// The key issued by the control plane works on the data plane.
	if c := e.gwStatus(key); c != 404 { // 404 = authenticated, key absent
		t.Fatalf("data plane rejected the signup key: %d", c)
	}
	ck := r.cookie()
	if ck == nil || !ck.HttpOnly || ck.SameSite != http.SameSiteStrictMode || ck.Value == "" {
		t.Fatalf("session cookie: %+v", ck)
	}
	// Session token is stored hashed, never raw.
	if _, err := e.store.GetSession(context.Background(), ck.Value); err == nil {
		t.Fatal("raw session token found in the store")
	}
	if me := e.do("GET", "/api/v1/me", nil, withCookie(ck)); me.Code != 200 {
		t.Fatalf("me after signup: %d", me.Code)
	}
}

func TestSignupValidation(t *testing.T) {
	e := newCtl(t, nil)
	e.signup("a@x.io", "tenant-a")
	cases := []struct {
		name string
		body map[string]string
		want int
		code string
	}{
		{"bad email", map[string]string{"email": "nope", "password": "a-good-password"}, 400, "invalid_email"},
		{"display-name email", map[string]string{"email": "Bob <b@x.io>", "password": "a-good-password"}, 400, "invalid_email"},
		{"weak password", map[string]string{"email": "b@x.io", "password": "short"}, 400, "weak_password"},
		{"bad tenant id", map[string]string{"email": "b@x.io", "password": "a-good-password", "tenant_id": "Bad:ID"}, 400, "invalid_tenant_id"},
		{"duplicate email", map[string]string{"email": "A@x.io", "password": "a-good-password", "tenant_id": "other"}, 409, "email_taken"},
		{"duplicate tenant", map[string]string{"email": "c@x.io", "password": "a-good-password", "tenant_id": "tenant-a"}, 409, "tenant_taken"},
	}
	for i, c := range cases {
		r := e.do("POST", "/api/v1/auth/signup", c.body, withIP("10.2.0."+string(rune('1'+i))))
		if r.Code != c.want || r.code() != c.code {
			t.Errorf("%s: got %d %q (%s)", c.name, r.Code, r.code(), r.Body.String())
		}
	}
	// Failed signups must not leave a half-created tenant behind.
	if _, err := e.store.GetTenant(context.Background(), "other"); err == nil {
		t.Fatal("tenant created by a failed signup")
	}
	// Auto-generated tenant id.
	r := e.do("POST", "/api/v1/auth/signup", map[string]string{"email": "d@x.io", "password": "a-good-password"}, withIP("10.3.0.1"))
	if r.Code != 201 || !strings.HasPrefix(r.json()["tenant"].(map[string]any)["id"].(string), "t-") {
		t.Fatalf("auto tenant: %d %s", r.Code, r.Body.String())
	}
}

func TestSignupDisabled(t *testing.T) {
	e := newCtl(t, func(c *ControlConfig) { c.AllowSignup = false })
	r := e.do("POST", "/api/v1/auth/signup", map[string]string{"email": "a@x.io", "password": "a-good-password"})
	if r.Code != 403 || r.code() != "signup_disabled" {
		t.Fatalf("%d %s", r.Code, r.Body.String())
	}
}

func TestLoginLogoutAndUniformErrors(t *testing.T) {
	e := newCtl(t, nil)
	e.signup("a@x.io", "tenant-a")

	bad1 := e.do("POST", "/api/v1/auth/login", map[string]string{"email": "a@x.io", "password": "wrong-password!"}, withIP("10.4.0.1"))
	bad2 := e.do("POST", "/api/v1/auth/login", map[string]string{"email": "ghost@x.io", "password": "wrong-password!"}, withIP("10.4.0.2"))
	if bad1.Code != 401 || bad2.Code != 401 || bad1.Body.String() != bad2.Body.String() {
		t.Fatalf("login errors differ (user enumeration): %q vs %q", bad1.Body.String(), bad2.Body.String())
	}

	ok := e.do("POST", "/api/v1/auth/login", map[string]string{"email": "A@X.io", "password": "a-good-password"}, withIP("10.4.0.3"))
	if ok.Code != 200 || ok.cookie() == nil {
		t.Fatalf("login: %d %s", ok.Code, ok.Body.String())
	}
	ck := ok.cookie()
	if r := e.do("GET", "/api/v1/me", nil, withCookie(ck)); r.Code != 200 {
		t.Fatalf("me: %d", r.Code)
	}
	if r := e.do("POST", "/api/v1/auth/logout", nil, withCookie(ck)); r.Code != 204 {
		t.Fatalf("logout: %d", r.Code)
	}
	if r := e.do("GET", "/api/v1/me", nil, withCookie(ck)); r.Code != 401 {
		t.Fatalf("session usable after logout: %d", r.Code)
	}
	if r := e.do("GET", "/api/v1/me", nil); r.Code != 401 || r.code() != "unauthenticated" {
		t.Fatalf("no cookie: %d", r.Code)
	}
}

func TestSessionExpires(t *testing.T) {
	e := newCtl(t, func(c *ControlConfig) { c.SessionTTL = time.Hour })
	a := e.signup("a@x.io", "tenant-a")
	if r := e.do("GET", "/api/v1/me", nil, withCookie(a.cookie)); r.Code != 200 {
		t.Fatalf("fresh session: %d", r.Code)
	}
	*e.clock = e.clock.Add(61 * time.Minute)
	if r := e.do("GET", "/api/v1/me", nil, withCookie(a.cookie)); r.Code != 401 {
		t.Fatalf("expired session accepted: %d", r.Code)
	}
}

func TestLoginRateLimits(t *testing.T) {
	e := newCtl(t, func(c *ControlConfig) { c.LoginAttemptsPerMinute = 3; c.AuthAttemptsPerMinute = 100 })
	e.signup("a@x.io", "tenant-a")
	body := map[string]string{"email": "a@x.io", "password": "wrong-password!"}
	for i := 0; i < 3; i++ {
		if r := e.do("POST", "/api/v1/auth/login", body); r.Code != 401 {
			t.Fatalf("attempt %d: %d", i, r.Code)
		}
	}
	r := e.do("POST", "/api/v1/auth/login", body)
	if r.Code != 429 || r.Header().Get("Retry-After") == "" {
		t.Fatalf("per-email limit: %d %s", r.Code, r.Body.String())
	}
	// The correct password is also blocked while limited: no oracle.
	if r := e.do("POST", "/api/v1/auth/login", map[string]string{"email": "a@x.io", "password": "a-good-password"}); r.Code != 429 {
		t.Fatalf("limited account still checks passwords: %d", r.Code)
	}
	*e.clock = e.clock.Add(time.Minute)
	if r := e.do("POST", "/api/v1/auth/login", map[string]string{"email": "a@x.io", "password": "a-good-password"}); r.Code != 200 {
		t.Fatalf("no recovery after the window: %d", r.Code)
	}

	// Per-IP limit on signup.
	e2 := newCtl(t, func(c *ControlConfig) { c.AuthAttemptsPerMinute = 2 })
	for i := 0; i < 2; i++ {
		e2.do("POST", "/api/v1/auth/signup", map[string]string{"email": "x" + string(rune('a'+i)) + "@x.io", "password": "a-good-password"})
	}
	if r := e2.do("POST", "/api/v1/auth/signup", map[string]string{"email": "z@x.io", "password": "a-good-password"}); r.Code != 429 {
		t.Fatalf("per-IP limit: %d", r.Code)
	}
	if r := e2.do("POST", "/api/v1/auth/signup", map[string]string{"email": "z@x.io", "password": "a-good-password"}, withIP("10.7.7.7")); r.Code == 429 {
		t.Fatal("another IP was limited")
	}
}

func TestChangePasswordSignsOutOtherSessions(t *testing.T) {
	// Password changes share the per-account attempt budget with login.
	e := newCtl(t, func(c *ControlConfig) { c.LoginAttemptsPerMinute = 20 })
	a := e.signup("a@x.io", "tenant-a")
	other := e.do("POST", "/api/v1/auth/login", map[string]string{"email": "a@x.io", "password": "a-good-password"}, withIP("10.5.0.1")).cookie()

	if r := e.do("PUT", "/api/v1/me/password", map[string]string{"current_password": "nope-nope-nope", "new_password": "brand-new-password"}, withCookie(a.cookie)); r.Code != 401 {
		t.Fatalf("wrong current password: %d", r.Code)
	}
	if r := e.do("PUT", "/api/v1/me/password", map[string]string{"current_password": "a-good-password", "new_password": "short"}, withCookie(a.cookie)); r.Code != 400 {
		t.Fatalf("weak new password: %d", r.Code)
	}
	if r := e.do("PUT", "/api/v1/me/password", map[string]string{"current_password": "a-good-password", "new_password": "brand-new-password"}, withCookie(a.cookie)); r.Code != 204 {
		t.Fatalf("change: %d", r.Code)
	}
	if r := e.do("GET", "/api/v1/me", nil, withCookie(a.cookie)); r.Code != 200 {
		t.Fatalf("current session killed: %d", r.Code)
	}
	if r := e.do("GET", "/api/v1/me", nil, withCookie(other)); r.Code != 401 {
		t.Fatalf("other session survived the password change: %d", r.Code)
	}
	if r := e.do("POST", "/api/v1/auth/login", map[string]string{"email": "a@x.io", "password": "a-good-password"}, withIP("10.5.0.2")); r.Code != 401 {
		t.Fatalf("old password still works: %d", r.Code)
	}
	if r := e.do("POST", "/api/v1/auth/login", map[string]string{"email": "a@x.io", "password": "brand-new-password"}, withIP("10.5.0.3")); r.Code != 200 {
		t.Fatalf("new password rejected: %d", r.Code)
	}
}

func TestKeyLifecycleReachesDataPlane(t *testing.T) {
	e := newCtl(t, nil)
	a := e.signup("a@x.io", "tenant-a")

	r := e.do("POST", "/api/v1/tenant/keys", map[string]any{"name": "prod", "ttl_hours": 24}, withCookie(a.cookie))
	if r.Code != 201 {
		t.Fatalf("create key: %d %s", r.Code, r.Body.String())
	}
	prod := r.json()["key"].(string)
	prodPrefix := r.json()["info"].(map[string]any)["prefix"].(string)
	if e.gwStatus(prod) != 404 {
		t.Fatal("new key not accepted by the data plane")
	}

	// List never leaks plaintext or hashes.
	l := e.do("GET", "/api/v1/tenant/keys", nil, withCookie(a.cookie))
	if l.Code != 200 || strings.Contains(l.Body.String(), prod) || strings.Contains(l.Body.String(), hashKey(prod)) {
		t.Fatalf("list leaks secrets: %s", l.Body.String())
	}
	if n := len(l.json()["keys"].([]any)); n != 2 {
		t.Fatalf("want 2 keys (default + prod), got %d", n)
	}

	// Rotate: new key works, old key dies immediately.
	rot := e.do("POST", "/api/v1/tenant/keys/"+prodPrefix+"/rotate", nil, withCookie(a.cookie))
	if rot.Code != 201 {
		t.Fatalf("rotate: %d %s", rot.Code, rot.Body.String())
	}
	if e.gwStatus(prod) != 401 {
		t.Fatal("rotated-out key still works")
	}
	if e.gwStatus(rot.json()["key"].(string)) != 404 {
		t.Fatal("rotated-in key rejected")
	}
	if r := e.do("POST", "/api/v1/tenant/keys/"+prodPrefix+"/rotate", nil, withCookie(a.cookie)); r.Code != 409 || r.code() != "key_not_active" {
		t.Fatalf("rotating a revoked key: %d %s", r.Code, r.Body.String())
	}

	// Revoke the signup key.
	pfx := a.key[:displayPrefix]
	if r := e.do("DELETE", "/api/v1/tenant/keys/"+pfx, nil, withCookie(a.cookie)); r.Code != 204 {
		t.Fatalf("revoke: %d", r.Code)
	}
	if e.gwStatus(a.key) != 401 {
		t.Fatal("revoked key still works on the data plane")
	}
	if r := e.do("DELETE", "/api/v1/tenant/keys/dkv_live_00000000", nil, withCookie(a.cookie)); r.Code != 404 {
		t.Fatalf("unknown prefix: %d", r.Code)
	}
	if r := e.do("DELETE", "/api/v1/tenant/keys/not-a-prefix", nil, withCookie(a.cookie)); r.Code != 400 {
		t.Fatalf("bad prefix: %d", r.Code)
	}
}

func TestKeyInputValidationAndLimit(t *testing.T) {
	e := newCtl(t, func(c *ControlConfig) { c.MaxKeysPerTenant = 3 })
	a := e.signup("a@x.io", "tenant-a") // 1 key already
	for _, body := range []map[string]any{
		{"name": ""}, {"name": strings.Repeat("x", 65)}, {"name": "ok", "ttl_hours": -1}, {"name": "ok", "ttl_hours": 999999},
	} {
		if r := e.do("POST", "/api/v1/tenant/keys", body, withCookie(a.cookie)); r.Code != 400 {
			t.Errorf("%v: %d %s", body, r.Code, r.Body.String())
		}
	}
	for i := 0; i < 2; i++ {
		if r := e.do("POST", "/api/v1/tenant/keys", map[string]any{"name": "k"}, withCookie(a.cookie)); r.Code != 201 {
			t.Fatalf("key %d: %d", i, r.Code)
		}
	}
	if r := e.do("POST", "/api/v1/tenant/keys", map[string]any{"name": "k"}, withCookie(a.cookie)); r.Code != 409 || r.code() != "key_limit_reached" {
		t.Fatalf("limit: %d %s", r.Code, r.Body.String())
	}
	// Revoked keys do not count against the limit.
	e.do("DELETE", "/api/v1/tenant/keys/"+a.key[:displayPrefix], nil, withCookie(a.cookie))
	if r := e.do("POST", "/api/v1/tenant/keys", map[string]any{"name": "k"}, withCookie(a.cookie)); r.Code != 201 {
		t.Fatalf("after revoke: %d", r.Code)
	}
}

func TestTenantIsolationInControlPlane(t *testing.T) {
	e := newCtl(t, nil)
	a := e.signup("a@x.io", "tenant-a")
	b := e.signup("b@x.io", "tenant-b")

	// B cannot revoke or rotate A's key by guessing its prefix.
	pfx := a.key[:displayPrefix]
	if r := e.do("DELETE", "/api/v1/tenant/keys/"+pfx, nil, withCookie(b.cookie)); r.Code != 404 {
		t.Fatalf("cross-tenant revoke: %d", r.Code)
	}
	if r := e.do("POST", "/api/v1/tenant/keys/"+pfx+"/rotate", nil, withCookie(b.cookie)); r.Code != 404 {
		t.Fatalf("cross-tenant rotate: %d", r.Code)
	}
	if e.gwStatus(a.key) != 404 {
		t.Fatal("A's key was affected by B")
	}
	// B's listing never shows A's keys; B's usage is B's.
	l := e.do("GET", "/api/v1/tenant/keys", nil, withCookie(b.cookie))
	if strings.Contains(l.Body.String(), pfx) {
		t.Fatal("B can see A's key prefix")
	}
	// Customers cannot use admin routes.
	for _, p := range []string{"/api/v1/admin/tenants", "/api/v1/admin/tenants/tenant-a", "/api/v1/admin/tenants/tenant-a/keys"} {
		if r := e.do("GET", p, nil, withCookie(b.cookie)); r.Code != 403 {
			t.Errorf("%s as customer: %d", p, r.Code)
		}
	}
	if r := e.do("PUT", "/api/v1/admin/tenants/tenant-a/quota", map[string]any{"rps": 1, "burst": 1}, withCookie(b.cookie)); r.Code != 403 {
		t.Fatalf("customer set quota: %d", r.Code)
	}
}

func TestUsageComesFromDataPlane(t *testing.T) {
	e := newCtl(t, nil)
	a := e.signup("a@x.io", "tenant-a")
	for i := 0; i < 3; i++ {
		e.gwStatus(a.key)
	}
	r := e.do("GET", "/api/v1/tenant/usage", nil, withCookie(a.cookie))
	if r.Code != 200 {
		t.Fatalf("usage: %d %s", r.Code, r.Body.String())
	}
	u := r.json()["usage"].(map[string]any)
	if u["requests"] != float64(3) {
		t.Fatalf("requests = %v, want 3", u["requests"])
	}
}

func TestUsageSeriesEndpoint(t *testing.T) {
	e := newCtl(t, nil)
	a := e.signup("a@x.io", "tenant-a")
	b := e.signup("b@x.io", "tenant-b")
	for i := 0; i < 3; i++ {
		e.gwStatus(a.key)
	}
	r := e.do("GET", "/api/v1/tenant/usage/series?range=24h", nil, withCookie(a.cookie))
	if r.Code != 200 {
		t.Fatalf("series: %d %s", r.Code, r.Body.String())
	}
	ser := r.json()["series"].(map[string]any)
	pts := ser["points"].([]any)
	if len(pts) != 24 {
		t.Fatalf("points = %d, want 24", len(pts))
	}
	var sum float64
	for _, p := range pts {
		sum += p.(map[string]any)["requests"].(float64)
	}
	if sum != 3 {
		t.Fatalf("series sum = %v, want 3", sum)
	}
	rb := e.do("GET", "/api/v1/tenant/usage/series?range=24h", nil, withCookie(b.cookie))
	var bsum float64
	for _, p := range rb.json()["series"].(map[string]any)["points"].([]any) {
		bsum += p.(map[string]any)["requests"].(float64)
	}
	if bsum != 0 {
		t.Fatalf("tenant-b saw %v of tenant-a traffic", bsum)
	}
	if bad := e.do("GET", "/api/v1/tenant/usage/series?range=year", nil, withCookie(a.cookie)); bad.Code != 400 {
		t.Fatalf("bad range: %d", bad.Code)
	}
}

func TestAdminTenantManagement(t *testing.T) {
	e := newCtl(t, nil)
	a := e.signup("a@x.io", "tenant-a")
	e.signup("b@x.io", "tenant-b")
	e.signup("c@x.io", "tenant-c")
	adm := e.makeAdmin("root@x.io")

	l := e.do("GET", "/api/v1/admin/tenants?limit=2", nil, withCookie(adm))
	if l.Code != 200 || len(l.json()["tenants"].([]any)) != 2 || l.json()["next_after"] != "tenant-b" {
		t.Fatalf("page 1: %d %s", l.Code, l.Body.String())
	}
	l2 := e.do("GET", "/api/v1/admin/tenants?limit=2&after=tenant-b", nil, withCookie(adm))
	if ts := l2.json()["tenants"].([]any); len(ts) != 1 || ts[0].(map[string]any)["id"] != "tenant-c" || l2.json()["next_after"] != "" {
		t.Fatalf("page 2: %s", l2.Body.String())
	}
	if r := e.do("GET", "/api/v1/admin/tenants?limit=0", nil, withCookie(adm)); r.Code != 400 {
		t.Fatalf("bad limit: %d", r.Code)
	}
	if r := e.do("GET", "/api/v1/admin/tenants/nope", nil, withCookie(adm)); r.Code != 404 {
		t.Fatalf("unknown tenant: %d", r.Code)
	}
	if r := e.do("GET", "/api/v1/admin/tenants/BAD:ID", nil, withCookie(adm)); r.Code != 400 {
		t.Fatalf("invalid id: %d", r.Code)
	}

	q := e.do("PUT", "/api/v1/admin/tenants/tenant-a/quota", map[string]any{"rps": 5, "burst": 7}, withCookie(adm))
	if q.Code != 200 || q.json()["tenant"].(map[string]any)["effective_burst"] != float64(7) {
		t.Fatalf("set quota: %d %s", q.Code, q.Body.String())
	}
	// Customer sees it; the data-plane limiter picks it up from the store.
	if me := e.do("GET", "/api/v1/tenant", nil, withCookie(a.cookie)); me.json()["tenant"].(map[string]any)["quota_rps"] != float64(5) {
		t.Fatalf("customer view: %s", me.Body.String())
	}
	tn, _ := e.store.GetTenant(context.Background(), "tenant-a")
	if tn.QuotaRPS != 5 || tn.QuotaBurst != 7 {
		t.Fatalf("store quota: %+v", tn)
	}
	for _, bad := range []map[string]any{{"rps": -1, "burst": 1}, {"rps": 1}, {"burst": 1}, {"rps": 2e6, "burst": 1}} {
		if r := e.do("PUT", "/api/v1/admin/tenants/tenant-a/quota", bad, withCookie(adm)); r.Code != 400 {
			t.Errorf("quota %v: %d", bad, r.Code)
		}
	}

	// Operator revokes a customer key; the data plane stops accepting it.
	if r := e.do("DELETE", "/api/v1/admin/tenants/tenant-a/keys/"+a.key[:displayPrefix], nil, withCookie(adm)); r.Code != 204 {
		t.Fatalf("admin revoke: %d", r.Code)
	}
	if e.gwStatus(a.key) != 401 {
		t.Fatal("admin-revoked key still works")
	}
	ks := e.do("GET", "/api/v1/admin/tenants/tenant-a/keys", nil, withCookie(adm))
	if ks.Code != 200 || !strings.Contains(ks.Body.String(), `"status":"revoked"`) {
		t.Fatalf("admin list keys: %s", ks.Body.String())
	}
	if r := e.do("GET", "/api/v1/admin/tenants/tenant-a/usage", nil, withCookie(adm)); r.Code != 200 {
		t.Fatalf("admin usage: %d", r.Code)
	}
	// An operator account has no tenant of its own.
	if r := e.do("GET", "/api/v1/tenant/keys", nil, withCookie(adm)); r.Code != 404 || r.code() != "no_tenant" {
		t.Fatalf("admin on customer route: %d %s", r.Code, r.Body.String())
	}
}

func TestCSRFAndCORS(t *testing.T) {
	e := newCtl(t, nil)
	a := e.signup("a@x.io", "tenant-a")
	body := map[string]any{"name": "k"}

	// A foreign origin may not change state, even with a valid cookie.
	if r := e.do("POST", "/api/v1/tenant/keys", body, withCookie(a.cookie), withHeader("Origin", "http://evil.example")); r.Code != 403 || r.code() != "origin_not_allowed" {
		t.Fatalf("evil origin: %d %s", r.Code, r.Body.String())
	}
	if r := e.do("POST", "/api/v1/tenant/keys", body, withCookie(a.cookie), withHeader("Origin", "null")); r.Code != 403 {
		t.Fatalf("null origin: %d", r.Code)
	}
	// The configured frontend works and gets credentialed-CORS headers.
	r := e.do("POST", "/api/v1/tenant/keys", body, withCookie(a.cookie), withHeader("Origin", "http://app.example"))
	if r.Code != 201 || r.Header().Get("Access-Control-Allow-Origin") != "http://app.example" || r.Header().Get("Access-Control-Allow-Credentials") != "true" {
		t.Fatalf("allowed origin: %d %v", r.Code, r.Header())
	}
	// Same-origin (Origin host == Host) is allowed without configuration.
	if r := e.do("POST", "/api/v1/tenant/keys", body, withCookie(a.cookie), withHeader("Origin", "http://example.com"), func(rq *http.Request) { rq.Host = "example.com" }); r.Code != 201 {
		t.Fatalf("same-origin: %d %s", r.Code, r.Body.String())
	}
	// Foreign origins never get CORS headers.
	g := e.do("GET", "/api/v1/me", nil, withCookie(a.cookie), withHeader("Origin", "http://evil.example"))
	if g.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("CORS granted to a foreign origin")
	}
	// Preflight.
	p := e.do("OPTIONS", "/api/v1/tenant/keys", nil, withHeader("Origin", "http://app.example"), withHeader("Access-Control-Request-Method", "POST"))
	if p.Code != 204 || !strings.Contains(p.Header().Get("Access-Control-Allow-Methods"), "DELETE") {
		t.Fatalf("preflight: %d %v", p.Code, p.Header())
	}
	// Content type must be JSON (blocks form-post CSRF).
	form := e.do("POST", "/api/v1/tenant/keys", "name=k", withCookie(a.cookie), withHeader("Content-Type", "application/x-www-form-urlencoded"))
	if form.Code != 415 {
		t.Fatalf("form post: %d", form.Code)
	}
	if h := r.Header(); h.Get("Cache-Control") != "no-store" || h.Get("X-Content-Type-Options") != "nosniff" || h.Get("X-Request-ID") == "" {
		t.Fatalf("security headers: %v", h)
	}
}

func TestJSONErrorsForBadRoutesAndBodies(t *testing.T) {
	e := newCtl(t, nil)
	if r := e.do("GET", "/api/v1/nope", nil); r.Code != 404 || r.code() != "not_found" {
		t.Fatalf("404: %d %s", r.Code, r.Body.String())
	}
	r := e.do("DELETE", "/api/v1/auth/login", nil)
	if r.Code != 405 || r.code() != "method_not_allowed" || r.Header().Get("Allow") != "POST" {
		t.Fatalf("405: %d %v", r.Code, r.Header())
	}
	if r := e.do("POST", "/api/v1/auth/login", "{not json"); r.Code != 400 || r.code() != "invalid_json" {
		t.Fatalf("bad json: %d %s", r.Code, r.Body.String())
	}
	if r := e.do("POST", "/api/v1/auth/login", `{"email":"a@x.io","password":"x","extra":1}`); r.Code != 400 {
		t.Fatalf("unknown field: %d", r.Code)
	}
	if r := e.do("POST", "/api/v1/auth/login", nil); r.Code != 400 || r.code() != "body_required" {
		t.Fatalf("empty body: %d %s", r.Code, r.Body.String())
	}
	big := `{"email":"` + strings.Repeat("a", 70000) + `"}`
	if r := e.do("POST", "/api/v1/auth/login", big); r.Code != 413 {
		t.Fatalf("oversize body: %d", r.Code)
	}
	if r := e.do("GET", "/api/v1/health", nil); r.Code != 200 {
		t.Fatalf("health: %d", r.Code)
	}
}

func TestSecureCookieFlag(t *testing.T) {
	e := newCtl(t, func(c *ControlConfig) { c.CookieSecure = true })
	a := e.signup("a@x.io", "tenant-a")
	if !a.cookie.Secure {
		t.Fatal("cookie not Secure when CookieSecure is set")
	}
}

// --------------------------------------------- shared account-store suite

func runAccountStoreSuite(t *testing.T, mk func(t *testing.T) ControlStore) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Millisecond)

	t.Run("account lifecycle", func(t *testing.T) {
		s := mk(t)
		u := User{ID: "u1", Email: "a@x.io", PasswordHash: "h", TenantID: "ta", CreatedAt: now}
		if err := s.CreateAccount(ctx, u, &Tenant{ID: "ta", Name: "A", CreatedAt: now}); err != nil {
			t.Fatal(err)
		}
		got, err := s.GetUserByEmail(ctx, "a@x.io")
		if err != nil || got.ID != "u1" || got.TenantID != "ta" || got.IsAdmin {
			t.Fatalf("get by email: %+v %v", got, err)
		}
		if _, err := s.GetUserByID(ctx, "u1"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.GetTenant(ctx, "ta"); err != nil {
			t.Fatalf("tenant not created: %v", err)
		}
		// Duplicate email: nothing from the failed attempt is left behind.
		err = s.CreateAccount(ctx, User{ID: "u2", Email: "a@x.io", CreatedAt: now}, &Tenant{ID: "tb", Name: "B", CreatedAt: now})
		if err != ErrUserExists {
			t.Fatalf("dup email: %v", err)
		}
		if _, err := s.GetTenant(ctx, "tb"); err != ErrTenantNotFound {
			t.Fatalf("tenant leaked from failed account creation: %v", err)
		}
		// Duplicate tenant.
		err = s.CreateAccount(ctx, User{ID: "u3", Email: "c@x.io", CreatedAt: now}, &Tenant{ID: "ta", Name: "dup", CreatedAt: now})
		if err != ErrTenantExists {
			t.Fatalf("dup tenant: %v", err)
		}
		if _, err := s.GetUserByEmail(ctx, "c@x.io"); err != ErrUserNotFound {
			t.Fatalf("user leaked from failed account creation: %v", err)
		}
		// Admin without a tenant.
		if err := s.CreateAccount(ctx, User{ID: "adm", Email: "r@x.io", IsAdmin: true, CreatedAt: now}, nil); err != nil {
			t.Fatal(err)
		}
		if g, _ := s.GetUserByID(ctx, "adm"); !g.IsAdmin || g.TenantID != "" {
			t.Fatalf("admin: %+v", g)
		}
		if err := s.SetPassword(ctx, "u1", "h2"); err != nil {
			t.Fatal(err)
		}
		if g, _ := s.GetUserByID(ctx, "u1"); g.PasswordHash != "h2" {
			t.Fatal("password not updated")
		}
		if err := s.SetPassword(ctx, "ghost", "x"); err != ErrUserNotFound {
			t.Fatalf("set password ghost: %v", err)
		}
	})

	t.Run("sessions", func(t *testing.T) {
		s := mk(t)
		_ = s.CreateAccount(ctx, User{ID: "u1", Email: "a@x.io", CreatedAt: now}, nil)
		for i, h := range []string{"s1", "s2", "s3"} {
			exp := now.Add(time.Hour)
			if i == 2 {
				exp = now.Add(-time.Minute)
			}
			if err := s.CreateSession(ctx, Session{TokenHash: h, UserID: "u1", CreatedAt: now, ExpiresAt: exp}); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.CreateSession(ctx, Session{TokenHash: "sx", UserID: "ghost", CreatedAt: now, ExpiresAt: now}); err != ErrUserNotFound {
			t.Fatalf("session for unknown user: %v", err)
		}
		if g, err := s.GetSession(ctx, "s1"); err != nil || g.UserID != "u1" {
			t.Fatalf("get: %+v %v", g, err)
		}
		if n, err := s.DeleteExpiredSessions(ctx, now); err != nil || n != 1 {
			t.Fatalf("expired: %d %v", n, err)
		}
		if err := s.DeleteUserSessions(ctx, "u1", "s1"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.GetSession(ctx, "s2"); err != ErrSessionNotFound {
			t.Fatalf("s2 should be gone: %v", err)
		}
		if _, err := s.GetSession(ctx, "s1"); err != nil {
			t.Fatalf("kept session lost: %v", err)
		}
		_ = s.DeleteSession(ctx, "s1")
		if _, err := s.GetSession(ctx, "s1"); err != ErrSessionNotFound {
			t.Fatalf("delete: %v", err)
		}
	})

	t.Run("list tenants", func(t *testing.T) {
		s := mk(t)
		for _, id := range []string{"c", "a", "b", "d"} {
			if err := s.CreateTenant(ctx, Tenant{ID: id, Name: id, CreatedAt: now}); err != nil {
				t.Fatal(err)
			}
		}
		ts, err := s.ListTenants(ctx, "", 3)
		if err != nil || len(ts) != 3 || ts[0].ID != "a" || ts[2].ID != "c" {
			t.Fatalf("page 1: %+v %v", ts, err)
		}
		ts, _ = s.ListTenants(ctx, "c", 3)
		if len(ts) != 1 || ts[0].ID != "d" {
			t.Fatalf("page 2: %+v", ts)
		}
	})
}

func TestMemStoreAccounts(t *testing.T) {
	runAccountStoreSuite(t, func(*testing.T) ControlStore { return NewMemStore() })
}

func TestCreateAdminUserAndResetPassword(t *testing.T) {
	ctx := context.Background()
	s := NewMemStore()
	now := time.Now()
	if _, err := CreateAdminUser(ctx, s, "not-an-email", "a-good-password", 1000, now); err == nil {
		t.Fatal("bad email accepted")
	}
	if _, err := CreateAdminUser(ctx, s, "ops@x.io", "short", 1000, now); err == nil {
		t.Fatal("weak password accepted")
	}
	u, err := CreateAdminUser(ctx, s, "Ops@X.io", "a-good-password", 1000, now)
	if err != nil || !u.IsAdmin || u.Email != "ops@x.io" || u.TenantID != "" {
		t.Fatalf("create: %+v %v", u, err)
	}
	if _, err := CreateAdminUser(ctx, s, "ops@x.io", "a-good-password", 1000, now); err != ErrUserExists {
		t.Fatalf("duplicate admin: %v", err)
	}

	_ = s.CreateSession(ctx, Session{TokenHash: "s1", UserID: u.ID, CreatedAt: now, ExpiresAt: now.Add(time.Hour)})
	if err := ResetUserPassword(ctx, s, "ops@x.io", "another-good-pass", 1000); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetUserByEmail(ctx, "ops@x.io")
	if ok, _ := VerifyPassword("another-good-pass", got.PasswordHash); !ok {
		t.Fatal("new password does not verify")
	}
	if _, err := s.GetSession(ctx, "s1"); err != ErrSessionNotFound {
		t.Fatalf("session survived password reset: %v", err)
	}
	if err := ResetUserPassword(ctx, s, "ghost@x.io", "another-good-pass", 1000); err != ErrUserNotFound {
		t.Fatalf("reset unknown user: %v", err)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// API-key UX contract tests (signup one-time reveal requirements)
// ──────────────────────────────────────────────────────────────────────────────

// TestSignupCreatesExactlyOneDefaultKey verifies that a fresh signup creates
// exactly one active API key and that it is included in the listing endpoint.
func TestSignupCreatesExactlyOneDefaultKey(t *testing.T) {
	e := newCtl(t, nil)
	a := e.signup("a@x.io", "tenant-a")

	// The signup helper already asserts a 201 and extracts the key.
	// Now verify via the listing endpoint that exactly one key exists.
	l := e.do("GET", "/api/v1/tenant/keys", nil, withCookie(a.cookie))
	if l.Code != 200 {
		t.Fatalf("list keys: %d %s", l.Code, l.Body.String())
	}
	keys, ok := l.json()["keys"].([]any)
	if !ok {
		t.Fatalf("keys field missing or wrong type: %s", l.Body.String())
	}
	if len(keys) != 1 {
		t.Fatalf("want exactly 1 default key after signup, got %d", len(keys))
	}
	k := keys[0].(map[string]any)
	if k["name"] != "default" {
		t.Fatalf("default key name: %v", k["name"])
	}
	if k["status"] != "active" {
		t.Fatalf("default key status: %v", k["status"])
	}
}

// TestSignupResponseContainsFullSecretOnlyOnce verifies that:
//  1. The signup response contains the full plaintext key.
//  2. A subsequent GET /api/v1/tenant/keys (the normal listing path used by the
//     dashboard) does NOT expose the plaintext or its hash.
//  3. Creating another key via POST also returns a full secret, but only in
//     that specific response — listing still does not expose it.
func TestSignupResponseContainsFullSecretOnlyOnce(t *testing.T) {
	e := newCtl(t, nil)

	// --- 1. Signup response contains the full secret ---
	body := map[string]string{"email": "b@x.io", "password": "a-good-password", "tenant_id": "tenant-b"}
	r := e.do("POST", "/api/v1/auth/signup", body, withIP("10.10.0.1"))
	if r.Code != 201 {
		t.Fatalf("signup: %d %s", r.Code, r.Body.String())
	}
	apiKeyBlock, ok := r.json()["api_key"].(map[string]any)
	if !ok {
		t.Fatal("signup response missing api_key block")
	}
	plain, ok := apiKeyBlock["key"].(string)
	if !ok || plain == "" {
		t.Fatalf("signup api_key.key missing or empty: %v", apiKeyBlock)
	}
	if !strings.HasPrefix(plain, KeyPrefix) || len(plain) != keyLen {
		t.Fatalf("unexpected key shape: %q", plain)
	}

	// --- 2. Listing endpoint does NOT expose the plaintext or hash ---
	ck := r.cookie()
	l := e.do("GET", "/api/v1/tenant/keys", nil, withCookie(ck))
	if l.Code != 200 {
		t.Fatalf("list: %d %s", l.Code, l.Body.String())
	}
	if strings.Contains(l.Body.String(), plain) {
		t.Fatal("listing leaked the plaintext key")
	}
	if strings.Contains(l.Body.String(), hashKey(plain)) {
		t.Fatal("listing leaked the key hash")
	}

	// --- 3. Creating another key also returns a secret only in that response ---
	cr := e.do("POST", "/api/v1/tenant/keys", map[string]any{"name": "prod"}, withCookie(ck))
	if cr.Code != 201 {
		t.Fatalf("create key: %d %s", cr.Code, cr.Body.String())
	}
	plain2, _ := cr.json()["key"].(string)
	if plain2 == "" {
		t.Fatal("create-key response missing plaintext")
	}
	l2 := e.do("GET", "/api/v1/tenant/keys", nil, withCookie(ck))
	if strings.Contains(l2.Body.String(), plain2) {
		t.Fatal("listing leaked the second key plaintext")
	}
	if strings.Contains(l2.Body.String(), hashKey(plain2)) {
		t.Fatal("listing leaked the second key hash")
	}
}

// TestKeyListNeverExposesFullSecret is a focused assertion that the tenant
// key listing endpoint's JSON body never contains any string that looks like
// a full plaintext key (starts with KeyPrefix and is keyLen characters long).
func TestKeyListNeverExposesFullSecret(t *testing.T) {
	e := newCtl(t, nil)
	a := e.signup("a@x.io", "tenant-a")

	// Create a couple more keys.
	for _, name := range []string{"staging", "ci"} {
		r := e.do("POST", "/api/v1/tenant/keys", map[string]any{"name": name}, withCookie(a.cookie))
		if r.Code != 201 {
			t.Fatalf("create %s: %d %s", name, r.Code, r.Body.String())
		}
	}

	l := e.do("GET", "/api/v1/tenant/keys", nil, withCookie(a.cookie))
	if l.Code != 200 {
		t.Fatalf("list: %d %s", l.Code, l.Body.String())
	}
	body := l.Body.String()

	// Check that no token in the response body looks like a full API key.
	for _, tok := range strings.Fields(body) {
		// Strip common JSON punctuation characters.
		tok = strings.Trim(tok, `",[]{}`)
		if strings.HasPrefix(tok, KeyPrefix) && len(tok) == keyLen {
			t.Fatalf("listing contains a full plaintext key: %q", tok)
		}
	}

	// Three keys in total (default + staging + ci), none has a "key" field.
	keys := l.json()["keys"].([]any)
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}
	for _, raw := range keys {
		k := raw.(map[string]any)
		if _, hasKey := k["key"]; hasKey {
			t.Fatalf("key listing item contains 'key' field: %v", k)
		}
		if _, hasHash := k["hash"]; hasHash {
			t.Fatalf("key listing item contains 'hash' field: %v", k)
		}
	}
}

// TestSignupResponseApiKeyShape checks the exact shape of the api_key field
// in the signup response so the frontend can rely on key, prefix, and name.
func TestSignupResponseApiKeyShape(t *testing.T) {
	e := newCtl(t, nil)
	r := e.do("POST", "/api/v1/auth/signup", map[string]string{
		"email": "c@x.io", "password": "a-good-password", "tenant_id": "tenant-c", "name": "Corp",
	}, withIP("10.20.0.1"))
	if r.Code != 201 {
		t.Fatalf("signup: %d %s", r.Code, r.Body.String())
	}
	ak, ok := r.json()["api_key"].(map[string]any)
	if !ok {
		t.Fatal("api_key block missing from signup response")
	}
	// key: full plaintext
	key, _ := ak["key"].(string)
	if !strings.HasPrefix(key, KeyPrefix) || len(key) != keyLen {
		t.Fatalf("api_key.key shape: %q", key)
	}
	// prefix: the first displayPrefix chars of the plaintext key
	prefix, _ := ak["prefix"].(string)
	if prefix == "" {
		t.Fatal("api_key.prefix missing")
	}
	if !strings.HasPrefix(key, prefix) {
		t.Fatalf("api_key.prefix %q is not a prefix of the key %q", prefix, key)
	}
	// name: "default"
	if ak["name"] != "default" {
		t.Fatalf("api_key.name: %v", ak["name"])
	}
	// The full key must NOT appear anywhere in the listing.
	ck := r.cookie()
	l := e.do("GET", "/api/v1/tenant/keys", nil, withCookie(ck))
	if strings.Contains(l.Body.String(), key) {
		t.Fatal("key appeared in listing after signup")
	}
}
