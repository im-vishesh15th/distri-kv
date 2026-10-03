package gateway

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"mime"
	"net"
	"net/http"
	"net/mail"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ControlAPI is the JSON API behind the web console: signup/login, API-key
// management, quota and usage views for customers, and tenant administration
// for operators. It is a separate http.Handler (and, in cmd/gateway, a
// separate listener) from the data-plane gateway, so it can be firewalled and
// scaled independently.
//
// Auth is a server-side session in an HttpOnly, SameSite=Strict cookie. The
// cookie value is a random 256-bit token; only its SHA-256 is stored.
// State-changing requests additionally require an allowed (or same-origin)
// Origin header when the browser sends one, and a JSON content type.

// ControlConfig configures the control-plane API. Zero values get defaults.
type ControlConfig struct {
	BindAddr       string
	AllowedOrigins []string // exact origins allowed for CORS + CSRF, e.g. "http://localhost:5173"
	AllowSignup    bool     // POST /api/v1/auth/signup works only when true
	CookieSecure   bool     // set the Secure cookie flag (use whenever served over HTTPS)
	SessionTTL     time.Duration

	TLSCertFile string
	TLSKeyFile  string

	PasswordIterations int // PBKDF2 iterations for new hashes (default 600000)
	MaxKeysPerTenant   int // active keys per tenant (default 10)

	AuthAttemptsPerMinute  int // signup+login per client IP (default 10)
	LoginAttemptsPerMinute int // login/password checks per email (default 5)
	TrustForwardedFor      bool

	// Effective data-plane defaults, shown to customers next to their quota.
	DefaultRateRPS   float64
	DefaultRateBurst int
	MaxConcurrent    int
	Usage            func(tenantID string) Usage                                      // data-plane counters (Gateway.Metrics().Usage)
	Storage          func(ctx context.Context, tenantID string) (StorageUsage, error) // nil = not configured
	UsageSeries      func(tenantID, rng string) (UsageSeries, error)
	Logger           *slog.Logger // audit log; nil = off
	now              func() time.Time
}

func (c *ControlConfig) applyDefaults() {
	if c.SessionTTL <= 0 {
		c.SessionTTL = 24 * time.Hour
	}
	if c.PasswordIterations <= 0 {
		c.PasswordIterations = defaultPBKDF2Iter
	}
	if c.MaxKeysPerTenant <= 0 {
		c.MaxKeysPerTenant = 10
	}
	if c.AuthAttemptsPerMinute <= 0 {
		c.AuthAttemptsPerMinute = 10
	}
	if c.LoginAttemptsPerMinute <= 0 {
		c.LoginAttemptsPerMinute = 5
	}
	if c.DefaultRateRPS <= 0 {
		c.DefaultRateRPS = 1000
	}
	if c.DefaultRateBurst <= 0 {
		c.DefaultRateBurst = 2000
	}
	if c.MaxConcurrent <= 0 {
		c.MaxConcurrent = 128
	}
	if c.now == nil {
		c.now = time.Now
	}
}

const (
	sessionCookie = "dkv_session"
	maxCtlBody    = 64 << 10
)

var (
	keyPrefixRe = regexp.MustCompile(`^dkv_live_[0-9a-f]{8}$`)
	reqIDRe     = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
)

// ControlAPI implements http.Handler via Handler().
type ControlAPI struct {
	cfg     ControlConfig
	store   ControlStore
	allowed map[string]bool

	dummyHash string // verified against when the email is unknown (timing)

	ipLim    *keyedLimiter
	emailLim *keyedLimiter
}

// NewControlAPI builds the API over a store that holds tenants, keys, users
// and sessions (SQLiteStore or MemStore).
func NewControlAPI(cfg ControlConfig, store ControlStore) (*ControlAPI, error) {
	cfg.applyDefaults()
	c := &ControlAPI{cfg: cfg, store: store, allowed: map[string]bool{}}
	for _, o := range cfg.AllowedOrigins {
		c.allowed[strings.TrimRight(strings.TrimSpace(o), "/")] = true
	}
	h, err := HashPassword("dummy-password-for-timing", cfg.PasswordIterations)
	if err != nil {
		return nil, err
	}
	c.dummyHash = h
	now := cfg.now()
	c.ipLim = newKeyedLimiter(float64(cfg.AuthAttemptsPerMinute)/60, cfg.AuthAttemptsPerMinute, now)
	c.emailLim = newKeyedLimiter(float64(cfg.LoginAttemptsPerMinute)/60, cfg.LoginAttemptsPerMinute, now)
	return c, nil
}

// keyedLimiter is a bounded map of token buckets.
type keyedLimiter struct {
	mu    sync.Mutex
	m     map[string]*rateLimiter
	rate  float64
	burst int
	max   int
}

func newKeyedLimiter(rate float64, burst int, _ time.Time) *keyedLimiter {
	return &keyedLimiter{m: map[string]*rateLimiter{}, rate: rate, burst: burst, max: 20000}
}

func (k *keyedLimiter) allow(key string, now time.Time) (bool, time.Duration) {
	k.mu.Lock()
	l, ok := k.m[key]
	if !ok {
		if len(k.m) >= k.max { // drop buckets idle for 10+ minutes
			for kk, v := range k.m {
				v.mu.Lock()
				idle := now.Sub(v.last) > 10*time.Minute
				v.mu.Unlock()
				if idle {
					delete(k.m, kk)
				}
			}
		}
		l = newLimiter(k.rate, k.burst, now)
		k.m[key] = l
	}
	k.mu.Unlock()
	return l.allow(now)
}

// ---------------------------------------------------------------- plumbing

func (c *ControlAPI) audit(r *http.Request, event string, attrs ...any) {
	if c.cfg.Logger == nil {
		return
	}
	attrs = append([]any{
		slog.String("event", event),
		slog.String("ip", c.clientIP(r)),
		slog.String("request_id", r.Header.Get("X-Request-ID")),
	}, attrs...)
	c.cfg.Logger.Info("audit", attrs...)
}

func (c *ControlAPI) clientIP(r *http.Request) string {
	if c.cfg.TrustForwardedFor {
		if xf := r.Header.Get("X-Forwarded-For"); xf != "" {
			parts := strings.Split(xf, ",")
			if ip := strings.TrimSpace(parts[len(parts)-1]); ip != "" {
				return ip
			}
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func randHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func sha256Hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func rfc3339(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func rfc3339p(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := rfc3339(*t)
	return &s
}

func tooMany(w http.ResponseWriter, wait time.Duration, code, msg string) {
	secs := int(math.Ceil(wait.Seconds()))
	if secs < 1 {
		secs = 1
	}
	w.Header().Set("Retry-After", strconv.Itoa(secs))
	writeError(w, http.StatusTooManyRequests, code, msg)
}

// decode reads a JSON body (<= 64 KiB, unknown fields rejected). With
// optional=true an empty body is accepted and leaves dst untouched.
func (c *ControlAPI) decode(w http.ResponseWriter, r *http.Request, dst any, optional bool) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxCtlBody)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var big *http.MaxBytesError
		if errors.As(err, &big) {
			writeError(w, http.StatusRequestEntityTooLarge, "body_too_large", "request body too large")
		} else {
			writeError(w, http.StatusBadRequest, "invalid_body", "could not read request body")
		}
		return false
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		if optional {
			return true
		}
		writeError(w, http.StatusBadRequest, "body_required", "a JSON body is required")
		return false
	}
	mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mt != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
		return false
	}
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "request body is not valid JSON for this endpoint")
		return false
	}
	return true
}

func methods(m map[string]http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h, ok := m[r.Method]; ok {
			h(w, r)
			return
		}
		allow := make([]string, 0, len(m))
		for k := range m {
			allow = append(allow, k)
		}
		w.Header().Set("Allow", strings.Join(allow, ", "))
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed for this route")
	}
}

func safeMethod(m string) bool {
	return m == http.MethodGet || m == http.MethodHead || m == http.MethodOptions
}

func (c *ControlAPI) originOK(r *http.Request) bool {
	o := r.Header.Get("Origin")
	if o == "" {
		return true // not a browser cross-site request
	}
	if c.allowed[strings.TrimRight(o, "/")] {
		return true
	}
	u, err := url.Parse(o)
	return err == nil && u.Host != "" && u.Host == r.Host
}

// Handler returns the full API with CORS, CSRF and security headers applied.
func (c *ControlAPI) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/health", methods(map[string]http.HandlerFunc{http.MethodGet: c.health}))

	mux.HandleFunc("/api/v1/auth/signup", methods(map[string]http.HandlerFunc{http.MethodPost: c.signup}))
	mux.HandleFunc("/api/v1/auth/login", methods(map[string]http.HandlerFunc{http.MethodPost: c.login}))
	mux.HandleFunc("/api/v1/auth/logout", methods(map[string]http.HandlerFunc{http.MethodPost: c.auth(c.logout)}))

	mux.HandleFunc("/api/v1/me", methods(map[string]http.HandlerFunc{http.MethodGet: c.auth(c.me)}))
	mux.HandleFunc("/api/v1/me/password", methods(map[string]http.HandlerFunc{http.MethodPut: c.auth(c.changePassword)}))

	mux.HandleFunc("/api/v1/tenant", methods(map[string]http.HandlerFunc{http.MethodGet: c.tenantH(c.getTenant)}))
	mux.HandleFunc("/api/v1/tenant/usage", methods(map[string]http.HandlerFunc{http.MethodGet: c.tenantH(c.tenantUsage)}))
	mux.HandleFunc("/api/v1/tenant/usage/series", methods(map[string]http.HandlerFunc{http.MethodGet: c.tenantH(c.tenantUsageSeries)}))
	mux.HandleFunc("/api/v1/tenant/storage", methods(map[string]http.HandlerFunc{http.MethodGet: c.tenantH(c.tenantStorage)}))
	mux.HandleFunc("/api/v1/tenant/keys", methods(map[string]http.HandlerFunc{
		http.MethodGet:  c.tenantH(c.listKeys),
		http.MethodPost: c.tenantH(c.createKey),
	}))
	mux.HandleFunc("/api/v1/tenant/keys/{prefix}", methods(map[string]http.HandlerFunc{http.MethodDelete: c.tenantH(c.revokeKey)}))
	mux.HandleFunc("/api/v1/tenant/keys/{prefix}/rotate", methods(map[string]http.HandlerFunc{http.MethodPost: c.tenantH(c.rotateKey)}))

	mux.HandleFunc("/api/v1/admin/tenants", methods(map[string]http.HandlerFunc{http.MethodGet: c.admin(c.adminListTenants)}))
	mux.HandleFunc("/api/v1/admin/tenants/{id}", methods(map[string]http.HandlerFunc{http.MethodGet: c.admin(c.adminGetTenant)}))
	mux.HandleFunc("/api/v1/admin/tenants/{id}/quota", methods(map[string]http.HandlerFunc{http.MethodPut: c.admin(c.adminSetQuota)}))
	mux.HandleFunc("/api/v1/admin/tenants/{id}/usage", methods(map[string]http.HandlerFunc{http.MethodGet: c.admin(c.adminUsage)}))
	mux.HandleFunc("/api/v1/admin/tenants/{id}/usage/series", methods(map[string]http.HandlerFunc{http.MethodGet: c.admin(c.adminUsageSeries)}))
	mux.HandleFunc("/api/v1/admin/tenants/{id}/keys", methods(map[string]http.HandlerFunc{http.MethodGet: c.admin(c.adminListKeys)}))
	mux.HandleFunc("/api/v1/admin/tenants/{id}/keys/{prefix}", methods(map[string]http.HandlerFunc{http.MethodDelete: c.admin(c.adminRevokeKey)}))
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, "not_found", "unknown route")
	})

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rid := r.Header.Get("X-Request-ID")
		if len(rid) == 0 || len(rid) > 64 || !reqIDRe.MatchString(rid) {
			rid, _ = randHex(8)
			r.Header.Set("X-Request-ID", rid)
		}
		h := w.Header()
		h.Set("X-Request-ID", rid)
		h.Set("Cache-Control", "no-store")
		h.Set("X-Content-Type-Options", "nosniff")

		if o := r.Header.Get("Origin"); o != "" && c.allowed[strings.TrimRight(o, "/")] {
			h.Set("Access-Control-Allow-Origin", o)
			h.Set("Access-Control-Allow-Credentials", "true")
			h.Add("Vary", "Origin")
			if r.Method == http.MethodOptions {
				h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
				h.Set("Access-Control-Allow-Headers", "Content-Type, X-Request-ID")
				h.Set("Access-Control-Max-Age", "600")
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		if !safeMethod(r.Method) && !c.originOK(r) {
			writeError(w, http.StatusForbidden, "origin_not_allowed", "this origin may not call the control API")
			return
		}
		mux.ServeHTTP(w, r)
	})
}

// ------------------------------------------------------------------- DTOs

type userDTO struct {
	ID        string `json:"id"`
	Email     string `json:"email"`
	IsAdmin   bool   `json:"is_admin"`
	TenantID  string `json:"tenant_id,omitempty"`
	CreatedAt string `json:"created_at"`
}

type tenantDTO struct {
	ID                 string  `json:"id"`
	Name               string  `json:"name"`
	CreatedAt          string  `json:"created_at"`
	QuotaRPS           float64 `json:"quota_rps"`   // 0 = gateway default
	QuotaBurst         int     `json:"quota_burst"` // 0 = gateway default
	EffectiveRPS       float64 `json:"effective_rps"`
	EffectiveBurst     int     `json:"effective_burst"`
	MaxConcurrent      int     `json:"max_concurrent"`
	MaxKeys            int     `json:"max_keys"`
	AllowedKeyTTLHours int     `json:"max_key_ttl_hours"`
}

type keyDTO struct {
	Prefix    string  `json:"prefix"`
	Name      string  `json:"name"`
	Status    string  `json:"status"` // active | expired | revoked
	CreatedAt string  `json:"created_at"`
	ExpiresAt *string `json:"expires_at"`
	RevokedAt *string `json:"revoked_at"`
}

const maxKeyTTLHours = 24 * 365

func userOut(u User) userDTO {
	return userDTO{ID: u.ID, Email: u.Email, IsAdmin: u.IsAdmin, TenantID: u.TenantID, CreatedAt: rfc3339(u.CreatedAt)}
}

func (c *ControlAPI) tenantOut(t Tenant) tenantDTO {
	rps, burst := c.cfg.DefaultRateRPS, c.cfg.DefaultRateBurst
	if t.QuotaRPS > 0 {
		rps = t.QuotaRPS
	}
	if t.QuotaBurst > 0 {
		burst = t.QuotaBurst
	}
	return tenantDTO{
		ID: t.ID, Name: t.Name, CreatedAt: rfc3339(t.CreatedAt),
		QuotaRPS: t.QuotaRPS, QuotaBurst: t.QuotaBurst,
		EffectiveRPS: rps, EffectiveBurst: burst,
		MaxConcurrent: c.cfg.MaxConcurrent, MaxKeys: c.cfg.MaxKeysPerTenant,
		AllowedKeyTTLHours: maxKeyTTLHours,
	}
}

func (c *ControlAPI) keyOut(k APIKey) keyDTO {
	status := "active"
	now := c.cfg.now()
	switch {
	case k.RevokedAt != nil:
		status = "revoked"
	case k.ExpiresAt != nil && !now.Before(*k.ExpiresAt):
		status = "expired"
	}
	return keyDTO{Prefix: k.Prefix, Name: k.Name, Status: status, CreatedAt: rfc3339(k.CreatedAt),
		ExpiresAt: rfc3339p(k.ExpiresAt), RevokedAt: rfc3339p(k.RevokedAt)}
}

// ------------------------------------------------------------------- auth

type authed struct {
	user      User
	tokenHash string
}

func (c *ControlAPI) setSessionCookie(w http.ResponseWriter, token string, expires time.Time) {
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: token, Path: "/", Expires: expires,
		MaxAge:   int(time.Until(expires).Seconds()),
		HttpOnly: true, Secure: c.cfg.CookieSecure, SameSite: http.SameSiteStrictMode,
	})
}

func (c *ControlAPI) clearSessionCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: "", Path: "/", MaxAge: -1,
		HttpOnly: true, Secure: c.cfg.CookieSecure, SameSite: http.SameSiteStrictMode,
	})
}

// newSession stores a session for u and sets the cookie.
func (c *ControlAPI) newSession(ctx context.Context, w http.ResponseWriter, u User) error {
	token, err := randHex(32)
	if err != nil {
		return err
	}
	now := c.cfg.now()
	exp := now.Add(c.cfg.SessionTTL)
	if err := c.store.CreateSession(ctx, Session{TokenHash: sha256Hex(token), UserID: u.ID, CreatedAt: now, ExpiresAt: exp}); err != nil {
		return err
	}
	c.setSessionCookie(w, token, exp)
	return nil
}

func (c *ControlAPI) auth(h func(http.ResponseWriter, *http.Request, *authed)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ck, err := r.Cookie(sessionCookie)
		if err != nil || ck.Value == "" || len(ck.Value) > 128 {
			writeError(w, http.StatusUnauthorized, "unauthenticated", "log in first")
			return
		}
		th := sha256Hex(ck.Value)
		s, err := c.store.GetSession(r.Context(), th)
		if err == nil && !c.cfg.now().Before(s.ExpiresAt) {
			_ = c.store.DeleteSession(r.Context(), th)
			err = ErrSessionNotFound
		}
		if errors.Is(err, ErrSessionNotFound) {
			c.clearSessionCookie(w)
			writeError(w, http.StatusUnauthorized, "unauthenticated", "session expired; log in again")
			return
		}
		if err != nil {
			c.internal(w, "get session", err)
			return
		}
		u, err := c.store.GetUserByID(r.Context(), s.UserID)
		if err != nil {
			if errors.Is(err, ErrUserNotFound) {
				c.clearSessionCookie(w)
				writeError(w, http.StatusUnauthorized, "unauthenticated", "log in again")
				return
			}
			c.internal(w, "get user", err)
			return
		}
		h(w, r, &authed{user: u, tokenHash: th})
	}
}

func (c *ControlAPI) admin(h func(http.ResponseWriter, *http.Request, *authed)) http.HandlerFunc {
	return c.auth(func(w http.ResponseWriter, r *http.Request, a *authed) {
		if !a.user.IsAdmin {
			writeError(w, http.StatusForbidden, "forbidden", "operator access required")
			return
		}
		h(w, r, a)
	})
}

func (c *ControlAPI) tenantH(h func(http.ResponseWriter, *http.Request, *authed, Tenant)) http.HandlerFunc {
	return c.auth(func(w http.ResponseWriter, r *http.Request, a *authed) {
		if a.user.TenantID == "" {
			writeError(w, http.StatusNotFound, "no_tenant", "this account has no tenant")
			return
		}
		t, err := c.store.GetTenant(r.Context(), a.user.TenantID)
		if err != nil {
			if errors.Is(err, ErrTenantNotFound) {
				writeError(w, http.StatusNotFound, "no_tenant", "tenant not found")
				return
			}
			c.internal(w, "get tenant", err)
			return
		}
		h(w, r, a, t)
	})
}

func (c *ControlAPI) internal(w http.ResponseWriter, what string, err error) {
	if c.cfg.Logger != nil {
		c.cfg.Logger.Error("control_internal_error", slog.String("op", what), slog.String("error", err.Error()))
	}
	writeError(w, http.StatusInternalServerError, "internal", "internal error")
}

// --------------------------------------------------------------- handlers

func (c *ControlAPI) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func normalizeEmail(s string) (string, bool) {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" || len(s) > 254 {
		return "", false
	}
	a, err := mail.ParseAddress(s)
	if err != nil || a.Address != s || !strings.Contains(s, "@") {
		return "", false
	}
	return s, true
}

func (c *ControlAPI) ipAllowed(w http.ResponseWriter, r *http.Request) bool {
	if ok, wait := c.ipLim.allow(c.clientIP(r), c.cfg.now()); !ok {
		tooMany(w, wait, "too_many_attempts", "too many attempts from this address; slow down")
		return false
	}
	return true
}

func (c *ControlAPI) signup(w http.ResponseWriter, r *http.Request) {
	if !c.cfg.AllowSignup {
		writeError(w, http.StatusForbidden, "signup_disabled", "self-service signup is disabled")
		return
	}
	if !c.ipAllowed(w, r) {
		return
	}
	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
		TenantID string `json:"tenant_id"`
		Name     string `json:"name"`
	}
	if !c.decode(w, r, &req, false) {
		return
	}
	email, ok := normalizeEmail(req.Email)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid_email", "enter a valid email address")
		return
	}
	if err := ValidatePassword(req.Password); err != nil {
		writeError(w, http.StatusBadRequest, "weak_password", err.Error())
		return
	}
	tenantID := strings.TrimSpace(req.TenantID)
	if tenantID == "" {
		suf, err := randHex(5)
		if err != nil {
			c.internal(w, "tenant id", err)
			return
		}
		tenantID = "t-" + suf
	}
	if err := ValidateTenantID(tenantID); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_tenant_id", err.Error())
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		name = tenantID
	}
	if len(name) > 80 {
		writeError(w, http.StatusBadRequest, "invalid_name", "name must be at most 80 characters")
		return
	}
	hash, err := HashPassword(req.Password, c.cfg.PasswordIterations)
	if err != nil {
		c.internal(w, "hash password", err)
		return
	}
	uid, err := randHex(16)
	if err != nil {
		c.internal(w, "user id", err)
		return
	}
	now := c.cfg.now()
	u := User{ID: uid, Email: email, PasswordHash: hash, TenantID: tenantID, CreatedAt: now}
	t := Tenant{ID: tenantID, Name: name, CreatedAt: now}
	switch err := c.store.CreateAccount(r.Context(), u, &t); {
	case errors.Is(err, ErrUserExists):
		writeError(w, http.StatusConflict, "email_taken", "an account with this email already exists")
		return
	case errors.Is(err, ErrTenantExists):
		writeError(w, http.StatusConflict, "tenant_taken", "that tenant ID is already taken")
		return
	case err != nil:
		c.internal(w, "create account", err)
		return
	}
	c.audit(r, "signup", slog.String("user_id", u.ID), slog.String("tenant", tenantID))

	if err := c.newSession(r.Context(), w, u); err != nil {
		c.internal(w, "create session", err)
		return
	}
	resp := map[string]any{"user": userOut(u), "tenant": c.tenantOut(t)}
	// First API key, shown once, so the console can show a working quickstart.
	if plain, ak, err := CreateAPIKey(r.Context(), c.store, tenantID, "default", 0, now); err == nil {
		k := c.keyOut(ak)
		resp["api_key"] = map[string]any{"key": plain, "prefix": k.Prefix, "name": k.Name, "created_at": k.CreatedAt}
	} else {
		c.audit(r, "signup_first_key_failed", slog.String("tenant", tenantID), slog.String("error", err.Error()))
	}
	writeJSON(w, http.StatusCreated, resp)
}

func (c *ControlAPI) login(w http.ResponseWriter, r *http.Request) {
	if !c.ipAllowed(w, r) {
		return
	}
	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if !c.decode(w, r, &req, false) {
		return
	}
	email := strings.ToLower(strings.TrimSpace(req.Email))
	if ok, wait := c.emailLim.allow(email, c.cfg.now()); !ok {
		tooMany(w, wait, "too_many_attempts", "too many login attempts for this account; try again later")
		return
	}
	u, err := c.store.GetUserByEmail(r.Context(), email)
	hash := c.dummyHash
	if err == nil {
		hash = u.PasswordHash
	} else if !errors.Is(err, ErrUserNotFound) {
		c.internal(w, "get user", err)
		return
	}
	match, verr := VerifyPassword(req.Password, hash) // always runs: constant-ish time
	if err != nil || verr != nil || !match {
		c.audit(r, "login_failed")
		writeError(w, http.StatusUnauthorized, "invalid_credentials", "invalid email or password")
		return
	}
	if err := c.newSession(r.Context(), w, u); err != nil {
		c.internal(w, "create session", err)
		return
	}
	c.audit(r, "login", slog.String("user_id", u.ID))
	resp := map[string]any{"user": userOut(u)}
	if u.TenantID != "" {
		if t, err := c.store.GetTenant(r.Context(), u.TenantID); err == nil {
			resp["tenant"] = c.tenantOut(t)
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (c *ControlAPI) logout(w http.ResponseWriter, r *http.Request, a *authed) {
	_ = c.store.DeleteSession(r.Context(), a.tokenHash)
	c.clearSessionCookie(w)
	c.audit(r, "logout", slog.String("user_id", a.user.ID))
	w.WriteHeader(http.StatusNoContent)
}

func (c *ControlAPI) me(w http.ResponseWriter, r *http.Request, a *authed) {
	resp := map[string]any{"user": userOut(a.user)}
	if a.user.TenantID != "" {
		if t, err := c.store.GetTenant(r.Context(), a.user.TenantID); err == nil {
			resp["tenant"] = c.tenantOut(t)
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (c *ControlAPI) changePassword(w http.ResponseWriter, r *http.Request, a *authed) {
	var req struct {
		CurrentPassword string `json:"current_password"`
		NewPassword     string `json:"new_password"`
	}
	if !c.decode(w, r, &req, false) {
		return
	}
	if ok, wait := c.emailLim.allow(a.user.Email, c.cfg.now()); !ok {
		tooMany(w, wait, "too_many_attempts", "too many attempts; try again later")
		return
	}
	if ok, _ := VerifyPassword(req.CurrentPassword, a.user.PasswordHash); !ok {
		writeError(w, http.StatusUnauthorized, "invalid_credentials", "current password is incorrect")
		return
	}
	if err := ValidatePassword(req.NewPassword); err != nil {
		writeError(w, http.StatusBadRequest, "weak_password", err.Error())
		return
	}
	hash, err := HashPassword(req.NewPassword, c.cfg.PasswordIterations)
	if err != nil {
		c.internal(w, "hash password", err)
		return
	}
	if err := c.store.SetPassword(r.Context(), a.user.ID, hash); err != nil {
		c.internal(w, "set password", err)
		return
	}
	// Every other device is signed out.
	_ = c.store.DeleteUserSessions(r.Context(), a.user.ID, a.tokenHash)
	c.audit(r, "password_changed", slog.String("user_id", a.user.ID))
	w.WriteHeader(http.StatusNoContent)
}

func (c *ControlAPI) getTenant(w http.ResponseWriter, _ *http.Request, _ *authed, t Tenant) {
	writeJSON(w, http.StatusOK, map[string]any{"tenant": c.tenantOut(t)})
}

func (c *ControlAPI) usageFor(tenantID string) Usage {
	if c.cfg.Usage == nil {
		return Usage{ByOp: map[string]uint64{}, ByStatus: map[string]uint64{}}
	}
	return c.cfg.Usage(tenantID)
}

func (c *ControlAPI) tenantUsage(w http.ResponseWriter, _ *http.Request, _ *authed, t Tenant) {
	writeJSON(w, http.StatusOK, map[string]any{"tenant_id": t.ID, "usage": c.usageFor(t.ID)})
}

func (c *ControlAPI) seriesFor(w http.ResponseWriter, tenantID, rng string) (UsageSeries, bool) {
	if rng == "" {
		rng = "24h"
	}
	if c.cfg.UsageSeries == nil {
		return UsageSeries{Range: rng, Points: []UsagePoint{}}, true
	}
	s, err := c.cfg.UsageSeries(tenantID, rng)
	if err != nil {
		if errors.Is(err, errInvalidUsageRange) {
			writeError(w, http.StatusBadRequest, "invalid_range", "range must be 24h, 7d or 30d")
			return UsageSeries{}, false
		}
		c.internal(w, "usage series", err)
		return UsageSeries{}, false
	}
	return s, true
}

func (c *ControlAPI) tenantUsageSeries(w http.ResponseWriter, r *http.Request, _ *authed, t Tenant) {
	s, ok := c.seriesFor(w, t.ID, r.URL.Query().Get("range"))
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tenant_id": t.ID, "series": s})
}

func (c *ControlAPI) tenantStorage(w http.ResponseWriter, r *http.Request, _ *authed, t Tenant) {
	if c.cfg.Storage == nil {
		writeError(w, http.StatusServiceUnavailable, "storage_not_configured", "storage accounting is not configured on this gateway")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	u, err := c.cfg.Storage(ctx, t.ID)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "storage_unavailable", "storage usage is temporarily unavailable; try again shortly")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tenant_id": t.ID, "storage": u})
}

func (c *ControlAPI) listKeysFor(r *http.Request, tenantID string) ([]keyDTO, error) {
	keys, err := c.store.ListKeys(r.Context(), tenantID)
	if err != nil {
		return nil, err
	}
	out := make([]keyDTO, 0, len(keys))
	for _, k := range keys {
		out = append(out, c.keyOut(k))
	}
	return out, nil
}

func (c *ControlAPI) listKeys(w http.ResponseWriter, r *http.Request, _ *authed, t Tenant) {
	out, err := c.listKeysFor(r, t.ID)
	if err != nil {
		c.internal(w, "list keys", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"keys": out})
}

func (c *ControlAPI) activeKeyCount(r *http.Request, tenantID string) (int, error) {
	keys, err := c.listKeysFor(r, tenantID)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, k := range keys {
		if k.Status == "active" {
			n++
		}
	}
	return n, nil
}

func ttlFromHours(h *int) (time.Duration, string) {
	if h == nil {
		return 0, ""
	}
	if *h < 0 || *h > maxKeyTTLHours {
		return 0, "ttl_hours must be between 0 (never expires) and " + strconv.Itoa(maxKeyTTLHours)
	}
	return time.Duration(*h) * time.Hour, ""
}

func (c *ControlAPI) createKey(w http.ResponseWriter, r *http.Request, a *authed, t Tenant) {
	var req struct {
		Name     string `json:"name"`
		TTLHours *int   `json:"ttl_hours"`
	}
	if !c.decode(w, r, &req, false) {
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" || len(name) > 64 || strings.ContainsAny(name, "\r\n\t") {
		writeError(w, http.StatusBadRequest, "invalid_name", "name must be 1-64 characters")
		return
	}
	ttl, msg := ttlFromHours(req.TTLHours)
	if msg != "" {
		writeError(w, http.StatusBadRequest, "invalid_ttl", msg)
		return
	}
	n, err := c.activeKeyCount(r, t.ID)
	if err != nil {
		c.internal(w, "count keys", err)
		return
	}
	if n >= c.cfg.MaxKeysPerTenant {
		writeError(w, http.StatusConflict, "key_limit_reached", "revoke an existing key before creating another (limit "+strconv.Itoa(c.cfg.MaxKeysPerTenant)+")")
		return
	}
	plain, ak, err := CreateAPIKey(r.Context(), c.store, t.ID, name, ttl, c.cfg.now())
	if err != nil {
		c.internal(w, "create key", err)
		return
	}
	c.audit(r, "key_created", slog.String("user_id", a.user.ID), slog.String("tenant", t.ID), slog.String("prefix", ak.Prefix))
	k := c.keyOut(ak)
	writeJSON(w, http.StatusCreated, map[string]any{"key": plain, "info": k})
}

func (c *ControlAPI) findKey(r *http.Request, tenantID, prefix string) (APIKey, error) {
	keys, err := c.store.ListKeys(r.Context(), tenantID)
	if err != nil {
		return APIKey{}, err
	}
	for _, k := range keys {
		if k.Prefix == prefix {
			return k, nil
		}
	}
	return APIKey{}, ErrAPIKeyNotFound
}

func (c *ControlAPI) rotateKey(w http.ResponseWriter, r *http.Request, a *authed, t Tenant) {
	prefix := r.PathValue("prefix")
	if !keyPrefixRe.MatchString(prefix) {
		writeError(w, http.StatusBadRequest, "invalid_prefix", "key prefix must look like dkv_live_ab12cd34")
		return
	}
	var req struct {
		TTLHours *int `json:"ttl_hours"`
	}
	if !c.decode(w, r, &req, true) {
		return
	}
	ttl, msg := ttlFromHours(req.TTLHours)
	if msg != "" {
		writeError(w, http.StatusBadRequest, "invalid_ttl", msg)
		return
	}
	old, err := c.findKey(r, t.ID, prefix)
	if errors.Is(err, ErrAPIKeyNotFound) {
		writeError(w, http.StatusNotFound, "key_not_found", "no such key")
		return
	}
	if err != nil {
		c.internal(w, "find key", err)
		return
	}
	if c.keyOut(old).Status != "active" {
		writeError(w, http.StatusConflict, "key_not_active", "only active keys can be rotated")
		return
	}
	plain, ak, err := RotateAPIKey(r.Context(), c.store, t.ID, prefix, ttl, c.cfg.now())
	if err != nil {
		c.internal(w, "rotate key", err)
		return
	}
	c.audit(r, "key_rotated", slog.String("user_id", a.user.ID), slog.String("tenant", t.ID),
		slog.String("old_prefix", prefix), slog.String("new_prefix", ak.Prefix))
	writeJSON(w, http.StatusCreated, map[string]any{"key": plain, "info": c.keyOut(ak), "revoked_prefix": prefix})
}

func (c *ControlAPI) revokeKey(w http.ResponseWriter, r *http.Request, a *authed, t Tenant) {
	prefix := r.PathValue("prefix")
	if !keyPrefixRe.MatchString(prefix) {
		writeError(w, http.StatusBadRequest, "invalid_prefix", "key prefix must look like dkv_live_ab12cd34")
		return
	}
	err := c.store.RevokeKey(r.Context(), t.ID, prefix, c.cfg.now())
	if errors.Is(err, ErrAPIKeyNotFound) {
		writeError(w, http.StatusNotFound, "key_not_found", "no such key")
		return
	}
	if err != nil {
		c.internal(w, "revoke key", err)
		return
	}
	c.audit(r, "key_revoked", slog.String("user_id", a.user.ID), slog.String("tenant", t.ID), slog.String("prefix", prefix))
	w.WriteHeader(http.StatusNoContent)
}

// ------------------------------------------------------------------ admin

func (c *ControlAPI) adminTenant(w http.ResponseWriter, r *http.Request) (Tenant, bool) {
	id := r.PathValue("id")
	if err := ValidateTenantID(id); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_tenant_id", err.Error())
		return Tenant{}, false
	}
	t, err := c.store.GetTenant(r.Context(), id)
	if errors.Is(err, ErrTenantNotFound) {
		writeError(w, http.StatusNotFound, "tenant_not_found", "no such tenant")
		return Tenant{}, false
	}
	if err != nil {
		c.internal(w, "get tenant", err)
		return Tenant{}, false
	}
	return t, true
}

func (c *ControlAPI) adminListTenants(w http.ResponseWriter, r *http.Request, _ *authed) {
	limit := 50
	if s := r.URL.Query().Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > 200 {
			writeError(w, http.StatusBadRequest, "invalid_limit", "limit must be 1-200")
			return
		}
		limit = n
	}
	after := r.URL.Query().Get("after")
	ts, err := c.store.ListTenants(r.Context(), after, limit)
	if err != nil {
		c.internal(w, "list tenants", err)
		return
	}
	out := make([]tenantDTO, 0, len(ts))
	for _, t := range ts {
		out = append(out, c.tenantOut(t))
	}
	next := ""
	if len(ts) == limit {
		next = ts[len(ts)-1].ID
	}
	writeJSON(w, http.StatusOK, map[string]any{"tenants": out, "next_after": next})
}

func (c *ControlAPI) adminGetTenant(w http.ResponseWriter, r *http.Request, _ *authed) {
	if t, ok := c.adminTenant(w, r); ok {
		writeJSON(w, http.StatusOK, map[string]any{"tenant": c.tenantOut(t)})
	}
}

func (c *ControlAPI) adminSetQuota(w http.ResponseWriter, r *http.Request, a *authed) {
	t, ok := c.adminTenant(w, r)
	if !ok {
		return
	}
	var req struct {
		RPS   *float64 `json:"rps"`
		Burst *int     `json:"burst"`
	}
	if !c.decode(w, r, &req, false) {
		return
	}
	if req.RPS == nil || req.Burst == nil {
		writeError(w, http.StatusBadRequest, "invalid_quota", "both rps and burst are required (0 0 = gateway default)")
		return
	}
	if *req.RPS < 0 || *req.RPS > 1_000_000 || math.IsNaN(*req.RPS) || *req.Burst < 0 || *req.Burst > 10_000_000 {
		writeError(w, http.StatusBadRequest, "invalid_quota", "rps must be 0-1000000 and burst 0-10000000")
		return
	}
	if err := c.store.SetTenantQuota(r.Context(), t.ID, *req.RPS, *req.Burst); err != nil {
		c.internal(w, "set quota", err)
		return
	}
	c.audit(r, "quota_set", slog.String("admin_id", a.user.ID), slog.String("tenant", t.ID),
		slog.Float64("rps", *req.RPS), slog.Int("burst", *req.Burst))
	t.QuotaRPS, t.QuotaBurst = *req.RPS, *req.Burst
	writeJSON(w, http.StatusOK, map[string]any{"tenant": c.tenantOut(t)})
}

func (c *ControlAPI) adminUsage(w http.ResponseWriter, r *http.Request, _ *authed) {
	if t, ok := c.adminTenant(w, r); ok {
		writeJSON(w, http.StatusOK, map[string]any{"tenant_id": t.ID, "usage": c.usageFor(t.ID)})
	}
}

func (c *ControlAPI) adminUsageSeries(w http.ResponseWriter, r *http.Request, _ *authed) {
	t, ok := c.adminTenant(w, r)
	if !ok {
		return
	}
	s, ok := c.seriesFor(w, t.ID, r.URL.Query().Get("range"))
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tenant_id": t.ID, "series": s})
}

func (c *ControlAPI) adminListKeys(w http.ResponseWriter, r *http.Request, _ *authed) {
	t, ok := c.adminTenant(w, r)
	if !ok {
		return
	}
	out, err := c.listKeysFor(r, t.ID)
	if err != nil {
		c.internal(w, "list keys", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"keys": out})
}

func (c *ControlAPI) adminRevokeKey(w http.ResponseWriter, r *http.Request, a *authed) {
	t, ok := c.adminTenant(w, r)
	if !ok {
		return
	}
	prefix := r.PathValue("prefix")
	if !keyPrefixRe.MatchString(prefix) {
		writeError(w, http.StatusBadRequest, "invalid_prefix", "key prefix must look like dkv_live_ab12cd34")
		return
	}
	err := c.store.RevokeKey(r.Context(), t.ID, prefix, c.cfg.now())
	if errors.Is(err, ErrAPIKeyNotFound) {
		writeError(w, http.StatusNotFound, "key_not_found", "no such key")
		return
	}
	if err != nil {
		c.internal(w, "revoke key", err)
		return
	}
	c.audit(r, "admin_key_revoked", slog.String("admin_id", a.user.ID), slog.String("tenant", t.ID), slog.String("prefix", prefix))
	w.WriteHeader(http.StatusNoContent)
}

// -------------------------------------------------------------------- run

// Run listens on cfg.BindAddr and serves until ctx is cancelled.
func (c *ControlAPI) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", c.cfg.BindAddr)
	if err != nil {
		return err
	}
	return c.Serve(ctx, ln)
}

// Serve serves on ln (HTTPS when both TLS files are set) and prunes expired
// sessions every 10 minutes.
func (c *ControlAPI) Serve(ctx context.Context, ln net.Listener) error {
	if (c.cfg.TLSCertFile == "") != (c.cfg.TLSKeyFile == "") {
		_ = ln.Close()
		return errors.New("control: TLS needs both a certificate and a key file")
	}
	srv := &http.Server{
		Handler:           c.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	go func() {
		t := time.NewTicker(10 * time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = srv.Shutdown(sctx)
				return
			case <-t.C:
				_, _ = c.store.DeleteExpiredSessions(ctx, c.cfg.now())
			}
		}
	}()
	if c.cfg.TLSCertFile != "" {
		srv.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		return srv.ServeTLS(ln, c.cfg.TLSCertFile, c.cfg.TLSKeyFile)
	}
	return srv.Serve(ln)
}
