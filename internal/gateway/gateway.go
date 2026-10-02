// Package gateway is the public HTTP front door of DistriKV: it authenticates
// API keys, maps each key to a tenant, rate-limits per tenant, namespaces every
// key so tenants cannot see each other's data, and forwards to the cluster.
//
// The data plane never sees a tenant ID from a client: the only place that
// builds storage keys is NamespaceKey (namespace.go).
package gateway

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

var (
	ErrInvalidAPIKey = errors.New("invalid API key")
	ErrMissingAuth   = errors.New("missing Authorization header")
	ErrMalformedAuth = errors.New("malformed Authorization header: expected 'Bearer <key>'")
	ErrRateLimit     = errors.New("rate limit exceeded")

	// Data-plane errors. The KV adapter (pool.go) translates the cluster
	// client's errors into these, so this package stays free of gRPC.
	ErrKeyNotFound = errors.New("key not found")
	ErrNotInteger  = errors.New("stored value is not an integer")
	ErrOverflow    = errors.New("integer overflow")
	ErrUnavailable = errors.New("cluster unavailable")
)

// KV is the data plane as the gateway sees it. CAS follows the cluster
// semantics: expected == nil means "the key must not exist yet".
type KV interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
	CAS(ctx context.Context, key string, expected, newValue []byte) (bool, error)
	Incr(ctx context.Context, key string, delta int64) (int64, error)
	Close() error
}

// Config holds gateway settings. Zero values get defaults.
type Config struct {
	BindAddr       string
	RateLimitRPS   float64       // default per-tenant requests/second (default 1000)
	RateLimitBurst int           // default per-tenant burst (default 2000)
	MaxBodyBytes   int64         // max request body (default 1 MiB)
	MaxValueBytes  int           // max value size (default 256 KiB)
	RequestTimeout time.Duration // per-request deadline toward the cluster (default 5s)

	// MaxConcurrentPerTenant caps in-flight requests per tenant; extra
	// requests get 429 immediately (never queued). Default 128.
	MaxConcurrentPerTenant int

	// TLSCertFile and TLSKeyFile enable HTTPS when both are set. Setting
	// only one is an error. Empty = plain HTTP (dev/demo, or behind a
	// TLS-terminating proxy).
	TLSCertFile string
	TLSKeyFile  string

	// MetricsAddr, if set, serves /metrics on a separate listener. Keep it
	// on a private network: it exposes tenant IDs.
	MetricsAddr string
}

func (c *Config) applyDefaults() {
	if c.RateLimitRPS <= 0 {
		c.RateLimitRPS = 1000
	}
	if c.RateLimitBurst <= 0 {
		c.RateLimitBurst = 2000
	}
	if c.MaxBodyBytes <= 0 {
		c.MaxBodyBytes = 1 << 20
	}
	if c.MaxValueBytes <= 0 {
		c.MaxValueBytes = 256 << 10
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 5 * time.Second
	}
	if c.MaxConcurrentPerTenant <= 0 {
		c.MaxConcurrentPerTenant = 128
	}
}

// Gateway implements http.Handler.
type Gateway struct {
	cfg   Config
	store Store
	kv    KV
	now   func() time.Time // injectable for tests

	metrics *Metrics

	mu       sync.Mutex
	limiters map[string]*rateLimiter
	sems     map[string]chan struct{} // per-tenant concurrency slots
}

// New builds a gateway over a Store (accounts and keys) and a KV (data plane).
func New(cfg Config, store Store, kv KV) *Gateway {
	cfg.applyDefaults()
	g := &Gateway{
		cfg:      cfg,
		store:    store,
		kv:       kv,
		now:      time.Now,
		limiters: make(map[string]*rateLimiter),
		sems:     make(map[string]chan struct{}),
		metrics:  NewMetrics(time.Now()),
	}
	g.metrics.now = func() time.Time { return g.now() }
	if p, ok := store.(UsagePersister); ok {
		g.metrics.SetPersister(p)
	}
	return g
}

// Metrics returns the gateway's counters (for tests and embedding).
func (g *Gateway) Metrics() *Metrics { return g.metrics }

// semFor returns the tenant's concurrency semaphore.
func (g *Gateway) semFor(tenantID string) chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	s, ok := g.sems[tenantID]
	if !ok {
		s = make(chan struct{}, g.cfg.MaxConcurrentPerTenant)
		g.sems[tenantID] = s
	}
	return s
}

// Close flushes durable usage then closes the data-plane client. The caller owns the Store.
func (g *Gateway) Close() error {
	fctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_ = g.metrics.Flush(fctx)
	cancel()
	return g.kv.Close()
}

// ---------------------------------------------------------------- rate limit

type rateLimiter struct {
	mu       sync.Mutex
	tokens   float64
	capacity float64
	rate     float64
	last     time.Time
}

func newLimiter(rate float64, burst int, now time.Time) *rateLimiter {
	return &rateLimiter{tokens: float64(burst), capacity: float64(burst), rate: rate, last: now}
}

// allow takes one token. When it refuses, it also reports how long until a
// token is available (used for the Retry-After header).
func (l *rateLimiter) allow(now time.Time) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if el := now.Sub(l.last).Seconds(); el > 0 {
		l.tokens = math.Min(l.capacity, l.tokens+el*l.rate)
		l.last = now
	}
	if l.tokens >= 1 {
		l.tokens--
		return true, 0
	}
	wait := time.Duration((1 - l.tokens) / l.rate * float64(time.Second))
	return false, wait
}

// limiterFor returns the tenant's limiter, rebuilding it if the tenant's
// quota changed since it was created (so `gatewayctl set-quota` takes effect
// without a restart).
func (g *Gateway) limiterFor(t Tenant) *rateLimiter {
	rate, burst := g.cfg.RateLimitRPS, g.cfg.RateLimitBurst
	if t.QuotaRPS > 0 {
		rate = t.QuotaRPS
	}
	if t.QuotaBurst > 0 {
		burst = t.QuotaBurst
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	l, ok := g.limiters[t.ID]
	if !ok || l.rate != rate || l.capacity != float64(burst) {
		l = newLimiter(rate, burst, g.now())
		g.limiters[t.ID] = l
	}
	return l
}

// ---------------------------------------------------------------------- auth

func (g *Gateway) authenticate(ctx context.Context, r *http.Request) (Tenant, error) {
	h := r.Header.Get("Authorization")
	if h == "" {
		return Tenant{}, ErrMissingAuth
	}
	if !strings.HasPrefix(h, "Bearer ") {
		return Tenant{}, ErrMalformedAuth
	}
	key := strings.TrimSpace(strings.TrimPrefix(h, "Bearer "))
	// Cheap shape check first: junk never reaches the database.
	if len(key) != keyLen || !strings.HasPrefix(key, KeyPrefix) {
		return Tenant{}, ErrInvalidAPIKey
	}
	ak, err := g.store.GetKeyByHash(ctx, hashKey(key))
	if err != nil {
		if errors.Is(err, ErrAPIKeyNotFound) {
			return Tenant{}, ErrInvalidAPIKey
		}
		return Tenant{}, err
	}
	now := g.now()
	if ak.RevokedAt != nil {
		return Tenant{}, ErrInvalidAPIKey
	}
	if ak.ExpiresAt != nil && !now.Before(*ak.ExpiresAt) {
		return Tenant{}, ErrInvalidAPIKey
	}
	t, err := g.store.GetTenant(ctx, ak.TenantID)
	if err != nil {
		if errors.Is(err, ErrTenantNotFound) {
			return Tenant{}, ErrInvalidAPIKey
		}
		return Tenant{}, err
	}
	return t, nil
}

// ---------------------------------------------------------------------- HTTP

type errBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	var b errBody
	b.Error.Code, b.Error.Message = code, msg
	writeJSON(w, status, b)
}

// ServeHTTP: health check, then auth, then rate limit, then concurrency
// cap, then route. Every non-health request is counted in the metrics.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
		return
	}

	start := time.Now()
	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK, op: routeOp(r)}
	w = rec
	tenantLabel := "none" // unauthenticated requests
	defer func() {
		g.metrics.observe(tenantLabel, rec.op, rec.status, time.Since(start))
	}()

	ctx, cancel := context.WithTimeout(r.Context(), g.cfg.RequestTimeout)
	defer cancel()
	r = r.WithContext(ctx)

	tenant, err := g.authenticate(ctx, r)
	switch {
	case errors.Is(err, ErrMissingAuth), errors.Is(err, ErrMalformedAuth), errors.Is(err, ErrInvalidAPIKey):
		w.Header().Set("WWW-Authenticate", `Bearer realm="distrikv"`)
		writeError(w, http.StatusUnauthorized, "unauthorized", err.Error())
		return
	case err != nil:
		log.Printf("gateway: auth backend error: %v", err)
		writeError(w, http.StatusServiceUnavailable, "auth_unavailable", "authentication temporarily unavailable")
		return
	}
	tenantLabel = tenant.ID

	if ok, wait := g.limiterFor(tenant).allow(g.now()); !ok {
		g.metrics.incRateLimited(tenant.ID)
		secs := int(math.Ceil(wait.Seconds()))
		if secs < 1 {
			secs = 1
		}
		w.Header().Set("Retry-After", strconv.Itoa(secs))
		writeError(w, http.StatusTooManyRequests, "rate_limited", ErrRateLimit.Error())
		return
	}

	// Concurrency cap: non-blocking, so one tenant can never pile up
	// goroutines waiting for a slot.
	sem := g.semFor(tenant.ID)
	select {
	case sem <- struct{}{}:
		defer func() { <-sem }()
	default:
		g.metrics.incConcLimited(tenant.ID)
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusTooManyRequests, "too_many_concurrent_requests",
			"too many in-flight requests for this tenant")
		return
	}

	switch {
	case r.URL.Path == "/v1/usage" && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, g.metrics.Usage(tenant.ID))
	case r.URL.Path == "/tenant/usage/series" && r.Method == http.MethodGet:
		rng := r.URL.Query().Get("range")
		if rng == "" {
			rng = "24h"
		}
		series, err := g.metrics.Series(tenant.ID, rng)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_range", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"tenant_id": tenant.ID,
			"series":    series,
		})
	case r.URL.Path == "/kv" && r.Method == http.MethodPost:
		g.handleOp(w, r, tenant.ID)
	case strings.HasPrefix(r.URL.Path, "/kv/"):
		key := strings.TrimPrefix(r.URL.Path, "/kv/")
		switch r.Method {
		case http.MethodGet:
			g.handleGet(w, r, tenant.ID, key)
		case http.MethodPut:
			g.handlePut(w, r, tenant.ID, key)
		case http.MethodDelete:
			g.handleDelete(w, r, tenant.ID, key)
		default:
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use GET, PUT or DELETE on /kv/{key}")
		}
	default:
		writeError(w, http.StatusNotFound, "not_found", "unknown route")
	}
}

// storageKey is the single choke point for building data-plane keys.
func storageKey(w http.ResponseWriter, tenantID, key string) (string, bool) {
	ns, err := NamespaceKey(tenantID, key)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_key", err.Error())
		return "", false
	}
	return ns, true
}

func (g *Gateway) readJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, g.cfg.MaxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "body_too_large", "request body too large")
		} else {
			writeError(w, http.StatusBadRequest, "invalid_json", "request body is not valid JSON for this endpoint")
		}
		return false
	}
	return true
}

func (g *Gateway) valueOK(w http.ResponseWriter, v *string) bool {
	if v == nil {
		writeError(w, http.StatusBadRequest, "value_required", "\"value\" is required")
		return false
	}
	if len(*v) > g.cfg.MaxValueBytes || !utf8.ValidString(*v) {
		writeError(w, http.StatusRequestEntityTooLarge, "value_too_large", "value exceeds the maximum size")
		return false
	}
	return true
}

func (g *Gateway) handleGet(w http.ResponseWriter, r *http.Request, tenantID, key string) {
	ns, ok := storageKey(w, tenantID, key)
	if !ok {
		return
	}
	val, err := g.kv.Get(r.Context(), ns)
	if err != nil {
		g.kvError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"value": string(val)})
}

func (g *Gateway) handlePut(w http.ResponseWriter, r *http.Request, tenantID, key string) {
	ns, ok := storageKey(w, tenantID, key)
	if !ok {
		return
	}
	var req struct {
		Value *string `json:"value"`
	}
	if !g.readJSON(w, r, &req) || !g.valueOK(w, req.Value) {
		return
	}
	if err := g.kv.Put(r.Context(), ns, []byte(*req.Value)); err != nil {
		g.kvError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (g *Gateway) handleDelete(w http.ResponseWriter, r *http.Request, tenantID, key string) {
	ns, ok := storageKey(w, tenantID, key)
	if !ok {
		return
	}
	if err := g.kv.Delete(r.Context(), ns); err != nil {
		g.kvError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleOp serves POST /kv with {"op": "set"|"cas"|"incr"|"decr", "key": ...}.
//
//	set : {"op":"set","key":"k","value":"v"}
//	cas : {"op":"cas","key":"k","expected":"old","value":"new"}
//	      omit "expected" (or null) to mean "only if the key does not exist"
//	incr: {"op":"incr","key":"k","delta":5}   (delta defaults to 1, must be > 0)
//	decr: {"op":"decr","key":"k","delta":5}
func (g *Gateway) handleOp(w http.ResponseWriter, r *http.Request, tenantID string) {
	var req struct {
		Op       string  `json:"op"`
		Key      string  `json:"key"`
		Value    *string `json:"value"`
		Expected *string `json:"expected"`
		Delta    *int64  `json:"delta"`
	}
	if !g.readJSON(w, r, &req) {
		return
	}
	switch req.Op {
	case "set", "cas", "incr", "decr":
		setOp(w, req.Op)
	}
	ns, ok := storageKey(w, tenantID, req.Key)
	if !ok {
		return
	}

	switch req.Op {
	case "set":
		if !g.valueOK(w, req.Value) {
			return
		}
		if err := g.kv.Put(r.Context(), ns, []byte(*req.Value)); err != nil {
			g.kvError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})

	case "cas":
		if !g.valueOK(w, req.Value) {
			return
		}
		var expected []byte // nil => key must not exist
		if req.Expected != nil {
			if len(*req.Expected) > g.cfg.MaxValueBytes {
				writeError(w, http.StatusRequestEntityTooLarge, "value_too_large", "expected value exceeds the maximum size")
				return
			}
			expected = []byte(*req.Expected)
			if expected == nil {
				expected = []byte{} // present-but-empty is not "absent"
			}
		}
		applied, err := g.kv.CAS(r.Context(), ns, expected, []byte(*req.Value))
		if err != nil {
			g.kvError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"applied": applied})

	case "incr", "decr":
		delta := int64(1)
		if req.Delta != nil {
			delta = *req.Delta
		}
		if delta <= 0 {
			writeError(w, http.StatusBadRequest, "invalid_delta", "delta must be a positive integer")
			return
		}
		if req.Op == "decr" {
			delta = -delta
		}
		v, err := g.kv.Incr(r.Context(), ns, delta)
		if err != nil {
			g.kvError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]int64{"value": v})

	default:
		writeError(w, http.StatusBadRequest, "unknown_op", "op must be one of: set, cas, incr, decr")
	}
}

// kvError maps data-plane errors to HTTP. Internal details are logged, never
// returned to the customer.
func (g *Gateway) kvError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrKeyNotFound):
		writeError(w, http.StatusNotFound, "not_found", "key not found")
	case errors.Is(err, ErrNotInteger):
		writeError(w, http.StatusConflict, "not_integer", "stored value is not an integer")
	case errors.Is(err, ErrOverflow):
		writeError(w, http.StatusConflict, "overflow", "integer overflow")
	case errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusGatewayTimeout, "timeout", "the cluster did not answer in time")
	case errors.Is(err, ErrUnavailable), errors.Is(err, context.Canceled):
		writeError(w, http.StatusServiceUnavailable, "unavailable", "cluster temporarily unavailable")
	default:
		log.Printf("gateway: internal error: %v", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error")
	}
}

// Run serves HTTP (or HTTPS) until ctx is cancelled.
func (g *Gateway) Run(ctx context.Context) error {
	tlsOn := g.cfg.TLSCertFile != "" || g.cfg.TLSKeyFile != ""
	if tlsOn && (g.cfg.TLSCertFile == "" || g.cfg.TLSKeyFile == "") {
		return errors.New("gateway: TLS needs both a certificate and a key file")
	}

	srv := &http.Server{
		Addr:              g.cfg.BindAddr,
		Handler:           g,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	if tlsOn {
		srv.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	var msrv *http.Server
	if g.cfg.MetricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", g.metrics.Handler())
		msrv = &http.Server{Addr: g.cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			log.Printf("gateway metrics listening on %s", g.cfg.MetricsAddr)
			if err := msrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("gateway: metrics server: %v", err)
			}
		}()
	}

	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = g.metrics.Flush(sctx)
		_ = srv.Shutdown(sctx)
		if msrv != nil {
			_ = msrv.Shutdown(sctx)
		}
	}()

	if g.metrics.persister != nil {
		go g.metrics.runFlusher(ctx, 15*time.Second)
	}

	if tlsOn {
		log.Printf("gateway listening on %s (TLS)", g.cfg.BindAddr)
		return srv.ListenAndServeTLS(g.cfg.TLSCertFile, g.cfg.TLSKeyFile)
	}
	log.Printf("gateway listening on %s (plain HTTP)", g.cfg.BindAddr)
	return srv.ListenAndServe()
}
