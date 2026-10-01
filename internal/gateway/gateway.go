package gateway

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"distrikv/internal/kv"
	"distrikv/pkg/client"
)

var (
	ErrInvalidAPIKey   = errors.New("invalid API key")
	ErrTenantNotFound  = errors.New("tenant not found")
	ErrQuotaExceeded   = errors.New("quota exceeded")
	ErrRateLimit       = errors.New("rate limit exceeded")
	ErrMissingAuth     = errors.New("missing Authorization header")
	ErrMalformedAuth   = errors.New("malformed Authorization header")
)

// Config holds gateway configuration
type Config struct {
	KVAddresses   []string
	BindAddr      string
	RateLimitRPS  float64 // requests per second per tenant
	RateLimitBurst int    // burst allowance
}

// Tenant represents a tenant in the system
type Tenant struct {
	ID        string
	Name      string
	QuotaRPS  float64 // per-tenant override
	QuotaBurst int
	CreatedAt time.Time
}

// APIKey represents a hashed API key
type APIKey struct {
	Hash      string    // sha256(key)
	TenantID  string
	Name      string    // human-readable name
	CreatedAt time.Time
	ExpiresAt *time.Time // nil = no expiry
	Active    bool
}

// Gateway is the HTTP-to-gRPC proxy with auth/tenancy/rate-limiting
type Gateway struct {
	cfg         Config
	kvClient    *client.Client
	mu          sync.RWMutex
	tenants     map[string]*Tenant
	apiKeys     map[string]*APIKey // hash -> APIKey
	rateLimiters map[string]*rateLimiter // tenantID -> limiter
}

type rateLimiter struct {
	tokens     float64
	lastRefill time.Time
	mu         sync.Mutex
	capacity   float64
	rate       float64
}

// NewGateway creates a new gateway
func NewGateway(cfg Config) (*Gateway, error) {
	cli, err := client.Dial(context.Background(), cfg.KVAddresses[0])
	if err != nil {
		return nil, fmt.Errorf("dial kv: %w", err)
	}
	return &Gateway{
		cfg:          cfg,
		kvClient:     cli,
		tenants:      make(map[string]*Tenant),
		apiKeys:      make(map[string]*APIKey),
		rateLimiters: make(map[string]*rateLimiter),
	}, nil
}

// CreateTenant creates a new tenant
func (g *Gateway) CreateTenant(id, name string) *Tenant {
	g.mu.Lock()
	defer g.mu.Unlock()
	t := &Tenant{
		ID:        id,
		Name:      name,
		QuotaRPS:  g.cfg.RateLimitRPS,
		QuotaBurst: g.cfg.RateLimitBurst,
		CreatedAt: time.Now(),
	}
	g.tenants[id] = t
	return t
}

// GetTenant retrieves a tenant by ID
func (g *Gateway) GetTenant(id string) (*Tenant, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	t, ok := g.tenants[id]
	return t, ok
}

// CreateAPIKey generates a new API key for a tenant
func (g *Gateway) CreateAPIKey(tenantID, name string, ttl time.Duration) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.tenants[tenantID]; !ok {
		return "", ErrTenantNotFound
	}

	// Generate random key: "dkv_" + 32 random bytes
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	key := "dkv_" + hex.EncodeToString(raw)

	// Store hash
	hash := sha256.Sum256([]byte(key))
	hashStr := hex.EncodeToString(hash[:])

	var expires *time.Time
	if ttl > 0 {
		exp := time.Now().Add(ttl)
		expires = &exp
	} else if ttl < 0 {
		// Negative TTL = already expired
		exp := time.Now().Add(ttl)
		expires = &exp
	}

	g.apiKeys[hashStr] = &APIKey{
		Hash:      hashStr,
		TenantID:  tenantID,
		Name:      name,
		CreatedAt: time.Now(),
		ExpiresAt: expires,
		Active:    true,
	}

	return key, nil
}

// ValidateAPIKey checks the API key and returns the tenant ID
func (g *Gateway) ValidateAPIKey(key string) (string, error) {
	if !strings.HasPrefix(key, "dkv_") {
		return "", ErrInvalidAPIKey
	}
	hash := sha256.Sum256([]byte(key))
	hashStr := hex.EncodeToString(hash[:])

	g.mu.RLock()
	defer g.mu.RUnlock()

	ak, ok := g.apiKeys[hashStr]
	if !ok || !ak.Active {
		return "", ErrInvalidAPIKey
	}
	if ak.ExpiresAt != nil && time.Now().After(*ak.ExpiresAt) {
		return "", ErrInvalidAPIKey
	}
	return ak.TenantID, nil
}

// getLimiter returns (or creates) a rate limiter for a tenant
func (g *Gateway) getLimiter(tenantID string) *rateLimiter {
	g.mu.Lock()
	defer g.mu.Unlock()

	lim, ok := g.rateLimiters[tenantID]
	if !ok {
		// Use tenant-specific or global config
		tenant, _ := g.tenants[tenantID]
		rate := g.cfg.RateLimitRPS
		burst := g.cfg.RateLimitBurst
		if tenant != nil && tenant.QuotaRPS > 0 {
			rate = tenant.QuotaRPS
		}
		if tenant != nil && tenant.QuotaBurst > 0 {
			burst = tenant.QuotaBurst
		}
		lim = &rateLimiter{
			tokens:     float64(burst),
			lastRefill: time.Now(),
			capacity:   float64(burst),
			rate:       rate,
		}
		g.rateLimiters[tenantID] = lim
	}
	return lim
}

// Allow checks if the request is allowed under rate limit
func (l *rateLimiter) Allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(l.lastRefill).Seconds()
	l.tokens = min(l.capacity, l.tokens+elapsed*l.rate)
	l.lastRefill = now

	if l.tokens >= 1 {
		l.tokens--
		return true
	}
	return false
}

// namespaceKey adds tenant prefix to a key
func namespaceKey(tenantID, key string) string {
	return "tenant:" + tenantID + ":" + key
}

// ServeHTTP implements http.Handler
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Extract API key
	auth := r.Header.Get("Authorization")
	if auth == "" {
		http.Error(w, ErrMissingAuth.Error(), http.StatusUnauthorized)
		return
	}
	if !strings.HasPrefix(auth, "Bearer ") {
		http.Error(w, ErrMalformedAuth.Error(), http.StatusUnauthorized)
		return
	}
	key := strings.TrimPrefix(auth, "Bearer ")

	tenantID, err := g.ValidateAPIKey(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	// Rate limit
	if !g.getLimiter(tenantID).Allow() {
		http.Error(w, ErrRateLimit.Error(), http.StatusTooManyRequests)
		return
	}

	// Route
	switch r.Method {
	case http.MethodGet:
		g.handleGet(w, r, tenantID)
	case http.MethodPost:
		g.handlePost(w, r, tenantID)
	case http.MethodPut:
		g.handlePut(w, r, tenantID)
	case http.MethodDelete:
		g.handleDelete(w, r, tenantID)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleGet processes GET /kv/{key}
func (g *Gateway) handleGet(w http.ResponseWriter, r *http.Request, tenantID string) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}

	val, err := g.kvClient.Get(r.Context(), namespaceKey(tenantID, key))
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"value": string(val)})
}

// handlePost processes POST /kv (set, cas, increment, decrement)
func (g *Gateway) handlePost(w http.ResponseWriter, r *http.Request, tenantID string) {
	var req struct {
		Op       string `json:"op"` // "set", "cas", "incr", "decr"
		Key      string `json:"key"`
		Value    string `json:"value,omitempty"`
		Expected string `json:"expected,omitempty"` // for CAS
	}
	if r.Body == nil {
		http.Error(w, "request body required", http.StatusBadRequest)
		return
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	nsKey := namespaceKey(tenantID, req.Key)

	switch req.Op {
	case "set":
		if err := g.kvClient.Put(r.Context(), nsKey, []byte(req.Value)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)

	case "cas":
		if req.Expected == "" {
			http.Error(w, "expected value required for CAS", http.StatusBadRequest)
			return
		}
		applied, err := g.kvClient.CAS(r.Context(), nsKey, []byte(req.Expected), []byte(req.Value))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string]bool{"applied": applied})

	case "incr":
		val, err := g.kvClient.IncrBy(r.Context(), nsKey)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string]int64{"value": val})

	case "decr":
		val, err := g.kvClient.Decr(r.Context(), nsKey)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string]int64{"value": val})

	default:
		http.Error(w, "unknown op", http.StatusBadRequest)
	}
}

// handlePut is alias for set
func (g *Gateway) handlePut(w http.ResponseWriter, r *http.Request, tenantID string) {
	r.Method = http.MethodPost
	g.handlePost(w, r, tenantID)
}

// handleDelete processes DELETE /kv/{key}
func (g *Gateway) handleDelete(w http.ResponseWriter, r *http.Request, tenantID string) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}
	nsKey := namespaceKey(tenantID, key)
	if err := g.kvClient.Delete(r.Context(), nsKey); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// Run starts the HTTP server
func (g *Gateway) Run(ctx context.Context) error {
	srv := &http.Server{
		Addr:         g.cfg.BindAddr,
		Handler:      g,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()

	log.Printf("gateway listening on %s", g.cfg.BindAddr)
	return srv.ListenAndServe()
}

// Close closes the gateway
func (g *Gateway) Close() error {
	return g.kvClient.Close()
}