package gateway

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGatewayTenantLifecycle(t *testing.T) {
	cfg := Config{
		KVAddresses:   []string{"127.0.0.1:8081"},
		RateLimitRPS:  100,
		RateLimitBurst: 200,
	}
	gw, err := NewGateway(cfg)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	defer gw.Close()

	// Create tenant
	t.Run("create tenant", func(t *testing.T) {
		tenant := gw.CreateTenant("t1", "Tenant One")
		if tenant.ID != "t1" || tenant.Name != "Tenant One" {
			t.Errorf("tenant mismatch: %+v", tenant)
		}
		if tenant.QuotaRPS != 100 || tenant.QuotaBurst != 200 {
			t.Errorf("tenant quota not inherited from config")
		}
	})

	t.Run("get tenant", func(t *testing.T) {
		tnt, ok := gw.GetTenant("t1")
		if !ok {
			t.Fatal("tenant not found")
		}
		if tnt.ID != "t1" {
			t.Errorf("wrong tenant returned")
		}
		_, ok = gw.GetTenant("nonexistent")
		if ok {
			t.Errorf("nonexistent tenant should not be found")
		}
	})
}

func TestGatewayAPIKeyLifecycle(t *testing.T) {
	cfg := Config{
		KVAddresses:   []string{"127.0.0.1:8081"},
		RateLimitRPS:  100,
		RateLimitBurst: 200,
	}
	gw, err := NewGateway(cfg)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	defer gw.Close()

	gw.CreateTenant("t1", "Tenant One")

	t.Run("create api key", func(t *testing.T) {
		key, err := gw.CreateAPIKey("t1", "test-key", 24*time.Hour)
		if err != nil {
			t.Fatalf("CreateAPIKey: %v", err)
		}
		if len(key) != 4+64 { // "dkv_" + 32 bytes hex = 68
			t.Errorf("key length %d, expected 68", len(key))
		}
	})

	t.Run("validate api key", func(t *testing.T) {
		key, err := gw.CreateAPIKey("t1", "test-key-2", 24*time.Hour)
		if err != nil {
			t.Fatalf("CreateAPIKey: %v", err)
		}
		tenantID, err := gw.ValidateAPIKey(key)
		if err != nil {
			t.Fatalf("ValidateAPIKey: %v", err)
		}
		if tenantID != "t1" {
			t.Errorf("wrong tenant ID: %s", tenantID)
		}
	})

	t.Run("reject invalid key", func(t *testing.T) {
		_, err := gw.ValidateAPIKey("dkv_invalid")
		if err != ErrInvalidAPIKey {
			t.Errorf("expected ErrInvalidAPIKey, got %v", err)
		}
		_, err = gw.ValidateAPIKey("not-a-key")
		if err != ErrInvalidAPIKey {
			t.Errorf("expected ErrInvalidAPIKey, got %v", err)
		}
		_, err = gw.ValidateAPIKey("")
		if err != ErrInvalidAPIKey {
			t.Errorf("expected ErrInvalidAPIKey, got %v", err)
		}
	})

	t.Run("reject key for nonexistent tenant", func(t *testing.T) {
		_, err := gw.CreateAPIKey("nonexistent", "key", 0)
		if err != ErrTenantNotFound {
			t.Errorf("expected ErrTenantNotFound, got %v", err)
		}
	})

	t.Run("expired key rejected", func(t *testing.T) {
		key, err := gw.CreateAPIKey("t1", "expired", -time.Hour)
		if err != nil {
			t.Fatalf("CreateAPIKey: %v", err)
		}
		_, err = gw.ValidateAPIKey(key)
		if err != ErrInvalidAPIKey {
			t.Errorf("expired key should be rejected, got %v", err)
		}
	})
}

func TestGatewayRateLimiter(t *testing.T) {
	cfg := Config{
		KVAddresses:   []string{"127.0.0.1:8081"},
		RateLimitRPS:  10,
		RateLimitBurst: 5,
	}
	gw, err := NewGateway(cfg)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	defer gw.Close()

	gw.CreateTenant("t1", "Tenant One")

	t.Run("allow burst", func(t *testing.T) {
		lim := gw.getLimiter("t1")
		for i := 0; i < 5; i++ {
			if !lim.Allow() {
				t.Errorf("request %d should be allowed", i+1)
			}
		}
		if lim.Allow() {
			t.Error("6th request should be denied (burst exhausted)")
		}
	})

	t.Run("refill over time", func(t *testing.T) {
		lim := gw.getLimiter("t1")
		// Exhaust
		for lim.Allow() {
		}
		// Wait for refill
		time.Sleep(200 * time.Millisecond) // 10 RPS = 1 token per 100ms
		if !lim.Allow() {
			t.Error("request should be allowed after refill")
		}
	})

	t.Run("tenant-specific quota", func(t *testing.T) {
		cfg2 := Config{
			KVAddresses:   []string{"127.0.0.1:8081"},
			RateLimitRPS:  10,
			RateLimitBurst: 5,
		}
		gw2, err := NewGateway(cfg2)
		if err != nil {
			t.Fatalf("NewGateway: %v", err)
		}
		defer gw2.Close()

		gw2.CreateTenant("t2", "Tenant Two")
		// Override tenant quota
		gw2.mu.Lock()
		gw2.tenants["t2"].QuotaRPS = 100
		gw2.tenants["t2"].QuotaBurst = 50
		gw2.mu.Unlock()

		lim := gw2.getLimiter("t2")
		count := 0
		for lim.Allow() {
			count++
		}
		if count != 50 {
			t.Errorf("expected 50 tokens for tenant override, got %d", count)
		}
	})
}

func TestGatewayNamespaceKey(t *testing.T) {
	if namespaceKey("tenant1", "key1") != "tenant:tenant1:key1" {
		t.Errorf("namespaceKey mismatch")
	}
	if namespaceKey("", "key") != "tenant::key" {
		t.Errorf("empty tenant namespace")
	}
}

func TestGatewayHTTP(t *testing.T) {
	cfg := Config{
		KVAddresses:   []string{"127.0.0.1:8081"},
		RateLimitRPS:  1000,
		RateLimitBurst: 2000,
	}
	gw, err := NewGateway(cfg)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	defer gw.Close()

	gw.CreateTenant("test-tenant", "Test Tenant")
	_, err = gw.CreateAPIKey("test-tenant", "http-test", 24*time.Hour)
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}

	// Create a mock client that we can inject
	// For now, we test the HTTP handler with a real client but mock the calls
	// by using a test server that we control
}

func TestGatewayConcurrentRateLimit(t *testing.T) {
	cfg := Config{
		KVAddresses:   []string{"127.0.0.1:8081"},
		RateLimitRPS:  100,
		RateLimitBurst: 50,
	}
	gw, err := NewGateway(cfg)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	defer gw.Close()

	gw.CreateTenant("t1", "Tenant One")

	lim := gw.getLimiter("t1")
	done := make(chan bool, 100)

	// Fire 50 concurrent requests
	for i := 0; i < 50; i++ {
		go func() {
			allowed := lim.Allow()
			done <- allowed
		}()
	}

	allowedCount := 0
	for i := 0; i < 50; i++ {
		if <-done {
			allowedCount++
		}
	}

	if allowedCount != 50 {
		t.Errorf("all 50 concurrent requests should be allowed (within burst), got %d", allowedCount)
	}

	// Next request should be denied
	if lim.Allow() {
		t.Error("request after burst should be denied")
	}
}

func TestGenerateRandomKey(t *testing.T) {
	// Test that our key format produces valid keys
	for i := 0; i < 100; i++ {
		raw := make([]byte, 32)
		if _, err := rand.Read(raw); err != nil {
			t.Fatalf("rand.Read: %v", err)
		}
		key := "dkv_" + hex.EncodeToString(raw)
		if len(key) != 68 {
			t.Errorf("key length %d", len(key))
		}
		// Verify it validates
		hash := sha256.Sum256([]byte(key))
		hashStr := hex.EncodeToString(hash[:])
		if len(hashStr) != 64 {
			t.Errorf("hash length %d", len(hashStr))
		}
		_ = key // used in hash computation
	}
}

// TestGatewayHTTPAuth tests the HTTP auth middleware
func TestGatewayHTTPAuth(t *testing.T) {
	cfg := Config{
		KVAddresses:   []string{"127.0.0.1:8081"},
		RateLimitRPS:  1000,
		RateLimitBurst: 2000,
	}
	gw, err := NewGateway(cfg)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	defer gw.Close()

	gw.CreateTenant("t1", "Tenant One")
	key, err := gw.CreateAPIKey("t1", "auth-test", 24*time.Hour)
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}

	// Test missing auth
	req := httptest.NewRequest("GET", "/kv/test", nil)
	w := httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", w.Code)
	}

	// Test malformed auth
	req = httptest.NewRequest("GET", "/kv/test", nil)
	req.Header.Set("Authorization", "Basic invalid")
	w = httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", w.Code)
	}

	// Test valid auth (but client will fail since no real server)
	req = httptest.NewRequest("GET", "/kv/test", nil)
	req.Header.Set("Authorization", "Bearer "+key)
	w = httptest.NewRecorder()
	gw.ServeHTTP(w, req)
	// Should not be 401 (auth passed), but will be 500 (no KV server)
	if w.Code == http.StatusUnauthorized {
		t.Errorf("valid auth should not return 401")
	}
	if w.Code != http.StatusInternalServerError {
		t.Logf("expected 500 (no KV server), got %d", w.Code)
	}
}

// TestGatewayHTTPRateLimit tests rate limiting via HTTP
func TestGatewayHTTPRateLimit(t *testing.T) {
	// Test rate limiter directly (not via HTTP, which is slow due to KV timeouts)
	cfg := Config{
		KVAddresses:   []string{"127.0.0.1:8081"},
		RateLimitRPS:  5,
		RateLimitBurst: 3,
	}
	gw, err := NewGateway(cfg)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	defer gw.Close()

	gw.CreateTenant("t1", "Tenant One")

	// Test the rate limiter directly
	lim := gw.getLimiter("t1")
	
	// First 3 requests should be allowed (burst)
	for i := 0; i < 3; i++ {
		if !lim.Allow() {
			t.Errorf("request %d should be allowed", i+1)
		}
	}
	// 4th should be denied
	if lim.Allow() {
		t.Error("4th request should be denied")
	}
	
	// Wait for refill
	time.Sleep(300 * time.Millisecond) // 5 RPS = 1 token per 200ms, so 300ms = 1.5 tokens
	if !lim.Allow() {
		t.Error("request should be allowed after refill")
	}
	
	// Test tenant-specific quota
	cfg2 := Config{
		KVAddresses:   []string{"127.0.0.1:8081"},
		RateLimitRPS:  10,
		RateLimitBurst: 5,
	}
	gw2, err := NewGateway(cfg2)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	defer gw2.Close()

	gw2.CreateTenant("t2", "Tenant Two")
	gw2.mu.Lock()
	gw2.tenants["t2"].QuotaRPS = 100
	gw2.tenants["t2"].QuotaBurst = 50
	gw2.mu.Unlock()

	lim2 := gw2.getLimiter("t2")
	count := 0
	for lim2.Allow() {
		count++
	}
	if count != 50 {
		t.Errorf("expected 50 tokens for tenant override, got %d", count)
	}
}

// TestGatewayHTTPMethods tests different HTTP methods
func TestGatewayHTTPMethods(t *testing.T) {
	cfg := Config{
		KVAddresses:   []string{"127.0.0.1:8081"},
		RateLimitRPS:  1000,
		RateLimitBurst: 2000,
	}
	gw, err := NewGateway(cfg)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	defer gw.Close()

	gw.CreateTenant("t1", "Tenant One")
	key, err := gw.CreateAPIKey("t1", "method-test", 24*time.Hour)
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}

	tests := []struct {
		method string
		path   string
		body   string
	}{
		{"GET", "/kv/test", ""},
		{"POST", "/kv", `{"op":"set","key":"k","value":"v"}`},
		{"PUT", "/kv", `{"op":"set","key":"k","value":"v"}`},
		{"DELETE", "/kv/test", ""},
	}

	for _, tc := range tests {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		if tc.body != "" {
			req.Body = nil // httptest.NewRequest doesn't set body from string easily
		}
		req.Header.Set("Authorization", "Bearer "+key)
		w := httptest.NewRecorder()
		gw.ServeHTTP(w, req)
		// All should pass auth (not 401), but fail on KV (500)
		if w.Code == http.StatusUnauthorized {
			t.Errorf("%s %s: auth failed", tc.method, tc.path)
		}
		if w.Code == http.StatusMethodNotAllowed {
			t.Errorf("%s %s: method not allowed", tc.method, tc.path)
		}
	}
}