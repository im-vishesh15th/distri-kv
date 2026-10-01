package gateway

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

var (
	ErrTenantNotFound = errors.New("tenant not found")
	ErrTenantExists   = errors.New("tenant already exists")
	ErrAPIKeyNotFound = errors.New("api key not found")
	ErrAPIKeyExists   = errors.New("api key already exists")
)

// Tenant is one customer account.
type Tenant struct {
	ID         string
	Name       string
	QuotaRPS   float64 // 0 = use the gateway default
	QuotaBurst int     // 0 = use the gateway default
	CreatedAt  time.Time
}

// APIKey is the stored (hashed) form of a key. The plaintext key is shown to
// the customer once at creation and never stored.
type APIKey struct {
	Hash      string // hex sha256 of the full key
	Prefix    string // first characters of the key; safe to display
	TenantID  string
	Name      string
	CreatedAt time.Time
	ExpiresAt *time.Time // nil = no expiry
	RevokedAt *time.Time // nil = active
}

// Store persists tenants and API key hashes. Implementations must be safe for
// concurrent use. Every lookup reads current state, so a revoked key stops
// working on the very next request.
type Store interface {
	CreateTenant(ctx context.Context, t Tenant) error
	GetTenant(ctx context.Context, id string) (Tenant, error)
	SetTenantQuota(ctx context.Context, id string, rps float64, burst int) error

	CreateKey(ctx context.Context, k APIKey) error
	GetKeyByHash(ctx context.Context, hash string) (APIKey, error)
	ListKeys(ctx context.Context, tenantID string) ([]APIKey, error)
	// RevokeKey is idempotent: revoking an already-revoked key succeeds.
	RevokeKey(ctx context.Context, tenantID, prefix string, at time.Time) error

	Close() error
}

// MemStore is an in-memory Store for tests.
type MemStore struct {
	mu      sync.RWMutex
	tenants map[string]Tenant
	byHash  map[string]APIKey
}

func NewMemStore() *MemStore {
	return &MemStore{tenants: map[string]Tenant{}, byHash: map[string]APIKey{}}
}

func (m *MemStore) CreateTenant(_ context.Context, t Tenant) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tenants[t.ID]; ok {
		return ErrTenantExists
	}
	m.tenants[t.ID] = t
	return nil
}

func (m *MemStore) GetTenant(_ context.Context, id string) (Tenant, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.tenants[id]
	if !ok {
		return Tenant{}, ErrTenantNotFound
	}
	return t, nil
}

func (m *MemStore) SetTenantQuota(_ context.Context, id string, rps float64, burst int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tenants[id]
	if !ok {
		return ErrTenantNotFound
	}
	t.QuotaRPS, t.QuotaBurst = rps, burst
	m.tenants[id] = t
	return nil
}

func (m *MemStore) CreateKey(_ context.Context, k APIKey) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tenants[k.TenantID]; !ok {
		return ErrTenantNotFound
	}
	for _, e := range m.byHash {
		if e.Hash == k.Hash || e.Prefix == k.Prefix {
			return ErrAPIKeyExists
		}
	}
	m.byHash[k.Hash] = k
	return nil
}

func (m *MemStore) GetKeyByHash(_ context.Context, hash string) (APIKey, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	k, ok := m.byHash[hash]
	if !ok {
		return APIKey{}, ErrAPIKeyNotFound
	}
	return k, nil
}

func (m *MemStore) ListKeys(_ context.Context, tenantID string) ([]APIKey, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []APIKey
	for _, k := range m.byHash {
		if k.TenantID == tenantID {
			out = append(out, k)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].Prefix < out[j].Prefix
	})
	return out, nil
}

func (m *MemStore) RevokeKey(_ context.Context, tenantID, prefix string, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for h, k := range m.byHash {
		if k.TenantID == tenantID && k.Prefix == prefix {
			if k.RevokedAt == nil {
				t := at
				k.RevokedAt = &t
				m.byHash[h] = k
			}
			return nil
		}
	}
	return ErrAPIKeyNotFound
}

func (m *MemStore) Close() error { return nil }
