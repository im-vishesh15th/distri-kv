package gateway

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

var (
	ErrUserNotFound    = errors.New("user not found")
	ErrUserExists      = errors.New("user already exists")
	ErrSessionNotFound = errors.New("session not found")
)

// User is a console login. A customer user owns exactly one tenant
// (TenantID); an operator ("admin") user has IsAdmin and usually no tenant.
type User struct {
	ID           string
	Email        string // lower-cased
	PasswordHash string
	TenantID     string // "" for operator accounts
	IsAdmin      bool
	CreatedAt    time.Time
}

// Session is a login session. Only the SHA-256 of the cookie token is stored.
type Session struct {
	TokenHash string
	UserID    string
	CreatedAt time.Time
	ExpiresAt time.Time
}

// AccountStore persists console users and sessions. Implementations must be
// safe for concurrent use.
type AccountStore interface {
	// CreateAccount inserts the user and, when t != nil, its tenant,
	// atomically. ErrUserExists / ErrTenantExists on conflicts.
	CreateAccount(ctx context.Context, u User, t *Tenant) error
	GetUserByEmail(ctx context.Context, email string) (User, error)
	GetUserByID(ctx context.Context, id string) (User, error)
	SetPassword(ctx context.Context, userID, hash string) error

	CreateSession(ctx context.Context, s Session) error
	GetSession(ctx context.Context, tokenHash string) (Session, error)
	DeleteSession(ctx context.Context, tokenHash string) error
	// DeleteUserSessions removes all of a user's sessions except keepTokenHash
	// ("" removes all).
	DeleteUserSessions(ctx context.Context, userID, keepTokenHash string) error
	DeleteExpiredSessions(ctx context.Context, now time.Time) (int, error)

	// ListTenants returns up to limit tenants with ID > after, ordered by ID.
	ListTenants(ctx context.Context, after string, limit int) ([]Tenant, error)
}

// ControlStore is everything the control-plane API needs.
type ControlStore interface {
	Store
	AccountStore
}

// ------------------------------------------------------------ in-memory

type memAccounts struct {
	mu       sync.Mutex
	users    map[string]User // by ID
	byEmail  map[string]string
	sessions map[string]Session
}

func (m *MemStore) acc() *memAccounts {
	m.accOnce.Do(func() {
		m.accounts = &memAccounts{users: map[string]User{}, byEmail: map[string]string{}, sessions: map[string]Session{}}
	})
	return m.accounts
}

func (m *MemStore) CreateAccount(ctx context.Context, u User, t *Tenant) error {
	a := m.acc()
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.byEmail[u.Email]; ok {
		return ErrUserExists
	}
	if t != nil {
		if err := m.CreateTenant(ctx, *t); err != nil {
			return err
		}
	}
	a.users[u.ID] = u
	a.byEmail[u.Email] = u.ID
	return nil
}

func (m *MemStore) GetUserByEmail(_ context.Context, email string) (User, error) {
	a := m.acc()
	a.mu.Lock()
	defer a.mu.Unlock()
	id, ok := a.byEmail[email]
	if !ok {
		return User{}, ErrUserNotFound
	}
	return a.users[id], nil
}

func (m *MemStore) GetUserByID(_ context.Context, id string) (User, error) {
	a := m.acc()
	a.mu.Lock()
	defer a.mu.Unlock()
	u, ok := a.users[id]
	if !ok {
		return User{}, ErrUserNotFound
	}
	return u, nil
}

func (m *MemStore) SetPassword(_ context.Context, userID, hash string) error {
	a := m.acc()
	a.mu.Lock()
	defer a.mu.Unlock()
	u, ok := a.users[userID]
	if !ok {
		return ErrUserNotFound
	}
	u.PasswordHash = hash
	a.users[userID] = u
	return nil
}

func (m *MemStore) CreateSession(_ context.Context, s Session) error {
	a := m.acc()
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.users[s.UserID]; !ok {
		return ErrUserNotFound
	}
	a.sessions[s.TokenHash] = s
	return nil
}

func (m *MemStore) GetSession(_ context.Context, tokenHash string) (Session, error) {
	a := m.acc()
	a.mu.Lock()
	defer a.mu.Unlock()
	s, ok := a.sessions[tokenHash]
	if !ok {
		return Session{}, ErrSessionNotFound
	}
	return s, nil
}

func (m *MemStore) DeleteSession(_ context.Context, tokenHash string) error {
	a := m.acc()
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.sessions, tokenHash)
	return nil
}

func (m *MemStore) DeleteUserSessions(_ context.Context, userID, keep string) error {
	a := m.acc()
	a.mu.Lock()
	defer a.mu.Unlock()
	for h, s := range a.sessions {
		if s.UserID == userID && h != keep {
			delete(a.sessions, h)
		}
	}
	return nil
}

func (m *MemStore) DeleteExpiredSessions(_ context.Context, now time.Time) (int, error) {
	a := m.acc()
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for h, s := range a.sessions {
		if !now.Before(s.ExpiresAt) {
			delete(a.sessions, h)
			n++
		}
	}
	return n, nil
}

func (m *MemStore) ListTenants(_ context.Context, after string, limit int) ([]Tenant, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []Tenant
	for id, t := range m.tenants {
		if id > after {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
