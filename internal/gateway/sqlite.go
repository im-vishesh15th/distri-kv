package gateway

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// SQLiteStore is the durable Store. It does not import a driver: the binary
// (cmd/gateway, cmd/gatewayctl) blank-imports modernc.org/sqlite, a pure-Go
// driver, so CGO_ENABLED=0 builds and the Docker image keep working.
type SQLiteStore struct {
	db *sql.DB
}

const sqliteSchema = `
CREATE TABLE IF NOT EXISTS tenants (
	id          TEXT PRIMARY KEY,
	name        TEXT NOT NULL,
	quota_rps   REAL NOT NULL DEFAULT 0,
	quota_burst INTEGER NOT NULL DEFAULT 0,
	created_at  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS api_keys (
	hash       TEXT PRIMARY KEY,
	prefix     TEXT NOT NULL UNIQUE,
	tenant_id  TEXT NOT NULL REFERENCES tenants(id),
	name       TEXT NOT NULL,
	created_at INTEGER NOT NULL,
	expires_at INTEGER,
	revoked_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_api_keys_tenant ON api_keys(tenant_id);
`

// OpenSQLite opens (creating if needed) the database at path. Use a plain
// filesystem path; ":memory:" is not supported because each pooled connection
// would get its own empty database.
func OpenSQLite(path string) (*SQLiteStore, error) {
	dsn := "file:" + path +
		"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("gateway: open sqlite: %w", err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("gateway: ping sqlite: %w", err)
	}
	if _, err := db.Exec(sqliteSchema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("gateway: create schema: %w", err)
	}
	if _, err := db.Exec(accountSchema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("gateway: create account schema: %w", err)
	}
	if _, err := db.Exec(usageSchema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("gateway: create usage schema: %w", err)
	}
	// Key hashes are credentials: keep the file private to this user.
	_ = os.Chmod(path, 0o600)
	return &SQLiteStore{db: db}, nil
}

func (s *SQLiteStore) Close() error { return s.db.Close() }

func isUnique(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

func isForeignKey(err error) bool {
	return err != nil && strings.Contains(err.Error(), "FOREIGN KEY constraint failed")
}

func ms(t time.Time) int64 { return t.UnixMilli() }

func fromMS(v int64) time.Time { return time.UnixMilli(v) }

func nullMS(t *time.Time) sql.NullInt64 {
	if t == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: ms(*t), Valid: true}
}

func ptrTime(n sql.NullInt64) *time.Time {
	if !n.Valid {
		return nil
	}
	t := fromMS(n.Int64)
	return &t
}

func (s *SQLiteStore) CreateTenant(ctx context.Context, t Tenant) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO tenants(id, name, quota_rps, quota_burst, created_at) VALUES(?, ?, ?, ?, ?)`,
		t.ID, t.Name, t.QuotaRPS, t.QuotaBurst, ms(t.CreatedAt))
	if isUnique(err) {
		return ErrTenantExists
	}
	return err
}

func (s *SQLiteStore) GetTenant(ctx context.Context, id string) (Tenant, error) {
	var t Tenant
	var created int64
	err := s.db.QueryRowContext(ctx,
		`SELECT id, name, quota_rps, quota_burst, created_at FROM tenants WHERE id = ?`, id).
		Scan(&t.ID, &t.Name, &t.QuotaRPS, &t.QuotaBurst, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return Tenant{}, ErrTenantNotFound
	}
	if err != nil {
		return Tenant{}, err
	}
	t.CreatedAt = fromMS(created)
	return t, nil
}

func (s *SQLiteStore) SetTenantQuota(ctx context.Context, id string, rps float64, burst int) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE tenants SET quota_rps = ?, quota_burst = ? WHERE id = ?`, rps, burst, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrTenantNotFound
	}
	return nil
}

func (s *SQLiteStore) CreateKey(ctx context.Context, k APIKey) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO api_keys(hash, prefix, tenant_id, name, created_at, expires_at, revoked_at)
		 VALUES(?, ?, ?, ?, ?, ?, ?)`,
		k.Hash, k.Prefix, k.TenantID, k.Name, ms(k.CreatedAt), nullMS(k.ExpiresAt), nullMS(k.RevokedAt))
	switch {
	case isUnique(err):
		return ErrAPIKeyExists
	case isForeignKey(err):
		return ErrTenantNotFound
	}
	return err
}

func scanKey(sc interface{ Scan(...any) error }) (APIKey, error) {
	var k APIKey
	var created int64
	var exp, rev sql.NullInt64
	if err := sc.Scan(&k.Hash, &k.Prefix, &k.TenantID, &k.Name, &created, &exp, &rev); err != nil {
		return APIKey{}, err
	}
	k.CreatedAt = fromMS(created)
	k.ExpiresAt = ptrTime(exp)
	k.RevokedAt = ptrTime(rev)
	return k, nil
}

const keyCols = `hash, prefix, tenant_id, name, created_at, expires_at, revoked_at`

func (s *SQLiteStore) GetKeyByHash(ctx context.Context, hash string) (APIKey, error) {
	k, err := scanKey(s.db.QueryRowContext(ctx,
		`SELECT `+keyCols+` FROM api_keys WHERE hash = ?`, hash))
	if errors.Is(err, sql.ErrNoRows) {
		return APIKey{}, ErrAPIKeyNotFound
	}
	return k, err
}

func (s *SQLiteStore) ListKeys(ctx context.Context, tenantID string) ([]APIKey, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+keyCols+` FROM api_keys WHERE tenant_id = ? ORDER BY created_at, prefix`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []APIKey
	for rows.Next() {
		k, err := scanKey(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) RevokeKey(ctx context.Context, tenantID, prefix string, at time.Time) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE api_keys SET revoked_at = ? WHERE tenant_id = ? AND prefix = ? AND revoked_at IS NULL`,
		ms(at), tenantID, prefix)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	// Nothing updated: either unknown key, or already revoked (idempotent).
	var one int
	err = s.db.QueryRowContext(ctx,
		`SELECT 1 FROM api_keys WHERE tenant_id = ? AND prefix = ?`, tenantID, prefix).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrAPIKeyNotFound
	}
	return err
}
