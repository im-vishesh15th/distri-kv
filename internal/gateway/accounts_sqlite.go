package gateway

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Account tables live next to tenants/api_keys in the same SQLite file. They
// are created with IF NOT EXISTS, so an existing gateway.db upgrades in place
// (no existing table is altered).
const accountSchema = `
CREATE TABLE IF NOT EXISTS users (
	id         TEXT PRIMARY KEY,
	email      TEXT NOT NULL UNIQUE,
	pw_hash    TEXT NOT NULL,
	tenant_id  TEXT REFERENCES tenants(id),
	is_admin   INTEGER NOT NULL DEFAULT 0,
	created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_users_tenant ON users(tenant_id);
CREATE TABLE IF NOT EXISTS sessions (
	token_hash TEXT PRIMARY KEY,
	user_id    TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
	created_at INTEGER NOT NULL,
	expires_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id);
`

const userCols = `id, email, pw_hash, tenant_id, is_admin, created_at`

func scanUser(sc interface{ Scan(...any) error }) (User, error) {
	var u User
	var tenant sql.NullString
	var admin int
	var created int64
	if err := sc.Scan(&u.ID, &u.Email, &u.PasswordHash, &tenant, &admin, &created); err != nil {
		return User{}, err
	}
	u.TenantID = tenant.String
	u.IsAdmin = admin != 0
	u.CreatedAt = fromMS(created)
	return u, nil
}

func (s *SQLiteStore) CreateAccount(ctx context.Context, u User, t *Tenant) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if t != nil {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO tenants(id, name, quota_rps, quota_burst, created_at) VALUES(?, ?, ?, ?, ?)`,
			t.ID, t.Name, t.QuotaRPS, t.QuotaBurst, ms(t.CreatedAt))
		if isUnique(err) {
			return ErrTenantExists
		}
		if err != nil {
			return err
		}
	}
	var tenantID sql.NullString
	if t != nil {
		tenantID = sql.NullString{String: t.ID, Valid: true}
	}
	admin := 0
	if u.IsAdmin {
		admin = 1
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO users(`+userCols+`) VALUES(?, ?, ?, ?, ?, ?)`,
		u.ID, u.Email, u.PasswordHash, tenantID, admin, ms(u.CreatedAt))
	if isUnique(err) {
		return ErrUserExists
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLiteStore) GetUserByEmail(ctx context.Context, email string) (User, error) {
	u, err := scanUser(s.db.QueryRowContext(ctx, `SELECT `+userCols+` FROM users WHERE email = ?`, email))
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, ErrUserNotFound
	}
	return u, err
}

func (s *SQLiteStore) GetUserByID(ctx context.Context, id string) (User, error) {
	u, err := scanUser(s.db.QueryRowContext(ctx, `SELECT `+userCols+` FROM users WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, ErrUserNotFound
	}
	return u, err
}

func (s *SQLiteStore) SetPassword(ctx context.Context, userID, hash string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE users SET pw_hash = ? WHERE id = ?`, hash, userID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrUserNotFound
	}
	return nil
}

func (s *SQLiteStore) CreateSession(ctx context.Context, sess Session) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO sessions(token_hash, user_id, created_at, expires_at) VALUES(?, ?, ?, ?)`,
		sess.TokenHash, sess.UserID, ms(sess.CreatedAt), ms(sess.ExpiresAt))
	if isForeignKey(err) {
		return ErrUserNotFound
	}
	return err
}

func (s *SQLiteStore) GetSession(ctx context.Context, tokenHash string) (Session, error) {
	var sess Session
	var created, expires int64
	err := s.db.QueryRowContext(ctx,
		`SELECT token_hash, user_id, created_at, expires_at FROM sessions WHERE token_hash = ?`, tokenHash).
		Scan(&sess.TokenHash, &sess.UserID, &created, &expires)
	if errors.Is(err, sql.ErrNoRows) {
		return Session{}, ErrSessionNotFound
	}
	if err != nil {
		return Session{}, err
	}
	sess.CreatedAt, sess.ExpiresAt = fromMS(created), fromMS(expires)
	return sess, nil
}

func (s *SQLiteStore) DeleteSession(ctx context.Context, tokenHash string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM sessions WHERE token_hash = ?`, tokenHash)
	return err
}

func (s *SQLiteStore) DeleteUserSessions(ctx context.Context, userID, keep string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM sessions WHERE user_id = ? AND token_hash <> ?`, userID, keep)
	return err
}

func (s *SQLiteStore) DeleteExpiredSessions(ctx context.Context, now time.Time) (int, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM sessions WHERE expires_at <= ?`, ms(now))
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

func (s *SQLiteStore) ListTenants(ctx context.Context, after string, limit int) ([]Tenant, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, name, quota_rps, quota_burst, created_at FROM tenants WHERE id > ? ORDER BY id LIMIT ?`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Tenant
	for rows.Next() {
		var t Tenant
		var created int64
		if err := rows.Scan(&t.ID, &t.Name, &t.QuotaRPS, &t.QuotaBurst, &created); err != nil {
			return nil, err
		}
		t.CreatedAt = fromMS(created)
		out = append(out, t)
	}
	return out, rows.Err()
}

// Backup writes a consistent copy of the whole database (tenants, keys, users,
// sessions) to path using VACUUM INTO. path must not exist.
func (s *SQLiteStore) Backup(ctx context.Context, path string) error {
	_, err := s.db.ExecContext(ctx, `VACUUM INTO ?`, path)
	return err
}
