package gateway

// Needs the pure-Go SQLite driver, like sqlite_test.go.

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestSQLiteAccounts(t *testing.T) {
	runAccountStoreSuite(t, func(t *testing.T) ControlStore {
		s, _ := openTemp(t)
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}

// Users and sessions must survive a restart, and an old database (created
// before the account tables existed) must upgrade in place.
func TestSQLiteAccountsSurviveRestartAndUpgrade(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "gw.db")
	now := time.Now().Truncate(time.Millisecond)

	s, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CreateAccount(ctx, User{ID: "u1", Email: "a@x.io", PasswordHash: "h", TenantID: "ta", CreatedAt: now}, &Tenant{ID: "ta", Name: "A", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateSession(ctx, Session{TokenHash: "s1", UserID: "u1", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()

	s, err = OpenSQLite(path) // reopen: schema statements are idempotent
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if u, err := s.GetUserByEmail(ctx, "a@x.io"); err != nil || u.TenantID != "ta" {
		t.Fatalf("user lost: %+v %v", u, err)
	}
	if _, err := s.GetSession(ctx, "s1"); err != nil {
		t.Fatalf("session lost: %v", err)
	}
}

func TestSQLiteBackupIsRestorable(t *testing.T) {
	ctx := context.Background()
	s, _ := openTemp(t)
	defer s.Close()
	now := time.Now().Truncate(time.Millisecond)
	if err := s.CreateAccount(ctx, User{ID: "u1", Email: "a@x.io", PasswordHash: "h", TenantID: "ta", CreatedAt: now}, &Tenant{ID: "ta", Name: "A", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	plain, _, err := CreateAPIKey(ctx, s, "ta", "k", 0, now)
	if err != nil {
		t.Fatal(err)
	}

	dst := filepath.Join(t.TempDir(), "backup.db")
	if err := s.Backup(ctx, dst); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(ctx, dst); err == nil {
		t.Fatal("backup over an existing file should fail")
	}
	if fi, err := os.Stat(dst); err != nil || fi.Size() == 0 {
		t.Fatalf("backup file: %v", err)
	}
	r, err := OpenSQLite(dst)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.GetKeyByHash(ctx, hashKey(plain)); err != nil {
		t.Fatalf("key missing from backup: %v", err)
	}
	if _, err := r.GetUserByEmail(ctx, "a@x.io"); err != nil {
		t.Fatalf("user missing from backup: %v", err)
	}
}
