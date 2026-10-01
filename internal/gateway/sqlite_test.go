package gateway

// These tests need the pure-Go SQLite driver:
//
//	go get modernc.org/sqlite
//
// They run the shared store suite against SQLiteStore and check durability.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func openTemp(t *testing.T) (*SQLiteStore, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gw.db")
	s, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	return s, path
}

func TestSQLiteStore(t *testing.T) {
	runStoreSuite(t, func(t *testing.T) Store {
		s, _ := openTemp(t)
		return s
	})
}

// A restart must keep tenants, keys and revocations: the in-memory version
// lost everything, which locked every customer out.
func TestSQLiteSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "gw.db")
	now := time.Now()

	s1, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateTenant(ctx, s1, "alpha", "Alpha", now); err != nil {
		t.Fatal(err)
	}
	live, _, err := CreateAPIKey(ctx, s1, "alpha", "live", 0, now)
	if err != nil {
		t.Fatal(err)
	}
	dead, deadRec, err := CreateAPIKey(ctx, s1, "alpha", "dead", 0, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.RevokeKey(ctx, "alpha", deadRec.Prefix, now); err != nil {
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	gw := New(Config{}, s2, newFakeKV())
	do := func(key string) int {
		e := &env{gw: gw}
		return e.do("GET", "/kv/x", key, "").Code
	}
	if c := do(live); c != 404 { // authenticated; key simply absent
		t.Fatalf("live key after restart: %d", c)
	}
	if c := do(dead); c != 401 {
		t.Fatalf("revoked key after restart: %d", c)
	}
}

// Only hashes may be stored: the plaintext key must appear nowhere on disk.
func TestSQLiteNeverStoresPlaintextKeys(t *testing.T) {
	ctx := context.Background()
	s, path := openTemp(t)
	if _, err := CreateTenant(ctx, s, "alpha", "Alpha", time.Now()); err != nil {
		t.Fatal(err)
	}
	plain, rec, err := CreateAPIKey(ctx, s, "alpha", "k", 0, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	secret := strings.TrimPrefix(plain, rec.Prefix) // the part after the display prefix
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	files, _ := filepath.Glob(path + "*") // db, -wal, -shm
	if len(files) == 0 {
		t.Fatal("no database files found")
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), plain) || strings.Contains(string(b), secret) {
			t.Fatalf("plaintext API key found in %s", f)
		}
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm()&0o077 != 0 {
		t.Fatalf("database file is accessible to other users: %v", st.Mode())
	}
}
