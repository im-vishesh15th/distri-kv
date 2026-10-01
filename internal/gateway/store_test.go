package gateway

import (
	"context"
	"errors"
	"testing"
	"time"
)

// runStoreSuite is shared by every Store implementation (MemStore here,
// SQLiteStore in sqlite_test.go).
func runStoreSuite(t *testing.T, open func(t *testing.T) Store) {
	ctx := context.Background()
	now := time.UnixMilli(1_700_000_000_000)

	t.Run("tenants", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		if err := s.CreateTenant(ctx, Tenant{ID: "alpha", Name: "Alpha", CreatedAt: now}); err != nil {
			t.Fatal(err)
		}
		if err := s.CreateTenant(ctx, Tenant{ID: "alpha", Name: "dup", CreatedAt: now}); !errors.Is(err, ErrTenantExists) {
			t.Fatalf("duplicate tenant: got %v", err)
		}
		got, err := s.GetTenant(ctx, "alpha")
		if err != nil || got.Name != "Alpha" || !got.CreatedAt.Equal(now) {
			t.Fatalf("get: %+v %v", got, err)
		}
		if _, err := s.GetTenant(ctx, "nope"); !errors.Is(err, ErrTenantNotFound) {
			t.Fatalf("missing tenant: got %v", err)
		}
		if err := s.SetTenantQuota(ctx, "alpha", 5, 10); err != nil {
			t.Fatal(err)
		}
		got, _ = s.GetTenant(ctx, "alpha")
		if got.QuotaRPS != 5 || got.QuotaBurst != 10 {
			t.Fatalf("quota not stored: %+v", got)
		}
		if err := s.SetTenantQuota(ctx, "nope", 1, 1); !errors.Is(err, ErrTenantNotFound) {
			t.Fatalf("quota on missing tenant: got %v", err)
		}
	})

	t.Run("keys", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		_ = s.CreateTenant(ctx, Tenant{ID: "alpha", Name: "A", CreatedAt: now})
		exp := now.Add(time.Hour)
		k := APIKey{Hash: "h1", Prefix: "dkv_live_aaaaaaaa", TenantID: "alpha", Name: "prod", CreatedAt: now, ExpiresAt: &exp}
		if err := s.CreateKey(ctx, k); err != nil {
			t.Fatal(err)
		}
		if err := s.CreateKey(ctx, APIKey{Hash: "h1", Prefix: "dkv_live_bbbbbbbb", TenantID: "alpha", CreatedAt: now}); !errors.Is(err, ErrAPIKeyExists) {
			t.Fatalf("duplicate hash: got %v", err)
		}
		if err := s.CreateKey(ctx, APIKey{Hash: "h2", Prefix: "dkv_live_aaaaaaaa", TenantID: "alpha", CreatedAt: now}); !errors.Is(err, ErrAPIKeyExists) {
			t.Fatalf("duplicate prefix: got %v", err)
		}
		if err := s.CreateKey(ctx, APIKey{Hash: "h3", Prefix: "dkv_live_cccccccc", TenantID: "ghost", CreatedAt: now}); !errors.Is(err, ErrTenantNotFound) {
			t.Fatalf("key for missing tenant: got %v", err)
		}

		got, err := s.GetKeyByHash(ctx, "h1")
		if err != nil || got.TenantID != "alpha" || got.ExpiresAt == nil || !got.ExpiresAt.Equal(exp) || got.RevokedAt != nil {
			t.Fatalf("get: %+v %v", got, err)
		}
		if _, err := s.GetKeyByHash(ctx, "zzz"); !errors.Is(err, ErrAPIKeyNotFound) {
			t.Fatalf("missing key: got %v", err)
		}

		list, err := s.ListKeys(ctx, "alpha")
		if err != nil || len(list) != 1 || list[0].Prefix != "dkv_live_aaaaaaaa" {
			t.Fatalf("list: %+v %v", list, err)
		}
		if other, _ := s.ListKeys(ctx, "beta"); len(other) != 0 {
			t.Fatalf("list leaks other tenant's keys: %+v", other)
		}

		at := now.Add(time.Minute)
		if err := s.RevokeKey(ctx, "alpha", "dkv_live_aaaaaaaa", at); err != nil {
			t.Fatal(err)
		}
		got, _ = s.GetKeyByHash(ctx, "h1")
		if got.RevokedAt == nil || !got.RevokedAt.Equal(at) {
			t.Fatalf("not revoked: %+v", got)
		}
		if err := s.RevokeKey(ctx, "alpha", "dkv_live_aaaaaaaa", at.Add(time.Hour)); err != nil {
			t.Fatalf("revoke must be idempotent: %v", err)
		}
		got, _ = s.GetKeyByHash(ctx, "h1")
		if !got.RevokedAt.Equal(at) {
			t.Fatal("second revoke must not change the revocation time")
		}
		if err := s.RevokeKey(ctx, "alpha", "dkv_live_unknown0", at); !errors.Is(err, ErrAPIKeyNotFound) {
			t.Fatalf("revoke unknown: got %v", err)
		}
		// A tenant cannot revoke another tenant's key by guessing its prefix.
		_ = s.CreateTenant(ctx, Tenant{ID: "beta", Name: "B", CreatedAt: now})
		if err := s.RevokeKey(ctx, "beta", "dkv_live_aaaaaaaa", at); !errors.Is(err, ErrAPIKeyNotFound) {
			t.Fatalf("cross-tenant revoke: got %v", err)
		}
	})
}

func TestMemStore(t *testing.T) {
	runStoreSuite(t, func(*testing.T) Store { return NewMemStore() })
}
