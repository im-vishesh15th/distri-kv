package gateway

import (
	"strings"
	"testing"
)

// The bug that motivated this file: with the old "tenant:"+id+":"+key
// encoding and no tenant-ID validation, these two pairs collided.
func TestNamespaceOldCollisionIsImpossible(t *testing.T) {
	if _, err := NamespaceKey("a:b", "c"); err == nil {
		t.Fatal("tenant id containing ':' must be rejected")
	}
	k1, err := NamespaceKey("a", "b:c")
	if err != nil {
		t.Fatal(err)
	}
	// The only way to produce the other pair's string is the invalid tenant.
	if k1 != "t:a:b:c" {
		t.Fatalf("unexpected encoding %q", k1)
	}
}

func TestValidateTenantID(t *testing.T) {
	good := []string{"a", "shoestore", "shop-1", "shop_1", "0abc", strings.Repeat("a", 63)}
	bad := []string{"", "A", "Shop", "a:b", "a/b", "a b", "-a", "_a", "a.b", "a\x00", "../x",
		strings.Repeat("a", 64), "café"}
	for _, id := range good {
		if err := ValidateTenantID(id); err != nil {
			t.Errorf("%q should be valid: %v", id, err)
		}
	}
	for _, id := range bad {
		if err := ValidateTenantID(id); err == nil {
			t.Errorf("%q should be invalid", id)
		}
	}
}

func TestValidateKey(t *testing.T) {
	for _, k := range []string{"a", "stock:shoes", "a/b/c", "../x", "tenant:beta:x", "ключ", strings.Repeat("k", 512)} {
		if err := ValidateKey(k); err != nil {
			t.Errorf("%q should be valid: %v", k, err)
		}
	}
	for _, k := range []string{"", strings.Repeat("k", 513), "bad\xff"} {
		if err := ValidateKey(k); err == nil {
			t.Errorf("%q should be invalid", k)
		}
	}
}

func TestNamespaceRoundTrip(t *testing.T) {
	cases := []struct{ tenant, key string }{
		{"a", "b:c"}, {"a", ":"}, {"a", "::"}, {"a-b", "c"}, {"a", "t:a:b"},
		{"alpha", "tenant:beta:x"}, {"alpha", "../beta/x"}, {"alpha", "x\x00y"},
	}
	for _, c := range cases {
		ns, err := NamespaceKey(c.tenant, c.key)
		if err != nil {
			t.Fatalf("%v: %v", c, err)
		}
		tn, k, err := splitNamespaced(ns)
		if err != nil || tn != c.tenant || k != c.key {
			t.Errorf("round trip %v -> %q -> (%q,%q,%v)", c, ns, tn, k, err)
		}
	}
}

// Two different (tenant, key) pairs must never map to the same storage key.
func FuzzNamespaceInjective(f *testing.F) {
	f.Add("a", "b:c", "a-b", "c")
	f.Add("alpha", "x", "alpha", "x")
	f.Add("a", ":", "a", "::")
	f.Fuzz(func(t *testing.T, t1, k1, t2, k2 string) {
		n1, err1 := NamespaceKey(t1, k1)
		n2, err2 := NamespaceKey(t2, k2)
		if err1 != nil || err2 != nil {
			return // invalid inputs never reach storage
		}
		if n1 == n2 && (t1 != t2 || k1 != k2) {
			t.Fatalf("collision: (%q,%q) and (%q,%q) both -> %q", t1, k1, t2, k2, n1)
		}
		tn, k, err := splitNamespaced(n1)
		if err != nil || tn != t1 || k != k1 {
			t.Fatalf("not reversible: (%q,%q) -> %q -> (%q,%q,%v)", t1, k1, n1, tn, k, err)
		}
	})
}
