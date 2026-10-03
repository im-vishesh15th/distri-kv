package kv

import (
	"reflect"
	"testing"
)

func TestTenantStorageAccounting(t *testing.T) {
	e := NewMemEngine()
	// "t:acme:k1" -> user key "k1" (2 bytes) + value 5 = 7
	_ = e.Put("t:acme:k1", []byte("hello"))
	_ = e.Put("t:acme:k2", []byte("xx"))   // 2 + 2 = 4
	_ = e.Put("t:beta:k1", []byte("y"))    // 2 + 1 = 3
	_ = e.Put("plain-key", []byte("zzzz")) // outside the gateway namespace: ignored

	want := map[string]TenantStorage{"acme": {Bytes: 11, Keys: 2}, "beta": {Bytes: 3, Keys: 1}}
	if got := e.TenantStorage(); !reflect.DeepEqual(got, want) {
		t.Fatalf("after puts: %v want %v", got, want)
	}

	// Overwrite adjusts bytes, not the key count.
	_ = e.Put("t:acme:k1", []byte("hi")) // 2+2 = 4 (was 7)
	if g := e.TenantStorage()["acme"]; g.Bytes != 8 || g.Keys != 2 {
		t.Fatalf("after overwrite: %+v", g)
	}

	// Delete removes bytes and the key; deleting twice / a missing key is a no-op.
	_ = e.Delete("t:acme:k2")
	_ = e.Delete("t:acme:k2")
	_ = e.Delete("t:acme:never")
	if g := e.TenantStorage()["acme"]; g.Bytes != 4 || g.Keys != 1 {
		t.Fatalf("after delete: %+v", g)
	}
	_ = e.Delete("t:beta:k1")
	if _, ok := e.TenantStorage()["beta"]; ok {
		t.Fatal("tenant with no keys should disappear from usage")
	}

	// Apply paths: set, CAS (only on success), incr, delete.
	_, _ = e.Apply(Command{Op: OpSet, Key: "t:gamma:c", Value: []byte("1")}) // 2
	if r, _ := e.Apply(Command{Op: OpCAS, Key: "t:gamma:c", Expected: []byte("nope"), Value: []byte("2222")}); r.Applied {
		t.Fatal("CAS should have failed")
	}
	if g := e.TenantStorage()["gamma"]; g.Bytes != 2 || g.Keys != 1 {
		t.Fatalf("failed CAS changed usage: %+v", g)
	}
	_, _ = e.Apply(Command{Op: OpCAS, Key: "t:gamma:c", Expected: []byte("1"), Value: []byte("2222")}) // 1+4 = 5
	_, _ = e.Apply(Command{Op: OpIncr, Key: "t:gamma:n", Delta: 12})                                   // 1+2 = 3
	if g := e.TenantStorage()["gamma"]; g.Bytes != 8 || g.Keys != 2 {
		t.Fatalf("after cas+incr: %+v", g)
	}
	_, _ = e.Apply(Command{Op: OpDelete, Key: "t:gamma:c"})
	if g := e.TenantStorage()["gamma"]; g.Bytes != 3 || g.Keys != 1 {
		t.Fatalf("after apply delete: %+v", g)
	}
}

// Snapshot -> Restore must reproduce the same usage (a node restarting from a
// snapshot, or a follower installing one, reports identical numbers).
func TestTenantStorageSurvivesSnapshotRestore(t *testing.T) {
	a := NewMemEngine()
	for _, k := range []string{"t:acme:a", "t:acme:bb", "t:beta:c", "loose"} {
		_ = a.Put(k, []byte("12345"))
	}
	snap, err := a.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	b := NewMemEngine()
	_ = b.Put("t:stale:x", []byte("stale")) // must be replaced by Restore
	if err := b.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a.TenantStorage(), b.TenantStorage()) {
		t.Fatalf("usage differs after restore: %v vs %v", a.TenantStorage(), b.TenantStorage())
	}
	if _, ok := b.TenantStorage()["stale"]; ok {
		t.Fatal("stale tenant survived Restore")
	}
}

func TestStorageTenantParsing(t *testing.T) {
	cases := []struct {
		key    string
		tenant string
		klen   int
		ok     bool
	}{
		{"t:acme:k", "acme", 1, true},
		{"t:acme:a:b:c", "acme", 5, true}, // user keys may contain ':'
		{"t:acme:", "acme", 0, true},
		{"t:acme", "", 0, false},
		{"acme:k", "", 0, false},
		{"t:", "", 0, false},
	}
	for _, c := range cases {
		tn, kl, ok := storageTenant(c.key)
		if tn != c.tenant || kl != c.klen || ok != c.ok {
			t.Errorf("%q -> (%q,%d,%v) want (%q,%d,%v)", c.key, tn, kl, ok, c.tenant, c.klen, c.ok)
		}
	}
}
