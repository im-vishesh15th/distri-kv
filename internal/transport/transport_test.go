package transport

import (
	"errors"
	"testing"
)

func TestParsePeers(t *testing.T) {
	t.Run("valid list", func(t *testing.T) {
		peers, err := ParsePeers("node1@127.0.0.1:8080,node2@127.0.0.1:8081, node3@127.0.0.1:8082 ")
		if err != nil {
			t.Fatalf("ParsePeers: %v", err)
		}
		if len(peers) != 3 {
			t.Fatalf("len = %d, want 3", len(peers))
		}
		want := []Peer{
			{ID: "node1", Addr: "127.0.0.1:8080"},
			{ID: "node2", Addr: "127.0.0.1:8081"},
			{ID: "node3", Addr: "127.0.0.1:8082"},
		}
		for i, p := range peers {
			if p != want[i] {
				t.Fatalf("peer[%d] = %+v, want %+v", i, p, want[i])
			}
		}
	})

	t.Run("empty spec is standalone", func(t *testing.T) {
		peers, err := ParsePeers("")
		if err != nil || peers != nil {
			t.Fatalf("ParsePeers(\"\") = %v, %v; want nil, nil", peers, err)
		}
		peers, err = ParsePeers("   ")
		if err != nil || peers != nil {
			t.Fatalf("ParsePeers(whitespace) = %v, %v; want nil, nil", peers, err)
		}
	})

	bad := map[string]string{
		"missing @":        "node1:8080",
		"empty id":         "@127.0.0.1:8080",
		"empty addr":       "node1@",
		"duplicate id":     "node1@a:1,node1@b:2",
		"empty entry":      "node1@a:1,,node2@b:2",
		"lone comma":       ",",
		"trailing garbage": "node1@a:1,",
	}
	for name, spec := range bad {
		t.Run(name, func(t *testing.T) {
			if _, err := ParsePeers(spec); err == nil {
				t.Fatalf("ParsePeers(%q) = nil error, want failure", spec)
			}
		})
	}
}

func TestParsePeersRejectsBadFormatWithBothSidesEmpty(t *testing.T) {
	// "@" with nothing on either side must fail (strings.Cut succeeds with
	// empty pieces — we validate explicitly).
	for _, spec := range []string{"@,node2@b:1", "node1@,node2@b:1"} {
		if _, err := ParsePeers(spec); err == nil {
			t.Fatalf("ParsePeers(%q) = nil error, want failure", spec)
		}
	}
}

func TestPeerMap(t *testing.T) {
	m := PeerMap([]Peer{{ID: "a", Addr: "x:1"}, {ID: "b", Addr: "y:2"}})
	if len(m) != 2 || m["a"] != "x:1" || m["b"] != "y:2" {
		t.Fatalf("PeerMap = %v", m)
	}
}

// TestTransportErrorsAreDistinct guards the sentinel errors the Raft core and
// tests will match with errors.Is.
func TestTransportErrorsAreDistinct(t *testing.T) {
	sentinels := []error{ErrUnknownPeer, ErrPeerMismatch, ErrClosed}
	for i, a := range sentinels {
		if a == nil {
			t.Fatalf("sentinel %d is nil", i)
		}
		for j, b := range sentinels {
			if i != j && errors.Is(a, b) {
				t.Fatalf("sentinel %d matches sentinel %d", i, j)
			}
		}
	}
}
