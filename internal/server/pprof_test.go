package server

import (
	"net"
	"net/http"
	"net/http/pprof"
	"testing"
	"time"
)

// TestPprofEndpoints verifies that pprof HTTP endpoints are accessible
// when the pprof HTTP server is started.
func TestPprofEndpoints(t *testing.T) {
	// Start a pprof HTTP server on a random port
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	// Additional profiles
	mux.HandleFunc("/debug/pprof/heap", pprof.Handler("heap").ServeHTTP)
	mux.HandleFunc("/debug/pprof/goroutine", pprof.Handler("goroutine").ServeHTTP)
	mux.HandleFunc("/debug/pprof/threadcreate", pprof.Handler("threadcreate").ServeHTTP)
	mux.HandleFunc("/debug/pprof/block", pprof.Handler("block").ServeHTTP)
	mux.HandleFunc("/debug/pprof/mutex", pprof.Handler("mutex").ServeHTTP)

	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer func() { _ = l.Close() }()

	go func() { _ = http.Serve(l, mux) }()

	// Wait a moment for server to start
	time.Sleep(100 * time.Millisecond)

	// Test each pprof endpoint
	endpoints := []string{
		"/debug/pprof/",
		"/debug/pprof/cmdline",
		"/debug/pprof/profile",
		"/debug/pprof/symbol",
		"/debug/pprof/trace",
		"/debug/pprof/heap",
		"/debug/pprof/goroutine",
		"/debug/pprof/threadcreate",
		"/debug/pprof/block",
		"/debug/pprof/mutex",
	}

	for _, ep := range endpoints {
		resp, err := http.Get("http://" + l.Addr().String() + ep)
		if err != nil {
			t.Errorf("GET %s: %v", ep, err)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode >= 400 && resp.StatusCode != 405 { // 405 for trace (needs POST)
			t.Errorf("GET %s: status %d", ep, resp.StatusCode)
		}
	}
}

// TestPprofInIntegration verifies that a pprof server can start and serve endpoints.
func TestPprofInIntegration(t *testing.T) {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	go func() { _ = http.Serve(l, mux) }()
	defer func() { _ = l.Close() }()

	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get("http://" + l.Addr().String() + "/debug/pprof/")
	if err != nil {
		t.Fatalf("pprof index: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("pprof index status: %d", resp.StatusCode)
	}
}
