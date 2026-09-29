package kv

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

// runEngineConformance is the shared contract suite every Engine
// implementation must pass. Future engines (e.g. a disk-backed one) reuse it
// by calling runEngineConformance(t, NewTheirEngine) from their own test.
func runEngineConformance(t *testing.T, name string, newEngine func() Engine) {
	t.Helper()

	t.Run(name+"/CRUD", func(t *testing.T) {
		e := newEngine()

		// Get on missing key.
		if _, err := e.Get("missing"); err != ErrKeyNotFound {
			t.Fatalf("Get(missing) = %v, want ErrKeyNotFound", err)
		}
		if e.Exists("missing") {
			t.Fatal("Exists(missing) = true, want false")
		}

		// Put then Get.
		if err := e.Put("k", []byte("v1")); err != nil {
			t.Fatalf("Put: %v", err)
		}
		got, err := e.Get("k")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !bytes.Equal(got, []byte("v1")) {
			t.Fatalf("Get = %q, want %q", got, "v1")
		}
		if !e.Exists("k") {
			t.Fatal("Exists(k) = false, want true")
		}

		// Overwrite.
		if err := e.Put("k", []byte("v2")); err != nil {
			t.Fatalf("Put overwrite: %v", err)
		}
		got, _ = e.Get("k")
		if !bytes.Equal(got, []byte("v2")) {
			t.Fatalf("after overwrite Get = %q, want %q", got, "v2")
		}

		// Delete then Get.
		if err := e.Delete("k"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if _, err := e.Get("k"); err != ErrKeyNotFound {
			t.Fatalf("Get after Delete = %v, want ErrKeyNotFound", err)
		}

		// Deleting a missing key is idempotent, not an error.
		if err := e.Delete("k"); err != nil {
			t.Fatalf("Delete of missing key = %v, want nil (idempotent)", err)
		}

		// Empty value is a valid value.
		if err := e.Put("empty", []byte{}); err != nil {
			t.Fatalf("Put empty: %v", err)
		}
		got, err = e.Get("empty")
		if err != nil {
			t.Fatalf("Get empty: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("Get(empty) len = %d, want 0", len(got))
		}
		if !e.Exists("empty") {
			t.Fatal("Exists(empty key) = false, want true")
		}
	})

	t.Run(name+"/DefensiveCopies", func(t *testing.T) {
		e := newEngine()

		// Mutation of the caller's slice after Put must not affect the engine.
		in := []byte("original")
		if err := e.Put("k", in); err != nil {
			t.Fatalf("Put: %v", err)
		}
		copy(in, "MUTATED!")

		got, err := e.Get("k")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !bytes.Equal(got, []byte("original")) {
			t.Fatalf("engine state aliased caller memory: got %q", got)
		}

		// Mutation of the returned slice must not affect the engine either.
		copy(got, "MUTATED!")
		got2, _ := e.Get("k")
		if !bytes.Equal(got2, []byte("original")) {
			t.Fatalf("engine state aliased returned memory: got %q", got2)
		}
	})
}

func TestMemEngine(t *testing.T) {
	runEngineConformance(t, "MemEngine", func() Engine { return NewMemEngine() })
}

// TestMemEngineConcurrentReadWriters exercises Get/Put/Delete/Exists from many
// goroutines at once. Its primary value is running under -race: any unsynchronized
// access fails the test.
func TestMemEngineConcurrentReadWriters(t *testing.T) {
	e := NewMemEngine()

	const (
		writers = 8
		readers = 8
		perG    = 500
	)

	var wg sync.WaitGroup

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				key := fmt.Sprintf("w%d-%d", w, i%50)
				if err := e.Put(key, []byte{byte(i)}); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
				if i%3 == 0 {
					if err := e.Delete(key); err != nil {
						t.Errorf("Delete: %v", err)
						return
					}
				}
			}
		}(w)
	}

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				key := fmt.Sprintf("w%d-%d", r%writers, i%50)
				_, _ = e.Get(key) // ErrKeyNotFound is fine here
				_ = e.Exists(key) // any answer is fine here
			}
		}(r)
	}

	wg.Wait()
}

// TestMemEngineConcurrentApplyAtomicity verifies that concurrent Apply calls
// are atomic: N goroutines incrementing M times must yield exactly N*M.
// This is the single-writer invariant the state machine depends on.
func TestMemEngineConcurrentApplyAtomicity(t *testing.T) {
	e := NewMemEngine()

	const (
		goroutines = 16
		perG       = 1000
	)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				if _, err := e.Apply(Incr("counter")); err != nil {
					t.Errorf("Apply(Incr): %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	got, err := e.Get("counter")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	want := fmt.Sprintf("%d", goroutines*perG)
	if string(got) != want {
		t.Fatalf("counter = %s, want %s (lost updates!)", got, want)
	}
}

// TestMemEngineConcurrentCASExactlyOneWinner verifies that among concurrent
// CAS attempts on the same key from the same expected state, exactly one wins.
func TestMemEngineConcurrentCASExactlyOneWinner(t *testing.T) {
	e := NewMemEngine()
	if err := e.Put("k", []byte("base")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	const contenders = 32
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		winners int
	)
	wg.Add(contenders)
	for c := 0; c < contenders; c++ {
		go func(c int) {
			defer wg.Done()
			res, err := e.Apply(CAS("k", []byte("base"), []byte(fmt.Sprintf("winner-%d", c))))
			if err != nil {
				t.Errorf("Apply(CAS): %v", err)
				return
			}
			if res.Applied {
				mu.Lock()
				winners++
				mu.Unlock()
			}
		}(c)
	}
	wg.Wait()

	if winners != 1 {
		t.Fatalf("winners = %d, want exactly 1", winners)
	}
	v, _ := e.Get("k")
	if !bytes.HasPrefix(v, []byte("winner-")) {
		t.Fatalf("value = %q, want a winner-* value", v)
	}
}

// TestMemEngineApplyImplementsInterface is a compile-time check that MemEngine
// satisfies the full Engine interface (including Apply).
func TestMemEngineApplyImplementsInterface(t *testing.T) {
	var e Engine = NewMemEngine()
	if _, err := e.Apply(Set("k", []byte("v"))); err != nil {
		t.Fatalf("Apply(Set): %v", err)
	}
}
