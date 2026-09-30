package server_test

// Phase 10: atomic operations + concurrency correctness (spec §8, §9) —
// multiple INDEPENDENT sessions racing through the full stack
// (SDK -> gRPC -> Raft -> SM -> engine). Phase 9 proved a session cannot
// duplicate itself; these tests prove sessions cannot corrupt EACH OTHER:
// no lost updates across sessions, exactly-one CAS winner, and the §8
// stock example (DECREMENT_IF_POSITIVE) with its single successful
// decrement. The engine-level equivalents (concurrent Apply, CAS
// one-winner) live in internal/kv/mem_test.go since Phase 1.

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"distrikv/pkg/client"
)

// casIncrement performs one increment of key through the optimistic
// read-CAS-retry loop that CAS exists for. An absent key is created via
// CAS's absent expectation (nil); a lost race is applied=false, not an
// error — the loop re-reads and retries. Bounded so a livelock fails
// loudly instead of hanging the test.
func casIncrement(ctx context.Context, c *client.Client, key string) error {
	for attempt := 0; attempt < 200; attempt++ {
		v, err := c.Get(ctx, key)
		var expected, next []byte
		switch {
		case errors.Is(err, client.ErrKeyNotFound):
			expected, next = nil, []byte("1") // absent: nil expectation
		case err != nil:
			return err
		default:
			n, aerr := strconv.Atoi(string(v))
			if aerr != nil {
				return fmt.Errorf("cas increment %q: %w", key, aerr)
			}
			expected, next = v, []byte(strconv.Itoa(n+1))
		}
		ok, err := c.CAS(ctx, key, expected, next)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
	}
	return fmt.Errorf("cas increment %q: no progress in 200 attempts", key)
}

// decrementIfPositive is the spec §8 DECREMENT_IF_POSITIVE built from
// DistriKV's generic primitives: read, refuse at <= 0, CAS down, retry
// on a lost race. Returns whether THIS caller won the decrement. The
// business rule lives in the application — the database only guarantees
// that each CAS attempt is indivisible.
func decrementIfPositive(ctx context.Context, c *client.Client, key string) (bool, error) {
	for attempt := 0; attempt < 200; attempt++ {
		v, err := c.Get(ctx, key)
		if errors.Is(err, client.ErrKeyNotFound) {
			return false, nil // nothing to decrement
		}
		if err != nil {
			return false, err
		}
		n, aerr := strconv.Atoi(string(v))
		if aerr != nil {
			return false, fmt.Errorf("decrement %q: %w", key, aerr)
		}
		if n <= 0 {
			return false, nil // spec §8: 0 -> cannot decrement
		}
		ok, err := c.CAS(ctx, key, v, []byte(strconv.Itoa(n-1)))
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
		// Lost the race: the value changed under us. Re-read and retry.
	}
	return false, fmt.Errorf("decrement %q: no progress in 200 attempts", key)
}

// TestMultiSessionConcurrentIncr: N independent sessions × M INCRs
// racing concurrently must total exactly N*M. Each session serializes
// internally (Phase 9); ACROSS sessions the only ordering is the event
// loop's in-order apply — a single lost update shows as a short count.
func TestMultiSessionConcurrentIncr(t *testing.T) {
	clients := newTestClients(t, 5)
	ctx := context.Background()

	const perClient = 40
	var wg sync.WaitGroup
	for _, c := range clients {
		wg.Add(1)
		go func(c *client.Client) {
			defer wg.Done()
			for i := 0; i < perClient; i++ {
				if _, err := c.IncrBy(ctx, "counter"); err != nil {
					t.Errorf("Incr: %v", err)
					return
				}
			}
		}(c)
	}
	wg.Wait()

	v, err := clients[0].Get(ctx, "counter")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if want := strconv.Itoa(len(clients) * perClient); string(v) != want {
		t.Fatalf("counter = %s, want %s (lost updates across sessions)", v, want)
	}
}

// TestCASOptimisticLoopConvergence: the read-modify-write pattern CAS
// exists for — N sessions × M CAS-loop increments must land exactly at
// N*M. Exercises absent-expectation CAS (create), lost-race retries
// (applied=false), and interleaving across sessions.
func TestCASOptimisticLoopConvergence(t *testing.T) {
	clients := newTestClients(t, 4)
	ctx := context.Background()

	const perClient = 25
	var wg sync.WaitGroup
	for _, c := range clients {
		wg.Add(1)
		go func(c *client.Client) {
			defer wg.Done()
			for i := 0; i < perClient; i++ {
				if err := casIncrement(ctx, c, "total"); err != nil {
					t.Errorf("cas increment: %v", err)
					return
				}
			}
		}(c)
	}
	wg.Wait()

	v, err := clients[0].Get(ctx, "total")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if want := strconv.Itoa(len(clients) * perClient); string(v) != want {
		t.Fatalf("total = %s, want %s (lost update in CAS loop)", v, want)
	}
}

// TestDecrementIfPositiveExactlyOneWinner: spec §8 verbatim — stock = 1,
// N sessions race DECREMENT_IF_POSITIVE. Exactly one succeeds, the final
// value is 0, and the stock is never driven negative.
func TestDecrementIfPositiveExactlyOneWinner(t *testing.T) {
	clients := newTestClients(t, 8)
	ctx := context.Background()

	if err := clients[0].Put(ctx, "stock", []byte("1")); err != nil {
		t.Fatalf("seed stock: %v", err)
	}

	var (
		wg         sync.WaitGroup
		successful atomic.Int64
	)
	for _, c := range clients {
		wg.Add(1)
		go func(c *client.Client) {
			defer wg.Done()
			won, err := decrementIfPositive(ctx, c, "stock")
			if err != nil {
				t.Errorf("decrement: %v", err)
				return
			}
			if won {
				successful.Add(1)
			}
		}(c)
	}
	wg.Wait()

	if got := successful.Load(); got != 1 {
		t.Fatalf("successful decrements = %d, want exactly 1 (double-spend)", got)
	}
	v, err := clients[0].Get(ctx, "stock")
	if err != nil {
		t.Fatalf("Get stock: %v", err)
	}
	if string(v) != "0" {
		t.Fatalf("stock = %s, want 0 (or: not driven negative)", v)
	}
}

// TestConcurrentReadsDuringWrites: readers run through the stack while
// writers mutate — every read must be a coherent value (a complete
// integer within the applied range, or absent before the first apply),
// never torn state. Level 1 (gRPC handlers concurrent with the apply
// loop) guarded by the engine's lock.
func TestConcurrentReadsDuringWrites(t *testing.T) {
	clients := newTestClients(t, 4) // 2 writer sessions, 2 reader sessions
	ctx := context.Background()

	const (
		writers   = 2
		perWriter = 75
		readers   = 2
		perReader = 300
	)

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(c *client.Client) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if _, err := c.IncrBy(ctx, "rw"); err != nil {
					t.Errorf("Incr: %v", err)
					return
				}
			}
		}(clients[w])
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(c *client.Client) {
			defer wg.Done()
			for i := 0; i < perReader; i++ {
				v, err := c.Get(ctx, "rw")
				if errors.Is(err, client.ErrKeyNotFound) {
					continue // first apply has not landed yet — fine
				}
				if err != nil {
					t.Errorf("Get: %v", err)
					return
				}
				n, aerr := strconv.Atoi(string(v))
				if aerr != nil {
					t.Errorf("torn/non-integer read %q: %v", v, aerr)
					return
				}
				if n < 0 || n > writers*perWriter {
					t.Errorf("read %d outside [0, %d]", n, writers*perWriter)
					return
				}
			}
		}(clients[2+r])
	}
	wg.Wait()
}
