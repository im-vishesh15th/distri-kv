package history

import (
	"sync"
	"testing"
	"time"
)

func TestRecorderSequential(t *testing.T) {
	r := NewRecorder()
	start := time.Now()
	r.Record("c1", OpSet, "k", "v", "", start, start.Add(time.Millisecond), true)
	r.Record("c1", OpGet, "k", "", "v", start.Add(2*time.Millisecond), start.Add(3*time.Millisecond), true)
	r.Record("c2", OpGet, "missing", "", "", start.Add(4*time.Millisecond), start.Add(5*time.Millisecond), false)

	ops := r.Ops()
	if len(ops) != 3 {
		t.Fatalf("len = %d, want 3", len(ops))
	}
	for i, op := range ops {
		if want := int64(i + 1); op.ID != want {
			t.Fatalf("ops[%d].ID = %d, want %d", i, op.ID, want)
		}
	}
	if ops[0].ClientID != "c1" || ops[0].Type != OpSet || ops[0].Key != "k" || ops[0].Input != "v" {
		t.Fatalf("ops[0] = %+v", ops[0])
	}
	if ops[1].Output != "v" || !ops[1].Success {
		t.Fatalf("ops[1] = %+v", ops[1])
	}
	if ops[2].Success {
		t.Fatal("failed op recorded as successful")
	}
}

func TestRecorderConcurrent(t *testing.T) {
	r := NewRecorder()
	const workers = 8
	const perWorker = 100

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			clientID := "client" + string(rune('a'+w))
			for i := 0; i < perWorker; i++ {
				now := time.Now()
				r.Record(clientID, OpGet, "k", "", "v", now, now.Add(time.Millisecond), true)
			}
		}(w)
	}
	wg.Wait()

	ops := r.Ops()
	if len(ops) != workers*perWorker {
		t.Fatalf("len = %d, want %d", len(ops), workers*perWorker)
	}
	// IDs are unique and dense (assigned under the mutex).
	seen := make(map[int64]bool, len(ops))
	for _, op := range ops {
		if op.ID < 1 || op.ID > int64(len(ops)) {
			t.Fatalf("ID %d out of range", op.ID)
		}
		if seen[op.ID] {
			t.Fatalf("duplicate ID %d", op.ID)
		}
		seen[op.ID] = true
	}
}

func TestOpsReturnsCopy(t *testing.T) {
	r := NewRecorder()
	now := time.Now()
	r.Record("c", OpSet, "k", "v", "", now, now, true)

	ops := r.Ops()
	ops[0].Key = "mutated"
	if r.Ops()[0].Key != "k" {
		t.Fatal("Ops() must return a copy — the recorder's state changed")
	}
}
