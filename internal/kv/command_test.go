package kv

import (
	"errors"
	"testing"
)

func TestApplySetDelete(t *testing.T) {
	e := NewMemEngine()

	// Set creates and overwrites.
	if res, err := e.Apply(Set("k", []byte("v1"))); err != nil || !res.Applied {
		t.Fatalf("Apply(Set) = %+v, %v; want Applied=true, nil", res, err)
	}
	if res, err := e.Apply(Set("k", []byte("v2"))); err != nil || !res.Applied {
		t.Fatalf("Apply(Set overwrite) = %+v, %v; want Applied=true, nil", res, err)
	}
	if v, _ := e.Get("k"); string(v) != "v2" {
		t.Fatalf("Get = %q, want %q", v, "v2")
	}

	// Delete removes and reports Applied (idempotent).
	if res, err := e.Apply(Delete("k")); err != nil || !res.Applied {
		t.Fatalf("Apply(Delete) = %+v, %v; want Applied=true, nil", res, err)
	}
	if res, err := e.Apply(Delete("k")); err != nil || !res.Applied {
		t.Fatalf("Apply(Delete missing) = %+v, %v; want Applied=true, nil (idempotent)", res, err)
	}
	if e.Exists("k") {
		t.Fatal("k still exists after Delete")
	}
}

func TestApplyUnknownOp(t *testing.T) {
	e := NewMemEngine()
	if _, err := e.Apply(Command{Op: 99, Key: "k"}); !errors.Is(err, ErrUnknownOp) {
		t.Fatalf("Apply(unknown op) = %v, want ErrUnknownOp", err)
	}
}

func TestApplyCAS(t *testing.T) {
	tests := []struct {
		name      string
		setup     map[string]string // initial state (string values)
		key       string
		expected  []byte // nil = must-be-absent precondition
		newValue  []byte
		wantApply bool
	}{
		{
			name:      "absent key + nil expected succeeds",
			key:       "k",
			expected:  nil,
			newValue:  []byte("v"),
			wantApply: true,
		},
		{
			name:      "absent key + non-nil expected fails",
			key:       "k",
			expected:  []byte("old"),
			newValue:  []byte("v"),
			wantApply: false,
		},
		{
			name:      "present key + matching expected succeeds",
			setup:     map[string]string{"k": "old"},
			key:       "k",
			expected:  []byte("old"),
			newValue:  []byte("new"),
			wantApply: true,
		},
		{
			name:      "present key + mismatched expected fails",
			setup:     map[string]string{"k": "old"},
			key:       "k",
			expected:  []byte("wrong"),
			newValue:  []byte("new"),
			wantApply: false,
		},
		{
			name:      "present key + nil expected fails (nil means must-be-absent)",
			setup:     map[string]string{"k": "old"},
			key:       "k",
			expected:  nil,
			newValue:  []byte("new"),
			wantApply: false,
		},
		{
			name:      "present empty value + non-nil empty expected succeeds",
			setup:     map[string]string{"k": ""},
			key:       "k",
			expected:  []byte{},
			newValue:  []byte("new"),
			wantApply: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewMemEngine()
			for k, v := range tt.setup {
				if err := e.Put(k, []byte(v)); err != nil {
					t.Fatalf("setup Put: %v", err)
				}
			}

			res, err := e.Apply(CAS(tt.key, tt.expected, tt.newValue))
			if err != nil {
				t.Fatalf("Apply(CAS): %v", err)
			}
			if res.Applied != tt.wantApply {
				t.Fatalf("Applied = %v, want %v", res.Applied, tt.wantApply)
			}

			if tt.wantApply {
				v, err := e.Get(tt.key)
				if err != nil {
					t.Fatalf("Get after successful CAS: %v", err)
				}
				if string(v) != string(tt.newValue) {
					t.Fatalf("value = %q, want %q", v, tt.newValue)
				}
			} else if _, has := tt.setup[tt.key]; has {
				v, _ := e.Get(tt.key)
				if string(v) != tt.setup[tt.key] {
					t.Fatalf("failed CAS mutated state: %q, want %q", v, tt.setup[tt.key])
				}
			}
		})
	}
}

func TestApplyIncrDecr(t *testing.T) {
	e := NewMemEngine()

	// Absent key treated as 0.
	res, err := e.Apply(Incr("n"))
	if err != nil || !res.Applied {
		t.Fatalf("Apply(Incr) = %+v, %v", res, err)
	}
	if string(res.Value) != "1" || !e.Exists("n") {
		t.Fatalf("Incr on absent key: value=%q exists=%v, want 1/true", res.Value, e.Exists("n"))
	}

	// Increment existing canonical value.
	if _, err := e.Apply(IncrBy("n", 41)); err != nil {
		t.Fatalf("Apply(IncrBy 41): %v", err)
	}
	v, _ := e.Get("n")
	if string(v) != "42" {
		t.Fatalf("n = %q, want 42", v)
	}

	// Decrement.
	res, err = e.Apply(Decr("n"))
	if err != nil || string(res.Value) != "41" {
		t.Fatalf("Decr: value=%q err=%v, want 41/nil", res.Value, err)
	}

	// Decrement on absent key creates a negative value.
	if _, err := e.Apply(Decr("neg")); err != nil {
		t.Fatalf("Apply(Decr absent): %v", err)
	}
	v, _ = e.Get("neg")
	if string(v) != "-1" {
		t.Fatalf("neg = %q, want -1", v)
	}

	// Non-integer content.
	if err := e.Put("s", []byte("hello")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := e.Apply(Incr("s")); !errors.Is(err, ErrNotInteger) {
		t.Fatalf("Incr on non-integer = %v, want ErrNotInteger", err)
	}

	// Overflow.
	if err := e.Put("max", []byte("9223372036854775807")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := e.Apply(Incr("max")); !errors.Is(err, ErrOverflow) {
		t.Fatalf("Incr at MaxInt64 = %v, want ErrOverflow", err)
	}

	// Underflow.
	if err := e.Put("min", []byte("-9223372036854775808")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := e.Apply(Decr("min")); !errors.Is(err, ErrOverflow) {
		t.Fatalf("Decr at MinInt64 = %v, want ErrOverflow", err)
	}

	// A failed op must not mutate state.
	v, _ = e.Get("max")
	if string(v) != "9223372036854775807" {
		t.Fatalf("failed Incr mutated state: %q", v)
	}
}

// TestApplyDeterminism verifies the core replicated-state-machine property:
// the same command sequence on identical prior state produces identical
// resulting state — the basis for state-hash comparison tests in Phase 7.
func TestApplyDeterminism(t *testing.T) {
	commands := []Command{
		Set("a", []byte("1")),
		Set("b", []byte("2")),
		Incr("a"),
		CAS("b", []byte("2"), []byte("twenty")),
		CAS("missing", nil, []byte("created")),
		Decr("a"),
		Delete("a"),
		IncrBy("counter", 10),
	}

	run := func() map[string]string {
		e := NewMemEngine()
		for _, cmd := range commands {
			if _, err := e.Apply(cmd); err != nil {
				t.Fatalf("Apply(%v): %v", cmd.Op, err)
			}
		}
		state := make(map[string]string)
		for _, k := range []string{"a", "b", "counter", "missing"} {
			if v, err := e.Get(k); err == nil {
				state[k] = string(v)
			}
		}
		return state
	}

	first := run()
	second := run()
	if len(first) != len(second) {
		t.Fatalf("nondeterministic state sizes: %v vs %v", first, second)
	}
	for k, v := range first {
		if second[k] != v {
			t.Fatalf("nondeterministic state at %q: %q vs %q", k, v, second[k])
		}
	}
}
