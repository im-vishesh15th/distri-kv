package kv

import (
	"testing"
)

// BenchmarkMemEngineSet measures raw engine write throughput.
func BenchmarkMemEngineSet(b *testing.B) {
	e := NewMemEngine()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Put("key", []byte("value"))
	}
}

// BenchmarkMemEngineGet measures raw engine read throughput.
func BenchmarkMemEngineGet(b *testing.B) {
	e := NewMemEngine()
	e.Put("key", []byte("value"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Get("key")
	}
}

// BenchmarkMemEngineCAS measures CAS throughput via Apply.
func BenchmarkMemEngineCAS(b *testing.B) {
	e := NewMemEngine()
	e.Apply(Command{Op: OpSet, Key: "key", Value: []byte("old")})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Apply(Command{Op: OpCAS, Key: "key", Expected: []byte("old"), Value: []byte("new")})
	}
}

// BenchmarkMemEngineIncr measures increment throughput via Apply.
func BenchmarkMemEngineIncr(b *testing.B) {
	e := NewMemEngine()
	e.Apply(Command{Op: OpSet, Key: "counter", Value: []byte("0")})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Apply(Command{Op: OpIncr, Key: "counter", Delta: 1})
	}
}

// BenchmarkMemEngineMixed simulates a mixed read/write workload.
func BenchmarkMemEngineMixed(b *testing.B) {
	e := NewMemEngine()
	for i := 0; i < 100; i++ {
		e.Put("k"+string(rune(i)), []byte("v"))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%3 == 0 {
			e.Put("k"+string(rune(i%100)), []byte("v"))
		} else {
			e.Get("k" + string(rune(i%100)))
		}
	}
}
