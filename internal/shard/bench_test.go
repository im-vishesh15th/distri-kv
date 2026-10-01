package shard

import (
	"encoding/json"
	"testing"
)

// BenchmarkSlot measures slot computation throughput.
func BenchmarkSlot(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Slot("some-key-" + string(rune(i%100)))
	}
}

// BenchmarkGroup measures group lookup throughput.
func BenchmarkGroup(b *testing.B) {
	m := NewMap(4)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Group("some-key-" + string(rune(i%100)))
	}
}

// BenchmarkGroupSlot measures slot→group lookup throughput.
func BenchmarkGroupSlot(b *testing.B) {
	m := NewMap(4)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.GroupSlot(uint64(i % NumSlots))
	}
}

// BenchmarkConfigLoad measures config loading overhead.
func BenchmarkConfigLoad(b *testing.B) {
	data := `{"version": 1, "groups": 4}`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = LoadConfigFromString(data)
	}
}

// LoadConfigFromString is a test helper for benchmarking.
func LoadConfigFromString(s string) (*Config, error) {
	var c Config
	if err := json.Unmarshal([]byte(s), &c); err != nil {
		return nil, err
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}
