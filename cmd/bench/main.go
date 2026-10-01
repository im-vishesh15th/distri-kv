package main

import (
	"flag"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type BenchmarkConfig struct {
	Shards       int
	ReadRatio    float64
	Concurrency  int
	Duration     time.Duration
	Keyspace     int
	PayloadSize  int
	UnsafeNoMigrate bool
}

type Result struct {
	Ops        int64
	Errors     int64
	Latencies  []time.Duration
	Duration   time.Duration
	mu         sync.Mutex
}

func (r *Result) Record(d time.Duration, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	atomic.AddInt64(&r.Ops, 1)
	if err != nil {
		atomic.AddInt64(&r.Errors, 1)
	} else {
		r.Latencies = append(r.Latencies, d)
	}
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func main() {
	cfg := BenchmarkConfig{
		Shards:       4,
		ReadRatio:    0.5,
		Concurrency:  8,
		Duration:     10 * time.Second,
		Keyspace:     1000,
		PayloadSize:  100,
	}

	flag.IntVar(&cfg.Shards, "shards", cfg.Shards, "Number of shards (groups)")
	flag.Float64Var(&cfg.ReadRatio, "read-ratio", cfg.ReadRatio, "Read ratio (0.0-1.0)")
	flag.IntVar(&cfg.Concurrency, "concurrency", cfg.Concurrency, "Number of concurrent workers")
	flag.DurationVar(&cfg.Duration, "duration", cfg.Duration, "Benchmark duration")
	flag.IntVar(&cfg.Keyspace, "keyspace", cfg.Keyspace, "Number of keys")
	flag.IntVar(&cfg.PayloadSize, "payload-size", cfg.PayloadSize, "Payload size in bytes")
	flag.BoolVar(&cfg.UnsafeNoMigrate, "unsafe", false, "Allow unsafe migration")
	flag.Parse()

	fmt.Printf("Benchmark config: %+v\n", cfg)

	fmt.Println("Benchmark infrastructure not yet fully implemented")
	fmt.Println("Config:", cfg)
	fmt.Println("Skeleton ready for real gRPC benchmark implementation")
}