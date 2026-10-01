// Command bench runs a latency/throughput benchmark against a live DistriKV
// cluster over real gRPC (pkg/client).
//
// It preloads a keyspace, runs a mixed read/write workload with a fixed
// worker pool (one SDK client per worker — mutations serialize per client),
// across a warmup window (discarded) and a measurement window, and reports
// throughput plus p50/p95/p99 latency for successful operations only.
// Errors are counted separately, broken down by gRPC code, and never enter
// the latency percentiles.
//
// The cluster itself is started by scripts/bench_*.sh (3 real server
// processes); this tool only talks to it as a client would.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"distrikv/internal/shard"
	"distrikv/pkg/client"

	"google.golang.org/grpc/status"
)

type config struct {
	addrs       []string
	shards      int
	label       string
	readRatio   float64
	concurrency int
	duration    time.Duration
	warmup      time.Duration
	keyspace    int
	payload     int
	timeout     time.Duration
	out         string
	progress    bool
	sweep       bool
	skipPreload bool
}

// runState coordinates the warmup/measure phases across workers.
type runState struct {
	measuring atomic.Bool
	stop      chan struct{} // closed when the measurement window ends
}

// workerStats holds one worker's contribution (no cross-worker locking).
type workerStats struct {
	readLat  []time.Duration
	writeLat []time.Duration
	reads    int64 // attempts
	writes   int64 // attempts
	errs     map[string]int64

	preloadErrs    int64
	firstPreloadEr string
}

func (w *workerStats) recordErr(err error) {
	code := status.Code(err).String()
	if code == "Unknown" {
		// Non-status error (e.g. SDK-wrapped domain error): keep it short.
		msg := err.Error()
		if len(msg) > 60 {
			msg = msg[:60]
		}
		code = "Unknown(" + msg + ")"
	}
	w.errs[code]++
}

// result is the machine-readable run summary (one JSON line per run).
type result struct {
	Label       string  `json:"label"`
	Shards      int     `json:"shards"`
	Addrs       string  `json:"addrs"`
	ReadRatio   float64 `json:"read_ratio"`
	Concurrency int     `json:"concurrency"`
	DurationS   float64 `json:"duration_s"`
	WarmupS     float64 `json:"warmup_s"`
	Keyspace    int     `json:"keyspace"`
	Payload     int     `json:"payload"`
	StartedAt   string  `json:"started_at"`

	Attempts    int64            `json:"attempts"`
	Reads       int64            `json:"reads"`
	Writes      int64            `json:"writes"`
	Errors      int64            `json:"errors"`
	ErrorCodes  map[string]int64 `json:"error_codes"`
	OpsPerSec   float64          `json:"ops_per_sec"`
	ReadOpsSec  float64          `json:"read_ops_per_sec"`
	WriteOpsSec float64          `json:"write_ops_per_sec"`
	P50Us       int64            `json:"p50_us"`
	P95Us       int64            `json:"p95_us"`
	P99Us       int64            `json:"p99_us"`
	MaxUs       int64            `json:"max_us"`
	MeanUs      int64            `json:"mean_us"`
	ReadP50Us   int64            `json:"read_p50_us"`
	ReadP95Us   int64            `json:"read_p95_us"`
	ReadP99Us   int64            `json:"read_p99_us"`
	WriteP50Us  int64            `json:"write_p50_us"`
	WriteP95Us  int64            `json:"write_p95_us"`
	WriteP99Us  int64            `json:"write_p99_us"`
	PreloadErrs int64            `json:"preload_errors"`
	SweepReads  int64            `json:"sweep_reads"`   // keys checked by -sweep (0 = off)
	SweepMisses int64            `json:"sweep_missing"` // keys absent or unreadable
}

func main() {
	cfg := config{}
	var addrs string
	flag.StringVar(&addrs, "addrs", "127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083",
		"comma-separated cluster endpoints (first is the dial target)")
	flag.IntVar(&cfg.shards, "shards", 1, "shard/group count (label only; the client fetches the real config)")
	flag.StringVar(&cfg.label, "label", "", "label included in the output (default derived from flags)")
	flag.Float64Var(&cfg.readRatio, "read-ratio", 0.5, "fraction of operations that are reads, 0..1")
	flag.IntVar(&cfg.concurrency, "concurrency", 8, "worker goroutines (each with its own SDK client)")
	flag.DurationVar(&cfg.duration, "duration", 10*time.Second, "measurement window")
	flag.DurationVar(&cfg.warmup, "warmup", 2*time.Second, "warmup window (discarded)")
	flag.IntVar(&cfg.keyspace, "keyspace", 1000, "number of distinct keys")
	flag.IntVar(&cfg.payload, "payload", 100, "value size in bytes for writes")
	flag.DurationVar(&cfg.timeout, "timeout", 5*time.Second, "per-operation timeout")
	flag.StringVar(&cfg.out, "out", "", "append one JSON result line to this file")
	flag.BoolVar(&cfg.progress, "progress", false, "print a per-second progress line during measurement")
	flag.BoolVar(&cfg.sweep, "sweep", false, "after measurement, read every key once and report missing ones (data-loss check)")
	flag.BoolVar(&cfg.skipPreload, "skip-preload", false, "do not write the keyspace before measuring (use with -sweep to verify existing data)")
	flag.Parse()

	if cfg.readRatio < 0 || cfg.readRatio > 1 {
		fatal(fmt.Errorf("read-ratio must be in [0,1], got %v", cfg.readRatio))
	}
	if cfg.concurrency < 1 || cfg.keyspace < 1 {
		fatal(fmt.Errorf("concurrency and keyspace must be >= 1"))
	}
	for _, a := range strings.Split(addrs, ",") {
		if a = strings.TrimSpace(a); a != "" {
			cfg.addrs = append(cfg.addrs, a)
		}
	}
	if len(cfg.addrs) == 0 {
		fatal(fmt.Errorf("no -addrs given"))
	}
	if cfg.label == "" {
		cfg.label = fmt.Sprintf("shards=%d read=%.2f conc=%d", cfg.shards, cfg.readRatio, cfg.concurrency)
	}

	if err := run(cfg); err != nil {
		fatal(err)
	}
}

func run(cfg config) error {
	ctx := context.Background()

	// Probe the shard config once so per-group sequence counters route
	// correctly from the very first preload write. Single-group servers
	// don't implement GetShardConfig (legacy Service) — that's fine, the
	// default routing is group 0.
	var shardCfg *shard.Config
	probe, err := client.Dial(ctx, cfg.addrs[0])
	if err != nil {
		return fmt.Errorf("dial %s: %w", cfg.addrs[0], err)
	}
	if resp, err := probe.GetShardConfig(ctx); err == nil && resp.ConfigJson != "" {
		var sc shard.Config
		if uerr := json.Unmarshal([]byte(resp.ConfigJson), &sc); uerr != nil {
			return fmt.Errorf("decode shard config: %w", uerr)
		}
		if verr := sc.Validate(); verr != nil {
			return fmt.Errorf("server shard config invalid: %w", verr)
		}
		shardCfg = &sc
	}
	_ = probe.Close()

	startedAt := time.Now()
	state := &runState{stop: make(chan struct{})}

	// Totals for the per-second progress line (measured phase only).
	var totAttempts, totReads, totWrites, totErrs atomic.Int64

	var (
		runWg, preWg sync.WaitGroup
		startCh      = make(chan struct{})
		stats        = make([]*workerStats, cfg.concurrency)
	)

	for i := 0; i < cfg.concurrency; i++ {
		stats[i] = &workerStats{errs: map[string]int64{}}
		wctx, wcfg, wst, wstats := ctx, cfg, state, stats[i]
		workerID := i
		runWg.Add(1)
		preWg.Add(1)
		go func() {
			defer runWg.Done()
			c, derr := client.Dial(wctx, wcfg.addrs[0])
			if derr != nil {
				wstats.preloadErrs++
				wstats.firstPreloadEr = derr.Error()
				preWg.Done()
				<-startCh
				return
			}
			defer c.Close()
			for _, a := range wcfg.addrs[1:] {
				_ = c.AddEndpoint(a) // failover candidates for repair()
			}
			if shardCfg != nil {
				c.SetShardMap(shardCfg)
			}

			// Preload: this worker owns keys i, i+conc, i+2*conc, ...
			value := make([]byte, wcfg.payload)
			for j := range value {
				value[j] = 'x'
			}
			// Bounded so a dead cluster fails the run instead of hanging.
			pctx, pcancel := context.WithTimeout(wctx, 60*time.Second)
			defer pcancel()
			for k := workerID; !wcfg.skipPreload && k < wcfg.keyspace; k += wcfg.concurrency {
				// Setup, not measurement: a group mid-election can exhaust
				// the SDK's transient-retry budget, so retry the key before
				// declaring the preload failed.
				var perr error
				for attempt := 0; attempt < 5; attempt++ {
					if perr = c.Put(pctx, keyFor(k), value); perr == nil {
						break
					}
					select {
					case <-pctx.Done():
					case <-time.After(500 * time.Millisecond):
					}
				}
				if perr != nil {
					wstats.preloadErrs++
					if wstats.firstPreloadEr == "" {
						wstats.firstPreloadEr = perr.Error()
					}
				}
			}
			preWg.Done()
			<-startCh

			// Measurement loop: runs through warmup (unrecorded) and the
			// measured window, until stop is closed.
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)*1_000_003))
			for {
				select {
				case <-wst.stop:
					return
				default:
				}
				k := rng.Intn(wcfg.keyspace)
				isRead := rng.Float64() < wcfg.readRatio
				opCtx, cancel := context.WithTimeout(wctx, wcfg.timeout)
				t0 := time.Now()
				var opErr error
				if isRead {
					_, opErr = c.Get(opCtx, keyFor(k))
				} else {
					opErr = c.Put(opCtx, keyFor(k), value)
				}
				dt := time.Since(t0)
				cancel()

				if !wst.measuring.Load() {
					continue
				}
				if isRead {
					wstats.reads++
					totReads.Add(1)
				} else {
					wstats.writes++
					totWrites.Add(1)
				}
				totAttempts.Add(1)
				if opErr != nil {
					wstats.recordErr(opErr)
					totErrs.Add(1)
					continue
				}
				if isRead {
					wstats.readLat = append(wstats.readLat, dt)
				} else {
					wstats.writeLat = append(wstats.writeLat, dt)
				}
			}
		}()
	}

	// Preload barrier: fail loudly rather than benchmark a partial keyspace.
	preWg.Wait()
	var preloadErrs int64
	var firstPreload string
	for _, s := range stats {
		preloadErrs += s.preloadErrs
		if firstPreload == "" {
			firstPreload = s.firstPreloadEr
		}
	}
	close(startCh) // release workers into the loop regardless (see below)
	if preloadErrs > 0 {
		// Stop the workers immediately and report.
		close(state.stop)
		runWg.Wait()
		return fmt.Errorf("preload failed: %d/%d keys errored (first: %s)",
			preloadErrs, cfg.keyspace, firstPreload)
	}
	if cfg.skipPreload {
		fmt.Println("preload skipped (-skip-preload)")
	} else {
		fmt.Printf("preloaded %d keys (%d workers)\n", cfg.keyspace, cfg.concurrency)
	}

	// Progress printer.
	progStop := make(chan struct{})
	if cfg.progress {
		go func() {
			tick := time.NewTicker(time.Second)
			defer tick.Stop()
			var lastA, lastE int64
			sec := 0
			for {
				select {
				case <-progStop:
					return
				case <-tick.C:
					if !state.measuring.Load() {
						continue
					}
					sec++
					a := totAttempts.Load()
					e := totErrs.Load()
					fmt.Printf("progress t=%3ds attempts=%d (+%d) errors=%d (+%d)\n",
						sec, a, a-lastA, e, e-lastE)
					lastA, lastE = a, e
				}
			}
		}()
	}

	// Warmup -> measure -> stop.
	time.Sleep(cfg.warmup)
	measureStart := time.Now()
	state.measuring.Store(true)
	time.Sleep(cfg.duration)
	measureEnd := time.Now()
	close(state.stop)
	runWg.Wait() // drain in-flight ops (still recorded: same window semantics)
	state.measuring.Store(false)
	close(progStop)

	elapsed := measureEnd.Sub(measureStart)
	if elapsed <= 0 {
		return fmt.Errorf("non-positive elapsed time")
	}

	// Aggregate.
	agg := result{
		Label:       cfg.label,
		Shards:      cfg.shards,
		Addrs:       strings.Join(cfg.addrs, ","),
		ReadRatio:   cfg.readRatio,
		Concurrency: cfg.concurrency,
		DurationS:   cfg.duration.Seconds(),
		WarmupS:     cfg.warmup.Seconds(),
		Keyspace:    cfg.keyspace,
		Payload:     cfg.payload,
		StartedAt:   startedAt.Format(time.RFC3339),
		ErrorCodes:  map[string]int64{},
	}
	var allLat, readLat, writeLat []time.Duration
	for _, s := range stats {
		agg.Attempts += s.reads + s.writes
		agg.Reads += s.reads
		agg.Writes += s.writes
		allLat = append(allLat, s.readLat...)
		allLat = append(allLat, s.writeLat...)
		readLat = append(readLat, s.readLat...)
		writeLat = append(writeLat, s.writeLat...)
		for code, n := range s.errs {
			agg.ErrorCodes[code] += n
			agg.Errors += n
		}
	}
	sortDurations(allLat)
	sortDurations(readLat)
	sortDurations(writeLat)
	agg.OpsPerSec = float64(len(allLat)) / elapsed.Seconds()
	agg.ReadOpsSec = float64(len(readLat)) / elapsed.Seconds()
	agg.WriteOpsSec = float64(len(writeLat)) / elapsed.Seconds()
	agg.P50Us, agg.P95Us, agg.P99Us, agg.MaxUs, agg.MeanUs = summarize(allLat)
	agg.ReadP50Us, agg.ReadP95Us, agg.ReadP99Us, _, _ = summarize(readLat)
	agg.WriteP50Us, agg.WriteP95Us, agg.WriteP99Us, _, _ = summarize(writeLat)

	// Optional sweep: read every key once (linearizably) to detect data loss
	// after failover/recovery scenarios. Uses its own client, after workers
	// are done, so it measures nothing.
	if cfg.sweep {
		misses, err := sweep(ctx, cfg, shardCfg)
		if err != nil {
			return fmt.Errorf("sweep: %w", err)
		}
		agg.SweepReads = int64(cfg.keyspace)
		agg.SweepMisses = misses
	}
	agg.PreloadErrs = preloadErrs

	printHuman(agg, elapsed)

	if agg.Attempts == 0 {
		return fmt.Errorf("no operations completed during the measurement window")
	}
	if cfg.out != "" {
		if err := appendJSONLine(cfg.out, agg); err != nil {
			return err
		}
	}
	return nil
}

func keyFor(i int) string { return fmt.Sprintf("bench-%06d", i) }

// sweep reads every key in the keyspace once with a fresh client and
// returns how many were absent (a Get returning ErrKeyNotFound). Transport
// errors abort the sweep — an unreadable key can't be distinguished from a
// missing one, and the caller must see that as a hard failure.
func sweep(ctx context.Context, cfg config, shardCfg *shard.Config) (int64, error) {
	c, err := client.Dial(ctx, cfg.addrs[0])
	if err != nil {
		return 0, err
	}
	defer c.Close()
	for _, a := range cfg.addrs[1:] {
		_ = c.AddEndpoint(a)
	}
	if shardCfg != nil {
		c.SetShardMap(shardCfg)
	}
	var misses int64
	for k := 0; k < cfg.keyspace; k++ {
		sctx, cancel := context.WithTimeout(ctx, cfg.timeout)
		_, err := c.Get(sctx, keyFor(k))
		cancel()
		if err == nil {
			continue
		}
		if errors.Is(err, client.ErrKeyNotFound) {
			misses++
			continue
		}
		return misses, fmt.Errorf("get %s: %w", keyFor(k), err)
	}
	return misses, nil
}

func sortDurations(s []time.Duration) { sort.Slice(s, func(i, j int) bool { return s[i] < s[j] }) }

// summarize returns p50/p95/p99/max/mean in microseconds for a sorted slice.
func summarize(sorted []time.Duration) (p50, p95, p99, max, mean int64) {
	if len(sorted) == 0 {
		return 0, 0, 0, 0, 0
	}
	pct := func(p float64) int64 {
		i := int(math.Ceil(p*float64(len(sorted)))) - 1
		if i < 0 {
			i = 0
		}
		if i >= len(sorted) {
			i = len(sorted) - 1
		}
		return int64(sorted[i] / time.Microsecond)
	}
	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	return pct(0.50), pct(0.95), pct(0.99),
		int64(sorted[len(sorted)-1] / time.Microsecond),
		int64(sum / time.Duration(len(sorted)) / time.Microsecond)
}

func printHuman(r result, elapsed time.Duration) {
	fmt.Printf("\n== bench result: %s ==\n", r.Label)
	fmt.Printf("window: %.1fs measured (warmup %.1fs, %d workers, keyspace %d, payload %dB)\n",
		elapsed.Seconds(), r.WarmupS, r.Concurrency, r.Keyspace, r.Payload)
	fmt.Printf("attempts: %d (reads %d, writes %d)  errors: %d\n",
		r.Attempts, r.Reads, r.Writes, r.Errors)
	fmt.Printf("throughput: %.1f ops/s (read %.1f/s, write %.1f/s, successful ops only)\n",
		r.OpsPerSec, r.ReadOpsSec, r.WriteOpsSec)
	fmt.Printf("latency all:   p50=%s p95=%s p99=%s max=%s mean=%s\n",
		us(r.P50Us), us(r.P95Us), us(r.P99Us), us(r.MaxUs), us(r.MeanUs))
	fmt.Printf("latency read:  p50=%s p95=%s p99=%s\n",
		us(r.ReadP50Us), us(r.ReadP95Us), us(r.ReadP99Us))
	fmt.Printf("latency write: p50=%s p95=%s p99=%s\n",
		us(r.WriteP50Us), us(r.WriteP95Us), us(r.WriteP99Us))
	if r.Errors > 0 {
		var parts []string
		for code, n := range r.ErrorCodes {
			parts = append(parts, fmt.Sprintf("%s=%d", code, n))
		}
		sort.Strings(parts)
		fmt.Printf("error codes (excluded from latency): %s\n", strings.Join(parts, " "))
	}
	if r.SweepReads > 0 {
		fmt.Printf("sweep: %d/%d keys present, %d missing\n",
			r.SweepReads-r.SweepMisses, r.SweepReads, r.SweepMisses)
	}
}

func us(v int64) string { return (time.Duration(v) * time.Microsecond).String() }

func appendJSONLine(path string, v any) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	return enc.Encode(v)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "bench:", err)
	os.Exit(1)
}
