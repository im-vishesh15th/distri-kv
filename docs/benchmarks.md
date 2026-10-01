# Benchmark Results

Every number on this page comes from a real run of the commands listed
below, on the machine described. Nothing is estimated, extrapolated,
simulated, or copied from other systems.

## Environment

| | |
|---|---|
| **Hardware** | Apple M2, 8 cores, 8 GB RAM |
| **OS** | macOS 27.0.1 (Darwin 27.0.0 arm64) |
| **Go** | go1.25.5 darwin/arm64 |
| **Commit** | `1028d09` (bench tooling `cad32b4`, election-timeout fix `7489e99`) |
| **Date** | 2026-10-01 |

> **Everything ran on this one machine.** All three server processes *and*
> the load generator shared one laptop: loopback network (no RTT), and CPU,
> memory, disk, and fsync bandwidth contended between 4 processes. Numbers
> from a multi-machine deployment will be different, in either direction.

## Cluster configuration

Three `cmd/server` processes started fresh for each shard count
(`-id`, `-addr`, `-peers`, `-data-dir`; everything else default):

- Raft tick 10 ms; `ElectionTicks=50` → randomized election timeout
  **[500, 990) ms**; `HeartbeatTicks=5` → **50 ms** heartbeat.
  (These were 10/3 = [100,190) ms until these benchmark runs showed that
  window to sit inside the event loop's fsync jitter and cause election
  storms under write concurrency — see `docs/raft.md`.)
- `-snapshot-every=1024` (default): state-machine snapshot + log
  compaction every 1024 applied entries.
- Single-machine gRPC over loopback for both client↔server and
  server↔server traffic.

Load generator: `cmd/bench` using the real `pkg/client` over real gRPC.

- One SDK client per worker (the SDK serializes mutations per client, so
  each worker has an independent session).
- Keyspace 1000 keys, payload 100 bytes, keys preloaded before each
  measured run (preload is setup, not measured).
- 2 s warmup, 10 s measurement window.
- Latency percentiles are computed from **successful operations only**;
  errors are counted separately and broken down by gRPC code.

## Exact commands

```bash
scripts/bench_matrix.sh 10s 2s      # full matrix: 48 runs, ~15 min
scripts/bench_failover.sh 30s 10s   # leader kill mid-run + data sweep
```

Raw machine-readable results: `bench-results/matrix.jsonl`,
`bench-results/failover.jsonl` (one JSON object per run).

## Matrix: shards × read ratio × concurrency

48 runs = shards {1, 4, 16} × read ratio {0, 0.5, 0.8, 1.0} ×
concurrency {1, 8, 32, 128}. Each shard count starts a fresh 3-node
cluster. Latencies are end-to-end client-observed (over loopback gRPC),
successful ops only.

### shards=1

| read ratio | conc | attempts | ops/s | errors | p50 | p95 | p99 | max |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.0 | 1 | 1168 | 116.8 | 0 | 8.2ms | 12.0ms | 18.9ms | 66.8ms |
| 0.0 | 8 | 2136 | 213.6 | 0 | 36.4ms | 51.9ms | 79.0ms | 115.3ms |
| 0.0 | 32 | 2998 | 299.8 | 0 | 105.6ms | 150.9ms | 194.4ms | 197.0ms |
| 0.0 | 128 | 3157 | 315.7 | 0 | 408.6ms | 586.9ms | 635.6ms | 638.1ms |
| 0.5 | 1 | 2125 | 212.5 | 0 | 4.7ms | 10.9ms | 14.5ms | 255.7ms |
| 0.5 | 8 | 3564 | 356.4 | 0 | 22.6ms | 34.0ms | 39.8ms | 97.2ms |
| 0.5 | 32 | 5312 | 531.1 | 0 | 57.9ms | 84.0ms | 113.4ms | 229.2ms |
| 0.5 | 128 | 5973 | 597.2 | 0 | 225.4ms | 276.8ms | 361.8ms | 373.8ms |
| 0.8 | 1 | 5655 | 565.5 | 0 | 203us | 8.7ms | 10.6ms | 22.5ms |
| 0.8 | 8 | 6924 | 692.4 | 0 | 11.1ms | 23.6ms | 35.6ms | 418.0ms |
| 0.8 | 32 | 6017 | 601.6 | 0 | 49.6ms | 102.7ms | 147.8ms | 205.8ms |
| 0.8 | 128 | 8643 | 864.2 | 0 | 138.6ms | 312.0ms | 417.2ms | 454.0ms |
| 1.0 | 1 | 40339 | 4033.9 | 0 | 204us | 435us | 793us | 47.4ms |
| 1.0 | 8 | 214624 | 21461.7 | 0 | 329us | 675us | 1.0ms | 8.5ms |
| 1.0 | 32 | 382947 | 38294.6 | 0 | 696us | 1.7ms | 3.0ms | 56.3ms |
| 1.0 | 128 | 447662 | 44766.0 | 0 | 2.4ms | 6.2ms | 11.8ms | 93.9ms |

### shards=4

| read ratio | conc | attempts | ops/s | errors | p50 | p95 | p99 | max |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.0 | 1 | 1272 | 127.2 | 0 | 7.7ms | 10.9ms | 14.1ms | 171.1ms |
| 0.0 | 8 | 2050 | 205.0 | 0 | 36.8ms | 67.0ms | 84.9ms | 221.5ms |
| 0.0 | 32 | 2320 | 232.0 | 0 | 109.5ms | 371.3ms | 462.9ms | 582.1ms |
| 0.0 | 128 | 3108 | 310.8 | 0 | 339.6ms | 1.23s | 2.11s | 2.62s |
| 0.5 | 1 | 2352 | 235.2 | 0 | 3.9ms | 10.7ms | 12.8ms | 34.9ms |
| 0.5 | 8 | 3729 | 372.9 | 0 | 21.8ms | 44.7ms | 58.1ms | 138.9ms |
| 0.5 | 32 | 3710 | 371.0 | 0 | 76.4ms | 187.6ms | 260.7ms | 310.5ms |
| 0.5 | 128 | 4095 | 409.5 | 0 | 241.9ms | 841.7ms | 1.03s | 1.15s |
| 0.8 | 1 | 4151 | 415.1 | 0 | 280us | 12.5ms | 15.9ms | 57.5ms |
| 0.8 | 8 | 5213 | 521.3 | 0 | 9.5ms | 46.0ms | 57.5ms | 80.1ms |
| 0.8 | 32 | 7211 | 721.0 | 0 | 43.1ms | 104.5ms | 133.6ms | 255.6ms |
| 0.8 | 128 | 8376 | 837.6 | 0 | 120.1ms | 388.9ms | 665.4ms | 877.9ms |
| 1.0 | 1 | 52043 | 5204.3 | 0 | 169us | 295us | 449us | 34.9ms |
| 1.0 | 8 | 184447 | 18444.6 | 0 | 341us | 925us | 1.8ms | 19.9ms |
| 1.0 | 32 | 335966 | 33596.6 | 0 | 727us | 2.4ms | 4.2ms | 44.1ms |
| 1.0 | 128 | 419120 | 41907.1 | 0 | 2.3ms | 7.9ms | 14.4ms | 104.1ms |

### shards=16

| read ratio | conc | attempts | ops/s | errors | p50 | p95 | p99 | max |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.0 | 1 | 1266 | 126.6 | 0 | 7.9ms | 10.7ms | 12.6ms | 50.5ms |
| 0.0 | 8 | 1929 | 192.8 | 0 | 38.2ms | 70.3ms | 94.3ms | 184.9ms |
| 0.0 | 32 | 2599 | 259.9 | 0 | 105.4ms | 282.8ms | 366.7ms | 527.7ms |
| 0.0 | 128 | 3132 | 308.6 | 46 | 189.9ms | 1.16s | 2.92s | 5.00s |
| 0.5 | 1 | 2773 | 277.3 | 0 | 742us | 9.1ms | 11.2ms | 29.3ms |
| 0.5 | 8 | 3426 | 342.6 | 0 | 24.1ms | 57.5ms | 86.6ms | 336.0ms |
| 0.5 | 32 | 5286 | 528.6 | 0 | 51.0ms | 161.9ms | 250.7ms | 541.8ms |
| 0.5 | 128 | 3437 | 331.9 | 118 | 151.9ms | 1.15s | 3.81s | 4.81s |
| 0.8 | 1 | 5004 | 500.4 | 0 | 239us | 9.2ms | 13.5ms | 466.3ms |
| 0.8 | 8 | 7565 | 756.4 | 0 | 469us | 38.0ms | 88.0ms | 677.5ms |
| 0.8 | 32 | 11781 | 1178.1 | 0 | 15.5ms | 88.6ms | 132.2ms | 255.6ms |
| 0.8 | 128 | 10763 | 1076.3 | 0 | 76.7ms | 393.3ms | 930.2ms | 1.23s |
| 1.0 | 1 | 49928 | 4968.9 | 0 | 179us | 301us | 428us | 126.8ms |
| 1.0 | 8 | 135021 | 13495.9 | 0 | 402us | 1.4ms | 3.4ms | 78.0ms |
| 1.0 | 32 | 233154 | 23309.9 | 0 | 970us | 3.7ms | 7.2ms | 63.9ms |
| 1.0 | 128 | 298585 | 29849.0 | 0 | 2.9ms | 12.2ms | 22.2ms | 116.1ms |

### Totals and errors

- **48 runs, 2,954,056 attempts, 164 errors (0.006%).**
- Every error is in a `shards=16, conc=128` write-heavy run:
  - `read=0, conc=128`: 46/3132 (1.5%) — `DeadlineExceeded=23`,
    `Unavailable=22`, `not-leader-located=1`
  - `read=0.5, conc=128`: 118/3437 (3.4%) — `not-leader-located=110`,
    `Unavailable=8`
  - the other 46 runs: **0 errors**.

### Observations (facts read off the tables)

- **Write throughput saturates ≈ 300–370 ops/s per group** on this
  machine regardless of concurrency (fsync per proposal is the ceiling);
  beyond that, extra concurrency buys latency, not throughput
  (shards=1, read=0: 8.2 ms p50 at conc=1 → 408.6 ms p50 at conc=128 for
  the same ~300 ops/s).
- **Reads (ratio 1.0) reach 44,766 ops/s** (shards=1, conc=128) — the
  ReadIndex path does not write to the log.
- **Mixed 0.8 workloads scale with group count**: 601.6 ops/s at
  shards=1 vs 1178.1 ops/s at shards=16 (same conc=32) — independent
  groups commit in parallel.
- **shards=16 write-heavy at conc=128 is the only cell with failures**
  (17 Raft groups × 128 writers on 8 shared cores); p99 grows to 2.9–3.8 s.

## Leader failover

`scripts/bench_failover.sh 30s 10s`: 3-node cluster, 1000 keys preloaded,
8 workers at read ratio 0.5, 30 s window, **leader (node3) killed between
t=2 s and t=3 s**. Raw progress lines:

```
progress t=  2s attempts=529 (+174) errors=0 (+0)
leader killed at 21:48:22 (pid 10411)
progress t=  3s attempts=694 (+165) errors=0 (+0)
progress t=  4s attempts=694 (+0)   errors=0 (+0)
progress t=  5s attempts=702 (+8)   errors=8 (+8)
progress t=  6s attempts=870 (+168) errors=8 (+0)
progress t=  7s attempts=1241 (+371) errors=8 (+0)
progress t=  8s attempts=1677 (+436) errors=8 (+0)
...
progress t= 30s attempts=10713 (+576) errors=8 (+0)
```

- **New leader elected 2 s after the kill** (`new leader after kill:
  node1:2s`, polled concurrently at 0.5 s granularity).
- **Client-observed outage ≈ 3 s** (t=4 stall, t=5 the 8 failures, t=6
  traffic resumes); after t=6 there are **no further errors for the
  remaining 24 s**.
- The 8 failures are `not leader and no leader could be located`
  (client-side), excluded from latency.
- Whole run: 10,721 attempts, 8 errors (0.08%), 357.1 ops/s,
  p50 16.3 ms, p95 48.7 ms, p99 105.4 ms, max 818.7 ms.

## Data-loss check (`-sweep`)

The failover run ends with a sweep: every preloaded key is read once,
with preloading disabled so a missing key cannot be recreated:

```
sweep: 1000/1000 keys present, 0 missing
```

16,409 read attempts over 2 s at 8,203.7 ops/s, **0 errors** — after a
leader was killed mid-run, no preloaded key was lost.

## Not covered (yet)

- **`scripts/bench_recovery.sh` (snapshot restore after node loss) has
  NOT been run — unverified.**
- Multi-machine (real network RTT), payload sizes other than 100 B,
  machine-crash durability, sustained runs longer than 30 s.
