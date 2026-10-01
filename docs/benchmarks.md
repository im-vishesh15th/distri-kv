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
| **Commit** | `67002f1` (group commit; baseline data kept in `bench-results/*-before-groupcommit.jsonl`) |
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
`bench-results/failover.jsonl` (one JSON object per run). The pre-group-commit
baseline runs are kept as `bench-results/matrix-before-groupcommit.jsonl` and
`bench-results/failover-before-groupcommit.jsonl`.

## Matrix: shards × read ratio × concurrency

48 runs = shards {1, 4, 16} × read ratio {0, 0.5, 0.8, 1.0} ×
concurrency {1, 8, 32, 128}. Each shard count starts a fresh 3-node
cluster. Latencies are end-to-end client-observed (over loopback gRPC),
successful ops only.

### shards=1

| read ratio | conc | attempts | ops/s | errors | p50 | p95 | p99 | max |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.0 | 1 | 1313 | 131.3 | 0 | 7.9ms | 9.6ms | 11.8ms | 57.0ms |
| 0.0 | 8 | 2798 | 279.8 | 0 | 19.1ms | 31.5ms | 92.5ms | 2.66s |
| 0.0 | 32 | 12926 | 1292.5 | 0 | 23.3ms | 34.4ms | 61.2ms | 83.0ms |
| 0.0 | 128 | 39452 | 3944.9 | 0 | 26.6ms | 68.3ms | 88.7ms | 110.1ms |
| 0.5 | 1 | 2826 | 282.6 | 0 | 1.4ms | 9.0ms | 10.2ms | 215.9ms |
| 0.5 | 8 | 4680 | 468.0 | 0 | 16.9ms | 25.0ms | 32.6ms | 215.6ms |
| 0.5 | 32 | 9758 | 975.8 | 0 | 32.2ms | 44.1ms | 65.1ms | 78.1ms |
| 0.5 | 128 | 12693 | 1269.3 | 0 | 101.3ms | 136.4ms | 156.2ms | 171.7ms |
| 0.8 | 1 | 7196 | 719.6 | 0 | 187us | 7.0ms | 8.8ms | 15.6ms |
| 0.8 | 8 | 7184 | 717.8 | 0 | 9.1ms | 19.9ms | 39.0ms | 518.5ms |
| 0.8 | 32 | 11928 | 1192.7 | 0 | 25.3ms | 40.1ms | 57.2ms | 225.4ms |
| 0.8 | 128 | 20149 | 2014.7 | 0 | 64.5ms | 86.1ms | 99.5ms | 123.5ms |
| 1.0 | 1 | 46306 | 4630.6 | 0 | 175us | 360us | 600us | 130.2ms |
| 1.0 | 8 | 145128 | 14512.7 | 0 | 406us | 1.1ms | 2.3ms | 181.5ms |
| 1.0 | 32 | 411929 | 41192.7 | 0 | 664us | 1.5ms | 2.6ms | 27.2ms |
| 1.0 | 128 | 394596 | 39458.9 | 0 | 2.3ms | 6.6ms | 17.1ms | 567.5ms |

### shards=4

| read ratio | conc | attempts | ops/s | errors | p50 | p95 | p99 | max |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.0 | 1 | 1451 | 145.1 | 0 | 6.6ms | 9.1ms | 10.3ms | 93.6ms |
| 0.0 | 8 | 2437 | 243.7 | 0 | 31.8ms | 50.8ms | 62.0ms | 191.6ms |
| 0.0 | 32 | 5610 | 561.0 | 0 | 53.5ms | 91.6ms | 166.1ms | 290.1ms |
| 0.0 | 128 | 13998 | 1399.8 | 0 | 68.5ms | 261.5ms | 405.0ms | 876.9ms |
| 0.5 | 1 | 2920 | 292.0 | 0 | 2.9ms | 8.7ms | 9.7ms | 14.0ms |
| 0.5 | 8 | 4188 | 418.8 | 0 | 19.9ms | 39.6ms | 50.2ms | 153.4ms |
| 0.5 | 32 | 7019 | 701.9 | 0 | 43.4ms | 84.1ms | 143.7ms | 255.7ms |
| 0.5 | 128 | 10848 | 1084.7 | 0 | 90.2ms | 281.6ms | 341.2ms | 418.8ms |
| 0.8 | 1 | 7081 | 708.1 | 0 | 216us | 7.0ms | 8.9ms | 16.8ms |
| 0.8 | 8 | 8512 | 851.1 | 0 | 6.0ms | 27.6ms | 35.8ms | 109.4ms |
| 0.8 | 32 | 12780 | 1277.9 | 0 | 25.1ms | 55.4ms | 70.9ms | 95.9ms |
| 0.8 | 128 | 18826 | 1882.5 | 0 | 59.4ms | 155.6ms | 192.5ms | 246.0ms |
| 1.0 | 1 | 58072 | 5807.2 | 0 | 157us | 249us | 342us | 77.1ms |
| 1.0 | 8 | 193649 | 19364.8 | 0 | 330us | 864us | 1.6ms | 26.1ms |
| 1.0 | 32 | 255273 | 25526.9 | 0 | 822us | 3.0ms | 6.6ms | 216.9ms |
| 1.0 | 128 | 376107 | 37608.9 | 0 | 2.4ms | 9.3ms | 16.5ms | 133.8ms |

### shards=16

| read ratio | conc | attempts | ops/s | errors | p50 | p95 | p99 | max |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.0 | 1 | 1392 | 139.2 | 0 | 7.2ms | 9.6ms | 11.0ms | 14.0ms |
| 0.0 | 8 | 2309 | 230.9 | 0 | 32.2ms | 57.9ms | 72.0ms | 89.9ms |
| 0.0 | 32 | 2981 | 298.1 | 0 | 92.7ms | 230.9ms | 311.1ms | 532.3ms |
| 0.0 | 128 | 4109 | 405.6 | 53 | 190.4ms | 920.5ms | 2.04s | 3.16s |
| 0.5 | 1 | 2735 | 273.5 | 0 | 4.2ms | 9.0ms | 10.3ms | 16.5ms |
| 0.5 | 8 | 4373 | 437.3 | 0 | 20.8ms | 43.5ms | 56.0ms | 123.1ms |
| 0.5 | 32 | 5473 | 547.3 | 0 | 52.8ms | 145.0ms | 215.0ms | 643.1ms |
| 0.5 | 128 | 6648 | 655.2 | 96 | 113.0ms | 522.1ms | 1.60s | 3.11s |
| 0.8 | 1 | 7153 | 715.3 | 0 | 219us | 7.0ms | 8.8ms | 33.6ms |
| 0.8 | 8 | 10226 | 1022.6 | 0 | 399us | 29.7ms | 39.6ms | 150.5ms |
| 0.8 | 32 | 12364 | 1236.3 | 0 | 13.8ms | 84.0ms | 129.3ms | 398.2ms |
| 0.8 | 128 | 14757 | 1475.7 | 0 | 65.2ms | 243.4ms | 640.6ms | 1.03s |
| 1.0 | 1 | 61055 | 6105.5 | 0 | 153us | 240us | 310us | 2.2ms |
| 1.0 | 8 | 191023 | 19102.3 | 0 | 324us | 897us | 1.7ms | 42.4ms |
| 1.0 | 32 | 286346 | 28634.6 | 0 | 824us | 2.9ms | 5.5ms | 47.6ms |
| 1.0 | 128 | 342205 | 34219.4 | 0 | 2.7ms | 10.1ms | 18.7ms | 122.3ms |

### Totals and errors

- **48 runs, 3,064,782 attempts, 149 errors (0.005%).**
- Every error is in a `shards=16, conc=128` write-heavy run:
  - `read=0, conc=128`: 53/4109 (1.3%)
  - `read=0.5, conc=128`: 96/6648 (1.4%)
  - all 149 are the same client-side error —
    `not leader and no leader could be located` (JSONL key
    `Unknown(client: not leader and no leader could be located: raft: not)`)
  - the other 46 runs: **0 errors**.

### Before and after group commit

The baseline (`bench-results/matrix-before-groupcommit.jsonl`, commit
`1028d09`) predates two changes: the election-timeout fix (`7489e99`) and
group commit (`67002f1`). Group commit alone was isolated with a matched
probe (3-node, write-only, 30 s, keyspace 10000, payload 100 B, same
post-election-timeout code before and after):

| conc | before | after | speedup | p50 before → after |
|---:|---:|---:|---:|---:|
| 8 | 251.4 ops/s | 291.9 ops/s | 1.2× | 31.5 ms → 22.0 ms |
| 32 | 302.2 ops/s | 924.0 ops/s | 3.1× | 101.9 ms → 26.7 ms |
| 128 | 347.3 ops/s | 1246.2 ops/s | 3.6× | 370.7 ms → 68.0 ms |

Baseline matrix vs this run, write cells (read=0):

| shards | conc | before ops/s | after ops/s | speedup | before p50 | after p50 |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 116.8 | 131.3 | 1.1× | 8.2ms | 7.9ms |
| 1 | 8 | 213.6 | 279.8 | 1.3× | 36.4ms | 19.1ms |
| 1 | 32 | 299.8 | 1292.5 | 4.3× | 105.6ms | 23.3ms |
| 1 | 128 | 315.7 | 3944.9 | 12.5× | 408.6ms | 26.6ms |
| 4 | 1 | 127.2 | 145.1 | 1.1× | 7.7ms | 6.6ms |
| 4 | 8 | 205.0 | 243.7 | 1.2× | 36.8ms | 31.8ms |
| 4 | 32 | 232.0 | 561.0 | 2.4× | 109.5ms | 53.5ms |
| 4 | 128 | 310.8 | 1399.8 | 4.5× | 339.6ms | 68.5ms |
| 16 | 1 | 126.6 | 139.2 | 1.1× | 7.9ms | 7.2ms |
| 16 | 8 | 192.8 | 230.9 | 1.2× | 38.2ms | 32.2ms |
| 16 | 32 | 259.9 | 298.1 | 1.1× | 105.4ms | 92.7ms |
| 16 | 128 | 308.6 | 405.6 | 1.3× | 189.9ms | 190.4ms |

Why it was flat (measured, not assumed):

- A 20 s CPU profile of the leader during the before-run's write load:
  **1.68 s samples over 20.05 s = 8.38% CPU** — the server was waiting,
  not computing. On-CPU time is `write(2)` and Go's darwin durability
  flush (`internal/poll.(*FD).Fsync → fcntl(F_FULLFSYNC)`).
- Raw `File.Sync()` probe on this disk: **avg 2.719 ms → 368 syncs/s**.
  The before-run measured 365.7 write ops/s — one proposal cost exactly
  one serialized F_FULLFSYNC on the group's event loop.
- Shared-disk contention was ruled out: the same probe costs 2.73 ms per
  write while all three nodes fsync the same volume, vs 2.72 ms idle.
- Fix (commit `67002f1`): consecutive queued proposals are drained into
  one batch — one log write, one fsync, one broadcast. fsync-before-ack
  is unchanged.

### Observations (facts read off the tables)

- **Write throughput now scales with offered load**: shards=1, read=0
  goes 131.3 → 279.8 → 1292.5 → 3944.9 ops/s at conc 1/8/32/128, and
  p50 barely moves from conc=32 to conc=128 (23.3 ms → 26.6 ms).
  At conc=1 there is nothing to batch, so the gain is only 1.1× —
  expected for a batching fix.
- **Read throughput is in the same band as the baseline run**
  (34,219–39,459 ops/s at conc=128 vs 29,849–44,766 before; run-to-run
  variance ±15%). ReadIndex never writes to the log, so group commit
  does not touch this path.
- **Aggregate write throughput falls as shard count rises**: at conc=128,
  3944.9 (shards=1) → 1399.8 (shards=4) → 405.6 (shards=16); at conc=32,
  1292.5 → 561.0 → 298.1. More groups means fewer writers per group and
  shallower queues (smaller fsync batches), plus 17 groups per node on
  8 shared cores. The shards=16 deficit and its conc=128 failures are
  under investigation.
- **Mixed 0.8 at conc=32 is flat across shard counts**
  (1192.7 / 1277.9 / 1236.3 ops/s).
- **The only failures remain the two `shards=16, conc=128` write-heavy
  cells** — 149 client-side `not leader and no leader could be located`
  errors, p99 up to 2.04 s; all other 46 runs are error-free.

## Leader failover

`scripts/bench_failover.sh 30s 10s`: 3-node cluster, 1000 keys preloaded,
8 workers at read ratio 0.5, 30 s window, **leader (node2) killed between
t=4 s and t=5 s**. Raw progress lines:

```
progress t=  4s attempts=1829 (+474) errors=0 (+0)
leader killed at 22:56:31 (pid 24643)
progress t=  5s attempts=1875 (+46)  errors=8 (+8)
progress t=  6s attempts=1875 (+0)   errors=8 (+0)
progress t=  7s attempts=2186 (+311) errors=8 (+0)
progress t=  8s attempts=2744 (+558) errors=8 (+0)
...
progress t= 30s attempts=17502 (+636) errors=8 (+0)
new leader after kill: node1:3s
```

- **New leader elected 3 s after the kill** (`new leader after kill:
  node1:3s`, polled concurrently at 0.5 s granularity).
- **Client-observed outage ≈ 3 s** (t=5 the 8 failures, t=6 stall, t=7
  traffic resumes); after t=7 there are **no further errors for the
  remaining 23 s**.
- The 8 failures are `not leader and no leader could be located`
  (client-side), excluded from latency.
- Whole run: 17,510 attempts, 8 errors (0.05%), 583.4 ops/s,
  p50 12.4 ms, p95 20.2 ms, p99 24.5 ms, max 1.58 s (a successful
  operation that waited through the election window).
- The pre-group-commit baseline run was 357.1 ops/s, p99 105.4 ms —
  group commit helps this mixed workload too.

## Data-loss check (`-sweep`)

The failover run ends with a sweep: every preloaded key is read once,
with preloading disabled so a missing key cannot be recreated:

```
sweep: 1000/1000 keys present, 0 missing
```

20,488 read attempts over 2 s at 10,103.8 ops/s, **0 errors** — after a
leader was killed mid-run, no preloaded key was lost.

## Not covered (yet)

- **`scripts/bench_recovery.sh` (snapshot restore after node loss) has
  NOT been run — unverified.**
- Multi-machine (real network RTT), payload sizes other than 100 B,
  machine-crash durability, sustained runs longer than 30 s.
