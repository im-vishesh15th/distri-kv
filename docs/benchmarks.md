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
| **Commit** | `33d6666` (pre-vote; baselines kept in `bench-results/*-before-groupcommit.jsonl` and `bench-results/*-before-prevote.jsonl`) |
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
- Elections run the two-phase **pre-vote** protocol (Raft dissertation
  §9.6, commit `33d6666`): an election-timeout node first asks a
  majority whether it *could* win — nothing persisted, no term moved —
  and only a pre-vote majority starts a real campaign. A node that
  cannot win (partitioned, behind, or facing a majority that still hears
  a leader) never changes a term.
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
scripts/bench_scaling.sh 10s 2s     # node scaling 1/3/5: 27 runs, ~7 min
scripts/bench_failover.sh 30s 10s   # leader kill mid-run + data sweep
scripts/bench_recovery.sh           # snapshot restore after node + leader loss
```

Raw machine-readable results: `bench-results/matrix.jsonl`,
`bench-results/failover.jsonl` (one JSON object per run). Baselines the
tables below were compared against are kept alongside:
`matrix-before-groupcommit.jsonl` / `failover-before-groupcommit.jsonl`
(commit `1028d09`, before group commit) and `matrix-before-prevote.jsonl`
/ `failover-before-prevote.jsonl` (commit `67002f1`, before pre-vote).

## Matrix: shards × read ratio × concurrency

48 runs = shards {1, 4, 16} × read ratio {0, 0.5, 0.8, 1.0} ×
concurrency {1, 8, 32, 128}. Each shard count starts a fresh 3-node
cluster. Latencies are end-to-end client-observed (over loopback gRPC),
successful ops only.

### shards=1

| read ratio | conc | attempts | ops/s | errors | p50 | p95 | p99 | max |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 1 | 1319 | 131.9 | 0 | 8.0ms | 9.1ms | 10.3ms | 77.5ms |
| 0 | 8 | 4065 | 406.5 | 0 | 19.1ms | 24.2ms | 53.5ms | 112.5ms |
| 0 | 32 | 13612 | 1361.1 | 0 | 22.5ms | 29.1ms | 61.1ms | 72.0ms |
| 0 | 128 | 45868 | 4586.7 | 0 | 23.3ms | 60.7ms | 68.8ms | 162.3ms |
| 0.5 | 1 | 2737 | 273.7 | 0 | 3.6ms | 9.0ms | 10.2ms | 124.6ms |
| 0.5 | 8 | 4356 | 435.6 | 0 | 17.0ms | 29.3ms | 74.9ms | 170.1ms |
| 0.5 | 32 | 8529 | 852.9 | 0 | 36.6ms | 54.5ms | 79.3ms | 118.0ms |
| 0.5 | 128 | 12512 | 1251.1 | 0 | 100.9ms | 139.1ms | 181.1ms | 250.7ms |
| 0.8 | 1 | 6337 | 633.6 | 0 | 196µs | 7.2ms | 17.5ms | 65.3ms |
| 0.8 | 8 | 9171 | 917.1 | 0 | 8.7ms | 18.0ms | 22.7ms | 35.9ms |
| 0.8 | 32 | 13471 | 1347.0 | 0 | 23.8ms | 34.2ms | 41.0ms | 51.4ms |
| 0.8 | 128 | 21183 | 2118.3 | 0 | 59.9ms | 83.9ms | 104.3ms | 130.9ms |
| 1 | 1 | 64405 | 6440.5 | 0 | 146µs | 223µs | 300µs | 803µs |
| 1 | 8 | 226235 | 22623.5 | 0 | 328µs | 574µs | 790µs | 19.2ms |
| 1 | 32 | 461346 | 46134.6 | 0 | 622µs | 1.3ms | 1.8ms | 13.8ms |
| 1 | 128 | 604948 | 60494.4 | 0 | 1.9ms | 3.7ms | 5.3ms | 29.9ms |

### shards=4

| read ratio | conc | attempts | ops/s | errors | p50 | p95 | p99 | max |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 1 | 1703 | 170.3 | 0 | 5.9ms | 9.0ms | 10.2ms | 14.9ms |
| 0 | 8 | 2523 | 252.3 | 0 | 30.9ms | 48.0ms | 55.6ms | 116.2ms |
| 0 | 32 | 5656 | 565.6 | 0 | 51.9ms | 91.8ms | 212.3ms | 318.5ms |
| 0 | 128 | 15661 | 1566.1 | 0 | 61.4ms | 269.4ms | 391.0ms | 475.4ms |
| 0.5 | 1 | 2952 | 295.2 | 0 | 1.1ms | 8.6ms | 9.6ms | 51.2ms |
| 0.5 | 8 | 4095 | 409.5 | 0 | 20.7ms | 38.7ms | 49.0ms | 127.4ms |
| 0.5 | 32 | 7091 | 709.1 | 0 | 44.0ms | 81.5ms | 123.3ms | 250.9ms |
| 0.5 | 128 | 11190 | 1119.0 | 0 | 93.0ms | 264.6ms | 356.9ms | 490.7ms |
| 0.8 | 1 | 7776 | 777.6 | 0 | 206µs | 6.7ms | 8.6ms | 14.2ms |
| 0.8 | 8 | 8673 | 867.3 | 0 | 6.1ms | 27.3ms | 34.8ms | 66.5ms |
| 0.8 | 32 | 13122 | 1312.2 | 0 | 24.1ms | 54.3ms | 72.1ms | 170.3ms |
| 0.8 | 128 | 18834 | 1883.4 | 0 | 56.5ms | 163.2ms | 214.7ms | 364.3ms |
| 1 | 1 | 57808 | 5780.8 | 0 | 163µs | 245µs | 332µs | 5.0ms |
| 1 | 8 | 179279 | 17927.9 | 0 | 324µs | 849µs | 1.8ms | 304.3ms |
| 1 | 32 | 362603 | 36260.3 | 0 | 699µs | 2.1ms | 3.5ms | 40.0ms |
| 1 | 128 | 450869 | 45086.9 | 0 | 2.2ms | 7.1ms | 12.1ms | 86.5ms |

### shards=16

| read ratio | conc | attempts | ops/s | errors | p50 | p95 | p99 | max |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 1 | 1401 | 140.1 | 0 | 7.0ms | 9.4ms | 11.9ms | 48.2ms |
| 0 | 8 | 2347 | 234.7 | 0 | 32.8ms | 53.6ms | 66.9ms | 98.5ms |
| 0 | 32 | 3232 | 323.2 | 0 | 88.4ms | 204.6ms | 273.7ms | 391.9ms |
| 0 | 128 | 4344 | 434.4 | 0 | 169.0ms | 1.23s | 2.30s | 3.28s |
| 0.5 | 1 | 2965 | 296.5 | 0 | 711µs | 8.4ms | 9.3ms | 19.1ms |
| 0.5 | 8 | 4535 | 453.5 | 0 | 19.8ms | 42.6ms | 55.4ms | 246.2ms |
| 0.5 | 32 | 5454 | 545.4 | 0 | 50.7ms | 151.0ms | 284.3ms | 606.7ms |
| 0.5 | 128 | 7732 | 773.2 | 0 | 122.1ms | 442.5ms | 1.13s | 2.43s |
| 0.8 | 1 | 7736 | 773.6 | 0 | 207µs | 6.6ms | 8.5ms | 20.6ms |
| 0.8 | 8 | 10555 | 1055.5 | 0 | 436µs | 28.9ms | 37.5ms | 168.8ms |
| 0.8 | 32 | 13505 | 1350.4 | 0 | 14.8ms | 75.4ms | 103.4ms | 176.7ms |
| 0.8 | 128 | 15991 | 1599.0 | 0 | 61.3ms | 225.0ms | 463.7ms | 744.4ms |
| 1 | 1 | 60572 | 6057.1 | 0 | 156µs | 233µs | 307µs | 1.1ms |
| 1 | 8 | 218814 | 21881.2 | 0 | 294µs | 726µs | 1.4ms | 28.7ms |
| 1 | 32 | 333445 | 33344.5 | 0 | 696µs | 2.6ms | 4.4ms | 45.1ms |
| 1 | 128 | 421922 | 42191.0 | 0 | 2.3ms | 8.1ms | 13.4ms | 126.8ms |

### Totals and errors

- **48 runs, 3,764,479 attempts, 0 errors.** Every cell clean.
- The run this one is compared against (`matrix-before-prevote.jsonl`,
  commit `67002f1`) had 149 errors (0.005%) — all client-side
  `not leader and no leader could be located`, all in the two
  `shards=16, conc=128` write-heavy cells (53/4109 and 96/6648), the
  other 46 runs error-free. The before/after breakdown is under
  [Before and after pre-vote](#before-and-after-pre-vote) below.

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

Baseline matrix vs the group-commit run (that run is now kept as
`matrix-before-prevote.jsonl`), write cells (read=0):

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

### Before and after pre-vote

The pre-pre-vote baseline is `bench-results/matrix-before-prevote.jsonl`
(commit `67002f1`). Its 149 errors were all client-side
`not leader and no leader could be located`, and its retained server
logs — correlated against each cell's `started_at` — show elections in
exactly the two failing cells and nowhere else in the run, with one
group's term inflating to **10** (nine failed rounds in under 10 s).
After pre-vote (`33d6666`):

| cell | before (`67002f1`) | after (`33d6666`) |
|---|---|---|
| shards=16, read=0, conc=128 | 405.6 ops/s, **53 errors**, in-window elections, terms to 6 | 434.4 ops/s, **0 errors**, in-window elections, terms bounded to 3 |
| shards=16, read=0.5, conc=128 | 655.2 ops/s, **96 errors**, in-window elections incl. term 10 | 773.2 ops/s, **0 errors**, one in-window election, term 5 |
| all other 46 cells | 0 errors | 0 errors |
| **matrix total** | 3,064,782 attempts, 149 errors | **3,764,479 attempts, 0 errors** |

What pre-vote changed, stated precisely:

- It did **not** remove the transient stalls. This run's server logs
  show elections still starting inside both target cells (4 and 1
  in-window `became_leader` events): 17 groups × 128 writers on 8 cores
  still occasionally starves a group's leader past its heartbeat
  budget.
- What it removed is the **cascade**. Before, those elections inflated
  terms to 6 and 10 and produced 149 client failures across the two
  cells; after, the first refused pre-vote round ends the attempt (a
  majority that still hears its leader grants nothing), terms stay at
  ≤3 / ≤5, elections stay inside their cell window, and no client
  operation fails.
- A separate fresh-cluster probe of the same two cells (identical
  parameters, ports 8991–8993) saw **zero** elections inside both
  windows and zero errors, plus a 30 s run of the same cell at zero
  errors — when no stall occurs, pre-vote is invisible.

`campaign` events are countable in these logs because campaigning was
promoted from debug to info level in the pre-vote commit; it was
invisible in the before-run's logs (`became_leader` was always info, so
the before/after election comparison above uses `became_leader` on both
sides).

### Observations (facts read off the tables)

- **Write throughput scales with offered load**: shards=1, read=0 goes
  131.9 → 406.5 → 1361.1 → 4586.7 ops/s at conc 1/8/32/128, and p50
  barely moves from conc=32 to conc=128 (22.5 → 23.3 ms). At conc=1
  there is nothing to batch — that cell is raw per-op cost.
- **Read throughput varies a lot between runs on this laptop**
  (shards=1, read=1.0, conc=128: 44,766 → 39,459 → 60,494 ops/s across
  the three documented runs). ReadIndex never writes to the log and
  neither fix touches this path, so run-to-run machine state dominates —
  treat read numbers as ±30%, not as trends.
- **Aggregate write throughput still falls as shard count rises**: at
  conc=128, 4586.7 (shards=1) → 1566.1 (shards=4) → 434.4 (shards=16);
  at conc=32, 1361.1 → 565.6 → 323.2. More groups means fewer writers
  per group and shallower queues — smaller fsync batches, so the fixed
  per-fsync cost (~2.7 ms) amortizes over fewer writes — plus 17 groups
  per node competing for 8 cores. Mechanism labeled as hypothesis: not
  separately profiled in this run.
- **The `shards=16, conc=128` p99 (2.30 s) is queueing, not
  instability**: with 128 closed-loop workers at 434.4 ops/s, Little's
  law pins mean latency at 128/434.4 = 295 ms — the measured mean is
  305.8 ms (within 4%), and p99 ≈ 8× the mean is the queue-drain tail
  of a saturated cell. The instability (errors) is gone; the tail is
  the cost of saturating the cell described by the row above.
- **Mixed 0.8 at conc=32 is flat across shard counts**
  (1347.0 / 1312.2 / 1350.4 ops/s).

## Node scaling: 1 / 3 / 5 nodes

`scripts/bench_scaling.sh 10s 2s`, run at `e2d8de3` (identical code to
`33d6666` — that commit differs only in docs): one Raft group, the same
keyspace/payload/warmup as the matrix cells, ports 9091–9095, fresh
cluster per node count. 27 runs = nodes {1, 3, 5} × read ratio
{0, 0.5, 1.0} × concurrency {1, 32, 128}.

The extra processes matter here: the 5-node runs put **6 processes on
8 cores** (5 servers + load generator), so the 5-column mixes
replication cost with CPU contention — there is no way to separate them
on this machine, and the numbers are reported that way.

Throughput (ops/s):

| read ratio | conc | 1 node | 3 nodes | 5 nodes |
|---:|---:|---:|---:|---:|
| 0 | 1 | 372.5 | 169.7 | 107.1 |
| 0 | 32 | 5222.0 | 1390.2 | 1140.4 |
| 0 | 128 | 16702.0 | 4629.5 | 3500.6 |
| 0.5 | 1 | 793.4 | 301.4 | 223.6 |
| 0.5 | 32 | 1408.0 | 947.9 | 784.3 |
| 0.5 | 128 | 1440.3 | 1333.2 | 1211.8 |
| 1 | 1 | 19620.7 | 6450.2 | 4260.2 |
| 1 | 32 | 79754.9 | 46711.0 | 37793.4 |
| 1 | 128 | 85263.8 | 61156.7 | 55470.6 |

Write latency (read=0), p50 / p99:

| conc | 1 node | 3 nodes | 5 nodes |
|---:|---:|---:|---:|
| 1 | 3.0 ms / 5.5 ms | 6.0 ms / 9.6 ms | 9.2 ms / 16.2 ms |
| 32 | 5.9 ms / 19.8 ms | 21.9 ms / 60.4 ms | 26.8 ms / 83.0 ms |
| 128 | 6.1 ms / 22.0 ms | 23.4 ms / 67.5 ms | 29.4 ms / 98.4 ms |

**All 27 runs: 0 errors, 4,381,624 attempts.**

Cross-script check: the 3-node column reproduces the matrix's shards=1
cells within run variance (read=0 conc=128: 4629.5 vs 4586.7; read=1.0
conc=128: 61156.7 vs 60494.4; read=0.5 conc=128: 1333.2 vs 1251.1), so
the two scripts measure the same thing.

What the numbers say:

- **Quorum cost, per op (conc=1, read=0):** 3.0 ms → 6.0 ms → 9.2 ms.
  1 node is one local F_FULLFSYNC (2.7 ms, measured probe above) and no
  network. 3 nodes ≈ leader fsync + the acking follower's fsync chained
  by the ack (every replica is durable before a write completes:
  fsync-before-ack) ≈ 2× one fsync plus two loopback hops. 5 nodes
  ≈ 3× — consistent with three F_FULLFSYNCs queueing serially on the
  one volume (leader + two acking followers), plus the six-process
  contention noted above; the split between those two is not isolated
  here.
- **Saturation (conc=128, read=0):** 16702 → 4629 → 3500 ops/s. The
  1-node leader batches all 128 writers' proposals into deep fsync
  batches; adding replicas adds a durable ack chain per batch and more
  processes competing for the same cores.
- **Reads pay the ReadIndex quorum, not fsyncs** (read=1.0, conc=128:
  85264 → 61157 → 55471 ops/s; conc=1 p50 48 µs → 145 µs → 226 µs):
  self-only vs 2-of-3 vs 3-of-5 confirmations over loopback, no disk
  write on this path.
- **Mixed traffic (read=0.5) barely gains from added concurrency and
  lands at a similar level for every node count** — 1-node
  1408 → 1440 ops/s from conc 32 → 128, 3-node 948 → 1333, 5-node
  784 → 1212 (pure writes gain 3–4× over the same step). Since the
  1-node column has no replication at all and still flattens, the
  binding constraint is not replication. Hypothesis, not verified: a
  read event between queued proposals ends the propose batch, so fsync
  batches collapse to ~1–2 writes and this disk's fsync ceiling
  (~368/s, measured under *Before and after group commit* above) binds
  again. Not separately profiled.

## Leader failover

`scripts/bench_failover.sh 30s 10s`: 3-node cluster, 1000 keys preloaded,
8 workers at read ratio 0.5, 30 s window, **leader (node2) killed between
t=4 s and t=5 s**. Raw progress lines:

```
progress t=  4s attempts=1917 (+506) errors=0 (+0)
leader killed at 23:54:58 (pid 40596)
progress t=  5s attempts=2036 (+119) errors=8 (+8)
progress t=  6s attempts=2621 (+585) errors=8 (+0)
progress t=  7s attempts=3101 (+480) errors=8 (+0)
...
progress t= 30s attempts=17495 (+574) errors=8 (+0)
new leader after kill: node3:1s
```

- **New leader elected 1 s after the kill** (`new leader after kill:
  node3:1s`, polled concurrently at 0.5 s granularity).
- **Client-observed degradation ≈ 1 s**: t=5 slows to +119 attempts and
  carries the 8 failures, t=6 is fully back (+585); no further errors
  for the remaining 24 s.
- The 8 failures are `not leader and no leader could be located`
  (client-side), excluded from latency.
- Whole run: 17,504 attempts, 8 errors (0.05%), 583.2 ops/s,
  p50 13.2 ms, p95 20.3 ms, p99 25.9 ms, **max 162.0 ms** — no
  successful operation could have spanned the kill-to-new-leader
  window (it lasts >500 ms; earlier runs showed max 819 ms and
  1.58 s).
- Pre-vote costs nothing when the leader is genuinely dead: every
  survivor is already past the minimum election timeout, so the first
  pre-vote round is granted immediately. Baselines in order:
  357.1 ops/s / p99 105.4 ms (pre-group-commit), 583.4 ops/s / p99
  24.5 ms / new leader 3 s (group commit, pre-pre-vote), 583.2 ops/s /
  p99 25.9 ms / new leader 1 s / max 162 ms (this run).

## Data-loss check (`-sweep`)

The failover run ends with a sweep: every preloaded key is read once,
with preloading disabled so a missing key cannot be recreated:

```
sweep: 1000/1000 keys present, 0 missing
```

20,555 read attempts over 2 s at 10,277.5 ops/s, **0 errors** — after a
leader was killed mid-run, no preloaded key was lost (prior runs:
16,409 reads at 8,203.7 ops/s, then 20,488 at 10,103.8 ops/s, also
0 missing).

## Snapshot recovery (node restart after log compaction)

`scripts/bench_recovery.sh` (default 8 s write phases), run at `e2d8de3`:
3 nodes with `-snapshot-every=100`. Raw stdout of the sequence:

```
=== starting 3 nodes (-snapshot-every=100) ===
leader: node1

=== write phase 1: 8s (read-ratio=0) ===
attempts: 2926 (reads 0, writes 2926)  errors: 0
throughput: 365.7 ops/s   latency all: p50=19.45ms p99=60.478ms

killing follower node2 (pid 45881)

=== write phase 2 with node down: 8s (forces compaction past it) ===
attempts: 3727 (reads 0, writes 3727)  errors: 0
throughput: 465.9 ops/s   latency all: p50=15.667ms p99=42.285ms

=== restarting node2 on its old data dir ===
recovery markers for node2 after restart:
  snapshot_restored (local snapshot load): 0.2s
  snapshot_installed (leader-shipped):     3.1s
  GetStatus shows a leader (rejoined):     3.1s

=== killing current leader to force election incl. restarted node ===
new leader: node2 (the restarted node)

=== post-recovery sweep (all 1000 keys must be present) ===
attempts: 28221 (reads 28221, writes 0)  errors: 0
throughput: 14110.4 ops/s
sweep: 1000/1000 keys present, 0 missing
```

- Both write phases ran with **0 errors while a follower was dead** —
  the surviving 2-node majority kept committing (phase 2 even ran
  faster, 465.9 vs 365.7 ops/s), and the leader snapshotted and
  compacted during phase 2, past node2's log position.
- After restart on its old data dir: **local snapshot load in 0.2 s**
  (`snapshot_restored`), then **3.1 s until the leader-shipped snapshot
  was installed** (`snapshot_installed` — observed in node2's log, so a
  plain log catch-up genuinely did not suffice) and the node rejoined
  (`GetStatus` answers with a known leader). Times are wall-clock from
  process start, polled at 0.1 s granularity per the script header.
- Killing the current leader next elected **the restarted node itself**
  (node2): an out-of-date node cannot win an election (§5.4.1), so this
  doubles as proof node2 had fully caught up.
- Post-recovery sweep: 1000/1000 keys present, 0 missing, 0 errors
  (28,221 read attempts at 14,110.4 ops/s) — snapshot recovery lost
  nothing.
- Raw results: `bench-results/recovery.jsonl` (3 records).

## Not covered (yet)

- Multi-machine (real network RTT), payload sizes other than 100 B,
  machine-crash durability, sustained runs longer than 30 s.
