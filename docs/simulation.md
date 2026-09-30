# Deterministic simulated network

Phase 14 delivers the testing half of the transport seam (spec §18). The
same Raft core that runs against gRPC in production runs against an
in-process network with seeded faults — no sockets, no Docker, no kernel
in the loop.

```
internal/transport/transport.go     Transport / Handler interfaces (the seam)
internal/transport/grpc/real.go     RealTransport: production gRPC
internal/transport/sim/network.go   Network: faults, queue, clock, controls
internal/transport/sim/transport.go Transport: the transport.Transport impl
```

## Why

Fault testing must not rest on Docker/network manipulation (spec §18):
containers are slow, nondeterministic, and unrepeatable — when a test
fails at 3 a.m. you need the *same* failure again, from a seed. A
transport abstraction gives every failure mode a precise, scriptable
control knob, and the Raft core is byte-for-byte the same code on both
sides of the seam.

## One Network per test cluster

A `sim.Network` owns everything shared: the seeded RNG, the delivery
queue, the topology rules, the virtual clock. Each node gets its own
`sim.Transport` (its identity) bound to the shared Network; the test
attaches inbound handlers with `SetHandler(id, node)`.

Every outbound RPC is a **call**: it is stamped with its virtual send
time, queued, and processed by the **driver** — the only component that
samples faults, schedules deliveries, and invokes handlers.

| Mode | Driver | Use |
|---|---|---|
| Manual (default) | the test, via `Advance(d)` | engine tests: deterministic quiescence points, zero goroutines |
| Auto (`Config.Auto`) | background goroutine, virtual time mapped 1:1 onto real time | cluster tests: nodes tick with the real-time ticker as usual |

`Advance(d)` moves virtual time and then processes *all* pending work —
fault draws for every enqueued call, every due delivery — before
returning. When it returns, the network is quiescent: a deterministic
assertion point. Auto mode maps `delay=500ms` to literally 500ms of wall
time (ordering is still queue-decided; the wall clock only bounds how
long the driver sleeps).

## Fault model

One draw pipeline per call, in this order, always from the single seeded
RNG — this single decision point is what "deterministic" means here:

| Spec fault | Control | Outcome |
|---|---|---|
| message delay | `Faults.MinDelay/MaxDelay` | sampled per call; delivery ordered by `(sendAt+delay, seq)` |
| packet loss | `Faults.DropRate` | `ErrDropped`; handler **never invoked** |
| message duplication | `Faults.DupRate` | responder invoked **twice** (two delay draws); caller gets exactly one response |
| message reordering | delay spread | emerges from `(deliverAt, seq)` — B sent after A arrives first if its delay is shorter |
| network partition | `Partition(groups...)`, `Block(a,b)` | `ErrBlocked` both directions; cleared by `Heal()` |
| node isolation | `Isolate(id)` | id ↔ world blocked, rest unaffected |
| slow node | `SetSlow(id, d)` | extra delay on any call touching id |
| node down | `SetHandler(id, nil)` | `ErrNoRoute` (connection-refused analog; draws no randomness) |

Semantics pinned by tests:

- **Partition is decided at send, not at delivery** — a message already
  in flight completes, like a packet already on the wire. New calls fail.
- **Drop is a failure the sender sees** — Raft's response paths treat
  every non-nil error as "this peer did not answer", which is exactly
  right for `ErrDropped`, `ErrBlocked`, and `ErrNoRoute`.
- **Duplication exercises the responder, not the caller** — the
  duplicate is a second delivery of the request; the first response wins.

## Determinism: the guarantee and its boundary

**Guaranteed.** Given the same seed and the same ordered history of
calls and control operations, every fault decision and the entire
delivery order are identical. Scripted manual-mode histories are
bit-reproducible: `TestDeterminismSameSeedSameHistory` runs the same
40-call mixed-fault script twice and requires deep equality, then a
different seed must diverge.

**Honest boundary.** The network cannot control when concurrently
running node goroutines arrive with their calls: sequence numbers follow
arrival order at the Network, and Go's scheduler decides that. Seeded
*decisions* reproduce failures across runs; bit-exact reproduction of a
live multi-node run requires a serialized call history. Tests therefore
assert convergence ("eventually", via `waitFor`) exactly like every
other DistriKV test — the simulation removes timing luck from *faults*,
not from assertion style.

**Not claimed (and not needed):** single-threaded deterministic
scheduling of the nodes themselves. That would require running all node
loops on one goroutine, which the synchronous `Handle*` contract rules
out (mutual calls would deadlock). The spec asks for seeded, repeatable
fault testing — not a deterministic Go runtime.

## Tests

Engine (`internal/transport/sim/network_test.go`, manual mode):

| Test | Pins |
|---|---|
| `TestFaultFreeDelivery` | baseline call/response |
| `TestDropSkipsHandlerAndErrors` | drop ⇒ `ErrDropped`, handler untouched |
| `TestDelayBoundsDelivery` | not delivered at `delay−ε`, delivered after |
| `TestReorderingByDelay` | B sent after A arrives before it |
| `TestDuplicateDeliversTwice` | responder invoked twice, one response |
| `TestBlockPairAndHeal` | `A X B` both directions; heal restores |
| `TestPartitionGroupsAndIsolate` | majority/minority split; node isolation |
| `TestInFlightSurvivesPartition` | in-flight completes, new calls blocked |
| `TestNoRouteUntilRegistered` | node down / restart semantics |
| `TestSlowNodeAddsDelay` | slow-node extra delay |
| `TestDeterminismSameSeedSameHistory` | same seed ⇒ identical run; new seed diverges |
| `TestAutoModeDeliversInRealTime` | background driver; `Advance` rejected in auto |
| `TestCloseFailsCalls`, `TestInvalidFaultsPanic` | lifecycle + config validation |

Cluster (`internal/raft/simcluster_test.go`): the identical Raft core
elects, replicates, keeps committing with a follower partitioned away,
and after heal the isolated replica catches up with byte-identical logs
— zero gRPC anywhere.

## What Phase 15 built on this

The fault-scenario suite (spec §19) lives in
`internal/raft/faults_test.go` and is documented in
[docs/faults.md](faults.md): the three §19 scenarios (kill/restart,
partition, minority-cannot-commit), a seeded chaos schedule exercising
§17's message failures end-to-end, and a simulated disk failure.

Phase 16 then recorded operation histories over the same Network and
checked them for linearizability (docs/linearizability.md) — Tier 1's
finish line.
