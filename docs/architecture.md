# Architecture — DistriKV

> Status: Phase 0 (design baseline). This document is the source of truth for
> architectural decisions. Any change to it must be explicit, justified, and
> committed alongside the code that implements it.

## 1. What DistriKV is

A fault-tolerant, horizontally scalable distributed key-value database in Go,
built as a correctness-first engineering project and offered as a self-serve
service: sign up → get an API key → use a replicated KV store over gRPC.

**Guiding principle: DEPTH > BREADTH.** Every component must have a clear
architectural reason to exist. No buzzword features, no fabricated claims.

## 2. Layering

```
Layer 4 (product):   Website / signup → API keys → Gateway (auth, quotas, rate limits)
                                    │
Layer 3 (scale):     Cluster Router → hash(key) % 16384 → Shard Map → Raft groups
                                    │
Layer 2 (consensus): Raft leader election + log replication + quorum commit
                                    │
Layer 1 (engine):    Persistent Raft log (disk) → Replicated State Machine → KV map
```

Layers are built strictly bottom-up. The product layer (P1–P5) does not begin
until the engine passes its correctness suite (Tier 1, end of Phase 16).

## 3. Non-negotiable invariants

1. **No `internal/wal/`** — the persistent Raft log *is* the durable ordered
   command log (append, fsync, index/term, checksum, recovery, replay,
   truncate, compaction). There is exactly one durable log. In interview terms:
   the Raft log serves the role a WAL would, so a second log would be redundant.
2. **Custom Raft** — we implement Follower/Candidate/Leader, RequestVote,
   AppendEntries, InstallSnapshot ourselves. No Hashicorp Raft or equivalent.
   State we handle: persistent `currentTerm / votedFor / log`; volatile
   `commitIndex / lastApplied`; leader `nextIndex[] / matchIndex[]`.
3. **Transport abstraction** — Raft communicates only through a `Transport`
   interface: `RealTransport` (gRPC) in production, `SimulatedTransport`
   (`internal/transport/sim`, Phase 14) in tests (seeded delay / drop /
   reorder / duplicate / partition / isolation / slow node). Docker is for
   demos, never the foundation of correctness testing.
4. **Fixed slots, not consistent hashing** — `slot = hash(key) % 16384`;
   routing follows `key → slot → shard → Raft group → leader → node`.
5. **Replication path:** `Client → Leader → Persistent Raft Log → majority →
   Commit → Replicated State Machine (KV)`. Uncommitted entries are never
   visible as user state.
6. **Determinism** — all replicas apply the same committed log in the same
   order and therefore reach identical state (verified by state-hash
   comparisons in tests).
7. **No correctness claims without tests** — each documented guarantee maps to
   the test that proves it. No fabricated benchmark numbers, ever.

## 4. Data flow (single Raft group, Phases 3–13)

```
Client --gRPC--> Leader --Persistent Raft Log--> Followers --majority--> Commit --> KV StateMachine
                      │                                                        ▲
                      └── apply pump: commitIndex → lastApplied → apply ───────┘
```

Write path: leader appends to its durable log → replicates via AppendEntries →
majority acks → entry committed → applied in order to the KV engine → response
returned to the client.

Read path (Phase 11): ReadIndex — the leader confirms it is still leader in the
current term (heartbeat quorum), records a safe read index, waits until
`lastApplied >= readIndex`, then serves the read. Reading "because we're the
leader" without this step is insufficient and is explicitly rejected.

## 5. Sharded target architecture (Phases 17–21)

```
Client SDK -> Cluster Router -> hash -> Shard Map (16384 slots)
    -> Raft Group 0 / 1 / 2 (independent leaders, terms, logs, state machines)
```

A metadata Raft group for the shard map is Tier 3 and optional; the static
config-driven map comes first.

Phase 17 delivered the Multi-Raft substrate: a node hosts N independent
`raft.Group`s, the transport multiplexes them by `group_id`, and each group
persists under `data/g<id>/<node>/` (see docs/multiraft.md). Phase 18 adds
the fixed-slot key→group mapping (see docs/shard.md).

## 6. Product architecture (after Tier 1)

```
Customer App --API key--> Gateway (auth + rate limit + tenant resolve)
                                │
                                └--> Router / Sharding (as above) --> engine
Control plane (tenants, hashed API keys, quotas, usage) — separate from data plane.
```

Tenancy is enforced at a single choke point: the gateway resolves a key to a
tenant, and data-plane keys are namespaced by tenant. The engine never trusts a
client-supplied tenant ID.

## 7. Persistence design

On-disk Raft log record framing (normative spec in [persistence.md](persistence.md)):

```
[len:u32][crc32:u32][index:u64][term:u64][payload...]
```

- **Durability window:** documented precisely once implemented (fsync policy
  per append batch; an acknowledged write must survive process crash).
- **Recovery:** open → validate CRC per record → torn tail is detected,
  truncated, and *reported*; mid-file corruption is a hard error, never
  silently ignored → rebuild index/term → replay committed entries into the
  state machine as appropriate.
- **Hard state** (`currentTerm`, `votedFor`) is durable before any RPC that
  depends on it is sent.
- **Snapshots** (Phase 12, implemented): `{lastIncludedIndex,
  lastIncludedTerm, state}` — state = engine key space + session table
  (`kv.SM.Snapshot`); atomic write (temp + fsync + rename); log prefix
  truncated behind the durable snapshot only; restart = load snapshot +
  replay the retained tail.
- **Crash model tested:** partial writes at every byte offset of a record.

## 8. Failure model

The system is tested against: process crash (leader/follower/multiple),
network partition (majority/minority), delay, packet loss, reordering,
duplication, slow node, and persistence failure (partial/corrupt record) —
all injected deterministically through the simulated transport
(`internal/transport/sim`, mechanism delivered in Phase 14, see
docs/simulation.md) with reproducible seeds (scenario suite delivered in
Phase 15, see docs/faults.md), then verified for linearizability
(Phase 16, see docs/linearizability.md) — operation histories recorded
through the real client path during those faults are checked for a
real-time-consistent total order explaining every observed output.

## 9. Consistency guarantees (normative definitions in docs/consistency.md)

- Writes: linearizable — committed only on majority quorum; acknowledged ⇒
  durable across the defined crash model.
- Reads: linearizable via ReadIndex (Phase 11: fresh heartbeat quorum →
  current-term no-op committed → applied ≥ read point; non-leaders refuse
  and the SDK redirects).
- Retries: effectively-once per `(client_id, sequence_number)` with the exact
  guarantee bounds documented (Phase 9). We do not casually claim "exactly once".
- Atomics: CAS/INCREMENT/DECREMENT apply as one indivisible step in the log's
  total order — concurrent sessions can neither lose nor double-apply an
  update (Phase 10, guarantee A1).
- Concurrency ownership — who locks what, and the three levels from the
  spec's §9 — lives in docs/concurrency.md.
- On quorum loss: no new commits; the system refuses to serve unsafe writes or
  reads rather than returning stale data.

## 10. Storage design

`internal/storage` interface: `Get / Put / Delete / Exists / Apply / Snapshot /
Restore`, initial implementation `map[string][]byte + sync.RWMutex` behind
`internal/kv.Engine`. We deliberately do **not** build an LSM/SSTable engine —
that is a different project. The focus is distributed systems.

## 11. Non-goals

Not building: SQL/relational features, ACID transactions, distributed SQL,
Redis protocol compatibility, Kubernetes, cloud autoscaling, a RocksDB-class
storage engine, multi-region replication. Security/tenancy/rate-limiting beyond
the product gateway are optional extras, never at the expense of engine
correctness.

## 12. Roadmap (phases)

| Milestone | Phases | Outcome |
|---|---|---|
| M0 Foundation | 0 | Repo, design docs, baseline |
| M1 Single-node DB | 1–2 | In-memory engine + gRPC API |
| M2 Durable consensus | 3–7 | Persistent log, election, replication, state machine |
| M3 Hardening | 8–11 | Routing, dedup, atomics, linearizable reads |
| M4 Lifecycle | 12–13 | Snapshots, compaction, InstallSnapshot |
| M5 Proof | 14–16 | Simulated network, fault tests, linearizability checking |
| M6 Scale | 17–21 | Multi-Raft, fixed-slot sharding, router, metadata, (rebalancing) |
| M7 Measure | 22–24 | Consistency spec, benchmarks, pprof |
| M8 Product | P1–P5 | API keys, tenancy, quotas, gateway, onboarding, deploy |
| M9 Launch | 25–30 | Docker, observability, CI, full suite, final docs |

Tier 1 (must-have) ends at M5. Rebalancing (21), metadata Raft, cloud (28) are
explicitly deferrable: a finished correct core beats a half-finished extra.
