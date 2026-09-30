# Cluster Router (Phase 20)

The **cluster router** ties together the full routing chain:

```
key → slot → Raft group → preferred leader endpoint → gRPC → correct Raft group
```

It makes multi-group clusters work end-to-end: every node routes keys to the right
Raft group, every client keeps per-group leader hints, and leader discovery is
group-aware.

---

## Routing Chain

| Step | Component | Description |
|------|-----------|-------------|
| **key → slot** | `shard.Slot(key)` | FNV-1a hash mod 16384 (deterministic, no coordination) |
| **slot → group** | `shard.Config.Group(slot)` | Contiguous balanced ranges; loaded from static versioned config |
| **group → leader** | `multiraft.Host.Status(gid)` | Per-group Raft status (leader ID, term, role) |
| **leader → node** | `leaderAddrs[leaderID]` | Static `-peers` map: NodeID → dialable gRPC address |
| **node → gRPC** | `grpc.Dial(addr)` | Client memoizes connections per endpoint |

---

## Server Side: `GroupedService`

`internal/server.NewGroupedService` wraps multiple `kv.Engine`s (one per Raft
group) and a `shard.Config`. Every KV request:

1. Computes `gid = shard.Config.Group(key)` (nil config → group 0).
2. Routes `Propose(gid, ...)` or `ReadIndex(gid, ...)` to the named group.
3. `GetStatus(key)` returns **that group's** leader info.

### Single-Group Compatibility

If no `-shard-config` is provided at startup, the server runs in the original
single-group mode: one Raft group (group 0), one engine, legacy `Service`.
All existing tests and deployments continue to work unchanged.

---

## Client Side: Per-Group Leader Cache

The SDK (`pkg/client`) keeps:

```go
leaders map[raft.GroupID]string  // preferred endpoint per group (guarded by mu)
```

- **Mutations** (`Put`, `Delete`, `CAS`, `Incr`): resolve `gid = groupFor(key)`,
  use `leaders[gid]` (or dial target if unset).
- **Reads** (`Get`, `Exists`): same per-group resolution.
- **Leader redirect** (`codes.Aborted`): call `GetStatus(key)`, adopt returned
  `leader_addr` **only for that group**.
- **Transport failure** (`codes.Unavailable`): re-point **that group** to
  another known endpoint.

Without a shard map, every key uses group 0 → single-group behavior preserved.

---

## `GetStatus(key)` Contract

| Request key | Response refers to |
|-------------|-------------------|
| `""` (empty) | group 0 (backward compatibility) |
| `"foo"` | group that owns `"foo"` per shard config |

Fields returned (same as before, now group-scoped):
- `node_id`: this node's identity
- `role`: `follower` \| `candidate` \| `leader`
- `leader_id`: ID of the node this group believes is leader
- `leader_addr`: dialable address of that leader (from `-peers` map)

---

## Server Startup: `-shard-config`

```
-shard-config <path-to-json>
```

- Loads the Phase 19 versioned shard config (`shard.LoadConfig`).
- For each configured group `g`:
  - Persistent Raft log at `data/g<g>/<node_id>/raft`
  - Own `kv.MemEngine` + `kv.SM`
  - Own `raft.Config{GroupID: g, ...}`
  - Registered in one `multiraft.Host`
- Transport handler = the `Host` (demuxes by `group_id`).
- Each group runs its own `Run()` loop + ticker.
- `-snapshot-every` applies per group.
- Any group halting (disk failure persisting term/vote) takes the node down.

---

## Why No Rebalancing Yet

Phase 20 uses the **static versioned config only**. The metadata Raft group
(which would ship new configs and move slot ranges atomically) is deferred to
Phase 21 / Tier 3. Until then:

- The assignment is fixed at startup.
- All nodes and clients load the same static file.
- `Version` field lets operators detect drift (manually or via future tooling).

---

## Why Metadata Raft Is Deferred

The architecture explicitly marks metadata Raft and rebalancing as **Tier 3
(optional)**. A finished, correct core (single-group + multi-group router)
beats a half-finished metadata layer. Phase 21 will add:

- Metadata Raft group (owns the authoritative shard config)
- Config change proposals (move slot ranges)
- Data migration (stream moved keys to new group)
- Coordinator to orchestrate the move safely

---

## Files Touched

| File | Role |
|------|------|
| `proto/kv.proto` | `GetStatusRequest.key` |
| `internal/server/service.go` | `GroupProposer`, `GroupStatusFunc`, `GroupedService` |
| `internal/multiraft/host.go` | `ReadIndex(gid)`, `Group(gid)` |
| `cmd/server/main.go` | `-shard-config`, multi-group startup |
| `pkg/client/client.go` | per-group `leaders` map, `switchToGroup`, `discover(key)` |
| `cmd/client/main.go` | `--shard-config` flag |
| `internal/server/group_routing_test.go` | unit tests for group routing |
| `internal/multiraft/host_test.go` | `ReadIndex` / `Group` accessor tests |
| `internal/raft/e2e_router_test.go` | 3×2 end-to-end test (sim + real gRPC) |
| `docs/router.md` | this document |
| `docs/shard.md` | "What's next" → Phase 21 |
| `README.md` | milestone M6: Phases 17–20 done |