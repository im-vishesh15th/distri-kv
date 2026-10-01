# Shard Rebalancing (Phase 21)

Phase 21 adds a **metadata Raft group** that owns the authoritative shard config,
enabling dynamic rebalancing of slot ranges between Raft groups.

---

## Metadata Raft Group

A dedicated Raft group (GroupID = `MaxUint64`) runs on **all nodes** and stores
the versioned shard config as its state machine.

- Log dir: `data/metadata/<node>/raft`
- State machine: `kv.MetadataSM` (stores `shard.Config`)
- Runs on all nodes; same peers as data groups
- Applies `MoveSlots` commands to evolve the config

### Config Propagation

- **Servers**: `GroupedService` holds a reference to `MetadataSM` and calls
  `Config()` on every request to get the current config. On metadata commit,
  the new config is visible immediately (atomic pointer swap).
- **Clients**: Cache `shard.Config` + version. On `GetStatus` responses,
  check `config_version` field. If server's version > cached version,
  trigger async `GetShardConfig` to fetch and swap local config.

---

## Rebalance API

### `MoveSlots` Request

```proto
message MoveSlotsRequest {
  uint64 start_slot = 1;   // inclusive
  uint64 end_slot = 2;     // exclusive
  uint64 from_group = 3;
  uint64 to_group = 4;
  uint64 new_version = 5;  // must be > current version
}
```

- Proposed to metadata Raft group via `KVService.MoveSlots`
- Encoded as a `COMMAND_OP_MOVE_SLOTS` command with JSON `MoveSlotsRequest` payload
- Replicated through metadata group's Raft log
- On commit, all nodes atomically swap to new config

### Validation Rules

- `new_version > current_version`
- `[start_slot, end_slot)` within `[0, 16384)`
- `from_group` currently owns the range
- `to_group` exists in current config
- Range is contiguous (single range per move)

---

## Data Migration

When a range moves from group A → B:

1. **Config cutover**: New config makes B owner; writes to moved slots go to B immediately
2. **Migration task**: Stream keys in `[start, end)` from A's engine to B:
   - Scan A's engine for keys with slot ∈ `[start, end)`
   - For each key: `CAS` into B with `expected_exists=false` (idempotent)
   - Track progress (checkpoint every N keys)
3. **Completion**: Optionally delete migrated keys from A (or keep until next snapshot)

### Consistency During Migration

- **Writes**: New writes to moved slots go to B (per new config)
- **Reads**: Reads for moved slots go to B; if key not yet copied, returns `ErrKeyNotFound` (client may retry)
- **Idempotency**: `CAS` with `expected_exists=false` ensures safe retries
- **Progress tracking**: Stored in metadata SM or side table; survives node restarts

---

## Client Config Sync

- Client caches `shard.Config` + `configVersion_`
- On `GetStatus` response: if `resp.ConfigVersion > configVersion_`, trigger async `GetShardConfig(if_version_gt=cached_version)`
- `GetShardConfig` returns full config JSON if version advanced
- Client atomically swaps `shardMap` and `configVersion_` under mutex

---

## Files Touched

| File | Role |
|------|------|
| `proto/kv.proto` | `GetShardConfig`, `MoveSlots`, `config_version` in `GetStatusResponse` |
| `internal/kv/sm.go` | `MetadataSM` (stores config, applies `MoveSlots`) |
| `internal/kv/command.go` | `OpMoveSlots`, `MoveSlots()` constructor |
| `internal/server/service.go` | `GroupedService` uses `MetadataSM`; `GetShardConfig`, `MoveSlots` |
| `internal/multiraft/host.go` | Boots metadata group |
| `cmd/server/main.go` | Boots metadata group; wires `MetadataSM` to `GroupedService` |
| `pkg/client/client.go` | Config version tracking, `syncShardConfig`, `GetShardConfig`, `MoveSlots` |
| `cmd/client/main.go` | `shard-config`, `move-slots` commands |
| `internal/kv/command.go` | `OpMoveSlots`, `MoveSlots()` |
| `internal/shard/shard.go` | Exported `Validate()` |

---

## Tests

- `internal/kv/sm_test.go`: `MetadataSM` apply/move/snapshot/restore
- `internal/server/group_routing_test.go`: Dynamic config via `MetadataSM`
- `internal/raft/e2e_router_test.go`: 3×2 cluster with metadata group
- `cmd/client`: `shard-config` / `move-slots` commands

---

## What's Next

Phase 21 is the last planned phase in M6. Future work (optional):

- **Phase 22+**: Consistency spec, benchmarks, pprof (M7)
- **Phase P1–P5**: API keys, tenancy, quotas, gateway, onboarding (M8)
- **Phase 25–30**: Docker, observability, CI, deploy (M9)

Rebalancing is explicitly **Tier 3 (optional)** per architecture.md.