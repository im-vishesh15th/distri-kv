# Fixed-slot sharding (Phase 18)

The key→group mapping: the first half of the routing chain
`key → slot → shard → Raft group → leader → node`. Fixed slots, **not**
consistent hashing — the slot space is fixed at 16384, so a key's slot never
changes and only the slot→group assignment can move (later, via the metadata
group).

## The slot function

`slot = hash(key) % 16384`, with a deterministic hash (FNV-1a). Deterministic
is the whole point: every node computes the same slot for the same key, so the
shard map is consistent cluster-wide **without any coordination**. No
metadata lookup, no consensus, no cache invalidation — just a pure function.

## The shard map

`shard.Map` assigns slots to Raft groups in **contiguous, balanced ranges**.
With G groups, group `g` owns slots `[g·16384/G, (g+1)·16384/G)`:

- **Balanced** — every group gets `16384/G` or `16384/G + 1` slots.
- **Total** — every slot is owned by exactly one group.
- **Contiguous** — each group's slots form one range, so a future metadata
  group can move a whole range atomically (rebalancing, Phase 21).

The map is pure (no state, no I/O): `NewMap(groups)`, `Group(key)`,
`GroupSlot(slot)`. Identical on every node.

## Routing

```
key --Slot()--> slot --Map.Group()--> group --(multiraft)--> leader --> node
```

Phase 18 delivers the first two arrows (`key → slot → group`). The
`multiraft.Host` (Phase 17) already routes a proposal to the right group's
leader, so the full path works end to end: route a key to its group, propose
to that group, the write lands in that group's state machine.

## Tests

- `internal/shard/shard_test.go` — Slot is deterministic, in range, and
  well-distributed; Map is balanced, total, contiguous, and consistent;
  single-group and many-group edge cases.
- `internal/raft/shard_integration_test.go` — `TestShardRoutingIntegration`
  routes a spread of keys to their groups over a 3-node, 2-group
  `multiraft.Cluster`, writes each key to its group, and verifies the key is
  present in its group's engine and absent from the other group's;
  `TestShardRoutingIsDeterministic` and `TestShardSlotSpace` pin the mapping's
  determinism and coverage.

## What's next

- **Phase 19**: the shard map becomes a versioned config (static file first),
  and the client SDK keeps a per-(client, group) sequence counter (see
  docs/multiraft.md's follow-up).
- **Phase 20**: the cluster router — the component that ties
  `key → slot → group → leader → node` together for clients.
- **Phase 21** (optional): rebalancing — moving slot ranges between groups
  via the metadata Raft group (Tier 3).
