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

`shard.Config` assigns slots to Raft groups in **contiguous, balanced ranges**.
With G groups, group `g` owns slots `[g·16384/G, (g+1)·16384/G)`:

- **Balanced** — every group gets `16384/G` or `16384/G + 1` slots.
- **Total** — every slot is owned by exactly one group.
- **Contiguous** — each group's slots form one range, so a future metadata
  group can move a whole range atomically (rebalancing, Phase 21).

The map is pure (no state, no I/O): `NewMap(groups)`, `Group(key)`,
`GroupSlot(slot)`. Identical on every node. `shard.Map` is a type alias for
`shard.Config` (the Phase 18 name), so existing call sites are unchanged.

## Versioned config (Phase 19)

Phase 18's map was computed in memory. Phase 19 makes it a **versioned config
loaded from a static file**, so a cluster can ship one assignment and every
node (and every client) can tell which version it has:

```go
type Config struct {
    Version uint64  // bumped when the assignment changes
    Groups  uint64  // number of Raft groups
    Ranges  []Range // optional explicit slot->group ranges
}
type Range struct{ Start, End, Group uint64 }
```

- **Default assignment.** With no `Ranges`, the map is the contiguous split
  by `Groups` — exactly Phase 18's `NewMap`. This is the common case.
- **Explicit ranges.** `Ranges` (a list of contiguous `[Start, End)` ranges
  each owned by one `Group`) can express a non-default assignment. This is
  what a future rebalance will use: the metadata group ships a new `Version`
  with moved ranges.
- **Loading + validation.** `shard.LoadConfig(path)` reads the JSON file, then
  validates the assignment is **total** (every slot owned), **non-overlapping**
  (no slot owned twice), and **in range** (every range within `[0, 16384)` and
  every group within `[0, Groups)`). The default split is always valid; an
  explicit assignment that leaves a slot unowned, double-owns one, or names an
  out-of-range group is rejected at load time.

Example config file:

```json
{ "version": 1, "groups": 3 }
```

The version is what makes the config *durable and movable*: Phase 18's map was
recomputed, so there was nothing to disagree about. Phase 19's config is
loaded and shared, so nodes and clients must agree on the same `Version` —
that's the metadata Raft group's job (Tier 3, Phase 21) to keep true. Until
then the config is a static file every component loads at startup.

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
  well-distributed; the map is balanced, total, contiguous, and consistent;
  single-group and many-group edge cases; and (Phase 19) `LoadConfig` round-trips
  a valid file, accepts an explicit-range assignment, and rejects zero-group,
  overlapping, unowned, and out-of-range assignments.
- `pkg/client/sequence_test.go` (Phase 19) — the client routes a key to the
  group the shard map says; without a map it falls back to group 0; and the
  per-group sequence counters are independent (each group sees `1,2,3,...` in
  issue order even when writes to different groups interleave).
- `internal/raft/shard_integration_test.go` — `TestShardRoutingIntegration`
  routes a spread of keys to their groups over a 3-node, 2-group
  `multiraft.Cluster`, writes each key to its group, and verifies the key is
  present in its group's engine and absent from the other group's;
  `TestShardRoutingIsDeterministic` and `TestShardSlotSpace` pin the mapping's
  determinism and coverage.

## Per-(client, group) sequence counters (Phase 19)

The second half of Phase 19 is client-side. Each Raft group keeps its **own
session table** and rejects a sequence number lower than the last one it
applied for a client (`ErrStaleSequence`). A client that writes to several
groups must therefore keep **one monotonic counter per (client, group)** — not
one global counter:

- The client loads the versioned shard map via `Client.SetShardMap`
  (`shard.LoadConfig`), so it can tell which group each key routes to.
- `Client.mutate` allocates the session sequence from **that group's** counter.
  Each group's session table therefore sees exactly `1, 2, 3, ...` in issue
  order, independent of what the client did to other groups.
- Without a shard map, the client falls back to the group-0 counter — the
  single-group behavior, unchanged.

This is what makes per-group session tables correct under interleaved,
multi-group writes (see docs/multiraft.md's resolved follow-up).

## What's next

- **Phase 20**: the cluster router — the component that ties
  `key → slot → group → leader → node` together for clients, and wires the
  versioned config (Phase 19) into the server so every node routes keys to the
  right group.
- **Phase 21** (optional): rebalancing — moving slot ranges between groups
  via the metadata Raft group (Tier 3).
