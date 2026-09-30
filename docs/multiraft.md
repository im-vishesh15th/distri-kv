# Multi-Raft (Phase 17)

A node hosts **N independent Raft groups**, each an independent consensus
group with its own leader, term, log, hard state, and state machine. This
is the substrate for sharding (Phase 18): keys will eventually be mapped to
groups, but Phase 17 is the groups themselves — independent, isolated, and
fault-tolerant per group.

## The refactor

The Raft core is instantiable per group: `raft.Group{GroupID, peers, log,
transport}` (previously `raft.Node`, renamed). A `multiraft.Host` owns the
groups on one node; a `multiraft.Cluster` boots a set of hosts for tests.

- **`raft.Group`** — one Raft replica for one group. Same event-loop actor
  model, same election/replication/snapshot logic as before; the type is
  just scoped to a group and carries a `GroupID`.
- **`multiraft.Host`** — one node's set of groups. Implements
  `transport.Handler` by demultiplexing each inbound RPC to the `raft.Group`
  named by the request's `group_id`.
- **`multiraft.Cluster`** — test harness: boots M hosts × N groups, each
  group's replicas forming one Raft group.

## Routing

One transport per node, **not** per group. Groups share one connection per
peer pair; `group_id` does the demultiplexing at the handler.

- The proto requests (`RequestVote`, `AppendEntries`, `InstallSnapshot`) carry
  a `group_id` field (new field numbers 5/7/6 — existing fields unchanged, a
  missing `group_id` reads as 0, so group 0 is the default and pre-existing
  single-group messages are unaffected).
- Every outbound RPC is stamped with the sending group's `GroupID`.
- The receiving host's `Handle*` looks up `groups[req.GroupID]` and forwards.
  A message for a group the node does not host returns an error — it never
  panics and never touches any other group's state.

## Directory layout

Everything for a group lives under `data/g<id>/`, and within a group each
node's replica gets its own subdirectory:

```
data/
  g0/
    n1/   <- node 1's replica of group 0 (log, hardstate, snapshots)
    n2/
    n3/
  g1/
    n1/
    n2/
    n3/
```

No files are shared between groups (`g0/` and `g1/` are disjoint), and no
files are shared between replicas within a group (`g0/n1/` vs `g0/n2/`).
Each group recovers independently from its own directory on restart.

## Group-level vs node-level failure model

The sim network's fault rules work at **both** levels:

| Level | Rule | Effect |
|---|---|---|
| node | `Block(a,b)`, `Isolate(id)`, `Partition(...)` | blocks **all** groups between the nodes |
| group | `BlockGroup(a,b,g)`, `IsolateGroup(id,g)`, `PartitionGroup(g,...)` | blocks only group `g` between the nodes |

A message is blocked if a node-level rule **or** a group-level rule applies.
So you can isolate one group's replica on a node while the other groups on
the same node keep serving — the group-level fault model.

The failure model this enables:

- **Group-level fault**: isolate/kill one group's replica on one node. That
  group re-elects (if it was the leader); every other group is untouched —
  no leader change, no term change.
- **Node-level fault**: crash a whole node. Every group it hosted re-elects
  independently and recovers, with no lost acknowledged writes.

## Invariants

- **No cross-group state leakage**: a write to group 0 never appears in
  group 1's state; a message addressed to group A never changes group B's
  state; an unknown group ID returns an error.
- **Per-group persistence**: each group's log, hard state, and snapshots are
  independent; a group recovers from its own `data/g<id>/<node>/` directory.
- **Independent elections**: N groups on the same nodes elect leaders
  independently (each group's election RNG is seeded from node ID + group ID,
  so groups on one node don't campaign in lockstep).
- **Independent replication**: each group commits on its own majority; logs
  are identical across replicas *within* a group (not across groups).

## Tests

`internal/raft/multiraft_test.go` (all over `multiraft.Cluster`):

1. `TestMultiGroupElection` — N groups elect leaders independently.
2. `TestMultiGroupIndependence` — a write to group 0 never appears in group
   1; state hashes stay separate.
3. `TestMultiGroupMisrouting` — a group-0 message never changes group 1;
   an unknown group ID returns an error on every entry point.
4. `TestMultiGroupGroupLevelFault` — isolate group 0's leader; group 0
   re-elects at a higher term; group 1 keeps serving with no leader/term
   change.
5. `TestMultiGroupNodeLevelFault` — crash a node; every group it hosted
   re-elects independently; no acknowledged write is lost.
6. `TestMultiGroupRestart` — stop the cluster, restart, each group recovers
   from its own directory with matching state hashes.
7. `TestMultiGroupReplication` — each group commits independently; logs are
   identical across replicas within a group.
8. `TestMultiGroupLinearizabilityUnderFault` — a Phase 16 style
   linearizability workload on group 0 while group 1 is under a group-level
   partition; group 0's history must be linearizable.

## Known follow-ups

### Phase 19: per-(client, group) sequence counters

Each group has its **own session table**, and the state machine rejects a
sequence number lower than the last applied one for a client
(`ErrStaleSequence`). The client SDK must therefore keep a **separate
sequence counter per (client, group)**. A single global counter across groups
can produce false stale-sequence errors when requests to different groups
arrive out of order (group A applies seq 5, group B sees seq 3 from the same
client and rejects it as stale, even though it's a legitimate new request for
group B). This is a client-side concern for Phase 19, not a Phase 17 one —
noted here so it isn't forgotten.

**Resolved in Phase 19.** The client SDK now keeps one monotonic counter per
(client, group): `Client.SetShardMap` loads the versioned shard map
(`shard.Config`, see docs/shard.md) so the client can route each key to the
right group, and `Client.mutate` allocates the session sequence from that
group's counter (never a shared global one). Without a shard map the client
falls back to the group-0 counter, preserving single-group behavior. See
`pkg/client/sequence_test.go` for the per-group monotonicity tests.
