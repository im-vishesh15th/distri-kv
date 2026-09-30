# Operation histories + linearizability checking

Phase 16 delivers spec §20/§21: record operation histories during fault
testing, then **analyze them for consistency** — the strongest end-to-end
proof the system is linearizable, not just "it converged".

## The two halves

| Package | Role |
|---|---|
| `internal/history` | the recording half: `Op` (spec §20's fields) + a thread-safe `Recorder` |
| `internal/lincheck` | the analysis half: decides whether a recorded history is linearizable |

### Recording (`internal/history`)

Every logical client operation is recorded with exactly the fields spec
§20 lists: `operation_id, client_id, key, operation type, input, output,
start time, finish time, success/failure`. Times are wall-clock at the
client — they define the real-time order the checker reasons about.

### Checking (`internal/lincheck`)

A history is **linearizable** when there exists a total order of its
operations — consistent with real time (an op that finished before
another started must precede it) — such that every successful op's
recorded output matches the KV model's state at its position. The model
is `internal/kv`'s exact semantics: Set upserts, Get reports the current
value or absence, IncrBy starts a missing key at 0, CAS reports whether
its precondition held.

**Failure semantics** (the subtle part): a failed write's outcome is
unknown — the entry may or may not have committed — so the checker
branches and tries both. A failed read returns no answer and imposes no
constraint, so it is dropped. This is what makes the check exact for
histories recorded through retrying clients (the SDK retries with the
same session sequence; dedup makes that safe — a retried op that had
committed returns its original result).

**Algorithm**: backtracking search over linearizations with memoization
on (placed-set, model-state). Two phases, because the failed-write
branching is the expensive part:

1. Linearize the successful ops alone — a failed write placed without
   effect is a valid outcome for an unknown-result op, so if the
   successful ops are linearizable on their own, the history is too. No
   branching.
2. Only if that fails, branch on the failed writes' effects and search
   again.

A node budget (5M) bounds the search so a pathological history fails
loudly instead of hanging the suite.

The spec suggests "Porcupine or another suitable checker"; this is a
custom checker (the same family of algorithm), kept dependency-free and
unit-tested against known-good and known-bad histories — including the
cases that distinguish a real checker from a vacuous one (a read of a
value written only later, overlapping reads with different values, a
failed write whose effect is and isn't observable).

## Tests

**Unit** (`internal/lincheck/lincheck_test.go`): sequential histories,
concurrent writes (any order), read-of-unwritten-value (rejected),
overlapping reads with different values (rejected), failed-write
optional effect (both branches), failed-read dropping, IncrBy from
absent, CAS preconditions (including expect-absent), real-time order
enforcement, and the returned linearization's validity.

**Integration** (`internal/raft/linearizability_test.go`):

- `TestLinearizableConcurrent` — the fault-free baseline: 4 concurrent
  clients through the real `server.Service` path (client sessions, dedup,
  ReadIndex barrier), no chaos. The recording and the checker must
  agree on a healthy run.
- `TestLinearizableChaos` (subtests per seed) — the same workload while
  a seeded chaos schedule injects partitions, isolations, slow nodes,
  and fault flickers, plus a total outage every ~10th tick (all nodes
  isolated for 5s — no leader can be elected, so the ops in flight
  fail, exercising the failed-write path end to end). The finished
  history — successful and failed ops together — must be linearizable.

Linearizability is a safety property: no timing or fault schedule can
make a correct system produce a non-linearizable history, so any failure
here is a real bug (or a checker bug, which the unit tests pin down).

## Why this is the Tier 1 finish line

Every earlier guarantee (majority commit, dedup, ReadIndex reads,
snapshot recovery) was verified by a test that asserts a specific
outcome. Linearizability subsumes them: a lost write, a stale read, a
phantom value, or a dedup failure all show up as a non-linearizable
history. One checker, run against histories recorded under the full
fault model, is the end-to-end proof the core is correct.
