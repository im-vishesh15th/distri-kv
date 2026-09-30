package raft_test

// Phase 15: automated fault testing (spec §19) — every scenario is
// driven through the Phase 14 simulated network, and every test verifies
// resulting SYSTEM STATE (logs, engines, applied marks), never just
// "the node crashed".
//
// Spec §19 scenarios:
//  1. kill leader → re-elect → writes → restart → catch-up → all agree
//  2. partition leader from one follower → majority available → heal →
//     consistent
//  3. minority vs majority partition → minority cannot commit →
//     majority continues → heal → logs converge
//
// Plus §17's failure list end-to-end: delay/loss/reordering/duplication/
// slow node under a seeded chaos schedule (TestSimSeededChaosConverges)
// and persistence failure ("simulated disk failure where practical" —
// TestSimDiskFailureHaltsNode).

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"distrikv/internal/kv"
	"distrikv/internal/raft"
	"distrikv/internal/transport"
	"distrikv/internal/transport/sim"
)

// requireConverged waits for a single leader among nodes, snapshots its
// committed index as the target, waits until every LIVE node in the list
// has logged and applied that far, then verifies the two properties spec
// §19 actually asks for: committed logs 1..target are byte-identical
// across replicas, and every entry in want resolves to the same value in
// every live node's engine. Callers pass a subset mid-partition (dead or
// isolated members excluded) and the full list after healing.
func requireConverged(t *testing.T, nodes []*simNode, want map[string]string) {
	t.Helper()
	var leader *simNode
	waitFor(t, 10*time.Second, "single leader for convergence check", func() bool {
		ls := simLeadersOf(nodes)
		if len(ls) != 1 {
			return false
		}
		leader = ls[0]
		return true
	})
	target := leader.rnode.Status().CommitIndex

	waitFor(t, 10*time.Second, fmt.Sprintf("live replicas reach applied=%d", target), func() bool {
		for _, n := range nodes {
			if n.dead {
				continue
			}
			s := n.rnode.Status()
			if s.LastApplied < target || s.LastLogIndex < target {
				return false
			}
		}
		return true
	})

	// Committed logs byte-identical (term + payload) for 1..target.
	var ref *simNode
	for _, n := range nodes {
		if !n.dead {
			ref = n
			break
		}
	}
	for i := uint64(1); i <= target; i++ {
		re, rerr := ref.rlog.Get(i)
		if rerr != nil {
			t.Fatalf("%s: log get %d: %v", ref.id, i, rerr)
		}
		for _, n := range nodes {
			if n.dead || n == ref {
				continue
			}
			ne, nerr := n.rlog.Get(i)
			if nerr != nil || ne.Term != re.Term || string(ne.Payload) != string(re.Payload) {
				t.Fatalf("log divergence at %d: %s=%+v(%v) %s=%+v(%v)",
					i, ref.id, re, rerr, n.id, ne, nerr)
			}
		}
	}

	// Engine state: every expected key visible with the same value.
	for k, wantV := range want {
		for _, n := range nodes {
			if n.dead {
				continue
			}
			got, err := n.engine.Get(k)
			if err != nil || string(got) != wantV {
				t.Fatalf("%s: engine[%q] = (%q, %v), want %q (applied=%d)",
					n.id, k, got, err, wantV, n.rnode.Status().LastApplied)
			}
		}
	}
}

// mustPropose commits cmd through sn (single attempt — use when
// leadership is stable) and returns the entry's log index.
func mustPropose(t *testing.T, sn *simNode, cmd kv.Command) uint64 {
	t.Helper()
	idx, _, err := sn.rnode.Propose(context.Background(), mustEncode(t, cmd))
	if err != nil {
		t.Fatalf("%s: propose %+v: %v", sn.id, cmd, err)
	}
	return idx
}

// tryPropose follows leadership and proposes with a deadline: true iff
// the write committed. Safe for goroutines (no t.Fatal inside).
func tryPropose(d time.Duration, nodes []*simNode, payload []byte) bool {
	deadline := time.Now().Add(d)
	for {
		if ls := simLeadersOf(nodes); len(ls) == 1 {
			ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
			_, _, err := ls[0].rnode.Propose(ctx, payload)
			cancel()
			if err == nil {
				return true
			}
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// mustProposeUntil is tryPropose with a test failure on timeout — for
// windows where leadership churns but a majority is up (spec's
// "majority remains available").
func mustProposeUntil(t *testing.T, d time.Duration, nodes []*simNode, cmd kv.Command) {
	t.Helper()
	if !tryPropose(d, nodes, mustEncode(t, cmd)) {
		t.Fatalf("write %+v did not commit within %v (majority unavailable?)", cmd, d)
	}
}

// TestSimLeaderKillRestartAgrees is spec §19 scenario 1, end to end over
// the sim network: write+verify → kill leader → re-elect → write →
// restart the old leader → catch-up → every replica agrees (byte-
// identical logs and identical engine state, including the restarted
// node's cold-engine replay).
func TestSimLeaderKillRestartAgrees(t *testing.T) {
	nodes, network := startSimCluster(t, 3, 1501)

	leader := waitForSimLeadership(t, 10*time.Second, "initial leader", nodes)
	oldTerm := leader.rnode.Status().Term

	// 1–3. Write data, verify committed data on every replica.
	want := map[string]string{}
	for i := 0; i < 3; i++ {
		k := fmt.Sprintf("before%d", i)
		want[k] = fmt.Sprintf("v%d", i)
		mustPropose(t, leader, kv.Set(k, []byte(want[k])))
	}
	requireConverged(t, nodes, want)

	// 4–5. Kill the leader; survivors elect one in a newer term.
	leader.killSim(t, network)
	waitFor(t, 10*time.Second, "survivors elect a new leader", func() bool {
		ls := simLeadersOf(nodes)
		return len(ls) == 1 && ls[0].rnode.Status().Term > oldTerm
	})
	newLeader := simLeadersOf(nodes)[0]

	// 6–7. More writes on the new leader (majority of the survivors).
	want["after"] = "kill"
	mustPropose(t, newLeader, kv.Set("after", []byte("kill")))
	requireConverged(t, nodes, want) // dead old leader skipped

	// 8–9. Restart the old leader from the same directory: it must
	// rejoin and catch up (its engine starts empty — only log replay can
	// converge it).
	restartSimNode(t, network, leader)
	final := waitForSimLeadership(t, 10*time.Second, "one leader after restart", nodes)
	waitFor(t, 10*time.Second, "restarted node caught up", func() bool {
		return leader.rnode.Status().LastLogIndex >= final.rnode.Status().LastLogIndex
	})

	// 10. All replicas agree.
	requireConverged(t, nodes, want)
}

// TestSimPartitionLeaderFromFollower is spec §19 scenario 2: a follower
// is cut off from the leader. The majority (leader + remaining follower)
// stays available and keeps committing; the isolated node provably never
// sees the new entries; after heal everyone agrees.
//
// Note: Isolate, not a bare leader↔follower Block — see
// TestSimBlockLeaderFromFollower for the literal one-link cut. Our Raft
// has no pre-vote (the spec doesn't require it), so a cut follower whose
// campaigns REACH the third node forces repeated re-elections; isolating
// the cut node keeps this test's focus on majority availability, and the
// Block test covers the churny variant separately.
func TestSimPartitionLeaderFromFollower(t *testing.T) {
	nodes, network := startSimCluster(t, 3, 1502)

	leader := waitForSimLeadership(t, 10*time.Second, "initial leader", nodes)
	var followers []*simNode
	for _, n := range nodes {
		if n != leader {
			followers = append(followers, n)
		}
	}
	cut := followers[0]
	other := followers[1]

	network.Isolate(cut.id)

	// The leader is undisturbed: the isolated node's campaign term
	// bumps cannot reach anyone, so leadership must not churn at all.
	assertFor(t, 2*time.Second, "leader stays put while a follower is isolated", func() bool {
		ls := simLeadersOf(nodes)
		return len(ls) == 1 && ls[0] == leader
	})

	// Majority available: writes commit on leader + other follower.
	want := map[string]string{}
	var firstNewIdx uint64
	for i := 0; i < 3; i++ {
		k := fmt.Sprintf("iso%d", i)
		want[k] = fmt.Sprintf("v%d", i)
		idx := mustPropose(t, leader, kv.Set(k, []byte(want[k])))
		if firstNewIdx == 0 {
			firstNewIdx = idx
		}
	}
	// The cut node must not have these: they were proposed after the
	// isolation closed, and every leader→cut path is blocked.
	if got := cut.rnode.Status().LastLogIndex; got >= firstNewIdx {
		t.Fatalf("isolated follower at index %d: entries from %d crossed the partition", got, firstNewIdx)
	}
	requireConverged(t, []*simNode{leader, other}, want)

	// Heal: the cut node catches up (expect a term-bump churn round —
	// its inflated campaign terms are legitimate Raft — then one leader),
	// and all three replicas agree.
	network.Heal()
	waitForSimLeadership(t, 10*time.Second, "single leader after heal", nodes)
	requireConverged(t, nodes, want)
}

// TestSimBlockLeaderFromFollower is spec §19 scenario 2 in its literal
// form: only the leader↔follower link is cut (the cut follower can still
// talk to the third node). Without pre-vote, that follower's campaigns
// bump the third node's term, forcing leadership to flip between the two
// reachable nodes while the partition lasts — so "majority remains
// available" is asserted as writes keep landing within a deadline (what
// an actual client experiences through retries), and heal must end in
// full agreement.
func TestSimBlockLeaderFromFollower(t *testing.T) {
	nodes, network := startSimCluster(t, 3, 1503)

	leader := waitForSimLeadership(t, 10*time.Second, "initial leader", nodes)
	var followers []*simNode
	for _, n := range nodes {
		if n != leader {
			followers = append(followers, n)
		}
	}
	cut := followers[0]

	network.Block(leader.id, cut.id)

	want := map[string]string{}
	for i := 0; i < 3; i++ {
		k := fmt.Sprintf("blk%d", i)
		want[k] = fmt.Sprintf("v%d", i)
		mustProposeUntil(t, 15*time.Second, nodes, kv.Set(k, []byte(want[k])))
	}

	// Heal: churn stops, one leader remains, everyone agrees — including
	// whichever side was cut out of the writes.
	network.Heal()
	waitForSimLeadership(t, 10*time.Second, "single leader after heal", nodes)
	requireConverged(t, nodes, want)
}

// TestSimMinorityLeaderCannotCommit is spec §19 scenario 3: the leader
// is partitioned into a minority of one. Its proposal must be accepted
// locally but never commit; the majority elects and keeps writing; on
// heal the stale leader steps down, its proposal fails rather than lying,
// and the stray entry is gone from every log (the new leader's no-op
// occupies the same index), so the key exists nowhere.
func TestSimMinorityLeaderCannotCommit(t *testing.T) {
	nodes, network := startSimCluster(t, 3, 1504)

	leader := waitForSimLeadership(t, 10*time.Second, "initial leader", nodes)
	var majority []*simNode
	for _, n := range nodes {
		if n != leader {
			majority = append(majority, n)
		}
	}

	// Quiesce so indexes are meaningful: every entry so far committed.
	waitFor(t, 5*time.Second, "leader quiescent before partition", func() bool {
		s := leader.rnode.Status()
		return s.CommitIndex == s.LastLogIndex
	})
	baseIdx := leader.rnode.Status().CommitIndex

	network.Partition(
		[]transport.NodeID{leader.id},
		[]transport.NodeID{majority[0].id, majority[1].id},
	)

	// 1. Minority cannot commit: the proposal appends locally — a leader
	// (no election timeout on heartbeat time) never self-demotes — but
	// must not commit or apply.
	type proposeResult struct{ err error }
	done := make(chan proposeResult, 1)
	payload := mustEncode(t, kv.Set("minority", []byte("lost")))
	go func() {
		_, _, err := leader.rnode.Propose(context.Background(), payload)
		done <- proposeResult{err: err}
	}()
	waitFor(t, 5*time.Second, "minority entry appended locally", func() bool {
		return leader.rnode.Status().LastLogIndex == baseIdx+1
	})
	if s := leader.rnode.Status(); s.CommitIndex != baseIdx || s.LastApplied != baseIdx {
		t.Fatalf("minority entry committed/applied without a majority: commit=%d applied=%d base=%d",
			s.CommitIndex, s.LastApplied, baseIdx)
	}

	// 2. The majority side elects in a newer term and continues writing.
	oldTerm := leader.rnode.Status().Term
	waitFor(t, 10*time.Second, "majority elects a new leader", func() bool {
		ls := simLeadersOf(majority)
		return len(ls) == 1 && ls[0].rnode.Status().Term > oldTerm
	})
	newLeader := simLeadersOf(majority)[0]
	mustPropose(t, newLeader, kv.Set("majority", []byte("kept")))

	// The minority proposal is still pending — no majority, no commit.
	select {
	case r := <-done:
		t.Fatalf("minority proposal resolved (%v) while still partitioned — it never had a quorum", r.err)
	case <-time.After(300 * time.Millisecond):
	}

	// 3. Heal: the stale leader steps down on the higher term and its
	// proposal must fail honestly. (The new leader's no-op was appended
	// at the same index baseIdx+1 in a newer term, so the stray entry
	// can never commit anywhere — its fate is "failed", not "maybe".)
	network.Heal()
	var pr proposeResult
	select {
	case pr = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("minority proposal never resolved after heal")
	}
	if pr.err == nil {
		t.Fatal("minority proposal reported success — an entry without quorum was applied")
	}
	if !errors.Is(pr.err, raft.ErrLeadershipLost) && !errors.Is(pr.err, raft.ErrNotLeader) {
		t.Fatalf("minority proposal err = %v, want ErrLeadershipLost or ErrNotLeader", pr.err)
	}

	// Heal convergence: one leader, logs converge, majority write
	// visible everywhere...
	waitForSimLeadership(t, 10*time.Second, "single leader after heal", nodes)
	requireConverged(t, nodes, map[string]string{"majority": "kept"})
	// ...and the minority write exists nowhere.
	for _, n := range nodes {
		if _, err := n.engine.Get("minority"); !errors.Is(err, kv.ErrKeyNotFound) {
			t.Fatalf("%s: minority key visible (%v) — an uncommitted write escaped the quorum rule", n.id, err)
		}
	}
}

// chaosAction applies one seeded fault action. `dead` tracks the single
// node that may be crashed at a time (with 3 nodes, two crashes would
// remove the majority entirely — no progress is possible, so the
// schedule keeps one crash max and revives it later).
func chaosAction(t *testing.T, nodes []*simNode, network *sim.Network, rng *rand.Rand, dead **simNode) {
	t.Helper()
	live := make([]*simNode, 0, len(nodes))
	for _, n := range nodes {
		if !n.dead {
			live = append(live, n)
		}
	}
	switch rng.Intn(6) {
	case 0:
		network.Heal()
	case 1: // random split: one node vs the rest
		perm := rng.Perm(len(nodes))
		groupA := []transport.NodeID{nodes[perm[0]].id}
		groupB := make([]transport.NodeID, 0, len(nodes)-1)
		for i := 1; i < len(nodes); i++ {
			groupB = append(groupB, nodes[perm[i]].id)
		}
		network.Partition(groupA, groupB)
	case 2:
		network.Isolate(live[rng.Intn(len(live))].id)
	case 3: // slow node on/off
		if rng.Intn(2) == 0 {
			network.SetSlow(live[rng.Intn(len(live))].id, time.Duration(20+rng.Intn(40))*time.Millisecond)
		} else {
			for _, n := range nodes {
				network.SetSlow(n.id, 0)
			}
		}
	case 4: // fault flicker: harsher vs baseline faults
		if rng.Intn(2) == 0 {
			network.SetFaults(sim.Faults{DropRate: 0.18, DupRate: 0.10, MinDelay: 15 * time.Millisecond, MaxDelay: 60 * time.Millisecond})
		} else {
			network.SetFaults(sim.Faults{DropRate: 0.08, DupRate: 0.04, MinDelay: 5 * time.Millisecond, MaxDelay: 25 * time.Millisecond})
		}
	case 5: // crash or revive (max one dead at a time)
		if *dead == nil {
			victim := live[rng.Intn(len(live))]
			victim.killSim(t, network)
			*dead = victim
		} else {
			restartSimNode(t, network, *dead)
			*dead = nil
		}
	}
}

// TestSimSeededChaosConverges is the "automated" half of Phase 15: the
// cluster is born under seeded delay/loss/duplication faults (§17's
// message failures exercised end-to-end through elections and commits,
// not just at the network layer), then a seeded random schedule of
// partitions, isolations, slow nodes, fault flickers, and crashes runs
// while best-effort writes race it. Invariants: nothing ever halts, and
// after the final heal every successfully committed write is visible in
// every replica with byte-identical committed logs. The seed replays the
// same fault schedule (docs/simulation.md states the reproducibility
// boundary honestly).
func TestSimSeededChaosConverges(t *testing.T) {
	for _, seed := range []int64{1505, 1506} {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			nodes, network := startSimClusterFaults(t, 3, seed, sim.Faults{
				DropRate: 0.08, DupRate: 0.04,
				MinDelay: 5 * time.Millisecond, MaxDelay: 25 * time.Millisecond,
			})
			rng := rand.New(rand.NewSource(seed))

			// Baseline under faults: election + writes must succeed
			// despite drops and delays.
			waitForSimLeadership(t, 15*time.Second, "election under seeded faults", nodes)
			committed := map[string]string{}
			for i := 0; i < 3; i++ {
				k := fmt.Sprintf("base%d", i)
				v := fmt.Sprintf("bv%d", i)
				committed[k] = v
				mustProposeUntil(t, 15*time.Second, nodes, kv.Set(k, []byte(v)))
			}

			// Seeded chaos: 4 rounds of fault action + best-effort writes.
			// One fault at a time: each round resets to the baseline
			// fault level before applying its action, so a round's
			// outcome is interpretable and faults never compound into
			// a total outage. (A crash is sticky by design — the node
			// stays dead until a restart action or the final revive.)
			var dead *simNode
			chaosOK := 0
			var mu sync.Mutex // guards chaosOK
			for round := 0; round < 4; round++ {
				network.Heal()
				for _, n := range nodes {
					network.SetSlow(n.id, 0)
				}
				network.SetFaults(sim.Faults{
					DropRate: 0.08, DupRate: 0.04,
					MinDelay: 5 * time.Millisecond, MaxDelay: 25 * time.Millisecond,
				})
				chaosAction(t, nodes, network, rng, &dead)

				var wg sync.WaitGroup
				for w := 0; w < 3; w++ {
					payload := mustEncode(t, kv.Set(
						fmt.Sprintf("ch%d-%d", round, w), []byte("x")))
					wg.Add(1)
					go func() {
						defer wg.Done()
						if tryPropose(600*time.Millisecond, nodes, payload) {
							mu.Lock()
							chaosOK++
							mu.Unlock()
						}
					}()
				}
				wg.Wait()

				// Invariant: no disk or logic failure may have halted a
				// live node mid-chaos (runErr is only written on exit).
				for _, n := range nodes {
					if n.dead {
						continue // killSim consumed its exit
					}
					select {
					case err := <-n.runErr:
						t.Fatalf("%s: node halted during chaos: %v", n.id, err)
					default:
					}
				}
			}

			if chaosOK == 0 {
				t.Error("no write committed during chaos — the schedule was pure outage for this seed")
			}

			// Quiesce everything, revive the dead node, converge.
			network.Heal()
			network.SetFaults(sim.Faults{})
			for _, n := range nodes {
				network.SetSlow(n.id, 0)
			}
			if dead != nil {
				restartSimNode(t, network, dead)
			}
			waitForSimLeadership(t, 15*time.Second, "single leader after chaos", nodes)
			requireConverged(t, nodes, committed)
		})
	}
}

// TestSimDiskFailureHaltsNode covers §17's persistence failure — the
// "simulated disk failure where practical": closing a live node's log
// makes every subsequent write fail. The node must HALT with an error
// (never ack or apply anything the disk didn't take), the halted node
// must not be mistaken for a healthy voter, and the surviving majority
// keeps committing.
func TestSimDiskFailureHaltsNode(t *testing.T) {
	nodes, _ := startSimCluster(t, 3, 1507)

	leader := waitForSimLeadership(t, 10*time.Second, "initial leader", nodes)
	var followers []*simNode
	for _, n := range nodes {
		if n != leader {
			followers = append(followers, n)
		}
	}
	victim := followers[0]

	want := map[string]string{"pre": "durable"}
	mustPropose(t, leader, kv.Set("pre", []byte("durable")))
	requireConverged(t, nodes, want) // victim has "pre" applied

	// The disk dies under the running node.
	if err := victim.rlog.Close(); err != nil {
		t.Fatalf("inject disk failure: %v", err)
	}

	// The majority commits the next write without the victim...
	want["post"] = "quorum"
	mustPropose(t, leader, kv.Set("post", []byte("quorum")))

	// ...and the victim HALTS when its next log write hits the dead
	// disk: a non-nil error, not a clean exit.
	select {
	case err := <-victim.runErr:
		if err == nil {
			t.Fatal("victim exited cleanly after disk failure — expected halt with an error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("victim kept running with a dead disk — it must halt rather than ack non-durable state")
	}
	// It leaves the electorate: mark dead (ticker cancelled) so
	// leadership checks and teardown treat it as gone.
	victim.dead = true
	victim.cancel()

	// State proof: the victim has the pre-failure write (applied while
	// the disk lived) and never the post-failure one.
	if got, err := victim.engine.Get("pre"); err != nil || string(got) != "durable" {
		t.Fatalf("victim engine[pre] = (%q, %v), want durable — it applied before the disk died", got, err)
	}
	if _, err := victim.engine.Get("post"); !errors.Is(err, kv.ErrKeyNotFound) {
		t.Fatalf("victim engine[post] visible (%v) — it applied an entry its disk never took", err)
	}

	// The surviving majority still works.
	want["after"] = "hal"
	mustPropose(t, leader, kv.Set("after", []byte("hal")))
	requireConverged(t, []*simNode{leader, followers[1]}, want)
}
