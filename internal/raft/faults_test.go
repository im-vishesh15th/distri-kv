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
	"distrikv/internal/raftlog"
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
			dead, rnode, _, _, _, _ := n.view()
			if dead {
				continue
			}
			s := rnode.Status()
			if s.LastApplied < target || s.LastLogIndex < target {
				return false
			}
		}
		return true
	})

	// Committed logs byte-identical (term + payload) for 1..target.
	var ref *simNode
	var refRlog *raftlog.Log
	for _, n := range nodes {
		dead, _, rlog, _, _, _ := n.view()
		if !dead {
			ref = n
			refRlog = rlog
			break
		}
	}
	for i := uint64(1); i <= target; i++ {
		re, rerr := refRlog.Get(i)
		if rerr != nil {
			t.Fatalf("%s: log get %d: %v", ref.id, i, rerr)
		}
		for _, n := range nodes {
			dead, _, rlog, _, _, _ := n.view()
			if dead || n == ref {
				continue
			}
			ne, nerr := rlog.Get(i)
			if nerr != nil || ne.Term != re.Term || string(ne.Payload) != string(re.Payload) {
				t.Fatalf("log divergence at %d: %s=%+v(%v) %s=%+v(%v)",
					i, ref.id, re, rerr, n.id, ne, nerr)
			}
		}
	}

	// Engine state: every expected key visible with the same value.
	for k, wantV := range want {
		for _, n := range nodes {
			dead, _, _, engine, _, _ := n.view()
			if dead {
				continue
			}
			got, err := engine.Get(k)
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
			_, rnode, _, _, _, _ := ls[0].view()
			ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
			_, _, err := rnode.Propose(ctx, payload)
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
// TestSimBlockLeaderFromFollower for the literal one-link cut, where the
// cut node can still reach a majority member. With pre-vote (§9.6) the
// isolated node's solicitation reaches nobody anyway, so this test's
// focus stays on majority availability; the Block test asserts the
// pre-vote protection itself (refusals from a majority that still hears
// its leader).
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

	// Heal: the cut node catches up — its term never moved (pre-vote
	// makes campaigning without a reachable majority impossible), so
	// there is no churn round at all: one leader, all three agree.
	network.Heal()
	waitForSimLeadership(t, 10*time.Second, "single leader after heal", nodes)
	requireConverged(t, nodes, want)
}

// TestSimBlockLeaderFromFollower is spec §19 scenario 2 in its literal
// form: only the leader↔follower link is cut (the cut follower can still
// talk to the third node). Before pre-vote, that follower's campaigns
// bumped the third node's term and leadership flipped between the two
// reachable nodes while the partition lasted. With pre-vote (§9.6), the
// third node still hears its leader and refuses the pre-votes, so no
// real campaign ever starts — this test asserts BOTH that the majority
// keeps writing (the deadline is what an actual client experiences
// through retries) and that no term moves anywhere during the partition,
// which is the disruption pre-vote exists to prevent. Heal must end in
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

	// Pre-vote in action: the cut follower times out repeatedly (100–190 ms
	// per round here) and solicits pre-votes, but the third node still
	// hears its leader and refuses them all — so nothing may change: not
	// the leader's identity, not its term. Without pre-vote, leadership
	// would flip to the cut side within a few rounds.
	termAtCut := leader.rnode.Status().Term
	assertFor(t, 2*time.Second, "leader holds seat and term while the cut follower pre-votes", func() bool {
		ls := simLeadersOf(nodes)
		return len(ls) == 1 && ls[0] == leader && ls[0].rnode.Status().Term == termAtCut
	})

	want := map[string]string{}
	for i := 0; i < 3; i++ {
		k := fmt.Sprintf("blk%d", i)
		want[k] = fmt.Sprintf("v%d", i)
		mustProposeUntil(t, 15*time.Second, nodes, kv.Set(k, []byte(want[k])))
	}

	// The cut node campaigned in the pre-round only: its own term must
	// still be exactly where it was when the link was cut (before
	// pre-vote, every failed campaign burned a term here).
	if got := cut.rnode.Status().Term; got != termAtCut {
		t.Fatalf("cut follower's term moved %d -> %d during the partition: pre-vote must burn no terms without a reachable majority",
			termAtCut, got)
	}

	// Heal: churn-free rejoin, one leader remains, everyone agrees —
	// including whichever side was cut out of the writes.
	network.Heal()
	waitForSimLeadership(t, 10*time.Second, "single leader after heal", nodes)
	requireConverged(t, nodes, want)
}

// TestSimPartitionedNodeDoesNotInflateTerm proves the §9.6 guarantee that
// motivated adding pre-vote: an isolated node's election timeouts cost no
// terms. Before pre-vote, this partition made the cut node bump its term
// on every campaign; on heal it re-entered the cluster with an inflated
// term and forced an election on a majority that had been healthy the
// whole time.
func TestSimPartitionedNodeDoesNotInflateTerm(t *testing.T) {
	nodes, network := startSimCluster(t, 3, 1510)

	leader := waitForSimLeadership(t, 10*time.Second, "initial leader", nodes)
	var others []*simNode
	for _, n := range nodes {
		if n != leader {
			others = append(others, n)
		}
	}
	cut, other := others[0], others[1]

	termAtCut := cut.rnode.Status().Term
	network.Isolate(cut.id)

	// Majority keeps committing the whole time.
	want := map[string]string{}
	for i := 0; i < 3; i++ {
		k := fmt.Sprintf("inf%d", i)
		want[k] = fmt.Sprintf("v%d", i)
		mustPropose(t, leader, kv.Set(k, []byte(want[k])))
	}
	requireConverged(t, []*simNode{leader, other}, want)

	// Hold long enough for the cut node to burn through several of its
	// election timeouts (100–190 ms each): every one of them must start
	// and end inside the pre-vote phase, moving no term anywhere.
	assertFor(t, 700*time.Millisecond, "majority leader undisturbed across many cut-node timeouts", func() bool {
		ls := simLeadersOf(nodes)
		return len(ls) == 1 && ls[0] == leader
	})
	if got := cut.rnode.Status().Term; got != termAtCut {
		t.Fatalf("isolated node's term moved %d -> %d while campaigning: pre-vote must burn no terms", termAtCut, got)
	}
	if got := leader.rnode.Status().Term; got != termAtCut {
		t.Fatalf("majority leader's term moved %d -> %d while a follower was isolated", termAtCut, got)
	}

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
// schedule keeps one crash max and revives it later). allowKill=false
// restricts the schedule to network faults (the linearizability test,
// where crashes would dominate the runtime without adding linearizability
// signal — crash+convergence is covered by TestSimSeededChaosConverges).
func chaosAction(t *testing.T, nodes []*simNode, network *sim.Network, rng *rand.Rand, dead **simNode, allowKill bool) {
	t.Helper()
	live := make([]*simNode, 0, len(nodes))
	for _, n := range nodes {
		dead, _, _, _, _, _ := n.view()
		if !dead {
			live = append(live, n)
		}
	}
	faultCases := 5
	if allowKill {
		faultCases = 6
	}
	switch rng.Intn(faultCases) {
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
				chaosAction(t, nodes, network, rng, &dead, true)

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
					dead, _, _, _, _, runErr := n.view()
					if dead {
						continue // killSim consumed its exit
					}
					select {
					case err := <-runErr:
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
	victim.markDead()
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
