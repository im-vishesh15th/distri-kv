// Package sim provides DistriKV's deterministic simulated network — the
// testing half of the transport seam (spec §18): Raft runs unchanged over
// an in-process network with seeded message delay, drop, reorder,
// duplication, partition, node isolation, and slow nodes, instead of
// sockets and wall-clock luck.
//
// Architecture: one Network instance per test cluster. Every outbound RPC
// is a call: the driver (the test's goroutine in manual mode, a background
// goroutine in auto mode) is the ONLY component that samples faults,
// schedules deliveries, and invokes handlers — all fault decisions come
// from a single seeded PRNG consumed in call-arrival order, and deliveries
// are ordered by (deliverAt, sequence). Given the same seed and the same
// ordered history of calls and control operations, decisions and delivery
// order are identical; that is what "deterministic and repeatable" means
// here.
//
// Honest boundary (documented, not papered over): the network cannot
// control when concurrently-running node goroutines arrive with their
// calls — sequence numbers follow arrival order at the network, and Go's
// scheduler decides that. Scripted, serialized histories are bit-
// reproducible; live multi-node runs are reproducible in their fault
// *decisions* given the same arrival history. Tests assert convergence
// ("eventually") like every other DistriKV test, never wall-clock races.
//
// Two clock modes:
//
//   - Manual (default): nothing moves until the test calls Advance(d),
//     which advances virtual time and processes ALL pending work (fault
//     draws, due deliveries) before returning — deterministic quiescence
//     points, no goroutines, no sleeps.
//   - Auto (Config.Auto): a background driver maps virtual time 1:1 onto
//     real time, so delay=500ms literally takes 500ms, and cluster tests
//     can tick nodes with the real-time ticker as usual. Ordering remains
//     (deliverAt, seq) — the wall clock only bounds latency, never
//     decides order.
package sim

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"distrikv/internal/transport"
)

// Fault outcomes surfaced to callers (Raft's response paths treat every
// non-nil error as "this peer did not answer", which is exactly right for
// all three).
var (
	// ErrDropped: the message was dropped per DropRate. The responder
	// never saw it.
	ErrDropped = errors.New("sim: message dropped")

	// ErrBlocked: a partition or isolation rule forbids this path right
	// now (symmetric: either endpoint's rule applies).
	ErrBlocked = errors.New("sim: blocked by partition/isolation")

	// ErrNoRoute: the target has no registered handler — the node is down
	// or never booted (the in-process analog of connection refused; no
	// randomness is drawn for it, keeping "node is dead" deterministic).
	ErrNoRoute = errors.New("sim: no route to node (down or never registered)")
)

// Faults are the per-RPC probabilistic faults, sampled once per call by
// the driver: first drop, then delay, then duplication. Zero values are
// fault-free. SetFaults changes them at any point (mid-test fault
// injection); validation happens there.
type Faults struct {
	// DropRate in [0,1]: probability the call fails with ErrDropped and
	// the handler is never invoked.
	DropRate float64
	// DupRate in [0,1]: probability the request is delivered TWICE (two
	// independent delay draws). The caller receives exactly one response;
	// the duplicate exercises the responder exactly like a real duplicate
	// delivery would.
	DupRate float64
	// Per-call delay sampled uniformly from [MinDelay, MaxDelay].
	MinDelay time.Duration
	MaxDelay time.Duration
}

func (f Faults) validate() {
	if f.DropRate < 0 || f.DropRate > 1 {
		panic(fmt.Sprintf("sim: DropRate %v out of [0,1]", f.DropRate))
	}
	if f.DupRate < 0 || f.DupRate > 1 {
		panic(fmt.Sprintf("sim: DupRate %v out of [0,1]", f.DupRate))
	}
	if f.MinDelay < 0 || f.MaxDelay < f.MinDelay {
		panic(fmt.Sprintf("sim: delay range [%v,%v] invalid", f.MinDelay, f.MaxDelay))
	}
}

// Config constructs a Network.
type Config struct {
	// Seed makes every fault draw a pure function of the call history.
	Seed int64
	// Faults are the initial per-call faults.
	Faults Faults
	// Auto starts a background driver that maps virtual time 1:1 onto
	// real time (cluster tests). Manual mode (false) is driven solely by
	// Advance.
	Auto bool
}

type call struct {
	from, to transport.NodeID
	// at is the virtual send time: sampled delays are measured from when
	// the call entered the network, not from when the driver got to it,
	// so a call that waits in the queue for a slow driver still arrives
	// exactly `delay` after it was sent.
	at     time.Time
	invoke func(transport.Handler) (any, error)
	resp   chan result // buffered 1: first response wins, extras discarded
}

type result struct {
	val any
	err error
}

func (c *call) respond(r result) {
	select {
	case c.resp <- r:
	default: // duplicate delivery's late response: nobody waiting
	}
}

// delivery is one scheduled invocation of a call at the target.
type delivery struct {
	call      *call
	deliverAt time.Time
	seq       uint64
}

// deliveryQueue is a heap ordered by (deliverAt, seq) — the total order
// that makes reordering a deliberate consequence of sampled delays and
// never of timing luck.
type deliveryQueue []*delivery

func (q deliveryQueue) Len() int { return len(q) }
func (q deliveryQueue) Less(i, j int) bool {
	if !q[i].deliverAt.Equal(q[j].deliverAt) {
		return q[i].deliverAt.Before(q[j].deliverAt)
	}
	return q[i].seq < q[j].seq
}
func (q deliveryQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *deliveryQueue) Push(x any)   { *q = append(*q, x.(*delivery)) }
func (q *deliveryQueue) Pop() any {
	old := *q
	n := len(old)
	d := old[n-1]
	old[n-1] = nil
	*q = old[:n-1]
	return d
}

// Network is the shared simulated network for one test cluster. All
// mutators are safe for concurrent use; exactly one driver (Advance
// caller or the auto goroutine) processes work at a time.
type Network struct {
	mu    sync.Mutex
	cfg   Config
	rng   *rand.Rand
	seq   uint64
	now   time.Time // virtual clock: zero epoch + elapsed
	start time.Time // wall anchor (auto mode only)

	waiting  []*call       // enqueued, not yet fault-sampled
	q        deliveryQueue // scheduled, ordered by (deliverAt, seq)
	handlers map[transport.NodeID]transport.Handler

	// Topology controls (all cleared by Heal).
	isolated map[transport.NodeID]bool
	blocked  map[[2]transport.NodeID]bool
	// Slow nodes: extra delay on any call whose endpoint matches.
	slow map[transport.NodeID]time.Duration

	stopped bool
	stop    chan struct{}
	wake    chan struct{} // buffered 1: coalesced driver wakeup
}

// New builds the network. In auto mode a background driver starts
// immediately (stopped by Close).
func New(cfg Config) *Network {
	cfg.Faults.validate()
	n := &Network{
		cfg:      cfg,
		rng:      rand.New(rand.NewSource(cfg.Seed)),
		handlers: make(map[transport.NodeID]transport.Handler),
		isolated: make(map[transport.NodeID]bool),
		blocked:  make(map[[2]transport.NodeID]bool),
		slow:     make(map[transport.NodeID]time.Duration),
		start:    time.Now(),
		stop:     make(chan struct{}),
		wake:     make(chan struct{}, 1),
	}
	if cfg.Auto {
		go n.drive()
	}
	return n
}

// SetHandler attaches (or with nil, detaches — a killed node) the inbound
// side for a node. Detaching does not cancel deliveries already
// scheduled: they fail with ErrNoRoute at delivery time, like a
// connection that dies in flight.
func (n *Network) SetHandler(id transport.NodeID, h transport.Handler) {
	n.mu.Lock()
	if h == nil {
		delete(n.handlers, id)
	} else {
		n.handlers[id] = h
	}
	n.mu.Unlock()
}

// SetFaults replaces the per-call fault configuration (mid-test
// injection). Invalid values panic — a test typo should fail loudly.
func (n *Network) SetFaults(f Faults) {
	f.validate()
	n.mu.Lock()
	n.cfg.Faults = f
	n.mu.Unlock()
}

// SetSlow adds extra delay to every call whose sender OR receiver is id —
// the spec's "slow node". Set 0 to clear.
func (n *Network) SetSlow(id transport.NodeID, d time.Duration) {
	if d < 0 {
		panic("sim: negative slow-node delay")
	}
	n.mu.Lock()
	if d == 0 {
		delete(n.slow, id)
	} else {
		n.slow[id] = d
	}
	n.mu.Unlock()
}

// Isolate cuts id off from every other node (both directions), the node's
// own sends included. Other pairs are unaffected.
func (n *Network) Isolate(id transport.NodeID) {
	n.mu.Lock()
	n.isolated[id] = true
	n.mu.Unlock()
}

// Block forbids traffic between a and b (both directions) — the spec's
// "Node A X Node B".
func (n *Network) Block(a, b transport.NodeID) {
	if a == b {
		panic("sim: Block of a node with itself")
	}
	n.mu.Lock()
	n.blocked[pairKey(a, b)] = true
	n.mu.Unlock()
}

// Partition splits the cluster into groups: every cross-group path is
// blocked (both directions), every within-group path stays open. One call
// with multiple groups is the majority/minority split:
//
//	net.Partition(majority, minority)
func (n *Network) Partition(groups ...[]transport.NodeID) {
	n.mu.Lock()
	for i := range groups {
		for j := i + 1; j < len(groups); j++ {
			for _, a := range groups[i] {
				for _, b := range groups[j] {
					n.blocked[pairKey(a, b)] = true
				}
			}
		}
	}
	n.mu.Unlock()
}

// Heal clears every isolation and block. Fault rates and slow-node
// delays are deliberately NOT cleared — they are fault configuration,
// not topology.
func (n *Network) Heal() {
	n.mu.Lock()
	n.isolated = make(map[transport.NodeID]bool)
	n.blocked = make(map[[2]transport.NodeID]bool)
	n.mu.Unlock()
}

func pairKey(a, b transport.NodeID) [2]transport.NodeID {
	if a > b {
		a, b = b, a
	}
	return [2]transport.NodeID{a, b}
}

// Call runs one RPC over the simulated network: enqueue, let the driver
// sample faults and deliver, block until the responder answers (or the
// call is dropped/blocked/undeliverable/ctx-cancelled).
func (n *Network) Call(ctx context.Context, from, to transport.NodeID, invoke func(transport.Handler) (any, error)) (any, error) {
	c := &call{from: from, to: to, invoke: invoke, resp: make(chan result, 1)}
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return nil, transport.ErrClosed
	}
	c.at = n.nowLocked()
	n.waiting = append(n.waiting, c)
	n.mu.Unlock()
	n.signal()

	select {
	case r := <-c.resp:
		return r.val, r.err
	case <-ctx.Done():
		return nil, ctx.Err() // in-flight delivery (if any) is discarded
	case <-n.stop:
		return nil, transport.ErrClosed
	}
}

// Advance is the manual-mode driver: it moves virtual time forward and
// then processes ALL pending work — fault-sampling every enqueued call,
// delivering every due message — before returning. The network is
// quiescent when Advance returns: a deterministic assertion point.
// Manual mode only (auto mode returns an error); one conductor at a time.
func (n *Network) Advance(d time.Duration) error {
	if d < 0 {
		return errors.New("sim: negative Advance")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return transport.ErrClosed
	}
	if n.cfg.Auto {
		return errors.New("sim: Advance is manual-mode only (Config.Auto runs its own driver)")
	}
	n.now = n.now.Add(d)
	for {
		if len(n.waiting) > 0 {
			c := n.waiting[0]
			n.waiting = n.waiting[1:]
			n.schedule(c)
			continue
		}
		if len(n.q) > 0 && !n.q[0].deliverAt.After(n.now) {
			d := popDelivery(&n.q)
			h := n.handler(d.call.to)
			n.mu.Unlock()
			res := deliver(h, d)
			n.mu.Lock()
			d.call.respond(res)
			continue
		}
		return nil
	}
}

// Pending counts calls enqueued but not yet processed by the driver —
// the test hook for serializing scripted send histories ("my call has
// arrived at the network").
func (n *Network) Pending() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.waiting)
}

// Now is the virtual clock (zero epoch in manual mode; wall-mapped in
// auto mode).
func (n *Network) Now() time.Time {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.now
}

// Close stops the auto driver (if any), fails in-flight waiters, and
// makes subsequent Calls return ErrClosed. Idempotent.
func (n *Network) Close() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	close(n.stop)
	n.mu.Unlock()
}

// signal wakes the auto driver (coalesced).
func (n *Network) signal() {
	select {
	case n.wake <- struct{}{}:
	default:
	}
}

func (n *Network) handler(id transport.NodeID) transport.Handler {
	return n.handlers[id] // caller holds n.mu
}

// nowLocked is the clock at this instant (caller holds n.mu): virtual in
// manual mode, wall-mapped in auto mode — Call stamps send times with it.
func (n *Network) nowLocked() time.Time {
	if n.cfg.Auto {
		return time.Time{}.Add(time.Since(n.start))
	}
	return n.now
}

// schedule draws the fault outcome for one call. Called only from the
// driver, under the mutex — this is the single decision point whose
// order the seed makes reproducible.
func (n *Network) schedule(c *call) {
	if n.isolated[c.from] || n.isolated[c.to] || n.blocked[pairKey(c.from, c.to)] {
		c.respond(result{err: ErrBlocked})
		return
	}
	if n.handler(c.to) == nil {
		c.respond(result{err: ErrNoRoute})
		return
	}
	f := n.cfg.Faults
	if f.DropRate > 0 && n.rng.Float64() < f.DropRate {
		c.respond(result{err: ErrDropped})
		return
	}
	delay := time.Duration(0)
	if f.MaxDelay > 0 {
		span := f.MaxDelay - f.MinDelay
		delay = f.MinDelay + time.Duration(n.rng.Float64()*float64(span))
	}
	delay += n.slow[c.from] + n.slow[c.to]
	n.enqueue(c, c.at.Add(delay))
	if f.DupRate > 0 && n.rng.Float64() < f.DupRate {
		dupDelay := time.Duration(0)
		if f.MaxDelay > 0 {
			span := f.MaxDelay - f.MinDelay
			dupDelay = f.MinDelay + time.Duration(n.rng.Float64()*float64(span))
		}
		dupDelay += n.slow[c.from] + n.slow[c.to]
		n.enqueue(c, c.at.Add(dupDelay))
	}
}

func (n *Network) enqueue(c *call, at time.Time) {
	heap.Push(&n.q, &delivery{call: c, deliverAt: at, seq: n.seq})
	n.seq++
}

// deliver invokes the target handler outside the lock (handlers may
// block on a node's event loop; they must never run under n.mu).
func deliver(h transport.Handler, d *delivery) result {
	if h == nil {
		return result{err: ErrNoRoute} // node died between scheduling and delivery
	}
	v, err := d.call.invoke(h)
	return result{val: v, err: err}
}

// drive is the auto-mode driver: map virtual time 1:1 onto real time,
// process enqueued calls immediately, deliver when due. Ordering is
// still (deliverAt, seq) — the wall clock only bounds how long we sleep,
// never which event wins.
func (n *Network) drive() {
	for {
		n.mu.Lock()
		if n.stopped {
			n.mu.Unlock()
			return
		}
		n.now = time.Time{}.Add(time.Since(n.start))
		for len(n.waiting) > 0 {
			c := n.waiting[0]
			n.waiting = n.waiting[1:]
			n.schedule(c)
		}
		for len(n.q) > 0 && !n.q[0].deliverAt.After(n.now) {
			d := popDelivery(&n.q)
			h := n.handler(d.call.to)
			n.mu.Unlock()
			res := deliver(h, d)
			n.mu.Lock()
			if n.stopped {
				n.mu.Unlock()
				return
			}
			d.call.respond(res)
		}
		var sleep time.Duration = -1
		if len(n.q) > 0 {
			sleep = n.q[0].deliverAt.Sub(n.now)
		}
		n.mu.Unlock()

		if sleep < 0 {
			select { // idle: only new work or shutdown can wake us
			case <-n.wake:
			case <-n.stop:
				return
			}
		} else {
			select {
			case <-n.wake:
			case <-n.stop:
				return
			case <-time.After(sleep):
			}
		}
	}
}

func popDelivery(q *deliveryQueue) *delivery {
	return heap.Pop(q).(*delivery)
}
