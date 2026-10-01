// Package nodemetrics exposes a DistriKV node's Raft state as Prometheus
// gauges on a private HTTP port.
//
// It deliberately depends on nothing in internal/raft: the server hands it a
// function returning plain GroupStatus values, so the engine stays untouched
// and this package is testable on its own.
//
// Leader changes are counted by sampling (every Run interval, and on each
// scrape). A leader that changes and changes back between two samples is not
// seen; at the default 200ms interval that is far shorter than an election.
// The first leader a group ever shows counts as one change.
package nodemetrics

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"
)

// GroupStatus is one Raft group's state on this node.
type GroupStatus struct {
	Group       string // "0", "1", ... or "metadata"
	Role        string // "leader", "follower", ...
	LeaderID    string // "" = unknown
	Term        uint64
	CommitIndex uint64
	LastApplied uint64
}

// Collector samples a status source and renders metrics.
type Collector struct {
	node string
	src  func() []GroupStatus

	mu         sync.Mutex
	latest     []GroupStatus
	lastLeader map[string]string
	changes    map[string]uint64
}

// New returns a Collector for the named node.
func New(node string, src func() []GroupStatus) *Collector {
	return &Collector{
		node:       node,
		src:        src,
		lastLeader: map[string]string{},
		changes:    map[string]uint64{},
	}
}

// Sample reads the source once and updates leader-change counts.
func (c *Collector) Sample() {
	st := c.src()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.latest = st
	for _, g := range st {
		if g.LeaderID != "" && g.LeaderID != c.lastLeader[g.Group] {
			c.changes[g.Group]++
			c.lastLeader[g.Group] = g.LeaderID
		}
	}
}

// Run samples every interval until ctx is cancelled.
func (c *Collector) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		c.Sample()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// WriteProm renders the latest sample in Prometheus text format.
func (c *Collector) WriteProm(w io.Writer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	gs := append([]GroupStatus(nil), c.latest...)
	sort.Slice(gs, func(i, j int) bool { return gs[i].Group < gs[j].Group })

	gauge := func(name, help string, val func(GroupStatus) uint64) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", name, help, name)
		for _, g := range gs {
			fmt.Fprintf(w, "%s{node=%q,group=%q} %d\n", name, c.node, g.Group, val(g))
		}
	}
	gauge("distrikv_raft_term", "Current Raft term.",
		func(g GroupStatus) uint64 { return g.Term })
	gauge("distrikv_raft_is_leader", "1 if this node is the group's leader.",
		func(g GroupStatus) uint64 {
			if g.Role == "leader" {
				return 1
			}
			return 0
		})
	gauge("distrikv_raft_commit_index", "Highest committed log index.",
		func(g GroupStatus) uint64 { return g.CommitIndex })
	gauge("distrikv_raft_last_applied", "Highest log index applied to the state machine.",
		func(g GroupStatus) uint64 { return g.LastApplied })
	gauge("distrikv_raft_apply_lag", "commit_index minus last_applied.",
		func(g GroupStatus) uint64 {
			if g.CommitIndex > g.LastApplied {
				return g.CommitIndex - g.LastApplied
			}
			return 0
		})

	fmt.Fprintln(w, "# HELP distrikv_raft_leader_changes_total Leader changes observed by this node.")
	fmt.Fprintln(w, "# TYPE distrikv_raft_leader_changes_total counter")
	for _, g := range gs {
		fmt.Fprintf(w, "distrikv_raft_leader_changes_total{node=%q,group=%q} %d\n", c.node, g.Group, c.changes[g.Group])
	}
}

// Handler serves /metrics, taking a fresh sample per scrape.
func (c *Collector) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		c.Sample()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		c.WriteProm(w)
	})
	return mux
}
