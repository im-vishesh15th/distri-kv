package nodemetrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLeaderChangesAndGauges(t *testing.T) {
	cur := []GroupStatus{{Group: "0", Role: "follower", LeaderID: "", Term: 1}}
	c := New("node1", func() []GroupStatus { return cur })

	c.Sample() // no leader yet: no change counted
	cur = []GroupStatus{{Group: "0", Role: "follower", LeaderID: "node2", Term: 2, CommitIndex: 10, LastApplied: 8}}
	c.Sample() // first leader: 1
	c.Sample() // same leader: still 1
	cur = []GroupStatus{{Group: "0", Role: "leader", LeaderID: "node1", Term: 3, CommitIndex: 11, LastApplied: 11}}

	rec := httptest.NewRecorder()
	c.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil)) // scrape samples: 2nd change
	out := rec.Body.String()
	for _, want := range []string{
		`distrikv_raft_term{node="node1",group="0"} 3`,
		`distrikv_raft_is_leader{node="node1",group="0"} 1`,
		`distrikv_raft_commit_index{node="node1",group="0"} 11`,
		`distrikv_raft_apply_lag{node="node1",group="0"} 0`,
		`distrikv_raft_leader_changes_total{node="node1",group="0"} 2`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q\n%s", want, out)
		}
	}
}

func TestGroupsAreIndependent(t *testing.T) {
	cur := []GroupStatus{
		{Group: "0", LeaderID: "a", Role: "follower"},
		{Group: "metadata", LeaderID: "b", Role: "follower"},
	}
	c := New("n", func() []GroupStatus { return cur })
	c.Sample()
	cur[0].LeaderID = "c"
	c.Sample()
	var sb strings.Builder
	c.WriteProm(&sb)
	if !strings.Contains(sb.String(), `leader_changes_total{node="n",group="0"} 2`) ||
		!strings.Contains(sb.String(), `leader_changes_total{node="n",group="metadata"} 1`) {
		t.Fatalf("unexpected output:\n%s", sb.String())
	}
}
