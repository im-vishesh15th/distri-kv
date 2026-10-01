package server

import (
	"context"
	"fmt"
	"testing"

	kv1 "distrikv/gen/kv/v1"
	"distrikv/internal/kv"
	"distrikv/internal/raft"
	"distrikv/internal/shard"
)

// fakeGroupProposer records the (gid, payload) pairs it receives.
type fakeGroupProposer struct {
	calls []fakeCall
}

type fakeCall struct {
	gid     raft.GroupID
	payload []byte
}

func (f *fakeGroupProposer) Propose(ctx context.Context, gid raft.GroupID, payload []byte) (uint64, any, error) {
	f.calls = append(f.calls, fakeCall{gid: gid, payload: payload})
	return 1, kv.Result{}, nil
}

func (f *fakeGroupProposer) ReadIndex(ctx context.Context, gid raft.GroupID) (uint64, error) {
	return 1, nil
}

type fakeGroupStatus struct {
	statuses map[raft.GroupID]raft.Status
}

func (f *fakeGroupStatus) Status(gid raft.GroupID) (raft.Status, error) {
	if st, ok := f.statuses[gid]; ok {
		return st, nil
	}
	return raft.Status{}, nil
}

// newTestService creates a GroupedService with a MetadataSM wrapping the given config.
func newTestService(t *testing.T, m *shard.Config) *GroupedService {
	t.Helper()
	metadataSM := kv.NewMetadataSM(kv.MetadataSMConfig{
		InitialConfig: m,
	})
	proposer := &fakeGroupProposer{}
	engines := map[raft.GroupID]kv.Engine{
		0: kv.NewMemEngine(),
		1: kv.NewMemEngine(),
	}
	status := &fakeGroupStatus{}
	return NewGroupedService(metadataSM, proposer, status.Status, nil, nil, engines)
}

// TestGroupedServiceRoutesKeysToCorrectGroup verifies that keys are routed
// to the group assigned by the shard configuration.
func TestGroupedServiceRoutesKeysToCorrectGroup(t *testing.T) {
	m := shard.NewMap(2)
	svc := newTestService(t, m)

	key0 := findKeyInGroup(t, m, 0)
	key1 := findKeyInGroup(t, m, 1)

	if _, err := svc.Put(context.Background(), &kv1.PutRequest{
		Key:            key0,
		Value:          []byte("v0"),
		ClientId:       "test-client",
		SequenceNumber: 1,
	}); err != nil {
		t.Fatalf("Put key0: %v", err)
	}

	if _, err := svc.Put(context.Background(), &kv1.PutRequest{
		Key:            key1,
		Value:          []byte("v1"),
		ClientId:       "test-client",
		SequenceNumber: 2,
	}); err != nil {
		t.Fatalf("Put key1: %v", err)
	}

	_ = key0
	_ = key1
}

// TestGroupedServiceGetStatusReportsCorrectGroup verifies that GetStatus(key)
// reports the status of the group that owns the key.
func TestGroupedServiceGetStatusReportsCorrectGroup(t *testing.T) {
	m := shard.NewMap(2)
	metadataSM := kv.NewMetadataSM(kv.MetadataSMConfig{
		InitialConfig: m,
	})
	proposer := &fakeGroupProposer{}
	engines := map[raft.GroupID]kv.Engine{
		0: kv.NewMemEngine(),
		1: kv.NewMemEngine(),
	}
	st0 := raft.Status{ID: "n1", Role: "leader", LeaderID: "n1", Term: 5}
	st1 := raft.Status{ID: "n1", Role: "follower", LeaderID: "n2", Term: 5}
	status := &fakeGroupStatus{
		statuses: map[raft.GroupID]raft.Status{
			0: st0,
			1: st1,
		},
	}
	svc := NewGroupedService(metadataSM, proposer, status.Status, nil, nil, engines)

	// Empty key -> group 0
	resp0, err := svc.GetStatus(context.Background(), &kv1.GetStatusRequest{Key: ""})
	if err != nil {
		t.Fatalf("GetStatus empty key: %v", err)
	}
	if resp0.Role != "leader" || resp0.LeaderId != "n1" {
		t.Fatalf("GetStatus empty key: got role=%s leader=%s, want leader=n1", resp0.Role, resp0.LeaderId)
	}

	// Key in group 1
	key1 := findKeyInGroup(t, m, 1)
	resp1, err := svc.GetStatus(context.Background(), &kv1.GetStatusRequest{Key: key1})
	if err != nil {
		t.Fatalf("GetStatus key1: %v", err)
	}
	if resp1.Role != "follower" || resp1.LeaderId != "n2" {
		t.Fatalf("GetStatus key1: got role=%s leader=%s, want follower=n2", resp1.Role, resp1.LeaderId)
	}
}

// TestNilShardConfigRoutesAllToGroup0 verifies that with no shard config,
// all keys map to group 0 (single-group compatibility).
func TestNilShardConfigRoutesAllToGroup0(t *testing.T) {
	metadataSM := kv.NewMetadataSM(kv.MetadataSMConfig{
		InitialConfig: nil, // defaults to single group
	})
	proposer := &fakeGroupProposer{}
	engines := map[raft.GroupID]kv.Engine{
		0: kv.NewMemEngine(),
	}
	svc := NewGroupedService(metadataSM, proposer, (&fakeGroupStatus{}).Status, nil, nil, engines)

	if _, err := svc.Put(context.Background(), &kv1.PutRequest{
		Key:            "any-key",
		Value:          []byte("v"),
		ClientId:       "test-client",
		SequenceNumber: 1,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if len(proposer.calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(proposer.calls))
	}
	if proposer.calls[0].gid != 0 {
		t.Fatalf("with nil shard, key routed to group %d, want 0", proposer.calls[0].gid)
	}
}

// TestGroupedServiceConfigVersion verifies that GetStatus returns the config version.
func TestGroupedServiceConfigVersion(t *testing.T) {
	m := shard.NewMap(2)
	m.Version = 42
	metadataSM := kv.NewMetadataSM(kv.MetadataSMConfig{
		InitialConfig: m,
	})
	proposer := &fakeGroupProposer{}
	engines := map[raft.GroupID]kv.Engine{
		0: kv.NewMemEngine(),
		1: kv.NewMemEngine(),
	}
	status := &fakeGroupStatus{}
	svc := NewGroupedService(metadataSM, proposer, status.Status, nil, nil, engines)

	resp, err := svc.GetStatus(context.Background(), &kv1.GetStatusRequest{Key: ""})
	if err != nil {
		t.Fatalf("GetStatus: %v", err)
	}
	if resp.ConfigVersion != 42 {
		t.Fatalf("config_version = %d, want 42", resp.ConfigVersion)
	}
}

// findKeyInGroup finds a key that hashes to the given group.
func findKeyInGroup(t *testing.T, m *shard.Config, wantGroup raft.GroupID) string {
	t.Helper()
	for i := 0; i < 10000; i++ {
		k := fmt.Sprintf("key-%d", i)
		if m.Group(k) == wantGroup {
			return k
		}
	}
	t.Fatalf("could not find key for group %d", wantGroup)
	return ""
}
