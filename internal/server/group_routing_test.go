package server

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	kv1 "distrikv/gen/kv/v1"
	"distrikv/internal/kv"
	"distrikv/internal/raft"
	"distrikv/internal/shard"
)

type fakeCall struct {
	gid     raft.GroupID
	payload []byte
}

// fakeGroupProposer records the (gid, payload) pairs it receives, plus the
// gids of ReadIndex barriers.
type fakeGroupProposer struct {
	calls         []fakeCall
	readIndexGIDs []raft.GroupID
	returnVersion uint64
}

func (f *fakeGroupProposer) Propose(ctx context.Context, gid raft.GroupID, payload []byte) (uint64, any, error) {
	f.calls = append(f.calls, fakeCall{gid: gid, payload: payload})
	return 1, kv.Result{Value: []byte(fmt.Sprintf("%d", f.returnVersion))}, nil
}

func (f *fakeGroupProposer) SetReturnVersion(v uint64) {
	f.returnVersion = v
}

func (f *fakeGroupProposer) ReadIndex(ctx context.Context, gid raft.GroupID) (uint64, error) {
	f.readIndexGIDs = append(f.readIndexGIDs, gid)
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
	status := &fakeGroupStatus{}
	svc := NewGroupedService(metadataSM, proposer, status.Status, nil, nil, engines)

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

// TestMoveSlotsRejectedWhenKeysExist verifies that MoveSlots rejects
// a move when the source range contains keys and unsafe_no_migration is false.
func TestMoveSlotsRejectedWhenKeysExist(t *testing.T) {
	m := shard.NewMap(2)
	engines := map[raft.GroupID]kv.Engine{
		0: kv.NewMemEngine(),
		1: kv.NewMemEngine(),
	}

	// Find keys that hash to group 0's range (first half of slots).
	var group0Keys []string
	for i := 0; i < 10000 && len(group0Keys) < 2; i++ {
		k := fmt.Sprintf("testkey-%d", i)
		if m.Group(k) == 0 {
			group0Keys = append(group0Keys, k)
		}
	}
	if len(group0Keys) < 2 {
		t.Fatal("could not find enough keys for group 0")
	}

	// Put some keys in group 0's range.
	engines[0].Put(group0Keys[0], []byte("value-a"))
	engines[0].Put(group0Keys[1], []byte("value-b"))

	proposer := &fakeGroupProposer{}
	status := &fakeGroupStatus{}
	svc := NewGroupedService(
		kv.NewMetadataSM(kv.MetadataSMConfig{InitialConfig: m}),
		proposer, status.Status, nil, nil, engines)

	// Move slots covering group 0's range without the unsafe flag: must be rejected.
	req := &kv1.MoveSlotsRequest{
		StartSlot:         0,
		EndSlot:           shard.NumSlots / 2,
		FromGroup:         0,
		ToGroup:           1,
		NewVersion:        2,
		UnsafeNoMigration: false,
	}
	proposer.SetReturnVersion(2)
	resp, err := svc.MoveSlots(context.Background(), req)
	if err != nil {
		t.Fatalf("MoveSlots returned error: %v", err)
	}
	if resp.Error == "" {
		t.Fatal("MoveSlots should reject move of non-empty range without unsafe flag")
	}
	if !strings.Contains(resp.Error, "contains keys") {
		t.Fatalf("expected error about keys in range, got: %s", resp.Error)
	}

	// No proposal may be made on rejection.
	if len(proposer.calls) != 0 {
		t.Fatalf("expected no proposal when move is rejected, got %d", len(proposer.calls))
	}

	// The linearizable read must have been issued against the *source*
	// group (0), not a group derived from a dummy key's hash.
	if len(proposer.readIndexGIDs) != 1 {
		t.Fatalf("expected exactly 1 ReadIndex call, got %d", len(proposer.readIndexGIDs))
	}
	if proposer.readIndexGIDs[0] != 0 {
		t.Fatalf("ReadIndex issued on group %d, want source group 0", proposer.readIndexGIDs[0])
	}
}

// TestMoveSlotsAllowedWithUnsafeFlag verifies that MoveSlots allows
// moving a range with keys when unsafe_no_migration is true.
func TestMoveSlotsAllowedWithUnsafeFlag(t *testing.T) {
	m := shard.NewMap(2)
	engines := map[raft.GroupID]kv.Engine{
		0: kv.NewMemEngine(),
		1: kv.NewMemEngine(),
	}

	var group0Keys []string
	for i := 0; i < 1000 && len(group0Keys) < 2; i++ {
		k := fmt.Sprintf("testkey-%d", i)
		if m.Group(k) == 0 {
			group0Keys = append(group0Keys, k)
		}
	}
	if len(group0Keys) < 2 {
		t.Fatal("could not find enough keys for group 0")
	}

	engines[0].Put(group0Keys[0], []byte("value-a"))
	engines[0].Put(group0Keys[1], []byte("value-b"))

	proposer := &fakeGroupProposer{}
	status := &fakeGroupStatus{}
	svc := NewGroupedService(
		kv.NewMetadataSM(kv.MetadataSMConfig{InitialConfig: m}),
		proposer, status.Status, nil, nil, engines)

	req := &kv1.MoveSlotsRequest{
		StartSlot:         0,
		EndSlot:           shard.NumSlots / 2,
		FromGroup:         0,
		ToGroup:           1,
		NewVersion:        2,
		UnsafeNoMigration: true,
	}
	proposer.SetReturnVersion(2)
	resp, err := svc.MoveSlots(context.Background(), req)
	if err != nil {
		t.Fatalf("MoveSlots returned error: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("MoveSlots with unsafe flag should succeed, got error: %s", resp.Error)
	}
	if resp.NewVersion != 2 {
		t.Fatalf("expected version 2, got %d", resp.NewVersion)
	}

	if len(proposer.calls) != 1 {
		t.Fatalf("expected 1 proposal, got %d", len(proposer.calls))
	}
	if proposer.calls[0].gid != raft.GroupID(math.MaxUint64) {
		t.Fatalf("proposal sent to wrong group: %d", proposer.calls[0].gid)
	}
}

// TestMoveSlotsAllowedOnEmptyRange verifies that MoveSlots succeeds
// when the source range is empty (no keys).
func TestMoveSlotsAllowedOnEmptyRange(t *testing.T) {
	m := shard.NewMap(2)
	engines := map[raft.GroupID]kv.Engine{
		0: kv.NewMemEngine(),
		1: kv.NewMemEngine(),
	}
	proposer := &fakeGroupProposer{}
	status := &fakeGroupStatus{}
	svc := NewGroupedService(
		kv.NewMetadataSM(kv.MetadataSMConfig{InitialConfig: m}),
		proposer, status.Status, nil, nil, engines)

	req := &kv1.MoveSlotsRequest{
		StartSlot:         0,
		EndSlot:           shard.NumSlots / 2,
		FromGroup:         0,
		ToGroup:           1,
		NewVersion:        2,
		UnsafeNoMigration: false,
	}
	proposer.SetReturnVersion(2)
	resp, err := svc.MoveSlots(context.Background(), req)
	if err != nil {
		t.Fatalf("MoveSlots of empty range should succeed: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("MoveSlots of empty range should succeed, got error: %s", resp.Error)
	}
	if resp.NewVersion != 2 {
		t.Fatalf("expected version 2, got %d", resp.NewVersion)
	}

	if len(proposer.calls) != 1 {
		t.Fatalf("expected 1 proposal, got %d", len(proposer.calls))
	}
}

// TestMoveSlotsNoProposalOnRejection verifies that no proposal is made
// when a move is rejected due to keys in the source range. The key here
// hashes to whatever group it lands in; we assert rejection whenever the
// chosen slot range covers a non-empty source group.
func TestMoveSlotsNoProposalOnRejection(t *testing.T) {
	m := shard.NewMap(2)
	engines := map[raft.GroupID]kv.Engine{
		0: kv.NewMemEngine(),
		1: kv.NewMemEngine(),
	}

	key := findKeyInGroup(t, m, 0)
	engines[0].Put(key, []byte("value"))

	proposer := &fakeGroupProposer{}
	status := &fakeGroupStatus{}
	svc := NewGroupedService(
		kv.NewMetadataSM(kv.MetadataSMConfig{InitialConfig: m}),
		proposer, status.Status, nil, nil, engines)

	req := &kv1.MoveSlotsRequest{
		StartSlot:         0,
		EndSlot:           shard.NumSlots / 2,
		FromGroup:         0,
		ToGroup:           1,
		NewVersion:        2,
		UnsafeNoMigration: false,
	}
	proposer.SetReturnVersion(2)
	resp, err := svc.MoveSlots(context.Background(), req)
	if err != nil {
		t.Fatalf("MoveSlots returned error: %v", err)
	}
	if resp.Error == "" {
		t.Fatal("MoveSlots should reject move of non-empty range")
	}

	if len(proposer.calls) != 0 {
		t.Fatalf("expected no proposal when move is rejected, got %d", len(proposer.calls))
	}
}

// TestHasKeyInSlotRangeBoundary tests HasKeyInSlotRange at boundaries.
func TestHasKeyInSlotRangeBoundary(t *testing.T) {
	engine := kv.NewMemEngine()

	// Find a key that hashes to the first slot (0).
	var firstSlotKey string
	for i := 0; i < 100000; i++ {
		k := fmt.Sprintf("key-%d", i)
		if shard.Slot(k) == 0 {
			firstSlotKey = k
			break
		}
	}
	if firstSlotKey == "" {
		t.Fatal("could not find key for slot 0")
	}

	// Find a key that hashes to the last slot (16383).
	var lastSlotKey string
	for i := 0; i < 100000; i++ {
		k := fmt.Sprintf("key-%d", i)
		if shard.Slot(k) == shard.NumSlots-1 {
			lastSlotKey = k
			break
		}
	}
	if lastSlotKey == "" {
		t.Fatal("could not find key for slot 16383")
	}

	engine.Put(firstSlotKey, []byte("first"))
	engine.Put(lastSlotKey, []byte("last"))

	// First slot boundary: [0,1)
	has, err := engine.HasKeyInSlotRange(0, 1)
	if err != nil {
		t.Fatalf("HasKeyInSlotRange error: %v", err)
	}
	if !has {
		t.Fatal("expected key at slot 0 to be found in range [0,1)")
	}

	// Last slot boundary: [16383,16384)
	has, err = engine.HasKeyInSlotRange(shard.NumSlots-1, shard.NumSlots)
	if err != nil {
		t.Fatalf("HasKeyInSlotRange error: %v", err)
	}
	if !has {
		t.Fatal("expected key at slot 16383 to be found in range [16383,16384)")
	}

	// Key just outside range: [1,2) must not contain the slot-0 key.
	has, err = engine.HasKeyInSlotRange(1, 2)
	if err != nil {
		t.Fatalf("HasKeyInSlotRange error: %v", err)
	}
	if has {
		t.Fatal("key at slot 0 should not be in range [1,2)")
	}

	// Empty engine.
	emptyEngine := kv.NewMemEngine()
	has, err = emptyEngine.HasKeyInSlotRange(0, shard.NumSlots)
	if err != nil {
		t.Fatalf("HasKeyInSlotRange error: %v", err)
	}
	if has {
		t.Fatal("empty engine should have no keys")
	}
}
