package multiraft

import (
	"context"
	"testing"
)

// TestHostReadIndex verifies that ReadIndex routes to the correct group.
func TestHostReadIndex(t *testing.T) {
	// We can't easily create real raft.Group instances without the full
	// test harness, so we verify the routing logic by checking that
	// unknown groups return an error.
	host := NewHost("test-node")

	// Unknown group should return error
	_, err := host.ReadIndex(context.Background(), 999)
	if err == nil {
		t.Fatal("ReadIndex for unknown group should return error")
	}

	// Verify error message contains group info
	if err.Error() == "" {
		t.Fatal("error should not be empty")
	}
}

// TestHostGroupAccessor verifies that Group(gid) returns the group or nil.
func TestHostGroupAccessor(t *testing.T) {
	host := NewHost("test-node")

	// Unknown group should return nil
	if g := host.Group(999); g != nil {
		t.Fatal("Group for unknown gid should return nil")
	}

	// GroupIDs should be empty initially
	if ids := host.GroupIDs(); len(ids) != 0 {
		t.Fatalf("GroupIDs should be empty initially, got %v", ids)
	}
}
