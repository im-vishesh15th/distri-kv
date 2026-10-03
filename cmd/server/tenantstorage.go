package main

import (
	"encoding/json"
	"net/http"
	"strconv"

	"distrikv/internal/kv"
	"distrikv/internal/raft"
)

// tenantStorageReporter is implemented by kv.MemEngine.
type tenantStorageReporter interface {
	TenantStorage() map[string]kv.TenantStorage
}

type groupStorage struct {
	Applied uint64                      `json:"applied"` // last applied Raft index when sampled
	Tenants map[string]kv.TenantStorage `json:"tenants"`
}

type nodeStorage struct {
	Node   string                  `json:"node"`
	Groups map[string]groupStorage `json:"groups"`
}

// tenantStorageHandler serves GET /tenant-storage on the node's private
// metrics port: the logical bytes/keys each tenant occupies in every DATA group
// this node hosts (the metadata group is skipped). The gateway polls all nodes
// and, per group, keeps the node with the highest applied index, so a lagging
// replica never lowers the number.
func tenantStorageHandler(node string, engines map[raft.GroupID]kv.Engine, applied func(raft.GroupID) uint64) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		out := nodeStorage{Node: node, Groups: map[string]groupStorage{}}
		for gid, eng := range engines {
			if gid == metadataGroupID {
				continue
			}
			rep, ok := eng.(tenantStorageReporter)
			if !ok {
				continue
			}
			ap := applied(gid) // read before the engine so the engine is never older than `applied`
			out.Groups[strconv.FormatUint(uint64(gid), 10)] = groupStorage{Applied: ap, Tenants: rep.TenantStorage()}
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(out)
	})
}
