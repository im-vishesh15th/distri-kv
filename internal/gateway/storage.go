package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// StorageUsage is a tenant's logical data footprint across all data groups.
//
// Bytes counts len(user key) + len(value) once per key. It is NOT disk usage,
// RAM usage, or replicated bytes (the data exists on 3 replicas), and it does
// not include Raft log/snapshot overhead. AsOf is when the nodes were polled.
type StorageUsage struct {
	Bytes  int64     `json:"bytes"`
	Keys   int64     `json:"keys"`
	Groups int       `json:"groups_reporting"` // data groups that contributed
	AsOf   time.Time `json:"as_of"`
}

// ErrStorageUnavailable means no node answered with usable data.
var ErrStorageUnavailable = errors.New("gateway: storage usage unavailable")

// Freshness policy (one shared snapshot for ALL tenants, so load does not grow
// with tenants, browser tabs or dashboard refreshes):
//
//	age <= storageFreshFor : serve the cached snapshot, no work
//	age <= storageHardTTL  : serve the cached snapshot AND start ONE background refresh
//	age >  storageHardTTL  : wait (<= request deadline) for ONE refresh; callers share it
//
// With no viewers there is no polling at all. A returned value is never older
// than storageHardTTL unless the nodes are failing; in that case the last good
// snapshot is served for up to storageFailStale (its as_of shows its real age),
// and refresh attempts are spaced storageRetryGap apart.
const (
	storageFreshFor  = 10 * time.Second
	storageHardTTL   = 30 * time.Second
	storageFailStale = 5 * time.Minute
	storageRetryGap  = 5 * time.Second
	storageNodeTO    = 1500 * time.Millisecond
)

type nodeStorageDoc struct {
	Node   string `json:"node"`
	Groups map[string]struct {
		Applied uint64 `json:"applied"`
		Tenants map[string]struct {
			Bytes int64 `json:"bytes"`
			Keys  int64 `json:"keys"`
		} `json:"tenants"`
	} `json:"groups"`
}

type storageFlight struct {
	done chan struct{}
	err  error
}

// StorageSource polls the nodes' private /tenant-storage endpoint. Per data
// group it uses the replica with the highest applied Raft index, then sums
// groups.
type StorageSource struct {
	urls   []string
	client *http.Client
	now    func() time.Time

	mu       sync.Mutex
	hasCache bool
	fetched  time.Time
	cache    map[string]StorageUsage // by tenant
	groups   int
	nextTry  time.Time // do not start another refresh before this (after a failure)
	inflight *storageFlight
}

// NewStorageSource takes node metrics base URLs, e.g. "http://distrikv-1:9101".
func NewStorageSource(baseURLs []string) *StorageSource {
	var urls []string
	for _, u := range baseURLs {
		if u = strings.TrimRight(strings.TrimSpace(u), "/"); u != "" {
			urls = append(urls, u+"/tenant-storage")
		}
	}
	return &StorageSource{urls: urls, client: &http.Client{Timeout: storageNodeTO}, now: time.Now}
}

func (s *StorageSource) fetchNode(ctx context.Context, url string) (*nodeStorageDoc, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: status %d", url, resp.StatusCode)
	}
	var doc nodeStorageDoc
	if err := json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&doc); err != nil {
		return nil, fmt.Errorf("%s: %w", url, err)
	}
	return &doc, nil
}

// poll asks every node IN PARALLEL (a dead node costs one timeout, not one per
// node) and aggregates. It holds no lock.
func (s *StorageSource) poll() (map[string]StorageUsage, int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*storageNodeTO)
	defer cancel()
	docs := make([]*nodeStorageDoc, len(s.urls))
	var wg sync.WaitGroup
	for i, u := range s.urls {
		wg.Add(1)
		go func(i int, u string) {
			defer wg.Done()
			if d, err := s.fetchNode(ctx, u); err == nil {
				docs[i] = d
			}
		}(i, u)
	}
	wg.Wait()

	type best struct {
		applied uint64
		tenants map[string]StorageUsage
	}
	perGroup := map[string]best{}
	for _, doc := range docs {
		if doc == nil {
			continue
		}
		for g, gs := range doc.Groups {
			if b, ok := perGroup[g]; ok && b.applied >= gs.Applied {
				continue
			}
			m := make(map[string]StorageUsage, len(gs.Tenants))
			for t, v := range gs.Tenants {
				m[t] = StorageUsage{Bytes: v.Bytes, Keys: v.Keys}
			}
			perGroup[g] = best{applied: gs.Applied, tenants: m}
		}
	}
	if len(perGroup) == 0 {
		return nil, 0, ErrStorageUnavailable
	}
	total := map[string]StorageUsage{}
	for _, b := range perGroup {
		for t, v := range b.tenants {
			u := total[t]
			u.Bytes += v.Bytes
			u.Keys += v.Keys
			total[t] = u
		}
	}
	return total, len(perGroup), nil
}

// startRefreshLocked returns the in-flight refresh, starting one if needed.
func (s *StorageSource) startRefreshLocked() *storageFlight {
	if s.inflight != nil {
		return s.inflight
	}
	f := &storageFlight{done: make(chan struct{})}
	s.inflight = f
	go func() {
		total, groups, err := s.poll()
		s.mu.Lock()
		if err == nil {
			s.cache, s.groups, s.hasCache, s.fetched = total, groups, true, s.now()
			s.nextTry = time.Time{}
		} else {
			s.nextTry = s.now().Add(storageRetryGap) // keep the last good snapshot
		}
		s.inflight = nil
		f.err = err
		s.mu.Unlock()
		close(f.done)
	}()
	return f
}

func (s *StorageSource) serveLocked(tenantID string) StorageUsage {
	u := s.cache[tenantID]
	u.Groups, u.AsOf = s.groups, s.fetched.UTC()
	return u
}

// Usage returns the tenant's footprint. A tenant with no data reports zeros.
func (s *StorageSource) Usage(ctx context.Context, tenantID string) (StorageUsage, error) {
	if s == nil || len(s.urls) == 0 {
		return StorageUsage{}, ErrStorageUnavailable
	}
	s.mu.Lock()
	now := s.now()
	age := now.Sub(s.fetched)
	switch {
	case s.hasCache && age <= storageFreshFor:
		defer s.mu.Unlock()
		return s.serveLocked(tenantID), nil
	case s.hasCache && age <= storageHardTTL:
		if !now.Before(s.nextTry) {
			s.startRefreshLocked()
		}
		defer s.mu.Unlock()
		return s.serveLocked(tenantID), nil
	case now.Before(s.nextTry): // recent failure: do not hammer dead nodes
		defer s.mu.Unlock()
		if s.hasCache && age <= storageFailStale {
			return s.serveLocked(tenantID), nil
		}
		return StorageUsage{}, ErrStorageUnavailable
	}

	f := s.startRefreshLocked()
	s.mu.Unlock()
	select {
	case <-f.done:
	case <-ctx.Done():
		return StorageUsage{}, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if f.err == nil || (s.hasCache && s.now().Sub(s.fetched) <= storageFailStale) {
		return s.serveLocked(tenantID), nil
	}
	return StorageUsage{}, ErrStorageUnavailable
}
