package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"
)

var errInvalidUsageRange = errors.New("invalid range")

// Metrics counts gateway traffic. It is hand-rolled (Prometheus text
// exposition format 0.0.4) so the gateway needs no extra dependency.
//
// Cardinality: requests_total carries a tenant label, so series count is
// tenants x ops x statuses. Fine for hundreds of tenants; cap or drop the
// label before running thousands. The latency histogram is labelled by op
// only, never by tenant.
//
// Live counters stay in memory (and Prometheus still reflects this process).
// When a UsagePersister is attached, deltas are flushed into hourly SQLite
// buckets so /v1/usage and the console survive gateway restarts.
type Metrics struct {
	mu          sync.Mutex
	started     time.Time
	now         func() time.Time
	persister   UsagePersister
	requests    map[reqKey]uint64
	rateLimited map[string]uint64 // tenant -> count
	concLimited map[string]uint64 // tenant -> count
	latency     map[string]*histogram

	flushedReq  map[reqKey]uint64
	flushedRate map[string]uint64
	flushedConc map[string]uint64
}

// UsagePersister stores hourly usage deltas. SQLiteStore implements it;
// MemStore does not, so tests keep process-local counters.
type UsagePersister interface {
	AddUsageDeltas(ctx context.Context, bucket time.Time, deltas []usageDelta) error
	TenantUsageTotals(ctx context.Context, tenantID string) (Usage, error)
	tenantUsageRows(ctx context.Context, tenantID string, from, to time.Time) ([]usageRow, error)
}

type reqKey struct {
	tenant string
	op     string
	status int
}

// latencyBuckets are upper bounds in seconds.
var latencyBuckets = []float64{
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5,
}

type histogram struct {
	counts []uint64 // per-bucket (non-cumulative); last slot is +Inf
	sum    float64
	total  uint64
}

func newHistogram() *histogram {
	return &histogram{counts: make([]uint64, len(latencyBuckets)+1)}
}

func (h *histogram) observe(sec float64) {
	i := sort.SearchFloat64s(latencyBuckets, sec) // first bucket with bound >= sec
	h.counts[i]++
	h.sum += sec
	h.total++
}

// NewMetrics returns an empty registry.
func NewMetrics(now time.Time) *Metrics {
	return &Metrics{
		started:     now,
		now:         time.Now,
		requests:    map[reqKey]uint64{},
		rateLimited: map[string]uint64{},
		concLimited: map[string]uint64{},
		latency:     map[string]*histogram{},
		flushedReq:  map[reqKey]uint64{},
		flushedRate: map[string]uint64{},
		flushedConc: map[string]uint64{},
	}
}

func (m *Metrics) SetPersister(p UsagePersister) { m.persister = p }

func (m *Metrics) clock() time.Time {
	if m.now != nil {
		return m.now()
	}
	return time.Now()
}

func (m *Metrics) observe(tenant, op string, status int, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests[reqKey{tenant, op, status}]++
	h := m.latency[op]
	if h == nil {
		h = newHistogram()
		m.latency[op] = h
	}
	h.observe(d.Seconds())
}

func (m *Metrics) incRateLimited(tenant string) {
	m.mu.Lock()
	m.rateLimited[tenant]++
	m.mu.Unlock()
}

func (m *Metrics) incConcLimited(tenant string) {
	m.mu.Lock()
	m.concLimited[tenant]++
	m.mu.Unlock()
}

// Usage is one tenant's traffic. With a persister this is durable totals
// (plus unflushed in-memory deltas); otherwise it is since process start.
type Usage struct {
	Since              time.Time         `json:"since"`
	Requests           uint64            `json:"requests"`
	ByOp               map[string]uint64 `json:"by_op"`
	ByStatus           map[string]uint64 `json:"by_status"`
	RateLimited        uint64            `json:"rate_limited"`
	ConcurrencyLimited uint64            `json:"concurrency_limited"`
}

// UsagePoint is one step on a tenant usage series.
type UsagePoint struct {
	T                  time.Time         `json:"t"`
	Requests           uint64            `json:"requests"`
	ByOp               map[string]uint64 `json:"by_op"`
	RateLimited        uint64            `json:"rate_limited"`
	ConcurrencyLimited uint64            `json:"concurrency_limited"`
}

// UsageSeries is a filled time series for the console charts.
type UsageSeries struct {
	Range  string       `json:"range"`
	Step   string       `json:"step"`
	Points []UsagePoint `json:"points"`
}

func emptyUsage(since time.Time) Usage {
	return Usage{Since: since.UTC(), ByOp: map[string]uint64{}, ByStatus: map[string]uint64{}}
}

func mergeUsage(dst *Usage, src Usage) {
	dst.Requests += src.Requests
	dst.RateLimited += src.RateLimited
	dst.ConcurrencyLimited += src.ConcurrencyLimited
	if dst.ByOp == nil {
		dst.ByOp = map[string]uint64{}
	}
	if dst.ByStatus == nil {
		dst.ByStatus = map[string]uint64{}
	}
	for k, n := range src.ByOp {
		dst.ByOp[k] += n
	}
	for k, n := range src.ByStatus {
		dst.ByStatus[k] += n
	}
}

func (m *Metrics) liveLocked(tenant string) Usage {
	u := emptyUsage(m.started)
	u.RateLimited = m.rateLimited[tenant]
	u.ConcurrencyLimited = m.concLimited[tenant]
	for k, n := range m.requests {
		if k.tenant != tenant {
			continue
		}
		u.Requests += n
		u.ByOp[k.op] += n
		u.ByStatus[strconv.Itoa(k.status)] += n
	}
	return u
}

func (m *Metrics) unflushedLocked(tenant string) Usage {
	u := emptyUsage(time.Time{})
	for k, n := range m.requests {
		if k.tenant != tenant {
			continue
		}
		d := n - m.flushedReq[k]
		if d == 0 {
			continue
		}
		u.Requests += d
		u.ByOp[k.op] += d
		u.ByStatus[strconv.Itoa(k.status)] += d
	}
	if d := m.rateLimited[tenant] - m.flushedRate[tenant]; d > 0 {
		u.RateLimited = d
	}
	if d := m.concLimited[tenant] - m.flushedConc[tenant]; d > 0 {
		u.ConcurrencyLimited = d
	}
	return u
}

func (m *Metrics) snapshotUnflushedLocked() []usageDelta {
	var out []usageDelta
	for k, n := range m.requests {
		if d := n - m.flushedReq[k]; d > 0 {
			out = append(out, usageDelta{Tenant: k.tenant, Kind: usageKindReq, Op: k.op, Status: k.status, Count: d})
		}
	}
	for t, n := range m.rateLimited {
		if d := n - m.flushedRate[t]; d > 0 {
			out = append(out, usageDelta{Tenant: t, Kind: usageKindRate, Count: d})
		}
	}
	for t, n := range m.concLimited {
		if d := n - m.flushedConc[t]; d > 0 {
			out = append(out, usageDelta{Tenant: t, Kind: usageKindConc, Count: d})
		}
	}
	return out
}

func (m *Metrics) markFlushedLocked(deltas []usageDelta) {
	for _, d := range deltas {
		switch d.Kind {
		case usageKindRate:
			m.flushedRate[d.Tenant] += d.Count
		case usageKindConc:
			m.flushedConc[d.Tenant] += d.Count
		default:
			m.flushedReq[reqKey{tenant: d.Tenant, op: d.Op, status: d.Status}] += d.Count
		}
	}
}

// Flush writes unflushed in-memory deltas into the current hour bucket.
func (m *Metrics) Flush(ctx context.Context) error {
	if m.persister == nil {
		return nil
	}
	m.mu.Lock()
	deltas := m.snapshotUnflushedLocked()
	m.mu.Unlock()
	if len(deltas) == 0 {
		return nil
	}
	bucket := m.clock().UTC().Truncate(time.Hour)
	if err := m.persister.AddUsageDeltas(ctx, bucket, deltas); err != nil {
		return err
	}
	m.mu.Lock()
	m.markFlushedLocked(deltas)
	m.mu.Unlock()
	return nil
}

func (m *Metrics) runFlusher(ctx context.Context, every time.Duration) {
	if m.persister == nil {
		return
	}
	if every <= 0 {
		every = 15 * time.Second
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			fctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			if err := m.Flush(fctx); err != nil {
				log.Printf("gateway: usage flush on shutdown: %v", err)
			}
			cancel()
			return
		case <-t.C:
			if err := m.Flush(ctx); err != nil {
				log.Printf("gateway: usage flush: %v", err)
			}
		}
	}
}

// Usage returns the tenant's counters (durable totals + unflushed deltas).
func (m *Metrics) Usage(tenant string) Usage {
	m.mu.Lock()
	live := m.liveLocked(tenant)
	unflushed := m.unflushedLocked(tenant)
	started := m.started
	p := m.persister
	m.mu.Unlock()
	if p == nil {
		return live
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stored, err := p.TenantUsageTotals(ctx, tenant)
	if err != nil {
		log.Printf("gateway: usage totals: %v", err)
		return live
	}
	if stored.ByOp == nil {
		stored.ByOp = map[string]uint64{}
	}
	if stored.ByStatus == nil {
		stored.ByStatus = map[string]uint64{}
	}
	mergeUsage(&stored, unflushed)
	if stored.Since.IsZero() {
		stored.Since = started.UTC()
	}
	return stored
}

func parseUsageRange(rng string, now time.Time) (from, to time.Time, step time.Duration, label, stepName string, ok bool) {
	now = now.UTC()
	switch rng {
	case "", "24h":
		to = now.Truncate(time.Hour).Add(time.Hour)
		from = to.Add(-24 * time.Hour)
		return from, to, time.Hour, "24h", "hour", true
	case "7d":
		day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		to = day.Add(24 * time.Hour)
		from = to.Add(-7 * 24 * time.Hour)
		return from, to, 24 * time.Hour, "7d", "day", true
	case "30d":
		day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		to = day.Add(24 * time.Hour)
		from = to.Add(-30 * 24 * time.Hour)
		return from, to, 24 * time.Hour, "30d", "day", true
	default:
		return time.Time{}, time.Time{}, 0, "", "", false
	}
}

func emptyPoint(t time.Time) UsagePoint {
	return UsagePoint{T: t.UTC(), ByOp: map[string]uint64{}}
}

func addRowToPoint(p *UsagePoint, kind, op string, n uint64) {
	switch kind {
	case usageKindRate:
		p.RateLimited += n
	case usageKindConc:
		p.ConcurrencyLimited += n
	default:
		p.Requests += n
		if op != "" {
			if p.ByOp == nil {
				p.ByOp = map[string]uint64{}
			}
			p.ByOp[op] += n
		}
	}
}

func mergePointUsage(p *UsagePoint, u Usage) {
	p.Requests += u.Requests
	p.RateLimited += u.RateLimited
	p.ConcurrencyLimited += u.ConcurrencyLimited
	if p.ByOp == nil {
		p.ByOp = map[string]uint64{}
	}
	for k, n := range u.ByOp {
		p.ByOp[k] += n
	}
}

// Series returns a zero-filled window of hourly (24h) or daily (7d/30d) points.
func (m *Metrics) Series(tenant, rng string) (UsageSeries, error) {
	from, to, step, label, stepName, ok := parseUsageRange(rng, m.clock())
	if !ok {
		return UsageSeries{}, errInvalidUsageRange
	}
	n := int(to.Sub(from) / step)
	points := make([]UsagePoint, n)
	index := map[int64]int{}
	for i := 0; i < n; i++ {
		t := from.Add(time.Duration(i) * step)
		points[i] = emptyPoint(t)
		index[t.Unix()] = i
	}

	if m.persister != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		rows, err := m.persister.tenantUsageRows(ctx, tenant, from, to)
		cancel()
		if err != nil {
			return UsageSeries{}, err
		}
		for _, r := range rows {
			bt := time.Unix(r.Bucket, 0).UTC().Truncate(step)
			i, hit := index[bt.Unix()]
			if !hit {
				continue
			}
			addRowToPoint(&points[i], r.Kind, r.Op, r.Count)
		}
	}

	m.mu.Lock()
	unflushed := m.unflushedLocked(tenant)
	m.mu.Unlock()
	if i, hit := index[m.clock().UTC().Truncate(step).Unix()]; hit {
		mergePointUsage(&points[i], unflushed)
	}
	return UsageSeries{Range: label, Step: stepName, Points: points}, nil
}

// WriteProm writes all metrics in Prometheus text format.
func (m *Metrics) WriteProm(w io.Writer) {
	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Fprintln(w, "# HELP distrikv_gateway_requests_total HTTP requests handled, by tenant, op and status.")
	fmt.Fprintln(w, "# TYPE distrikv_gateway_requests_total counter")
	keys := make([]reqKey, 0, len(m.requests))
	for k := range m.requests {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.tenant != b.tenant {
			return a.tenant < b.tenant
		}
		if a.op != b.op {
			return a.op < b.op
		}
		return a.status < b.status
	})
	for _, k := range keys {
		fmt.Fprintf(w, "distrikv_gateway_requests_total{tenant=%q,op=%q,status=\"%d\"} %d\n",
			k.tenant, k.op, k.status, m.requests[k])
	}

	writeTenantCounter(w, "distrikv_gateway_rate_limited_total",
		"Requests rejected by the per-tenant rate limit (HTTP 429).", m.rateLimited)
	writeTenantCounter(w, "distrikv_gateway_concurrency_limited_total",
		"Requests rejected by the per-tenant concurrency cap (HTTP 429).", m.concLimited)

	fmt.Fprintln(w, "# HELP distrikv_gateway_request_duration_seconds Request latency by op.")
	fmt.Fprintln(w, "# TYPE distrikv_gateway_request_duration_seconds histogram")
	ops := make([]string, 0, len(m.latency))
	for op := range m.latency {
		ops = append(ops, op)
	}
	sort.Strings(ops)
	for _, op := range ops {
		h := m.latency[op]
		var cum uint64
		for i, ub := range latencyBuckets {
			cum += h.counts[i]
			fmt.Fprintf(w, "distrikv_gateway_request_duration_seconds_bucket{op=%q,le=\"%g\"} %d\n", op, ub, cum)
		}
		cum += h.counts[len(latencyBuckets)]
		fmt.Fprintf(w, "distrikv_gateway_request_duration_seconds_bucket{op=%q,le=\"+Inf\"} %d\n", op, cum)
		fmt.Fprintf(w, "distrikv_gateway_request_duration_seconds_sum{op=%q} %g\n", op, h.sum)
		fmt.Fprintf(w, "distrikv_gateway_request_duration_seconds_count{op=%q} %d\n", op, h.total)
	}
}

func writeTenantCounter(w io.Writer, name, help string, m map[string]uint64) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", name, help, name)
	tenants := make([]string, 0, len(m))
	for t := range m {
		tenants = append(tenants, t)
	}
	sort.Strings(tenants)
	for _, t := range tenants {
		fmt.Fprintf(w, "%s{tenant=%q} %d\n", name, t, m[t])
	}
}

// Handler serves the metrics. It is NOT mounted on the public gateway
// handler; Run serves it on the separate, private Config.MetricsAddr.
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		m.WriteProm(w)
	})
}

// statusRecorder captures the response status and lets handlers name the
// op (needed for POST /kv, where the op is in the body).
type statusRecorder struct {
	http.ResponseWriter
	status int
	op     string
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// setOp records the op name on the wrapping recorder, if there is one.
func setOp(w http.ResponseWriter, op string) {
	if s, ok := w.(*statusRecorder); ok {
		s.op = op
	}
}

// routeOp is the op label known before the body is read.
func routeOp(r *http.Request) string {
	switch {
	case r.URL.Path == "/v1/usage":
		return "usage"
	case r.URL.Path == "/kv" && r.Method == http.MethodPost:
		return "kv_post" // handleOp replaces this with set/cas/incr/decr
	case len(r.URL.Path) > 4 && r.URL.Path[:4] == "/kv/":
		switch r.Method {
		case http.MethodGet:
			return "get"
		case http.MethodPut:
			return "put"
		case http.MethodDelete:
			return "delete"
		}
	}
	return "other"
}
