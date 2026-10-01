package gateway

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Metrics counts gateway traffic. It is hand-rolled (Prometheus text
// exposition format 0.0.4) so the gateway needs no extra dependency.
//
// Cardinality: requests_total carries a tenant label, so series count is
// tenants x ops x statuses. Fine for hundreds of tenants; cap or drop the
// label before running thousands. The latency histogram is labelled by op
// only, never by tenant.
//
// Counters live in memory and reset when the gateway restarts. /v1/usage
// reports the same counters, so it resets too (Usage.Since says when).
type Metrics struct {
	mu          sync.Mutex
	started     time.Time
	requests    map[reqKey]uint64
	rateLimited map[string]uint64 // tenant -> count
	concLimited map[string]uint64 // tenant -> count
	latency     map[string]*histogram
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
		requests:    map[reqKey]uint64{},
		rateLimited: map[string]uint64{},
		concLimited: map[string]uint64{},
		latency:     map[string]*histogram{},
	}
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

// Usage is one tenant's traffic since the gateway process started.
type Usage struct {
	Since              time.Time         `json:"since"`
	Requests           uint64            `json:"requests"`
	ByOp               map[string]uint64 `json:"by_op"`
	ByStatus           map[string]uint64 `json:"by_status"`
	RateLimited        uint64            `json:"rate_limited"`
	ConcurrencyLimited uint64            `json:"concurrency_limited"`
}

// Usage returns the tenant's counters.
func (m *Metrics) Usage(tenant string) Usage {
	m.mu.Lock()
	defer m.mu.Unlock()
	u := Usage{
		Since:              m.started.UTC(),
		ByOp:               map[string]uint64{},
		ByStatus:           map[string]uint64{},
		RateLimited:        m.rateLimited[tenant],
		ConcurrencyLimited: m.concLimited[tenant],
	}
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
