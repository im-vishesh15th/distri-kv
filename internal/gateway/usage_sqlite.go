package gateway

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"
)

// Hourly request/limit counters. IF NOT EXISTS so existing gateway.db files
// upgrade in place. tenant_id is not a foreign key: unauthenticated traffic
// is labelled "none".
const usageSchema = `
CREATE TABLE IF NOT EXISTS usage_buckets (
	tenant_id TEXT NOT NULL,
	bucket    INTEGER NOT NULL,
	kind      TEXT NOT NULL,
	op        TEXT NOT NULL,
	status    INTEGER NOT NULL,
	count     INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (tenant_id, bucket, kind, op, status)
);
CREATE INDEX IF NOT EXISTS idx_usage_buckets_tenant_bucket ON usage_buckets(tenant_id, bucket);
`

const (
	usageKindReq  = "req"
	usageKindRate = "rate"
	usageKindConc = "conc"
)

type usageDelta struct {
	Tenant string
	Kind   string
	Op     string
	Status int
	Count  uint64
}

// AddUsageDeltas adds counts into the hour starting at bucket (UTC truncated).
func (s *SQLiteStore) AddUsageDeltas(ctx context.Context, bucket time.Time, deltas []usageDelta) error {
	if len(deltas) == 0 {
		return nil
	}
	b := bucket.UTC().Truncate(time.Hour).Unix()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("usage begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO usage_buckets(tenant_id, bucket, kind, op, status, count)
		VALUES(?, ?, ?, ?, ?, ?)
		ON CONFLICT(tenant_id, bucket, kind, op, status)
		DO UPDATE SET count = count + excluded.count`)
	if err != nil {
		return fmt.Errorf("usage prepare: %w", err)
	}
	defer stmt.Close()
	for _, d := range deltas {
		if d.Count == 0 || d.Tenant == "" {
			continue
		}
		if _, err := stmt.ExecContext(ctx, d.Tenant, b, d.Kind, d.Op, d.Status, int64(d.Count)); err != nil {
			return fmt.Errorf("usage insert: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("usage commit: %w", err)
	}
	return nil
}

// TenantUsageTotals sums every stored bucket for the tenant.
func (s *SQLiteStore) TenantUsageTotals(ctx context.Context, tenantID string) (Usage, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT kind, op, status, SUM(count), MIN(bucket)
		FROM usage_buckets WHERE tenant_id = ?
		GROUP BY kind, op, status`, tenantID)
	if err != nil {
		return Usage{}, err
	}
	defer rows.Close()
	return scanUsageRows(rows)
}

func scanUsageRows(rows *sql.Rows) (Usage, error) {
	u := Usage{ByOp: map[string]uint64{}, ByStatus: map[string]uint64{}}
	var minBucket int64
	var haveMin bool
	for rows.Next() {
		var kind, op string
		var status int
		var n, bucket int64
		if err := rows.Scan(&kind, &op, &status, &n, &bucket); err != nil {
			return Usage{}, err
		}
		if n <= 0 {
			continue
		}
		if !haveMin || bucket < minBucket {
			minBucket, haveMin = bucket, true
		}
		applyUsageRow(&u, kind, op, status, uint64(n))
	}
	if err := rows.Err(); err != nil {
		return Usage{}, err
	}
	if haveMin {
		u.Since = time.Unix(minBucket, 0).UTC()
	}
	return u, nil
}

func applyUsageRow(u *Usage, kind, op string, status int, n uint64) {
	switch kind {
	case usageKindRate:
		u.RateLimited += n
	case usageKindConc:
		u.ConcurrencyLimited += n
	default:
		u.Requests += n
		if op != "" {
			u.ByOp[op] += n
		}
		if status != 0 {
			u.ByStatus[strconv.Itoa(status)] += n
		}
	}
}

type usageRow struct {
	Bucket int64
	Kind   string
	Op     string
	Status int
	Count  uint64
}

func (s *SQLiteStore) tenantUsageRows(ctx context.Context, tenantID string, from, to time.Time) ([]usageRow, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT bucket, kind, op, status, count
		FROM usage_buckets
		WHERE tenant_id = ? AND bucket >= ? AND bucket < ?`,
		tenantID, from.UTC().Unix(), to.UTC().Unix())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []usageRow
	for rows.Next() {
		var r usageRow
		var n int64
		if err := rows.Scan(&r.Bucket, &r.Kind, &r.Op, &r.Status, &n); err != nil {
			return nil, err
		}
		if n > 0 {
			r.Count = uint64(n)
			out = append(out, r)
		}
	}
	return out, rows.Err()
}
