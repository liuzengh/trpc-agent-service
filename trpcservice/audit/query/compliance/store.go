package compliance

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit/query"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// Store is the read-only audit query implementation over the independent
// compliance database.
type Store struct{ DB *sql.DB }

func New(db *sql.DB) *Store { return &Store{DB: db} }

// Query reads events with keyset pagination on (occurred_at DESC, audit_id DESC).
func (s *Store) Query(ctx context.Context, filter query.Filter) (query.Page, error) {
	if s == nil || s.DB == nil {
		return query.Page{}, runtime.ErrInvariantViolation
	}
	limit := filter.PageSize + 1
	cursorOccurred, cursorTenant, cursorAudit, err := decodeCursor(filter.Cursor)
	if err != nil {
		return query.Page{}, err
	}
	// Cross-tenant queries pass an empty tenant and match every tenant; the
	// row-level tenant re-check below is skipped for cross-tenant results.
	tenant := filter.TenantID
	if filter.CrossTenant {
		tenant = ""
	}
	rows, err := s.DB.QueryContext(ctx, `SELECT event_json FROM compliance.audit_event
	WHERE ($1::text = '' OR tenant_id=$1) AND occurred_at>=$2 AND occurred_at<$3
	  AND ($4::timestamptz IS NULL OR (occurred_at,tenant_id,audit_id) < ($4::timestamptz,$5::text,$6::text))
	ORDER BY occurred_at DESC, tenant_id DESC, audit_id DESC LIMIT $7`,
		tenant, filter.From.UTC(), filter.To.UTC(), cursorOccurred, cursorTenant, cursorAudit, limit)
	if err != nil {
		return query.Page{}, err
	}
	defer rows.Close()
	events := make([]audit.Event, 0, filter.PageSize)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return query.Page{}, err
		}
		var event audit.Event
		if err := json.Unmarshal([]byte(raw), &event); err != nil {
			return query.Page{}, runtime.ErrInvalidEnvelope
		}
		if !filter.CrossTenant && event.TenantID != filter.TenantID {
			return query.Page{}, runtime.ErrTenantScope
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return query.Page{}, err
	}
	hasMore := len(events) > filter.PageSize
	if hasMore {
		events = events[:filter.PageSize]
	}
	page := query.Page{Events: events}
	if hasMore && len(events) > 0 {
		last := events[len(events)-1]
		page.NextCursor = encodeCursor(last.OccurredAt, last.TenantID, last.AuditID)
	}
	return page, nil
}

// RecordAccess writes the immutable secondary-audit fact for a query.
func (s *Store) RecordAccess(ctx context.Context, record query.QueryRecord) error {
	if s == nil || s.DB == nil {
		return runtime.ErrInvariantViolation
	}
	_, err := s.DB.ExecContext(ctx, `INSERT INTO compliance.audit_query_record(
query_id,tenant_id,subject,cross_tenant,from_occurred_at,to_occurred_at,filter_digest,
result_count,result_digest,decision,reason_code,trace_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		record.QueryID, record.TenantID, record.Subject, record.CrossTenant,
		record.From.UTC(), record.To.UTC(), record.FilterDigest, record.ResultCount,
		record.ResultDigest, record.Decision, record.ReasonCode, record.TraceID)
	return err
}

func encodeCursor(occurred time.Time, tenantID, auditID string) string {
	payload := occurred.UTC().Format(time.RFC3339Nano) + "\n" + tenantID + "\n" + auditID
	return base64.RawURLEncoding.EncodeToString([]byte(payload))
}

func decodeCursor(cursor string) (any, string, string, error) {
	if cursor == "" {
		return nil, "", "", nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, "", "", query.ErrInvalidFilter
	}
	parts := strings.SplitN(string(payload), "\n", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return nil, "", "", query.ErrInvalidFilter
	}
	occurred, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return nil, "", "", query.ErrInvalidFilter
	}
	return occurred, parts[1], parts[2], nil
}

var _ query.Store = (*Store)(nil)
