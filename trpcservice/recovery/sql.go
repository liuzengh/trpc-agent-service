package recovery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// SQLStore is the production PostgreSQL recovery store.
type SQLStore struct {
	DB  *sql.DB
	Now func() time.Time
}

func (store *SQLStore) List(ctx context.Context, tenantID string, kind Kind, statuses []Status, limit int) ([]Item, error) {
	if err := store.validate(); err != nil {
		return nil, err
	}
	if tenantID == "" || limit < 1 || limit > 200 || len(statuses) == 0 {
		return nil, ErrInvalid
	}
	table, idColumn, allowed, err := tableFor(kind)
	if err != nil {
		return nil, err
	}
	args := []any{tenantID}
	marks := make([]string, 0, len(statuses))
	for _, status := range statuses {
		if !allowed[status] {
			return nil, ErrInvalid
		}
		args = append(args, status)
		marks = append(marks, fmt.Sprintf("$%d", len(args)))
	}
	args = append(args, limit)
	traceExpression := "COALESCE(trace_id,'')"
	if kind == KindOutbox {
		traceExpression = "COALESCE(payload_json->>'TraceID',payload_json->>'trace_id','')"
	}
	rows, err := store.DB.QueryContext(ctx, `SELECT tenant_id,`+idColumn+`,app_id,binding_id,status,attempts,`+traceExpression+`,created_at FROM `+table+` WHERE tenant_id=$1 AND status IN (`+strings.Join(marks, ",")+`) ORDER BY created_at,`+idColumn+` LIMIT $`+fmt.Sprint(len(args)), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]Item, 0)
	for rows.Next() {
		item := Item{Kind: kind}
		if err := rows.Scan(&item.TenantID, &item.ID, &item.AppID, &item.Binding, &item.Status, &item.Attempts, &item.TraceID, &item.Created); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (store *SQLStore) Redrive(ctx context.Context, tenantID string, kind Kind, id string, expected Status) (Item, error) {
	if err := store.validate(); err != nil {
		return Item{}, err
	}
	if tenantID == "" || id == "" || expected != StatusDLQ {
		return Item{}, ErrInvalid
	}
	now := store.now().UTC()
	var row *sql.Row
	switch kind {
	case KindInbox:
		row = store.DB.QueryRowContext(ctx, `UPDATE inbox_messages SET status='retry',attempts=0,next_attempt_at=$4,claim_owner=NULL,claim_token=NULL,lease_until=NULL,completed_at=NULL,last_error=NULL WHERE tenant_id=$1 AND inbox_id=$2 AND status=$3 RETURNING tenant_id,inbox_id,app_id,binding_id,status,attempts,COALESCE(trace_id,''),created_at`, tenantID, id, expected, now)
	case KindOutbox:
		row = store.DB.QueryRowContext(ctx, `UPDATE outbox_messages SET status='retry',attempts=0,retry_at=$4,claim_owner=NULL,claim_token=NULL,lease_until=NULL,completed_at=NULL,last_error=NULL WHERE tenant_id=$1 AND outbox_id=$2 AND status=$3 RETURNING tenant_id,outbox_id,app_id,binding_id,status,attempts,COALESCE(payload_json->>'TraceID',payload_json->>'trace_id',''),created_at`, tenantID, id, expected, now)
	default:
		return Item{}, ErrInvalid
	}
	return store.scanMutation(ctx, tenantID, kind, id, row)
}

func (store *SQLStore) ResolveOutbox(ctx context.Context, tenantID, id string, expected, decision Status) (Item, error) {
	if err := store.validate(); err != nil {
		return Item{}, err
	}
	if tenantID == "" || id == "" || expected != StatusUncertain || (decision != StatusSent && decision != StatusRetry) {
		return Item{}, ErrInvalid
	}
	now := store.now().UTC()
	var row *sql.Row
	if decision == StatusSent {
		row = store.DB.QueryRowContext(ctx, `UPDATE outbox_messages SET status='sent',sent_at=$4,completed_at=$4,claim_owner=NULL,claim_token=NULL,lease_until=NULL,retry_at=NULL,last_error=NULL WHERE tenant_id=$1 AND outbox_id=$2 AND status=$3 RETURNING tenant_id,outbox_id,app_id,binding_id,status,attempts,COALESCE(payload_json->>'TraceID',payload_json->>'trace_id',''),created_at`, tenantID, id, expected, now)
	} else {
		row = store.DB.QueryRowContext(ctx, `UPDATE outbox_messages SET status='retry',attempts=0,retry_at=$4,completed_at=NULL,claim_owner=NULL,claim_token=NULL,lease_until=NULL,last_error=NULL WHERE tenant_id=$1 AND outbox_id=$2 AND status=$3 RETURNING tenant_id,outbox_id,app_id,binding_id,status,attempts,COALESCE(payload_json->>'TraceID',payload_json->>'trace_id',''),created_at`, tenantID, id, expected, now)
	}
	return store.scanMutation(ctx, tenantID, KindOutbox, id, row)
}

func (store *SQLStore) scanMutation(ctx context.Context, tenantID string, kind Kind, id string, row *sql.Row) (Item, error) {
	item := Item{Kind: kind}
	if err := row.Scan(&item.TenantID, &item.ID, &item.AppID, &item.Binding, &item.Status, &item.Attempts, &item.TraceID, &item.Created); err == nil {
		return item, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return Item{}, err
	}
	table, idColumn, _, err := tableFor(kind)
	if err != nil {
		return Item{}, err
	}
	var exists bool
	if err := store.DB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM `+table+` WHERE tenant_id=$1 AND `+idColumn+`=$2)`, tenantID, id).Scan(&exists); err != nil {
		return Item{}, err
	}
	if !exists {
		return Item{}, ErrNotFound
	}
	return Item{}, ErrConflict
}

func tableFor(kind Kind) (string, string, map[Status]bool, error) {
	switch kind {
	case KindInbox:
		return "inbox_messages", "inbox_id", map[Status]bool{StatusDLQ: true}, nil
	case KindOutbox:
		return "outbox_messages", "outbox_id", map[Status]bool{StatusDLQ: true, StatusUncertain: true}, nil
	default:
		return "", "", nil, ErrInvalid
	}
}

func (store *SQLStore) validate() error {
	if store == nil || store.DB == nil {
		return errors.New("recovery: nil SQL database")
	}
	return nil
}

func (store *SQLStore) now() time.Time {
	if store.Now != nil {
		return store.Now()
	}
	return time.Now()
}

var _ Store = (*SQLStore)(nil)
