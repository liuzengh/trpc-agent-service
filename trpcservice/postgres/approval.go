package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	platformapproval "github.com/liuzengh/trpc-agent-service/trpcservice/approval"
	platformaudit "github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
)

var _ platformapproval.Repository = (*Store)(nil)

// ResolveOrCreate returns the latest decision for one exact tool-call context,
// creating one pending row when no decision exists. Only the argument digest
// is persisted; raw arguments never cross this repository boundary.
func (s *Store) ResolveOrCreate(ctx context.Context, request platformapproval.Request) (platformapproval.Record, error) {
	if err := s.validate(); err != nil {
		return platformapproval.Record{}, err
	}
	if request.ExpiresAt.IsZero() {
		request.ExpiresAt = time.Now().UTC().Add(platformapproval.DefaultTTL)
	}
	if err := request.Validate(); err != nil {
		return platformapproval.Record{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return platformapproval.Record{}, fmt.Errorf("begin tool approval resolution: %w", err)
	}
	defer func() { rollback(tx) }()

	record, found, err := findApprovalTx(ctx, tx, request)
	if err != nil {
		return platformapproval.Record{}, err
	}
	if found {
		var expired bool
		if err := tx.QueryRow(ctx, `
SELECT expires_at <= clock_timestamp()
FROM platform.tool_approval
WHERE tenant_id = $1 AND app_id = $2 AND approval_id = $3
FOR UPDATE`, record.TenantID, record.AppID, record.ApprovalID).Scan(&expired); err != nil {
			return platformapproval.Record{}, fmt.Errorf("check tool approval expiry: %w", err)
		}
		if expired && (record.Status == platformapproval.StatusPending || record.Status == platformapproval.StatusApproved) {
			var decidedAt time.Time
			if err := tx.QueryRow(ctx, `
UPDATE platform.tool_approval
SET status = 'EXPIRED', decided_at = COALESCE(decided_at, clock_timestamp())
WHERE tenant_id = $1 AND app_id = $2 AND approval_id = $3
  AND status IN ('PENDING', 'APPROVED')
RETURNING decided_at`, record.TenantID, record.AppID, record.ApprovalID).Scan(&decidedAt); err != nil {
				return platformapproval.Record{}, fmt.Errorf("expire tool approval: %w", err)
			}
			record.Status = platformapproval.StatusExpired
			record.DecidedAt = &decidedAt
			if err := s.recordApprovalAuditTx(ctx, tx, record, platformaudit.ApprovalExpired); err != nil {
				return platformapproval.Record{}, err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return platformapproval.Record{}, fmt.Errorf("commit tool approval resolution: %w", err)
		}
		return record, nil
	}
	commandTag, err := tx.Exec(ctx, `
INSERT INTO platform.tool_approval (
    approval_id, tenant_id, app_id, config_version, request_id, session_id,
    tool_name, tool_call_id, argument_digest, status, expires_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'PENDING',$10)
ON CONFLICT DO NOTHING`,
		uuid.NewString(), request.TenantID, request.AppID, request.ConfigVersion,
		request.RequestID, request.SessionID, request.ToolName, request.ToolCallID,
		request.ArgumentDigest, request.ExpiresAt.UTC())
	if err != nil {
		return platformapproval.Record{}, fmt.Errorf("create tool approval: %w", err)
	}
	record, found, err = findApprovalTx(ctx, tx, request)
	if err != nil {
		return platformapproval.Record{}, err
	}
	if !found {
		return platformapproval.Record{}, errors.New("created tool approval could not be read")
	}
	if commandTag.RowsAffected() == 1 && record.Status == platformapproval.StatusPending {
		if err := s.recordApprovalAuditTx(ctx, tx, record, platformaudit.ApprovalCreated); err != nil {
			return platformapproval.Record{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return platformapproval.Record{}, fmt.Errorf("commit tool approval creation: %w", err)
	}
	return record, nil
}

type approvalRowScanner interface {
	Scan(...any) error
}

func scanApprovalRecord(row approvalRowScanner) (platformapproval.Record, error) {
	var record platformapproval.Record
	var decidedAt *time.Time
	err := row.Scan(
		&record.ApprovalID, &record.TenantID, &record.AppID, &record.ConfigVersion,
		&record.RequestID, &record.SessionID, &record.ToolName, &record.ToolCallID,
		&record.ArgumentDigest, &record.Status, &record.ExpiresAt, &record.CreatedAt,
		&decidedAt,
	)
	if err != nil {
		return platformapproval.Record{}, err
	}
	record.DecidedAt = decidedAt
	if err := record.Validate(); err != nil {
		return platformapproval.Record{}, fmt.Errorf("stored tool approval: %w", err)
	}
	return record, nil
}

func findApprovalTx(ctx context.Context, tx pgx.Tx, request platformapproval.Request) (platformapproval.Record, bool, error) {
	record, err := scanApprovalRecord(tx.QueryRow(ctx, `
SELECT approval_id, tenant_id, app_id, config_version, request_id, session_id,
       tool_name, tool_call_id, argument_digest, status, expires_at, created_at,
       decided_at
FROM platform.tool_approval
WHERE tenant_id = $1 AND app_id = $2 AND config_version = $3
  AND request_id = $4 AND session_id = $5 AND tool_name = $6
  AND tool_call_id = $7 AND argument_digest = $8
ORDER BY created_at DESC, approval_id DESC
LIMIT 1
FOR UPDATE`,
		request.TenantID, request.AppID, request.ConfigVersion, request.RequestID,
		request.SessionID, request.ToolName, request.ToolCallID, request.ArgumentDigest))
	if errors.Is(err, pgx.ErrNoRows) {
		return platformapproval.Record{}, false, nil
	}
	if err != nil {
		return platformapproval.Record{}, false, fmt.Errorf("find tool approval: %w", err)
	}
	return record, true, nil
}

// List returns metadata-only approvals in one exact tenant/application scope.
func (s *Store) List(ctx context.Context, query platformapproval.Query) ([]platformapproval.Record, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if err := query.Validate(); err != nil {
		return nil, err
	}
	limit := query.Limit
	if limit == 0 {
		limit = 100
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin tool approval listing: %w", err)
	}
	defer func() { rollback(tx) }()
	expiredRows, err := tx.Query(ctx, `
UPDATE platform.tool_approval
SET status = 'EXPIRED', decided_at = COALESCE(decided_at, clock_timestamp())
WHERE tenant_id = $1 AND app_id = $2
  AND status IN ('PENDING', 'APPROVED')
  AND expires_at <= clock_timestamp()
RETURNING approval_id, tenant_id, app_id, config_version, request_id, session_id,
          tool_name, tool_call_id, argument_digest, status, expires_at, created_at,
          decided_at`, query.TenantID, query.AppID)
	if err != nil {
		return nil, fmt.Errorf("expire listed tool approvals: %w", err)
	}
	var expiredRecords []platformapproval.Record
	for expiredRows.Next() {
		record, scanErr := scanApprovalRecord(expiredRows)
		if scanErr != nil {
			expiredRows.Close()
			return nil, fmt.Errorf("scan expired tool approval: %w", scanErr)
		}
		expiredRecords = append(expiredRecords, record)
	}
	if err := expiredRows.Err(); err != nil {
		expiredRows.Close()
		return nil, fmt.Errorf("iterate expired tool approvals: %w", err)
	}
	expiredRows.Close()
	for _, record := range expiredRecords {
		if err := s.recordApprovalAuditTx(ctx, tx, record, platformaudit.ApprovalExpired); err != nil {
			return nil, err
		}
	}
	args := []any{query.TenantID, query.AppID}
	statusClause := ""
	if query.Status != "" {
		args = append(args, query.Status)
		statusClause = " AND status = $3"
	}
	args = append(args, limit)
	limitArg := len(args)
	rows, err := tx.Query(ctx, fmt.Sprintf(`
SELECT approval_id, tenant_id, app_id, config_version, request_id, session_id,
       tool_name, tool_call_id, argument_digest, status, expires_at, created_at,
       decided_at
FROM platform.tool_approval
WHERE tenant_id = $1 AND app_id = $2%s
ORDER BY created_at DESC, approval_id DESC
LIMIT $%d`, statusClause, limitArg), args...)
	if err != nil {
		return nil, fmt.Errorf("list tool approvals: %w", err)
	}
	defer rows.Close()
	result := make([]platformapproval.Record, 0)
	for rows.Next() {
		var record platformapproval.Record
		var decidedAt *time.Time
		if err := rows.Scan(
			&record.ApprovalID, &record.TenantID, &record.AppID, &record.ConfigVersion,
			&record.RequestID, &record.SessionID, &record.ToolName, &record.ToolCallID,
			&record.ArgumentDigest, &record.Status, &record.ExpiresAt, &record.CreatedAt,
			&decidedAt,
		); err != nil {
			return nil, fmt.Errorf("scan tool approval: %w", err)
		}
		record.DecidedAt = decidedAt
		if err := record.Validate(); err != nil {
			return nil, fmt.Errorf("stored tool approval: %w", err)
		}
		result = append(result, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tool approvals: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tool approval listing: %w", err)
	}
	return result, nil
}

// Decide applies one approval decision exactly once in tenant/application
// scope. Repeating the same decision is idempotent; a different decision is
// rejected and cannot overwrite the first one.
func (s *Store) Decide(ctx context.Context, tenantID, appID, approvalID string, status platformapproval.Status) (platformapproval.Record, error) {
	if err := s.validate(); err != nil {
		return platformapproval.Record{}, err
	}
	if tenantID == "" || appID == "" || approvalID == "" {
		return platformapproval.Record{}, errors.New("tenant_id, app_id, and approval_id are required")
	}
	if status != platformapproval.StatusApproved && status != platformapproval.StatusDenied {
		return platformapproval.Record{}, errors.New("approval decision must be approved or denied")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return platformapproval.Record{}, fmt.Errorf("begin tool approval decision: %w", err)
	}
	defer func() { rollback(tx) }()
	var record platformapproval.Record
	var decidedAt *time.Time
	err = tx.QueryRow(ctx, `
UPDATE platform.tool_approval
SET status = $4, decided_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND approval_id = $3
  AND status = 'PENDING' AND expires_at > clock_timestamp()
RETURNING approval_id, tenant_id, app_id, config_version, request_id, session_id,
          tool_name, tool_call_id, argument_digest, status, expires_at, created_at,
          decided_at`, tenantID, appID, approvalID, status).Scan(
		&record.ApprovalID, &record.TenantID, &record.AppID, &record.ConfigVersion,
		&record.RequestID, &record.SessionID, &record.ToolName, &record.ToolCallID,
		&record.ArgumentDigest, &record.Status, &record.ExpiresAt, &record.CreatedAt,
		&decidedAt,
	)
	if err == nil {
		record.DecidedAt = decidedAt
		if err := record.Validate(); err != nil {
			return platformapproval.Record{}, fmt.Errorf("stored tool approval: %w", err)
		}
		if err := s.recordApprovalAuditTx(ctx, tx, record, approvalEventType(record.Status)); err != nil {
			return platformapproval.Record{}, err
		}
		if err := resumeApprovalExecution(ctx, tx, record); err != nil {
			return platformapproval.Record{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return platformapproval.Record{}, fmt.Errorf("commit tool approval decision: %w", err)
		}
		return record, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return platformapproval.Record{}, fmt.Errorf("decide tool approval: %w", err)
	}
	var current platformapproval.Record
	var currentDecidedAt *time.Time
	var expired bool
	err = tx.QueryRow(ctx, `
SELECT approval_id, tenant_id, app_id, config_version, request_id, session_id,
       tool_name, tool_call_id, argument_digest, status, expires_at, created_at,
       decided_at, expires_at <= clock_timestamp()
FROM platform.tool_approval
WHERE tenant_id = $1 AND app_id = $2 AND approval_id = $3
FOR UPDATE`, tenantID, appID, approvalID).Scan(
		&current.ApprovalID, &current.TenantID, &current.AppID, &current.ConfigVersion,
		&current.RequestID, &current.SessionID, &current.ToolName, &current.ToolCallID,
		&current.ArgumentDigest, &current.Status, &current.ExpiresAt, &current.CreatedAt,
		&currentDecidedAt, &expired,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return platformapproval.Record{}, platformapproval.ErrNotFound
	}
	if err != nil {
		return platformapproval.Record{}, fmt.Errorf("find tool approval by id: %w", err)
	}
	current.DecidedAt = currentDecidedAt
	if err := current.Validate(); err != nil {
		return platformapproval.Record{}, fmt.Errorf("stored tool approval: %w", err)
	}
	if current.Status == platformapproval.StatusPending && expired {
		var expiryTime time.Time
		err = tx.QueryRow(ctx, `
UPDATE platform.tool_approval
SET status = 'EXPIRED', decided_at = COALESCE(decided_at, clock_timestamp())
WHERE tenant_id = $1 AND app_id = $2 AND approval_id = $3 AND status = 'PENDING'
RETURNING decided_at`, tenantID, appID, approvalID).Scan(&expiryTime)
		if err != nil {
			return platformapproval.Record{}, fmt.Errorf("expire tool approval: %w", err)
		}
		current.Status = platformapproval.StatusExpired
		current.DecidedAt = &expiryTime
		if err := s.recordApprovalAuditTx(ctx, tx, current, platformaudit.ApprovalExpired); err != nil {
			return platformapproval.Record{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return platformapproval.Record{}, fmt.Errorf("commit expired tool approval: %w", err)
		}
		return current, platformapproval.ErrExpired
	}
	if current.Status == platformapproval.StatusExpired {
		return current, platformapproval.ErrExpired
	}
	if current.Status == status {
		if err := resumeApprovalExecution(ctx, tx, current); err != nil {
			return platformapproval.Record{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return platformapproval.Record{}, fmt.Errorf("commit repeated tool approval decision: %w", err)
		}
		return current, nil
	}
	return current, platformapproval.ErrAlreadyDecided
}

func resumeApprovalExecution(ctx context.Context, tx pgx.Tx, record platformapproval.Record) error {
	if record.Status != platformapproval.StatusApproved && record.Status != platformapproval.StatusDenied {
		return nil
	}
	if _, err := tx.Exec(ctx, `UPDATE platform.execution
SET status='PENDING', last_error='', lease_owner=NULL, run_token=NULL,
    lease_until=NULL, next_attempt_at=clock_timestamp(), updated_at=clock_timestamp()
WHERE tenant_id=$1 AND app_id=$2 AND request_id=$3 AND status='WAITING_APPROVAL'`,
		record.TenantID, record.AppID, record.RequestID); err != nil {
		return fmt.Errorf("resume approval execution: %w", err)
	}
	return ensureApprovalDispatch(ctx, tx, record.TenantID, record.AppID, record.RequestID)
}

func requeueApprovalExecution(ctx context.Context, tx pgx.Tx, tenantID, appID, requestID string, lease queue.Lease) error {
	tag, err := tx.Exec(ctx, `UPDATE platform.execution
SET status='PENDING', last_error='', lease_owner=NULL, run_token=NULL,
    lease_until=NULL, next_attempt_at=clock_timestamp(), updated_at=clock_timestamp()
WHERE tenant_id=$1 AND app_id=$2 AND request_id=$3 AND status='RUNNING'
  AND lease_owner=$4 AND run_token=$5 AND lease_until > clock_timestamp()`,
		tenantID, appID, requestID, lease.Owner, lease.Token)
	if err != nil {
		return fmt.Errorf("requeue approval execution: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("requeue approval execution: %w", queue.ErrLeaseLost)
	}
	return ensureApprovalDispatch(ctx, tx, tenantID, appID, requestID)
}

func ensureApprovalDispatch(ctx context.Context, tx pgx.Tx, tenantID, appID, requestID string) error {
	if _, err := tx.Exec(ctx, `
INSERT INTO platform.dispatch_outbox (tenant_id, app_id, request_id, next_attempt_at)
SELECT e.tenant_id, e.app_id, e.request_id, clock_timestamp()
FROM platform.execution e
WHERE e.tenant_id=$1 AND e.app_id=$2 AND e.request_id=$3 AND e.status='PENDING'
  AND NOT EXISTS (
      SELECT 1 FROM platform.dispatch_outbox o
      WHERE o.tenant_id=e.tenant_id AND o.app_id=e.app_id
        AND o.request_id=e.request_id AND o.status IN ('PENDING','PUBLISHING','SENT')
  )`, tenantID, appID, requestID); err != nil {
		return fmt.Errorf("enqueue approval execution: %w", err)
	}
	return nil
}

func approvalEventType(status platformapproval.Status) string {
	switch status {
	case platformapproval.StatusApproved:
		return platformaudit.ApprovalApproved
	case platformapproval.StatusDenied:
		return platformaudit.ApprovalDenied
	case platformapproval.StatusExpired:
		return platformaudit.ApprovalExpired
	default:
		return platformaudit.ApprovalCreated
	}
}

func (s *Store) recordApprovalAuditTx(ctx context.Context, tx pgx.Tx, record platformapproval.Record, eventType string) error {
	var encoded []byte
	if err := tx.QueryRow(ctx, `
SELECT audit_policy
FROM platform.app_config_version
WHERE tenant_id = $1 AND app_id = $2 AND version = $3`,
		record.TenantID, record.AppID, record.ConfigVersion).Scan(&encoded); err != nil {
		return fmt.Errorf("read approval audit policy: %w", err)
	}
	policy, err := unmarshalAuditPolicy(encoded)
	if err != nil {
		return fmt.Errorf("decode approval audit policy: %w", err)
	}
	if !policy.Enabled || !policy.RecordToolDecisions {
		return nil
	}
	var traceID string
	err = tx.QueryRow(ctx, `
SELECT COALESCE(trace_id, '')
FROM platform.execution
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3`,
		record.TenantID, record.AppID, record.RequestID).Scan(&traceID)
	if errors.Is(err, pgx.ErrNoRows) {
		traceID = ""
	} else if err != nil {
		return fmt.Errorf("read approval execution trace: %w", err)
	}
	event := platformaudit.Event{
		TenantID:      record.TenantID,
		AppID:         record.AppID,
		SessionID:     record.SessionID,
		ToolName:      record.ToolName,
		Decision:      string(record.Status),
		TraceID:       traceID,
		RequestID:     record.RequestID,
		ConfigVersion: record.ConfigVersion,
		EventType:     eventType,
	}
	if policy.RedactPII {
		event = platformaudit.RedactEvent(event)
	}
	if err := insertAuditEventTx(ctx, tx, event); err != nil {
		return fmt.Errorf("record approval audit: %w", err)
	}
	return nil
}
