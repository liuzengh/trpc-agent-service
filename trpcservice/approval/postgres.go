package approval

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type PostgresRepository struct{ db *sql.DB }

func NewPostgresRepository(db *sql.DB) (*PostgresRepository, error) {
	if db == nil {
		return nil, fmt.Errorf("approval database is nil")
	}
	return &PostgresRepository{db: db}, nil
}

func (r *PostgresRepository) Request(ctx context.Context, request Request) (Record, error) {
	if err := validateRequest(&request); err != nil {
		return Record{}, err
	}
	id := StableID(request.TenantID, request.RequestID, request.ToolCallID)
	_, err := r.db.ExecContext(ctx, `
INSERT INTO tool_approval(
    approval_id, tenant_id, app_id, revision_id, channel_binding_id,
    request_id, message_id, user_id, session_id, tool_call_id, tool_name,
    arguments_hash, resume_text, reply_target, expires_at, origin_traceparent
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
ON CONFLICT (tenant_id, request_id, tool_call_id) DO NOTHING`,
		id, request.TenantID, request.AppID, request.RevisionID, request.ChannelBindingID,
		request.RequestID, request.MessageID, request.UserID, request.SessionID,
		request.ToolCallID, request.ToolName, request.ArgumentsHash, request.ResumeText,
		request.ReplyTarget, request.ExpiresAt, originTraceParent(ctx),
	)
	if err != nil {
		return Record{}, fmt.Errorf("insert tool approval: %w", err)
	}
	record, err := r.get(ctx, id, false)
	if err != nil {
		return Record{}, err
	}
	if record.ToolName != request.ToolName || record.ArgumentsHash != request.ArgumentsHash {
		return Record{}, ErrConflict
	}
	return record, nil
}

func (r *PostgresRepository) ListPendingByRequest(
	ctx context.Context,
	tenantID string,
	requestID string,
) ([]Record, error) {
	if _, err := r.db.ExecContext(ctx, `
UPDATE tool_approval SET status = 'expired'
WHERE tenant_id = $1 AND request_id = $2 AND status = 'pending' AND expires_at <= now()`, tenantID, requestID); err != nil {
		return nil, fmt.Errorf("expire tool approvals: %w", err)
	}
	rows, err := r.db.QueryContext(ctx, approvalSelect+`
WHERE tenant_id = $1 AND request_id = $2 AND status = 'pending'
ORDER BY created_at`, tenantID, requestID)
	if err != nil {
		return nil, fmt.Errorf("list pending tool approvals: %w", err)
	}
	defer rows.Close()
	result := make([]Record, 0)
	for rows.Next() {
		record, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tool approvals: %w", err)
	}
	return result, nil
}

func (r *PostgresRepository) Decide(ctx context.Context, decision Decision) (Record, error) {
	if !validDecisionStatus(decision.Status) || decision.ExternalMessageID == "" {
		return Record{}, errors.New("approval decision is invalid")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return Record{}, fmt.Errorf("begin approval decision: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	record, err := getWithQueryer(ctx, tx, decision.ApprovalID, true)
	if err != nil {
		return Record{}, err
	}
	if record.TenantID != decision.TenantID || record.ChannelBindingID != decision.ChannelBindingID ||
		record.UserID != decision.UserID || record.SessionID != decision.SessionID {
		return Record{}, ErrForbidden
	}
	if record.Status == StatusPending && !record.ExpiresAt.After(time.Now()) {
		if _, err := tx.ExecContext(ctx, `UPDATE tool_approval SET status='expired' WHERE approval_id=$1`, record.ApprovalID); err != nil {
			return Record{}, fmt.Errorf("expire tool approval: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return Record{}, fmt.Errorf("commit approval expiry: %w", err)
		}
		return Record{}, ErrExpired
	}
	if record.Status != StatusPending {
		if record.Status == StatusExpired {
			return Record{}, ErrExpired
		}
		if record.Status != decision.Status {
			return Record{}, ErrConflict
		}
		return record, nil
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE tool_approval
SET status=$2, decision_message_id=$3, decision_reason=$4, decided_at=now()
WHERE approval_id=$1 AND status='pending'`, record.ApprovalID, decision.Status,
		decision.ExternalMessageID, decision.Reason); err != nil {
		var constraintErr *pgconn.PgError
		if errors.As(err, &constraintErr) && constraintErr.Code == "23505" {
			return Record{}, ErrConflict
		}
		return Record{}, fmt.Errorf("update tool approval decision: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Record{}, fmt.Errorf("commit approval decision: %w", err)
	}
	return r.get(ctx, record.ApprovalID, false)
}

func (r *PostgresRepository) ListPendingBySession(ctx context.Context, tenantID, bindingID, userID, sessionID string) ([]Record, error) {
	rows, err := r.db.QueryContext(ctx, approvalSelect+`
WHERE tenant_id=$1 AND channel_binding_id=$2 AND user_id=$3 AND session_id=$4
    AND status='pending' AND expires_at > now() ORDER BY created_at LIMIT 10`, tenantID, bindingID, userID, sessionID)
	if err != nil {
		return nil, fmt.Errorf("list session approvals: %w", err)
	}
	defer rows.Close()
	result := make([]Record, 0)
	for rows.Next() {
		record, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	return result, rows.Err()
}

func (r *PostgresRepository) MarkResumed(ctx context.Context, approvalID string) error {
	result, err := r.db.ExecContext(ctx, `
UPDATE tool_approval SET resumed_at=COALESCE(resumed_at, now())
WHERE approval_id=$1 AND status IN ('approved','denied')`, approvalID)
	if err != nil {
		return fmt.Errorf("mark tool approval resumed: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		return ErrNotFound
	}
	return nil
}

func (r *PostgresRepository) Ready(ctx context.Context) error { return r.db.PingContext(ctx) }
func (r *PostgresRepository) Close() error                    { return nil }

func (r *PostgresRepository) get(ctx context.Context, id string, lock bool) (Record, error) {
	return getWithQueryer(ctx, r.db, id, lock)
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

const approvalSelect = `SELECT approval_id, tenant_id, app_id, revision_id,
channel_binding_id, request_id, message_id, user_id, session_id, tool_call_id,
tool_name, arguments_hash, resume_text, reply_target, status,
COALESCE(decision_message_id,''), COALESCE(decision_reason,''), expires_at,
created_at, decided_at, resumed_at, origin_traceparent FROM tool_approval `

func getWithQueryer(ctx context.Context, q queryer, id string, lock bool) (Record, error) {
	suffix := `WHERE approval_id=$1`
	if lock {
		suffix += ` FOR UPDATE`
	}
	return scanRecord(q.QueryRowContext(ctx, approvalSelect+suffix, id))
}

type scanner interface{ Scan(...any) error }

func scanRecord(row scanner) (Record, error) {
	var record Record
	var decidedAt, resumedAt sql.NullTime
	if err := row.Scan(
		&record.ApprovalID, &record.TenantID, &record.AppID, &record.RevisionID,
		&record.ChannelBindingID, &record.RequestID, &record.MessageID, &record.UserID,
		&record.SessionID, &record.ToolCallID, &record.ToolName, &record.ArgumentsHash,
		&record.ResumeText, &record.ReplyTarget, &record.Status, &record.DecisionMessageID,
		&record.DecisionReason, &record.ExpiresAt, &record.CreatedAt, &decidedAt, &resumedAt, &record.OriginTraceParent,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Record{}, ErrNotFound
		}
		return Record{}, fmt.Errorf("scan tool approval: %w", err)
	}
	if decidedAt.Valid {
		record.DecidedAt = decidedAt.Time
	}
	if resumedAt.Valid {
		record.ResumedAt = resumedAt.Time
	}
	return record, nil
}

var _ Repository = (*PostgresRepository)(nil)
