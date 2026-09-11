package governance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/dbscope"
)

var ErrApprovalRecordNotFound = errors.New("approval record not found")

const approvalRedisRecoveryGrace = time.Second

type ApprovalStatus string

const (
	ApprovalPending  ApprovalStatus = "pending"
	ApprovalApproved ApprovalStatus = "approved"
	ApprovalRejected ApprovalStatus = "rejected"
	ApprovalExpired  ApprovalStatus = "expired"
	ApprovalCanceled ApprovalStatus = "canceled"
)

type ApprovalRecord struct {
	Token             string
	TenantID          string
	AppCode           string
	ConfigVersion     uint64
	RequestID         string
	TraceID           string
	Channel           string
	BindingID         string
	ConversationID    string
	ConversationScope string
	ExternalUserID    string
	RequesterUserID   string
	ProgressMessageID string
	ToolName          string
	ToolDescription   string
	Status            ApprovalStatus
	NotificationID    string
	ResolvedBy        string
	CreatedAt         time.Time
	ExpiresAt         time.Time
	NotifiedAt        *time.Time
	ResolvedAt        *time.Time
}

type ApprovalStore interface {
	Create(context.Context, ApprovalRecord) error
	Get(context.Context, string, string) (ApprovalRecord, error)
	// ListPending returns active, not-yet-notified approvals for the platform
	// Channel dispatcher. The PostgreSQL implementation intentionally uses the
	// platform control-plane connection rather than a tenant-scoped role.
	ListPending(context.Context, int) ([]ApprovalRecord, error)
	MarkNotified(context.Context, string, string, string) error
	Resolve(context.Context, string, string, bool, string) error
	Close(context.Context, string, string, ApprovalStatus) error
}

type PostgresApprovalStore struct{ database *sql.DB }

func NewPostgresApprovalStore(database *sql.DB) (*PostgresApprovalStore, error) {
	if database == nil {
		return nil, errors.New("approval store database is required")
	}
	return &PostgresApprovalStore{database: database}, nil
}

func (s *PostgresApprovalStore) Create(ctx context.Context, record ApprovalRecord) error {
	if err := validateApprovalRecord(record); err != nil {
		return err
	}
	tx, err := dbscope.BeginTenantTransaction(ctx, s.database, record.TenantID)
	if err != nil {
		return fmt.Errorf("begin approval creation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, `
INSERT INTO tool_approvals (
    approval_token, tenant_id, app_code, config_version, request_id, trace_id,
    channel_type, binding_id, conversation_id, conversation_scope, external_user_id,
    requester_user_id, progress_message_id, tool_name, tool_description, status, created_at, expires_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,'pending',$16,$17)`,
		record.Token, record.TenantID, record.AppCode, record.ConfigVersion, strings.TrimSpace(record.RequestID), strings.TrimSpace(record.TraceID),
		record.Channel, record.BindingID, record.ConversationID, strings.TrimSpace(record.ConversationScope), record.ExternalUserID,
		strings.TrimSpace(record.RequesterUserID), strings.TrimSpace(record.ProgressMessageID), record.ToolName, strings.TrimSpace(record.ToolDescription), record.CreatedAt.UTC(), record.ExpiresAt.UTC(),
	)
	if err != nil {
		return fmt.Errorf("create approval record: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit approval creation: %w", err)
	}
	return nil
}

func (s *PostgresApprovalStore) Get(ctx context.Context, tenantID, token string) (ApprovalRecord, error) {
	tenantID, token = strings.TrimSpace(tenantID), strings.TrimSpace(token)
	if tenantID == "" || token == "" {
		return ApprovalRecord{}, errors.New("approval tenant and token are required")
	}
	tx, err := dbscope.BeginTenantTransaction(ctx, s.database, tenantID)
	if err != nil {
		return ApprovalRecord{}, err
	}
	defer func() { _ = tx.Rollback() }()
	record, err := scanApprovalRecord(tx.QueryRowContext(ctx, `
SELECT approval_token, tenant_id, app_code, config_version, request_id, trace_id,
       channel_type, binding_id, conversation_id, conversation_scope, external_user_id,
       requester_user_id, progress_message_id, tool_name, tool_description, status, notification_id, resolved_by,
       created_at, expires_at, notified_at, resolved_at
FROM tool_approvals WHERE tenant_id=$1 AND approval_token=$2`, tenantID, token))
	if errors.Is(err, sql.ErrNoRows) {
		return ApprovalRecord{}, ErrApprovalRecordNotFound
	}
	if err != nil {
		return ApprovalRecord{}, fmt.Errorf("read approval record: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ApprovalRecord{}, err
	}
	return record, nil
}

func (s *PostgresApprovalStore) ListPending(ctx context.Context, limit int) ([]ApprovalRecord, error) {
	if limit <= 0 || limit > 1000 {
		return nil, errors.New("approval pending limit must be between 1 and 1000")
	}
	rows, err := s.database.QueryContext(ctx, `
SELECT approval_token, tenant_id, app_code, config_version, request_id, trace_id,
       channel_type, binding_id, conversation_id, conversation_scope, external_user_id,
       requester_user_id, progress_message_id, tool_name, tool_description, status, notification_id, resolved_by,
       created_at, expires_at, notified_at, resolved_at
FROM tool_approvals
WHERE status='pending' AND notification_id='' AND expires_at > NOW()
  AND created_at < NOW() - INTERVAL '1 second'
ORDER BY created_at, approval_token
LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list durable pending approvals: %w", err)
	}
	defer rows.Close()
	result := make([]ApprovalRecord, 0, limit)
	for rows.Next() {
		record, err := scanApprovalRecord(rows)
		if err != nil {
			return nil, fmt.Errorf("scan durable pending approval: %w", err)
		}
		result = append(result, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate durable pending approvals: %w", err)
	}
	return result, nil
}

func (s *PostgresApprovalStore) MarkNotified(ctx context.Context, tenantID, token, notificationID string) error {
	tenantID, token, notificationID = strings.TrimSpace(tenantID), strings.TrimSpace(token), strings.TrimSpace(notificationID)
	if tenantID == "" || token == "" || notificationID == "" {
		return errors.New("approval tenant, token, and notification ID are required")
	}
	tx, err := dbscope.BeginTenantTransaction(ctx, s.database, tenantID)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `
UPDATE tool_approvals
SET notification_id=$3, notified_at=COALESCE(notified_at,NOW()), updated_at=NOW()
WHERE tenant_id=$1 AND approval_token=$2 AND status='pending'
  AND expires_at > NOW()
  AND (notification_id='' OR notification_id=$3)`, tenantID, token, notificationID)
	if err != nil {
		return fmt.Errorf("mark approval notified: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		record, readErr := scanApprovalRecord(tx.QueryRowContext(ctx, `
SELECT approval_token, tenant_id, app_code, config_version, request_id, trace_id,
       channel_type, binding_id, conversation_id, conversation_scope, external_user_id,
       requester_user_id, progress_message_id, tool_name, tool_description, status, notification_id, resolved_by,
       created_at, expires_at, notified_at, resolved_at
FROM tool_approvals WHERE tenant_id=$1 AND approval_token=$2 FOR UPDATE`, tenantID, token))
		if errors.Is(readErr, sql.ErrNoRows) {
			return ErrApprovalRecordNotFound
		}
		if readErr != nil {
			return readErr
		}
		if record.Status != ApprovalPending {
			return ErrApprovalResolved
		}
		if !record.ExpiresAt.After(time.Now().UTC()) {
			return ErrApprovalExpired
		}
		if record.NotificationID != notificationID {
			return errors.New("approval notification ID conflict")
		}
	}
	return tx.Commit()
}

func (s *PostgresApprovalStore) Resolve(ctx context.Context, tenantID, token string, approved bool, resolvedBy string) error {
	tenantID, token, resolvedBy = strings.TrimSpace(tenantID), strings.TrimSpace(token), strings.TrimSpace(resolvedBy)
	if tenantID == "" || token == "" || resolvedBy == "" {
		return errors.New("approval tenant, token, and resolver are required")
	}
	status := ApprovalRejected
	if approved {
		status = ApprovalApproved
	}
	tx, err := dbscope.BeginTenantTransaction(ctx, s.database, tenantID)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `
UPDATE tool_approvals
SET status=$3, resolved_by=$4, resolved_at=NOW(), updated_at=NOW()
WHERE tenant_id=$1 AND approval_token=$2 AND status='pending' AND expires_at > NOW()`,
		tenantID, token, string(status), resolvedBy)
	if err != nil {
		return fmt.Errorf("resolve approval record: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		record, readErr := scanApprovalRecord(tx.QueryRowContext(ctx, `
SELECT approval_token, tenant_id, app_code, config_version, request_id, trace_id,
       channel_type, binding_id, conversation_id, conversation_scope, external_user_id,
       requester_user_id, progress_message_id, tool_name, tool_description, status, notification_id, resolved_by,
       created_at, expires_at, notified_at, resolved_at
FROM tool_approvals WHERE tenant_id=$1 AND approval_token=$2 FOR UPDATE`, tenantID, token))
		if errors.Is(readErr, sql.ErrNoRows) {
			return ErrApprovalRecordNotFound
		}
		if readErr != nil {
			return readErr
		}
		if !record.ExpiresAt.After(time.Now().UTC()) && record.Status == ApprovalPending {
			return ErrApprovalExpired
		}
		if record.Status == status && record.ResolvedBy == resolvedBy {
			return nil
		}
		return ErrApprovalResolved
	}
	return tx.Commit()
}

func (s *PostgresApprovalStore) Close(ctx context.Context, tenantID, token string, status ApprovalStatus) error {
	if status != ApprovalExpired && status != ApprovalCanceled {
		return errors.New("approval close status must be expired or canceled")
	}
	tenantID, token = strings.TrimSpace(tenantID), strings.TrimSpace(token)
	if tenantID == "" || token == "" {
		return errors.New("approval tenant and token are required")
	}
	tx, err := dbscope.BeginTenantTransaction(ctx, s.database, tenantID)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, `
UPDATE tool_approvals
SET status=$3, resolved_at=COALESCE(resolved_at,NOW()), updated_at=NOW()
WHERE tenant_id=$1 AND approval_token=$2 AND status='pending'`, tenantID, token, string(status))
	if err != nil {
		return fmt.Errorf("close approval record: %w", err)
	}
	return tx.Commit()
}

func scanApprovalRecord(scanner interface{ Scan(...any) error }) (ApprovalRecord, error) {
	var record ApprovalRecord
	var status string
	var notifiedAt, resolvedAt sql.NullTime
	err := scanner.Scan(
		&record.Token, &record.TenantID, &record.AppCode, &record.ConfigVersion, &record.RequestID, &record.TraceID,
		&record.Channel, &record.BindingID, &record.ConversationID, &record.ConversationScope, &record.ExternalUserID,
		&record.RequesterUserID, &record.ProgressMessageID, &record.ToolName, &record.ToolDescription, &status, &record.NotificationID, &record.ResolvedBy,
		&record.CreatedAt, &record.ExpiresAt, &notifiedAt, &resolvedAt,
	)
	if err != nil {
		return ApprovalRecord{}, err
	}
	record.Status = ApprovalStatus(status)
	if notifiedAt.Valid {
		t := notifiedAt.Time
		record.NotifiedAt = &t
	}
	if resolvedAt.Valid {
		t := resolvedAt.Time
		record.ResolvedAt = &t
	}
	return record, nil
}

func validateApprovalRecord(record ApprovalRecord) error {
	if strings.TrimSpace(record.Token) == "" || strings.TrimSpace(record.TenantID) == "" || strings.TrimSpace(record.AppCode) == "" ||
		record.ConfigVersion == 0 || strings.TrimSpace(record.Channel) == "" || strings.TrimSpace(record.BindingID) == "" ||
		strings.TrimSpace(record.ConversationID) == "" || strings.TrimSpace(record.ExternalUserID) == "" || strings.TrimSpace(record.ToolName) == "" {
		return errors.New("approval record identity is incomplete")
	}
	if record.Status != ApprovalPending || record.CreatedAt.IsZero() || !record.ExpiresAt.After(record.CreatedAt) {
		return errors.New("approval record lifecycle is invalid")
	}
	return nil
}

type MemoryApprovalStore struct {
	mu      sync.Mutex
	records map[string]ApprovalRecord
}

func NewMemoryApprovalStore() *MemoryApprovalStore {
	return &MemoryApprovalStore{records: make(map[string]ApprovalRecord)}
}

func approvalRecordKey(tenantID, token string) string { return tenantID + "\x00" + token }

func (s *MemoryApprovalStore) Create(ctx context.Context, record ApprovalRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateApprovalRecord(record); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := approvalRecordKey(record.TenantID, record.Token)
	if _, exists := s.records[key]; exists {
		return errors.New("approval record already exists")
	}
	s.records[key] = record
	return nil
}

func (s *MemoryApprovalStore) Get(ctx context.Context, tenantID, token string) (ApprovalRecord, error) {
	if err := ctx.Err(); err != nil {
		return ApprovalRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[approvalRecordKey(tenantID, token)]
	if !ok {
		return ApprovalRecord{}, ErrApprovalRecordNotFound
	}
	return record, nil
}

func (s *MemoryApprovalStore) ListPending(ctx context.Context, limit int) ([]ApprovalRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 1000 {
		return nil, errors.New("approval pending limit must be between 1 and 1000")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	result := make([]ApprovalRecord, 0, limit)
	for _, record := range s.records {
		if record.Status != ApprovalPending || record.NotificationID != "" || !record.ExpiresAt.After(now) ||
			record.CreatedAt.Add(approvalRedisRecoveryGrace).After(now) {
			continue
		}
		result = append(result, record)
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (s *MemoryApprovalStore) MarkNotified(ctx context.Context, tenantID, token, notificationID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := approvalRecordKey(tenantID, token)
	record, ok := s.records[key]
	if !ok {
		return ErrApprovalRecordNotFound
	}
	if record.Status != ApprovalPending {
		return ErrApprovalResolved
	}
	if !record.ExpiresAt.After(time.Now().UTC()) {
		return ErrApprovalExpired
	}
	if record.NotificationID != "" && record.NotificationID != notificationID {
		return errors.New("approval notification ID conflict")
	}
	record.NotificationID = notificationID
	now := time.Now().UTC()
	record.NotifiedAt = &now
	s.records[key] = record
	return nil
}

func (s *MemoryApprovalStore) Resolve(ctx context.Context, tenantID, token string, approved bool, resolvedBy string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := approvalRecordKey(tenantID, token)
	record, ok := s.records[key]
	if !ok {
		return ErrApprovalRecordNotFound
	}
	status := ApprovalRejected
	if approved {
		status = ApprovalApproved
	}
	if record.Status != ApprovalPending {
		if record.Status == status && record.ResolvedBy == resolvedBy {
			return nil
		}
		return ErrApprovalResolved
	}
	if !record.ExpiresAt.After(time.Now().UTC()) {
		return ErrApprovalExpired
	}
	now := time.Now().UTC()
	record.Status, record.ResolvedBy, record.ResolvedAt = status, resolvedBy, &now
	s.records[key] = record
	return nil
}

func (s *MemoryApprovalStore) Close(ctx context.Context, tenantID, token string, status ApprovalStatus) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if status != ApprovalExpired && status != ApprovalCanceled {
		return errors.New("approval close status must be expired or canceled")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := approvalRecordKey(tenantID, token)
	record, ok := s.records[key]
	if !ok {
		return ErrApprovalRecordNotFound
	}
	if record.Status != ApprovalPending {
		return nil
	}
	now := time.Now().UTC()
	record.Status, record.ResolvedAt = status, &now
	s.records[key] = record
	return nil
}
