package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

// RecallRequest and RecallResult remain package aliases so callers can use
// the PostgreSQL implementation without depending on provider DTOs.
type RecallRequest = channels.RecallRequest
type RecallResult = channels.RecallResult

// HandleRecall idempotently records a verified recall and applies the
// smallest state change allowed by the execution status. It never deletes
// Inbox, Session, Memory, Tool, or Reply data.
func (s *Store) HandleRecall(ctx context.Context, request RecallRequest) (RecallResult, error) {
	if err := s.validate(); err != nil {
		return RecallResult{}, err
	}
	if err := request.Validate(); err != nil {
		return RecallResult{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RecallResult{}, fmt.Errorf("begin recall admission: %w", err)
	}
	defer func() { rollback(tx) }()

	if err := lockRecallBinding(ctx, tx, request); err != nil {
		return RecallResult{}, err
	}
	existing, found, err := findRecall(ctx, tx, request)
	if err != nil {
		return RecallResult{}, err
	}
	if found {
		if !bytes.Equal(existing.PayloadHash, request.PayloadHash) {
			return RecallResult{}, gateway.ErrIdempotencyConflict
		}
		result := RecallResult{
			RequestID:       existing.RequestID,
			ExecutionStatus: existing.Status,
			Replayed:        true,
		}
		if existing.RequestID != "" {
			status, statusErr := readExecutionStatus(ctx, tx, request, existing.RequestID)
			if statusErr != nil {
				return RecallResult{}, fmt.Errorf("read replayed recall execution: %w", statusErr)
			}
			result.ExecutionStatus = status
		}
		if err := tx.Commit(ctx); err != nil {
			return RecallResult{}, fmt.Errorf("commit replayed recall: %w", err)
		}
		return result, nil
	}

	inboxRequestID, found, err := findRecalledInbox(ctx, tx, request)
	if err != nil {
		return RecallResult{}, err
	}
	if !found {
		if err := insertRecall(ctx, tx, request, "", "REJECTED", "REQUEST_NOT_FOUND"); err != nil {
			return RecallResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return RecallResult{}, fmt.Errorf("commit rejected recall: %w", err)
		}
		return RecallResult{ExecutionStatus: "REJECTED"}, nil
	}

	execution, found, err := lockRecalledExecution(ctx, tx, request, inboxRequestID)
	if err != nil {
		return RecallResult{}, err
	}
	if !found {
		if err := insertRecall(ctx, tx, request, "", "REJECTED", "REQUEST_NOT_FOUND"); err != nil {
			return RecallResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return RecallResult{}, fmt.Errorf("commit unlinked recall: %w", err)
		}
		return RecallResult{ExecutionStatus: "REJECTED"}, nil
	}
	if channels.Channel(execution.Channel) != request.Channel {
		return RecallResult{}, channels.ErrBindingChannelMismatch
	}

	result := RecallResult{
		RequestID:       inboxRequestID,
		ExecutionStatus: execution.Status,
	}
	switch execution.Status {
	case "PENDING":
		if _, err := tx.Exec(ctx, `
UPDATE platform.execution
SET status = 'CANCELED', finished_at = clock_timestamp(),
    updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3 AND status = 'PENDING'`,
			request.TenantID, request.AppID, inboxRequestID); err != nil {
			return RecallResult{}, fmt.Errorf("cancel pending execution: %w", err)
		}
		result.ExecutionStatus = "CANCELED"
		if err := releaseExecutionQuotaTx(ctx, tx, request.TenantID, request.AppID, inboxRequestID); err != nil {
			return RecallResult{}, err
		}
	case "RUNNING":
		if _, err := tx.Exec(ctx, `
UPDATE platform.execution

		SET status = 'CANCELED',
		    lease_owner = NULL, run_token = NULL, lease_until = NULL,
    finished_at = clock_timestamp(),
    updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3 AND status = 'RUNNING'`,
			request.TenantID, request.AppID, inboxRequestID); err != nil {
			return RecallResult{}, fmt.Errorf("cancel running execution: %w", err)
		}
		result.ExecutionStatus = "CANCELED"
		if err := releaseExecutionQuotaTx(ctx, tx, request.TenantID, request.AppID, inboxRequestID); err != nil {
			return RecallResult{}, err
		}
	default:
		// Completed and previously canceled executions are retained.
	}

	if err := insertRecall(ctx, tx, request, inboxRequestID, "APPLIED", ""); err != nil {
		return RecallResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return RecallResult{}, fmt.Errorf("commit recall admission: %w", err)
	}
	return result, nil
}

// AdmitRecall implements the provider-neutral channel recall boundary.
func (s *Store) AdmitRecall(ctx context.Context, request channels.RecallRequest) (channels.RecallResult, error) {
	return s.HandleRecall(ctx, request)
}

type recallRecord struct {
	RequestID   string
	PayloadHash []byte
	Status      string
}

type recalledExecution struct {
	Status             string
	Channel            string
	UserID             string
	SessionPrincipalID string
	SessionID          string
	TraceID            string
}

func lockRecallBinding(ctx context.Context, tx pgx.Tx, request RecallRequest) error {
	var status string
	var channel channels.Channel
	err := tx.QueryRow(ctx, `
SELECT status, channel
FROM platform.channel_binding
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
FOR UPDATE`, request.TenantID, request.AppID, request.BindingID).Scan(&status, &channel)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("recall binding: %w", ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("lock recall binding: %w", err)
	}
	if channel != request.Channel {
		return channels.ErrBindingChannelMismatch
	}
	if status != string(channels.BindingActive) {
		return channels.ErrBindingInactive
	}
	return nil
}

func findRecall(ctx context.Context, tx pgx.Tx, request RecallRequest) (recallRecord, bool, error) {
	var record recallRecord
	err := tx.QueryRow(ctx, `
SELECT COALESCE(request_id, ''), payload_hash, status
FROM platform.channel_recall_inbox
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND external_event_id = $4
FOR UPDATE`, request.TenantID, request.AppID, request.BindingID, request.ExternalEventID).Scan(
		&record.RequestID, &record.PayloadHash, &record.Status,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return recallRecord{}, false, nil
	}
	if err != nil {
		return recallRecord{}, false, fmt.Errorf("find recall event: %w", err)
	}
	return record, true, nil
}

func findRecalledInbox(ctx context.Context, tx pgx.Tx, request RecallRequest) (string, bool, error) {
	var requestID string
	err := tx.QueryRow(ctx, `
SELECT request_id
FROM platform.channel_inbox
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND external_message_id = $4
FOR UPDATE`, request.TenantID, request.AppID, request.BindingID, request.ExternalMessageID).Scan(&requestID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("find recalled inbox: %w", err)
	}
	return requestID, true, nil
}

func lockRecalledExecution(
	ctx context.Context,
	tx pgx.Tx,
	request RecallRequest,
	requestID string,
) (recalledExecution, bool, error) {
	var execution recalledExecution
	err := tx.QueryRow(ctx, `
SELECT status, command->>'channel', user_id, session_principal_id, session_id, trace_id
FROM platform.execution
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3
  AND command->>'binding_id' = $4
FOR UPDATE`, request.TenantID, request.AppID, requestID, request.BindingID).Scan(
		&execution.Status,
		&execution.Channel,
		&execution.UserID,
		&execution.SessionPrincipalID,
		&execution.SessionID,
		&execution.TraceID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return recalledExecution{}, false, nil
	}
	if err != nil {
		return recalledExecution{}, false, fmt.Errorf("lock recalled execution: %w", err)
	}
	return execution, true, nil
}

func readExecutionStatus(ctx context.Context, tx pgx.Tx, request RecallRequest, requestID string) (string, error) {
	var status string
	err := tx.QueryRow(ctx, `
SELECT status
FROM platform.execution
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3`,
		request.TenantID, request.AppID, requestID).Scan(&status)
	return status, err
}

func insertRecall(
	ctx context.Context,
	tx pgx.Tx,
	request RecallRequest,
	requestID, status, rejectReason string,
) error {
	var storedRequestID any
	if requestID != "" {
		storedRequestID = requestID
	}
	var storedRejectReason any
	if rejectReason != "" {
		storedRejectReason = rejectReason
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO platform.channel_recall_inbox (
    tenant_id, app_id, binding_id, external_event_id, request_id,
    payload_hash, status, reject_reason
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		request.TenantID,
		request.AppID,
		request.BindingID,
		request.ExternalEventID,
		storedRequestID,
		request.PayloadHash,
		status,
		storedRejectReason,
	); err != nil {
		return fmt.Errorf("insert %s recall: %w", strings.ToLower(status), err)
	}
	return nil
}

// IsExecutionCanceled reads the durable recall result before a worker starts.
func (s *Store) IsExecutionCanceled(ctx context.Context, tenantID, appID, requestID string) (bool, error) {
	if err := s.validate(); err != nil {
		return false, err
	}
	if tenantID == "" || appID == "" || requestID == "" {
		return false, errors.New("execution cancellation scope is required")
	}
	var canceled bool
	err := s.pool.QueryRow(ctx, `
SELECT status = 'CANCELED'
FROM platform.execution
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3
	`, tenantID, appID, requestID).Scan(&canceled)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check execution cancellation: %w", err)
	}
	return canceled, nil
}

var _ channels.RecallAdmitter = (*Store)(nil)
