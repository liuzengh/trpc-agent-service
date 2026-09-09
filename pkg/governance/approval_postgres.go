// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package governance

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// PostgresApprovalStore is the multi-node approval implementation. Challenge
// creation, grant and consumption are backed by the control-plane database;
// Consume is one conditional UPDATE, so two workers cannot use one token.
// The store borrows the database handle and never closes it.
type PostgresApprovalStore struct {
	db *sql.DB
}

// NewPostgresApprovalStore creates a durable approval store over db. The
// caller remains responsible for opening, pinging and closing db.
func NewPostgresApprovalStore(db *sql.DB) *PostgresApprovalStore {
	return &PostgresApprovalStore{db: db}
}

func (s *PostgresApprovalStore) CreateChallenge(ctx context.Context, request ApprovalRequest, ttl time.Duration) (ApprovalChallenge, error) {
	if s == nil || s.db == nil {
		return ApprovalChallenge{}, ErrApprovalStoreUnavailable
	}
	if err := validateApprovalRequest(request); err != nil {
		return ApprovalChallenge{}, err
	}
	if ttl <= 0 {
		ttl = defaultApprovalTTL
	}
	if ttl < time.Second {
		ttl = time.Second
	}
	if ttl > maxApprovalTTL {
		return ApprovalChallenge{}, fmt.Errorf("%w: approval lifetime exceeds the maximum", ErrApprovalInvalid)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ApprovalChallenge{}, fmt.Errorf("begin approval challenge: %w", err)
	}
	defer tx.Rollback()

	// Serialize retries for the same invocation. PostgreSQL advisory locks are
	// transaction-scoped and do not add a uniqueness rule involving the
	// non-immutable clock expression used for expiry.
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, approvalRequestLockKey(request)); err != nil {
		return ApprovalChallenge{}, fmt.Errorf("lock approval challenge: %w", err)
	}
	var existing ApprovalChallenge
	err = tx.QueryRowContext(ctx, `
		SELECT challenge_id, expires_at
		FROM tool_approvals
		WHERE tenant_id=$1 AND user_id=$2 AND session_owner_id=$3 AND session_id=$4
		  AND tool_name=$5 AND args_hash=$6 AND invocation_id=$7
		  AND consumed_at IS NULL AND expires_at > clock_timestamp()
		ORDER BY created_at DESC
		LIMIT 1`, request.TenantID, request.UserID, request.SessionOwnerID, request.SessionID,
		request.ToolName, request.ArgsHash, request.InvocationID).Scan(&existing.ChallengeID, &existing.ExpiresAt)
	if err == nil {
		existing.Request = request
		if err := tx.Commit(); err != nil {
			return ApprovalChallenge{}, fmt.Errorf("commit existing approval challenge: %w", err)
		}
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ApprovalChallenge{}, fmt.Errorf("find approval challenge: %w", err)
	}

	id, err := randomToken(approvalIDBytes)
	if err != nil {
		return ApprovalChallenge{}, fmt.Errorf("create approval challenge: %w", err)
	}
	var expiresAt time.Time
	err = tx.QueryRowContext(ctx, `
		INSERT INTO tool_approvals (
			challenge_id, tenant_id, user_id, session_owner_id, session_id,
			tool_name, args_hash, invocation_id, expires_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,
			clock_timestamp() + ($9 * INTERVAL '1 second'))
		RETURNING expires_at`, id, request.TenantID, request.UserID, request.SessionOwnerID,
		request.SessionID, request.ToolName, request.ArgsHash, request.InvocationID,
		int64(ttl/time.Second)).Scan(&expiresAt)
	if err != nil {
		return ApprovalChallenge{}, fmt.Errorf("insert approval challenge: %w", err)
	}
	if err := insertApprovalAudit(ctx, tx, request.TenantID, "system", "tool.approval.challenge", id,
		approvalAuditDetails(request, "created")); err != nil {
		return ApprovalChallenge{}, err
	}
	if err := tx.Commit(); err != nil {
		return ApprovalChallenge{}, fmt.Errorf("commit approval challenge: %w", err)
	}
	return ApprovalChallenge{ChallengeID: id, Request: request, ExpiresAt: expiresAt}, nil
}

func (s *PostgresApprovalStore) Grant(ctx context.Context, challengeID, approver string) (ApprovalGrant, error) {
	if s == nil || s.db == nil {
		return ApprovalGrant{}, ErrApprovalStoreUnavailable
	}
	if !validApprovalID(challengeID) || !validApprovalPrincipal(approver) {
		return ApprovalGrant{}, ErrApprovalInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ApprovalGrant{}, fmt.Errorf("begin approval grant: %w", err)
	}
	defer tx.Rollback()

	var request ApprovalRequest
	var expiresAt time.Time
	var databaseNow time.Time
	var grantedAt, consumedAt sql.NullTime
	var grantedBy sql.NullString
	err = tx.QueryRowContext(ctx, `
		SELECT tenant_id, user_id, session_owner_id, session_id, tool_name,
	       args_hash, invocation_id, expires_at, granted_at, granted_by, consumed_at,
	       clock_timestamp()
		FROM tool_approvals WHERE challenge_id=$1 FOR UPDATE`, challengeID).Scan(
		&request.TenantID, &request.UserID, &request.SessionOwnerID, &request.SessionID,
		&request.ToolName, &request.ArgsHash, &request.InvocationID, &expiresAt,
		&grantedAt, &grantedBy, &consumedAt, &databaseNow)
	if errors.Is(err, sql.ErrNoRows) {
		return ApprovalGrant{}, ErrApprovalNotFound
	}
	if err != nil {
		return ApprovalGrant{}, fmt.Errorf("load approval challenge: %w", err)
	}
	if consumedAt.Valid || !expiresAt.After(databaseNow) {
		return ApprovalGrant{}, ErrApprovalInvalid
	}
	if grantedAt.Valid || grantedBy.Valid {
		return ApprovalGrant{}, ErrApprovalAlreadyUsed
	}
	token, err := randomToken(approvalTokenBytes)
	if err != nil {
		return ApprovalGrant{}, fmt.Errorf("grant approval: %w", err)
	}
	tokenHash := sha256.Sum256([]byte(token))
	result, err := tx.ExecContext(ctx, `
		UPDATE tool_approvals
		SET token_hash=$2, granted_at=clock_timestamp(), granted_by=$3
		WHERE challenge_id=$1 AND granted_at IS NULL AND consumed_at IS NULL
		  AND expires_at > clock_timestamp()`, challengeID, tokenHash[:], strings.TrimSpace(approver))
	if err != nil {
		return ApprovalGrant{}, fmt.Errorf("update approval grant: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return ApprovalGrant{}, fmt.Errorf("check approval grant: %w", err)
	}
	if rows != 1 {
		return ApprovalGrant{}, ErrApprovalInvalid
	}
	if err := insertApprovalAudit(ctx, tx, request.TenantID, strings.TrimSpace(approver), "tool.approval.grant", challengeID,
		approvalAuditDetails(request, "granted")); err != nil {
		return ApprovalGrant{}, err
	}
	if err := tx.Commit(); err != nil {
		return ApprovalGrant{}, fmt.Errorf("commit approval grant: %w", err)
	}
	return ApprovalGrant{ChallengeID: challengeID, Token: token, ExpiresAt: expiresAt}, nil
}

func (s *PostgresApprovalStore) Consume(ctx context.Context, request ApprovalRequest, token string) error {
	if s == nil || s.db == nil {
		return ErrApprovalStoreUnavailable
	}
	if err := validateApprovalRequest(request); err != nil {
		return err
	}
	if !validApprovalToken(token) {
		return ErrApprovalInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	tokenHash := sha256.Sum256([]byte(token))
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin approval consumption: %w", err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
		UPDATE tool_approvals
		SET consumed_at=clock_timestamp()
		WHERE tenant_id=$1 AND user_id=$2 AND session_owner_id=$3 AND session_id=$4
		  AND tool_name=$5 AND args_hash=$6 AND invocation_id=$7
		  AND token_hash=$8 AND granted_at IS NOT NULL AND consumed_at IS NULL
		  AND expires_at > clock_timestamp()`, request.TenantID, request.UserID,
		request.SessionOwnerID, request.SessionID, request.ToolName, request.ArgsHash,
		request.InvocationID, tokenHash[:])
	if err != nil {
		return fmt.Errorf("consume approval: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check approval consumption: %w", err)
	}
	if rows == 1 {
		if err := insertApprovalAudit(ctx, tx, request.TenantID, "system", "tool.approval.consume", request.InvocationID,
			approvalAuditDetails(request, "consumed")); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit approval consumption: %w", err)
		}
		return nil
	}
	var exists bool
	err = tx.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM tool_approvals
			WHERE tenant_id=$1 AND user_id=$2 AND session_owner_id=$3 AND session_id=$4
			  AND tool_name=$5 AND args_hash=$6 AND invocation_id=$7
			  AND consumed_at IS NULL)`, request.TenantID, request.UserID,
		request.SessionOwnerID, request.SessionID, request.ToolName, request.ArgsHash,
		request.InvocationID).Scan(&exists)
	if err != nil {
		return fmt.Errorf("inspect approval consumption: %w", err)
	}
	if exists {
		return ErrApprovalInvalid
	}
	return ErrApprovalNotFound
}

// ConsumeGranted atomically consumes an operator grant for the exact request
// identity. The HTTP Admin API never returns the raw token; a queue retry does
// not need it and it is never persisted in queue/session payloads.
func (s *PostgresApprovalStore) ConsumeGranted(ctx context.Context, request ApprovalRequest) error {
	if s == nil || s.db == nil {
		return ErrApprovalStoreUnavailable
	}
	if err := validateApprovalRequest(request); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin granted approval consumption: %w", err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
		UPDATE tool_approvals
		SET consumed_at=clock_timestamp()
		WHERE tenant_id=$1 AND user_id=$2 AND session_owner_id=$3 AND session_id=$4
		  AND tool_name=$5 AND args_hash=$6 AND invocation_id=$7
		  AND granted_at IS NOT NULL AND consumed_at IS NULL
		  AND expires_at > clock_timestamp()`, request.TenantID, request.UserID,
		request.SessionOwnerID, request.SessionID, request.ToolName, request.ArgsHash,
		request.InvocationID)
	if err != nil {
		return fmt.Errorf("consume granted approval: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check granted approval consumption: %w", err)
	}
	if rows == 1 {
		if err := insertApprovalAudit(ctx, tx, request.TenantID, "system", "tool.approval.consume", request.InvocationID,
			approvalAuditDetails(request, "consumed")); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit granted approval consumption: %w", err)
		}
		return nil
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM tool_approvals
			WHERE tenant_id=$1 AND user_id=$2 AND session_owner_id=$3 AND session_id=$4
			  AND tool_name=$5 AND args_hash=$6 AND invocation_id=$7
			  AND consumed_at IS NULL)`, request.TenantID, request.UserID,
		request.SessionOwnerID, request.SessionID, request.ToolName, request.ArgsHash,
		request.InvocationID).Scan(&exists); err != nil {
		return fmt.Errorf("inspect granted approval: %w", err)
	}
	if exists {
		return ErrApprovalNotGranted
	}
	return ErrApprovalNotFound
}

// ConsumeGrantedForChallenge is the strict retry path. In addition to the
// invocation scope it matches the challenge ID selected by atomic admission,
// so a stale worker cannot consume a replacement challenge for the same
// invocation after a grant race.
func (s *PostgresApprovalStore) ConsumeGrantedForChallenge(ctx context.Context, request ApprovalRequest, challengeID string) error {
	if s == nil || s.db == nil {
		return ErrApprovalStoreUnavailable
	}
	if err := validateApprovalRequest(request); err != nil {
		return err
	}
	if !validApprovalID(challengeID) {
		return ErrApprovalInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin exact granted approval consumption: %w", err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
		UPDATE tool_approvals
		SET consumed_at=clock_timestamp()
		WHERE challenge_id=$1 AND tenant_id=$2 AND user_id=$3 AND session_owner_id=$4 AND session_id=$5
		  AND tool_name=$6 AND args_hash=$7 AND invocation_id=$8
		  AND granted_at IS NOT NULL AND consumed_at IS NULL
		  AND expires_at > clock_timestamp()`, challengeID, request.TenantID, request.UserID,
		request.SessionOwnerID, request.SessionID, request.ToolName, request.ArgsHash,
		request.InvocationID)
	if err != nil {
		return fmt.Errorf("consume exact granted approval: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check exact granted approval consumption: %w", err)
	}
	if rows == 1 {
		if err := insertApprovalAudit(ctx, tx, request.TenantID, "system", "tool.approval.consume", request.InvocationID,
			approvalAuditDetails(request, "consumed")); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit exact granted approval consumption: %w", err)
		}
		return nil
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM tool_approvals
			WHERE challenge_id=$1 AND tenant_id=$2 AND user_id=$3 AND session_owner_id=$4 AND session_id=$5
			  AND tool_name=$6 AND args_hash=$7 AND invocation_id=$8 AND consumed_at IS NULL)`,
		challengeID, request.TenantID, request.UserID, request.SessionOwnerID, request.SessionID,
		request.ToolName, request.ArgsHash, request.InvocationID).Scan(&exists); err != nil {
		return fmt.Errorf("inspect exact granted approval: %w", err)
	}
	if exists {
		return ErrApprovalNotGranted
	}
	return ErrApprovalNotFound
}

// FindActiveApproval implements ApprovalResumeInspector. The query returns
// at most two rows so a corrupt or partially migrated store cannot cause a
// Worker to resume an arbitrary tool call.
func (s *PostgresApprovalStore) FindActiveApproval(ctx context.Context, scope ApprovalInvocationScope) (ApprovalChallenge, error) {
	if s == nil || s.db == nil {
		return ApprovalChallenge{}, ErrApprovalStoreUnavailable
	}
	if err := validateApprovalInvocationScope(scope); err != nil {
		return ApprovalChallenge{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT challenge_id, tool_name, args_hash, expires_at
		FROM tool_approvals
		WHERE tenant_id=$1 AND user_id=$2 AND session_owner_id=$3 AND session_id=$4
		  AND invocation_id=$5 AND consumed_at IS NULL AND expires_at > clock_timestamp()
		ORDER BY created_at DESC
		LIMIT 2`, scope.TenantID, scope.UserID, scope.SessionOwnerID, scope.SessionID, scope.InvocationID)
	if err != nil {
		return ApprovalChallenge{}, fmt.Errorf("find active approval: %w", err)
	}
	defer rows.Close()
	var matches []ApprovalChallenge
	for rows.Next() {
		challenge := ApprovalChallenge{Request: ApprovalRequest{
			TenantID: scope.TenantID, UserID: scope.UserID,
			SessionOwnerID: scope.SessionOwnerID, SessionID: scope.SessionID,
			InvocationID: scope.InvocationID,
		}}
		if err := rows.Scan(&challenge.ChallengeID, &challenge.Request.ToolName,
			&challenge.Request.ArgsHash, &challenge.ExpiresAt); err != nil {
			return ApprovalChallenge{}, fmt.Errorf("scan active approval: %w", err)
		}
		matches = append(matches, challenge)
	}
	if err := rows.Err(); err != nil {
		return ApprovalChallenge{}, fmt.Errorf("iterate active approval: %w", err)
	}
	switch len(matches) {
	case 0:
		return ApprovalChallenge{}, ErrApprovalNotFound
	case 1:
		return matches[0], nil
	default:
		return ApprovalChallenge{}, ErrApprovalAmbiguous
	}
}

// IsApprovalGranted reports whether an active challenge has been granted.
// The token itself is never returned. A separate read capability lets the
// Worker reject an ungranted queue retry before creating another execution
// record, while expired or consumed state remains fail-closed.
func (s *PostgresApprovalStore) IsApprovalGranted(ctx context.Context, challengeID string) (bool, error) {
	if s == nil || s.db == nil {
		return false, ErrApprovalStoreUnavailable
	}
	if !validApprovalID(challengeID) {
		return false, ErrApprovalInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var grantedAt, consumedAt sql.NullTime
	var expiresAt, databaseNow time.Time
	err := s.db.QueryRowContext(ctx, `
		SELECT granted_at, consumed_at, expires_at, clock_timestamp()
		FROM tool_approvals
		WHERE challenge_id=$1`, challengeID).Scan(
		&grantedAt, &consumedAt, &expiresAt, &databaseNow)
	if errors.Is(err, sql.ErrNoRows) {
		return false, ErrApprovalNotFound
	}
	if err != nil {
		return false, fmt.Errorf("inspect approval grant: %w", err)
	}
	if consumedAt.Valid || !expiresAt.After(databaseNow) {
		return false, ErrApprovalInvalid
	}
	return grantedAt.Valid, nil
}

// InspectApprovalResume implements ApprovalResumeStateInspector. Challenge
// identity and grant state are read by one SQL statement so a retry cannot
// observe a challenge from one version of the row and a grant flag from
// another. The LIMIT 2 query preserves the fail-closed ambiguity check used by
// FindActiveApproval.
func (s *PostgresApprovalStore) InspectApprovalResume(ctx context.Context, scope ApprovalInvocationScope) (ApprovalResumeState, error) {
	if s == nil || s.db == nil {
		return ApprovalResumeState{}, ErrApprovalStoreUnavailable
	}
	if err := validateApprovalInvocationScope(scope); err != nil {
		return ApprovalResumeState{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT challenge_id, tool_name, args_hash, expires_at, granted_at
		FROM tool_approvals
		WHERE tenant_id=$1 AND user_id=$2 AND session_owner_id=$3 AND session_id=$4
		  AND invocation_id=$5 AND consumed_at IS NULL AND expires_at > clock_timestamp()
		ORDER BY created_at DESC
		LIMIT 2`, scope.TenantID, scope.UserID, scope.SessionOwnerID, scope.SessionID, scope.InvocationID)
	if err != nil {
		return ApprovalResumeState{}, fmt.Errorf("inspect approval resume: %w", err)
	}
	defer rows.Close()
	var state ApprovalResumeState
	var found bool
	for rows.Next() {
		if found {
			return ApprovalResumeState{}, ErrApprovalAmbiguous
		}
		state.Challenge = ApprovalChallenge{Request: ApprovalRequest{
			TenantID: scope.TenantID, UserID: scope.UserID,
			SessionOwnerID: scope.SessionOwnerID, SessionID: scope.SessionID,
			InvocationID: scope.InvocationID,
		}}
		var grantedAt sql.NullTime
		if err := rows.Scan(&state.Challenge.ChallengeID,
			&state.Challenge.Request.ToolName, &state.Challenge.Request.ArgsHash,
			&state.Challenge.ExpiresAt, &grantedAt); err != nil {
			return ApprovalResumeState{}, fmt.Errorf("scan approval resume: %w", err)
		}
		state.Granted = grantedAt.Valid
		found = true
	}
	if err := rows.Err(); err != nil {
		return ApprovalResumeState{}, fmt.Errorf("iterate approval resume: %w", err)
	}
	if !found {
		return ApprovalResumeState{}, ErrApprovalNotFound
	}
	return state, nil
}

// GetChallenge implements ApprovalInspector. It intentionally omits token
// material and treats expired/consumed records as unavailable to callers.
func (s *PostgresApprovalStore) GetChallenge(ctx context.Context, challengeID string) (ApprovalChallenge, error) {
	if s == nil || s.db == nil {
		return ApprovalChallenge{}, ErrApprovalStoreUnavailable
	}
	if !validApprovalID(challengeID) {
		return ApprovalChallenge{}, ErrApprovalInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var challenge ApprovalChallenge
	var consumedAt sql.NullTime
	var databaseNow time.Time
	err := s.db.QueryRowContext(ctx, `
		SELECT tenant_id, user_id, session_owner_id, session_id, tool_name,
		       args_hash, invocation_id, expires_at, consumed_at, clock_timestamp()
		FROM tool_approvals
		WHERE challenge_id=$1`, challengeID).Scan(
		&challenge.Request.TenantID, &challenge.Request.UserID,
		&challenge.Request.SessionOwnerID, &challenge.Request.SessionID,
		&challenge.Request.ToolName, &challenge.Request.ArgsHash,
		&challenge.Request.InvocationID, &challenge.ExpiresAt, &consumedAt, &databaseNow)
	if errors.Is(err, sql.ErrNoRows) {
		return ApprovalChallenge{}, ErrApprovalNotFound
	}
	if err != nil {
		return ApprovalChallenge{}, fmt.Errorf("get approval challenge: %w", err)
	}
	if consumedAt.Valid || !challenge.ExpiresAt.After(databaseNow) {
		return ApprovalChallenge{}, ErrApprovalInvalid
	}
	challenge.ChallengeID = challengeID
	return challenge, nil
}

func (s *PostgresApprovalStore) ListChallenges(ctx context.Context, tenantID string, limit int) ([]ApprovalChallenge, error) {
	if s == nil || s.db == nil {
		return nil, ErrApprovalStoreUnavailable
	}
	if strings.TrimSpace(tenantID) == "" || len(tenantID) > 255 || !utf8.ValidString(tenantID) || limit < 0 || limit > 100 {
		return nil, ErrApprovalInvalid
	}
	if limit == 0 {
		limit = 50
	}
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT challenge_id, user_id, session_owner_id, session_id, tool_name,
		       args_hash, invocation_id, expires_at
		FROM tool_approvals
		WHERE tenant_id=$1 AND consumed_at IS NULL AND expires_at > clock_timestamp()
		ORDER BY expires_at, created_at
		LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, fmt.Errorf("list approval challenges: %w", err)
	}
	defer rows.Close()
	result := make([]ApprovalChallenge, 0)
	for rows.Next() {
		var challenge ApprovalChallenge
		challenge.Request.TenantID = tenantID
		if err := rows.Scan(&challenge.ChallengeID, &challenge.Request.UserID,
			&challenge.Request.SessionOwnerID, &challenge.Request.SessionID,
			&challenge.Request.ToolName, &challenge.Request.ArgsHash,
			&challenge.Request.InvocationID, &challenge.ExpiresAt); err != nil {
			return nil, fmt.Errorf("scan approval challenge: %w", err)
		}
		result = append(result, challenge)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate approval challenges: %w", err)
	}
	return result, nil
}

func approvalRequestLockKey(request ApprovalRequest) int64 {
	digest := sha256.Sum256([]byte(strings.Join([]string{
		request.TenantID, request.UserID, request.SessionOwnerID, request.SessionID,
		request.ToolName, request.ArgsHash, request.InvocationID,
	}, "\x00")))
	return int64(binary.BigEndian.Uint64(digest[:8]))
}

func insertApprovalAudit(ctx context.Context, tx *sql.Tx, tenantID, actor, action, resourceID string, details map[string]string) error {
	if strings.TrimSpace(actor) == "" {
		return ErrApprovalInvalid
	}
	if details == nil {
		details = map[string]string{}
	}
	payload, err := json.Marshal(details)
	if err != nil {
		return fmt.Errorf("marshal approval audit: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO control_plane_audit
			(tenant_id, actor, action, resource_type, resource_id, details)
		VALUES ($1,$2,$3,'tool_approval',$4,$5)`, tenantID, actor, action, resourceID, payload); err != nil {
		return fmt.Errorf("write approval audit: %w", err)
	}
	return nil
}

// approvalAuditDetails records only non-secret correlation metadata. Raw tool
// arguments and approval tokens are intentionally excluded; ArgsHash is safe
// to retain because it is already the authorization binding, not the payload.
func approvalAuditDetails(request ApprovalRequest, decision string) map[string]string {
	return map[string]string{
		"user_id":          request.UserID,
		"session_owner_id": request.SessionOwnerID,
		"session_id":       request.SessionID,
		"tool_name":        request.ToolName,
		"args_hash":        request.ArgsHash,
		"invocation_id":    request.InvocationID,
		"decision":         decision,
	}
}
