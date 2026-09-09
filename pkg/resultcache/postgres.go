// Package resultcache stores completed Worker responses for idempotent Inbox
// retries. It does not replace tool-level idempotency for external side effects.
package resultcache

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
)

var ErrPayloadConflict = errors.New("result idempotency key reused with different payload")

var (
	// ErrStoreUnavailable indicates that the result cache was not composed with
	// a usable database handle.
	ErrStoreUnavailable        = errors.New("result cache store unavailable")
	ErrRequestIdentityConflict = errors.New("cached result belongs to a different request identity")
	ErrUnverifiableResult      = errors.New("cached result has no verifiable producer execution")
	ErrExecutionFenceMismatch  = errors.New("cached result execution token does not match")
	ErrExecutionLeaseExpired   = errors.New("cached result producer execution lease has expired")
	ErrExecutionTerminal       = errors.New("cached result producer execution is already terminal")
	ErrResultProducerConflict  = errors.New("live cached result belongs to a different execution")
	ErrResultUnavailable       = errors.New("successful execution result is unavailable")
)

// Identity is the immutable scope of one durable invocation result.
type Identity struct {
	TenantID       string
	IdempotencyKey string
	PayloadHash    string
	SessionID      string
	AgentAppID     string
	AgentVersionID string
	DeploymentID   string
}

// Entry is a cached response together with the execution that produced it.
type Entry struct {
	Response    []byte
	ExecutionID int64
}

type Store struct{ db *sql.DB }

func New(db *sql.DB) *Store { return &Store{db: db} }

// GetScoped returns a response only when its producer execution has the exact
// immutable identity requested by the caller.
func (s *Store) GetScoped(ctx context.Context, identity Identity) (Entry, bool, error) {
	if err := identity.validate(); err != nil {
		return Entry{}, false, err
	}
	if s == nil || s.db == nil {
		return Entry{}, false, fmt.Errorf("lookup invocation result: %w", ErrStoreUnavailable)
	}
	ctx = resultCacheContext(ctx)

	var (
		entry                                             Entry
		storedHash, sessionID, appID, versionID, deployID string
		status                                            string
	)
	err := s.db.QueryRowContext(ctx, `
		SELECT r.payload_hash, r.response, COALESCE(r.execution_id,0),
		       COALESCE(e.session_id,''), COALESCE(e.agent_app_id,''),
		       COALESCE(e.agent_version_id,''), COALESCE(e.deployment_id,''),
		       COALESCE(e.status,'')
		FROM invocation_results r
		LEFT JOIN execution_records e
		  ON e.id=r.execution_id AND e.tenant_id=r.tenant_id
		WHERE r.tenant_id=$1 AND r.idempotency_key=$2 AND r.expires_at > now()`,
		identity.TenantID, identity.IdempotencyKey,
	).Scan(
		&storedHash, &entry.Response, &entry.ExecutionID, &sessionID,
		&appID, &versionID, &deployID, &status,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Entry{}, false, nil
	}
	if err != nil {
		return Entry{}, false, fmt.Errorf("lookup invocation result: %w", err)
	}
	if !strings.EqualFold(storedHash, identity.PayloadHash) {
		return Entry{}, false, ErrPayloadConflict
	}
	if entry.ExecutionID <= 0 {
		return Entry{}, false, ErrUnverifiableResult
	}
	if sessionID != identity.SessionID || appID != identity.AgentAppID ||
		versionID != identity.AgentVersionID || deployID != identity.DeploymentID {
		return Entry{}, false, ErrRequestIdentityConflict
	}
	if status != "SUCCEEDED" {
		return Entry{}, false, fmt.Errorf("%w: %s", ErrExecutionTerminal, status)
	}
	return entry, true, nil
}

// CommitSuccess atomically persists a response and completes the exact
// execution attempt that produced it. This removes the crash window between a
// standalone cache write and a separate execution status update.
func (s *Store) CommitSuccess(
	ctx context.Context,
	identity Identity,
	executionID int64,
	executionToken string,
	response []byte,
) error {
	if err := identity.validate(); err != nil {
		return err
	}
	if s == nil || s.db == nil {
		return fmt.Errorf("commit invocation result: %w", ErrStoreUnavailable)
	}
	ctx = resultCacheContext(ctx)
	if executionID <= 0 || executionToken == "" || len(executionToken) > 64 ||
		strings.ContainsAny(executionToken, "\x00\r\n") || len(response) == 0 {
		return fmt.Errorf("commit invocation result: execution handle and response are required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("commit invocation result begin: %w", err)
	}
	defer tx.Rollback()

	var (
		tenantID, sessionID, appID, versionID, deploymentID string
		key, payloadHash, storedToken, status               string
		leaseUntil                                          time.Time
	)
	if err := tx.QueryRowContext(ctx, `
		SELECT tenant_id, session_id, agent_app_id, agent_version_id,
		       deployment_id, idempotency_key, COALESCE(payload_hash,''),
		       execution_token, status, lease_until
		FROM execution_records WHERE id=$1 FOR UPDATE`, executionID).Scan(
		&tenantID, &sessionID, &appID, &versionID, &deploymentID,
		&key, &payloadHash, &storedToken, &status, &leaseUntil,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrUnverifiableResult
		}
		return fmt.Errorf("commit invocation result load execution: %w", err)
	}
	if tenantID != identity.TenantID || sessionID != identity.SessionID ||
		appID != identity.AgentAppID || versionID != identity.AgentVersionID ||
		deploymentID != identity.DeploymentID || key != identity.IdempotencyKey {
		return ErrRequestIdentityConflict
	}
	if !strings.EqualFold(payloadHash, identity.PayloadHash) {
		return ErrPayloadConflict
	}
	if storedToken != executionToken {
		return ErrExecutionFenceMismatch
	}
	if status == "SUCCEEDED" {
		return verifyExistingResult(ctx, tx, identity, executionID)
	}
	if status != "RUNNING" {
		return fmt.Errorf("%w: %s", ErrExecutionTerminal, status)
	}
	var storedHash string
	var storedExecutionID int64
	err = tx.QueryRowContext(ctx, `
		INSERT INTO invocation_results (
			tenant_id, idempotency_key, payload_hash, response, execution_id
		) VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (tenant_id,idempotency_key) DO UPDATE
		SET payload_hash=EXCLUDED.payload_hash,
		    response=EXCLUDED.response,
		    execution_id=EXCLUDED.execution_id,
		    created_at=now(),
		    expires_at=now() + INTERVAL '7 days'
		WHERE invocation_results.expires_at <= now()
		RETURNING payload_hash, execution_id`,
		identity.TenantID, identity.IdempotencyKey, strings.ToLower(identity.PayloadHash),
		response, executionID,
	).Scan(&storedHash, &storedExecutionID)
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.QueryRowContext(ctx, `
			SELECT payload_hash, COALESCE(execution_id,0)
			FROM invocation_results
			WHERE tenant_id=$1 AND idempotency_key=$2 AND expires_at > now()`,
			identity.TenantID, identity.IdempotencyKey,
		).Scan(&storedHash, &storedExecutionID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("commit invocation result: live row disappeared after conflict")
			}
			return fmt.Errorf("commit invocation result lookup conflict: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("commit invocation result store: %w", err)
	}
	if !strings.EqualFold(storedHash, identity.PayloadHash) {
		return ErrPayloadConflict
	}
	if storedExecutionID != executionID {
		return ErrResultProducerConflict
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE execution_records
		SET status='SUCCEEDED', error_message='', completed_at=now(), lease_until=now()
		WHERE id=$1 AND execution_token=$2 AND status='RUNNING'
		  AND lease_until > clock_timestamp()`, executionID, executionToken)
	if err != nil {
		return fmt.Errorf("commit invocation result finish execution: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("commit invocation result finish rows: %w", err)
	}
	if rows != 1 {
		// The execution row is locked for this transaction and its identity,
		// token and RUNNING status were checked above. A zero-row update therefore
		// means the database-side lease predicate rejected the commit. Keeping
		// this decision on the database clock avoids cross-node wall-clock skew.
		return ErrExecutionLeaseExpired
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit invocation result: %w", err)
	}
	return nil
}

func verifyExistingResult(ctx context.Context, tx *sql.Tx, identity Identity, executionID int64) error {
	var storedHash string
	var storedExecutionID int64
	if err := tx.QueryRowContext(ctx, `
		SELECT payload_hash, COALESCE(execution_id,0)
		FROM invocation_results
		WHERE tenant_id=$1 AND idempotency_key=$2 AND expires_at > now()`,
		identity.TenantID, identity.IdempotencyKey,
	).Scan(&storedHash, &storedExecutionID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrResultUnavailable
		}
		return fmt.Errorf("verify successful invocation result: %w", err)
	}
	if !strings.EqualFold(storedHash, identity.PayloadHash) {
		return ErrPayloadConflict
	}
	if storedExecutionID != executionID {
		return ErrResultProducerConflict
	}
	return nil
}

func (i Identity) validate() error {
	if i.TenantID == "" || i.IdempotencyKey == "" || i.SessionID == "" ||
		i.AgentAppID == "" || i.AgentVersionID == "" || i.DeploymentID == "" {
		return fmt.Errorf("cached result identity is incomplete")
	}
	if len(i.PayloadHash) != sha256.Size*2 {
		return fmt.Errorf("cached result payload hash is invalid")
	}
	if _, err := hex.DecodeString(i.PayloadHash); err != nil {
		return fmt.Errorf("cached result payload hash is invalid")
	}
	return nil
}

// RunCleanup removes expired responses until ctx is cancelled.
func (s *Store) RunCleanup(ctx context.Context, interval time.Duration) error {
	if s == nil || s.db == nil {
		return ErrStoreUnavailable
	}
	ctx = resultCacheContext(ctx)
	if interval <= 0 {
		interval = time.Hour
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := s.db.ExecContext(ctx, `DELETE FROM invocation_results WHERE expires_at <= now()`); err != nil && ctx.Err() == nil {
				// Cleanup is maintenance, not a liveness dependency. A transient
				// database outage must not permanently stop future cleanup cycles.
				log.Printf("cleanup invocation results failed; will retry: error=%s", telemetry.StableErrorCode(err))
			}
		}
	}
}

func resultCacheContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
