package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"
)

// BeginResult classifies the durable execution state of one inbound message.
type BeginResult string

const (
	// ExecutionFresh means this caller owns the execution and may proceed.
	ExecutionFresh BeginResult = "fresh"
	// ExecutionCompleted means a previous execution already delivered a reply.
	ExecutionCompleted BeginResult = "completed"
	// ExecutionInProgress means another node owns a live execution.
	ExecutionInProgress BeginResult = "in_progress"
)

// ExecutionDedupStore is the durable idempotency backstop. The Redis lease is
// the fast path; this store guarantees a message cannot execute twice even
// after Redis state loss: Begin writes a processing row, RecordExecution flips
// it to completed in the same transaction as the audit effects, and Abort
// removes it after a failed attempt so the message remains retryable.
type ExecutionDedupStore interface {
	// Begin claims one inbound message. window bounds how long a processing row
	// may stay live before another node is allowed to take it over.
	Begin(ctx context.Context, tenantID, channel, bindingID, messageID, traceID string, window time.Duration) (BeginResult, error)
	// Abort releases a processing claim after a failed execution. The trace
	// fence prevents one node from deleting a takeover owned by another.
	Abort(ctx context.Context, tenantID, channel, bindingID, messageID, traceID string) error
	// Renew extends a processing claim only when the same trace still owns it.
	Renew(ctx context.Context, tenantID, channel, bindingID, messageID, traceID string) error
}

// Claim is the dashboard-visible snapshot of one execution claim row.
type Claim struct {
	Channel   string    `json:"channel"`
	BindingID string    `json:"binding_id"`
	MessageID string    `json:"message_id"`
	Status    string    `json:"status"`
	TraceID   string    `json:"trace_id"`
	UpdatedAt time.Time `json:"updated_at"`
	AppCode   string    `json:"app_code,omitempty"`
}

// ClaimLister is the console read path implemented by the durable execution
// claim store. It is a separate optional interface so read-only surfaces
// depend on exactly what they need.
type ClaimLister interface {
	// ListClaims returns the most recent execution claims for one tenant and,
	// when appCode is non-empty, one application, newest first and capped at limit.
	ListClaims(ctx context.Context, tenantID, appCode string, limit int) ([]Claim, error)
}

// PostgresExecutionDedupStore persists execution claims in the messages table.
type PostgresExecutionDedupStore struct {
	database *sql.DB
}

// NewPostgresExecutionDedupStore constructs a durable claim store. The caller
// owns the database pool lifecycle.
func NewPostgresExecutionDedupStore(database *sql.DB) (*PostgresExecutionDedupStore, error) {
	if database == nil {
		return nil, fmt.Errorf("PostgreSQL database is required")
	}
	return &PostgresExecutionDedupStore{database: database}, nil
}

// Begin implements ExecutionDedupStore. The INSERT claim is atomic; on conflict
// a completed row reports ExecutionCompleted, a live processing row reports
// ExecutionInProgress, and a stale processing row is atomically taken over.
func (s *PostgresExecutionDedupStore) Begin(ctx context.Context, tenantID, channel, bindingID, messageID, traceID string, window time.Duration) (BeginResult, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := validateDedupArguments(tenantID, channel, bindingID, messageID, traceID); err != nil {
		return "", err
	}
	if window <= 0 {
		return "", fmt.Errorf("execution staleness window must be positive")
	}
	result, err := s.database.ExecContext(ctx, `
INSERT INTO messages (
    tenant_id, channel_type, binding_id, message_id, status, trace_id
) VALUES ($1, $2, $3, $4, 'processing', $5)
ON CONFLICT (tenant_id, channel_type, binding_id, message_id) DO NOTHING`,
		tenantID, channel, bindingID, messageID, traceID,
	)
	if err != nil {
		return "", fmt.Errorf("claim message execution: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("read execution claim result: %w", err)
	}
	if affected == 1 {
		return ExecutionFresh, nil
	}

	var status string
	var updatedAt time.Time
	err = s.database.QueryRowContext(ctx, `
SELECT status, updated_at
FROM messages
WHERE tenant_id = $1 AND channel_type = $2 AND binding_id = $3 AND message_id = $4`,
		tenantID, channel, bindingID, messageID,
	).Scan(&status, &updatedAt)
	if err == sql.ErrNoRows {
		// The row disappeared between the insert attempt and this read; the
		// claiming node may retry and claim fresh.
		return ExecutionInProgress, nil
	}
	if err != nil {
		return "", fmt.Errorf("read execution state: %w", err)
	}
	if status == "completed" {
		return ExecutionCompleted, nil
	}
	takeover, err := s.database.ExecContext(ctx, `
UPDATE messages
SET trace_id = $5, updated_at = NOW()
WHERE tenant_id = $1 AND channel_type = $2 AND binding_id = $3 AND message_id = $4
  AND status = 'processing'
  AND updated_at < NOW() - make_interval(secs => $6)`,
		tenantID, channel, bindingID, messageID, traceID, int64(window.Seconds()),
	)
	if err != nil {
		return "", fmt.Errorf("take over stale execution: %w", err)
	}
	taken, err := takeover.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("read execution takeover result: %w", err)
	}
	if taken == 1 {
		return ExecutionFresh, nil
	}
	return ExecutionInProgress, nil
}

// Abort removes a processing claim owned by traceID so a failed execution can
// be retried without being mistaken for a live duplicate.
func (s *PostgresExecutionDedupStore) Abort(ctx context.Context, tenantID, channel, bindingID, messageID, traceID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(channel) == "" || strings.TrimSpace(bindingID) == "" || strings.TrimSpace(messageID) == "" || strings.TrimSpace(traceID) == "" {
		return fmt.Errorf("abort execution claim requires tenant, channel, message, and trace IDs")
	}
	if _, err := s.database.ExecContext(ctx, `
DELETE FROM messages
WHERE tenant_id = $1 AND channel_type = $2 AND binding_id = $3 AND message_id = $4
  AND status = 'processing' AND trace_id = $5`,
		tenantID, channel, bindingID, messageID, traceID,
	); err != nil {
		return fmt.Errorf("abort message execution: %w", err)
	}
	return nil
}

// ListClaims implements ClaimLister for the durable store: newest updated
// claims first, capped at limit.
func (s *PostgresExecutionDedupStore) ListClaims(ctx context.Context, tenantID, appCode string, limit int) ([]Claim, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("claim list tenant ID is required")
	}
	appCode = strings.TrimSpace(appCode)
	if limit <= 0 {
		return nil, fmt.Errorf("claim list limit must be positive")
	}
	rows, err := s.database.QueryContext(ctx, `
SELECT m.channel_type, m.binding_id, m.message_id, m.status, m.trace_id, m.updated_at,
       COALESCE(r.app_code, t.app_code, '')
FROM messages m
LEFT JOIN inbound_message_routes r
  ON r.tenant_id=m.tenant_id AND r.channel_type=m.channel_type AND r.binding_id=m.binding_id AND r.message_id=m.message_id
LEFT JOIN execution_traces t
  ON t.tenant_id=m.tenant_id AND t.channel_type=m.channel_type AND t.binding_id=m.binding_id AND t.message_id=m.message_id
WHERE m.tenant_id = $1
  AND ($2 = '' OR COALESCE(r.app_code, t.app_code, '') = $2)
ORDER BY m.updated_at DESC, m.message_id
LIMIT $3`, tenantID, appCode, limit)
	if err != nil {
		return nil, fmt.Errorf("list execution claims: %w", err)
	}
	defer rows.Close()

	claims := make([]Claim, 0, limit)
	for rows.Next() {
		var claim Claim
		if err := rows.Scan(&claim.Channel, &claim.BindingID, &claim.MessageID, &claim.Status, &claim.TraceID, &claim.UpdatedAt, &claim.AppCode); err != nil {
			return nil, fmt.Errorf("scan execution claim: %w", err)
		}
		claims = append(claims, claim)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate execution claims: %w", err)
	}
	return claims, nil
}

func validateDedupArguments(tenantID, channel, bindingID, messageID, traceID string) error {
	if strings.TrimSpace(tenantID) == "" {
		return fmt.Errorf("execution claim tenant ID is required")
	}
	if strings.TrimSpace(channel) == "" || strings.TrimSpace(bindingID) == "" || strings.TrimSpace(messageID) == "" {
		return fmt.Errorf("execution claim channel, binding, and message IDs are required")
	}
	if strings.TrimSpace(traceID) == "" {
		return fmt.Errorf("execution claim trace ID is required")
	}
	return nil
}

// MemoryExecutionDedupStore is a deterministic in-process implementation for
// tests and local development. It is not durable.
type MemoryExecutionDedupStore struct {
	mu     sync.Mutex
	claims map[string]memoryExecutionClaim
	now    func() time.Time
}

type memoryExecutionClaim struct {
	status    string
	traceID   string
	updatedAt time.Time
}

// NewMemoryExecutionDedupStore constructs an isolated in-process claim store.
func NewMemoryExecutionDedupStore() *MemoryExecutionDedupStore {
	return &MemoryExecutionDedupStore{claims: make(map[string]memoryExecutionClaim), now: time.Now}
}

func memoryClaimKey(tenantID, channel, bindingID, messageID string) string {
	return tenantID + "\x00" + channel + "\x00" + bindingID + "\x00" + messageID
}

// Begin implements ExecutionDedupStore with wall-clock semantics matching the
// PostgreSQL implementation.
func (s *MemoryExecutionDedupStore) Begin(ctx context.Context, tenantID, channel, bindingID, messageID, traceID string, window time.Duration) (BeginResult, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := validateDedupArguments(tenantID, channel, bindingID, messageID, traceID); err != nil {
		return "", err
	}
	if window <= 0 {
		return "", fmt.Errorf("execution staleness window must be positive")
	}
	key := memoryClaimKey(tenantID, channel, bindingID, messageID)
	now := s.now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	claim, exists := s.claims[key]
	if !exists {
		s.claims[key] = memoryExecutionClaim{status: "processing", traceID: traceID, updatedAt: now}
		return ExecutionFresh, nil
	}
	if claim.status == "completed" {
		return ExecutionCompleted, nil
	}
	if claim.updatedAt.Add(window).After(now) {
		return ExecutionInProgress, nil
	}
	s.claims[key] = memoryExecutionClaim{status: "processing", traceID: traceID, updatedAt: now}
	return ExecutionFresh, nil
}

// Abort implements ExecutionDedupStore for the in-process store.
func (s *MemoryExecutionDedupStore) Abort(_ context.Context, tenantID, channel, bindingID, messageID, traceID string) error {
	key := memoryClaimKey(tenantID, channel, bindingID, messageID)
	s.mu.Lock()
	defer s.mu.Unlock()
	claim, exists := s.claims[key]
	if !exists || claim.traceID != traceID || claim.status != "processing" {
		return nil
	}
	delete(s.claims, key)
	return nil
}

// MarkCompleted flips a processing claim to completed. It exists for tests and
// mirrors the completion write performed transactionally by
// PostgresStateStore.RecordExecution.
func (s *MemoryExecutionDedupStore) MarkCompleted(tenantID, channel, bindingID, messageID string) {
	key := memoryClaimKey(tenantID, channel, bindingID, messageID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if claim, exists := s.claims[key]; exists {
		claim.status = "completed"
		s.claims[key] = claim
	}
}
