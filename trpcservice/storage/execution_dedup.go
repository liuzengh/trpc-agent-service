package storage

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
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
	Begin(ctx context.Context, tenantID, appCode, channel, bindingID, messageID, traceID string, window time.Duration) (BeginResult, error)
	// Abort releases a processing claim after a failed execution. The trace
	// fence prevents one node from deleting a takeover owned by another.
	Abort(ctx context.Context, tenantID, channel, bindingID, messageID, traceID string) error
	// Fail preserves a dashboard-visible terminal/intermediate failure while
	// keeping the message retryable. A later Begin may atomically transition a
	// failed claim back to processing under a new trace owner.
	Fail(ctx context.Context, tenantID, channel, bindingID, messageID, traceID string) error
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
func (s *PostgresExecutionDedupStore) Begin(ctx context.Context, tenantID, appCode, channel, bindingID, messageID, traceID string, window time.Duration) (BeginResult, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := validateBeginArguments(tenantID, appCode, channel, bindingID, messageID, traceID); err != nil {
		return "", err
	}
	if window <= 0 {
		return "", fmt.Errorf("execution staleness window must be positive")
	}
	result, err := s.database.ExecContext(ctx, `
INSERT INTO messages (
    tenant_id, app_code, channel_type, binding_id, message_id, status, trace_id
) VALUES ($1, $2, $3, $4, $5, 'processing', $6)
ON CONFLICT (tenant_id, channel_type, binding_id, message_id) DO NOTHING`,
		tenantID, appCode, channel, bindingID, messageID, traceID,
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

	var status, storedAppCode string
	var updatedAt time.Time
	err = s.database.QueryRowContext(ctx, `
SELECT app_code, status, updated_at
FROM messages
WHERE tenant_id = $1 AND channel_type = $2 AND binding_id = $3 AND message_id = $4`,
		tenantID, channel, bindingID, messageID,
	).Scan(&storedAppCode, &status, &updatedAt)
	if err == sql.ErrNoRows {
		// The row disappeared between the insert attempt and this read; the
		// claiming node may retry and claim fresh.
		return ExecutionInProgress, nil
	}
	if err != nil {
		return "", fmt.Errorf("read execution state: %w", err)
	}
	if storedAppCode != appCode {
		return "", fmt.Errorf("execution claim application mismatch: stored %q, requested %q", storedAppCode, appCode)
	}
	if status == "completed" {
		return ExecutionCompleted, nil
	}
	if status == "failed" {
		retry, err := s.database.ExecContext(ctx, `
UPDATE messages
SET status = 'processing', trace_id = $5, updated_at = NOW()
WHERE tenant_id = $1 AND channel_type = $2 AND binding_id = $3 AND message_id = $4
  AND status = 'failed'`,
			tenantID, channel, bindingID, messageID, traceID,
		)
		if err != nil {
			return "", fmt.Errorf("retry failed execution: %w", err)
		}
		claimed, err := retry.RowsAffected()
		if err != nil {
			return "", fmt.Errorf("read failed execution retry result: %w", err)
		}
		if claimed == 1 {
			return ExecutionFresh, nil
		}
		return ExecutionInProgress, nil
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

// Fail preserves the current owner's failed execution so the Console can show
// it. The trace fence prevents a stale owner from overwriting a takeover.
func (s *PostgresExecutionDedupStore) Fail(ctx context.Context, tenantID, channel, bindingID, messageID, traceID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateDedupArguments(tenantID, channel, bindingID, messageID, traceID); err != nil {
		return err
	}
	if _, err := s.database.ExecContext(ctx, `
UPDATE messages
SET status = 'failed', updated_at = NOW()
WHERE tenant_id = $1 AND channel_type = $2 AND binding_id = $3 AND message_id = $4
  AND status = 'processing' AND trace_id = $5`,
		tenantID, channel, bindingID, messageID, traceID,
	); err != nil {
		return fmt.Errorf("mark execution failed: %w", err)
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
       m.app_code
FROM messages m
WHERE m.tenant_id = $1
  AND ($2 = '' OR m.app_code = $2)
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

func validateBeginArguments(tenantID, appCode, channel, bindingID, messageID, traceID string) error {
	if strings.TrimSpace(appCode) == "" {
		return fmt.Errorf("execution claim application code is required")
	}
	return validateDedupArguments(tenantID, channel, bindingID, messageID, traceID)
}

// MemoryExecutionDedupStore is a deterministic in-process implementation for
// tests and local development. It is not durable.
type MemoryExecutionDedupStore struct {
	mu     sync.Mutex
	claims map[string]memoryExecutionClaim
	now    func() time.Time
}

type memoryExecutionClaim struct {
	appCode   string
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
func (s *MemoryExecutionDedupStore) Begin(ctx context.Context, tenantID, appCode, channel, bindingID, messageID, traceID string, window time.Duration) (BeginResult, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := validateBeginArguments(tenantID, appCode, channel, bindingID, messageID, traceID); err != nil {
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
		s.claims[key] = memoryExecutionClaim{appCode: appCode, status: "processing", traceID: traceID, updatedAt: now}
		return ExecutionFresh, nil
	}
	if claim.appCode != appCode {
		return "", fmt.Errorf("execution claim application mismatch: stored %q, requested %q", claim.appCode, appCode)
	}
	if claim.status == "completed" {
		return ExecutionCompleted, nil
	}
	if claim.status == "failed" {
		s.claims[key] = memoryExecutionClaim{appCode: appCode, status: "processing", traceID: traceID, updatedAt: now}
		return ExecutionFresh, nil
	}
	if claim.updatedAt.Add(window).After(now) {
		return ExecutionInProgress, nil
	}
	s.claims[key] = memoryExecutionClaim{appCode: appCode, status: "processing", traceID: traceID, updatedAt: now}
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

// Fail implements ExecutionDedupStore for the in-process store.
func (s *MemoryExecutionDedupStore) Fail(ctx context.Context, tenantID, channel, bindingID, messageID, traceID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateDedupArguments(tenantID, channel, bindingID, messageID, traceID); err != nil {
		return err
	}
	key := memoryClaimKey(tenantID, channel, bindingID, messageID)
	s.mu.Lock()
	defer s.mu.Unlock()
	claim, exists := s.claims[key]
	if !exists || claim.traceID != traceID || claim.status != "processing" {
		return nil
	}
	claim.status = "failed"
	claim.updatedAt = s.now().UTC()
	s.claims[key] = claim
	return nil
}

// ListClaims implements ClaimLister for tests and in-process development.
func (s *MemoryExecutionDedupStore) ListClaims(_ context.Context, tenantID, appCode string, limit int) ([]Claim, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("claim list tenant ID is required")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("claim list limit must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	claims := make([]Claim, 0, len(s.claims))
	for key, item := range s.claims {
		parts := strings.Split(key, "\x00")
		if len(parts) != 4 || parts[0] != tenantID || (appCode != "" && item.appCode != appCode) {
			continue
		}
		claims = append(claims, Claim{
			Channel: parts[1], BindingID: parts[2], MessageID: parts[3],
			Status: item.status, TraceID: item.traceID, UpdatedAt: item.updatedAt, AppCode: item.appCode,
		})
	}
	sort.Slice(claims, func(i, j int) bool {
		if claims[i].UpdatedAt.Equal(claims[j].UpdatedAt) {
			return claims[i].MessageID < claims[j].MessageID
		}
		return claims[i].UpdatedAt.After(claims[j].UpdatedAt)
	})
	if len(claims) > limit {
		claims = claims[:limit]
	}
	return claims, nil
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
