package storage

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
)

// RetryTracker stores the attempt count independently from an execution claim,
// so an aborted execution and a process restart do not reset the retry budget.
type RetryTracker interface {
	Increment(context.Context, string, string, string) (int, error)
	Clear(context.Context, string, string, string) error
}

// AttemptLister is the read path implemented by durable retry trackers. The
// platform console uses it to show how many delivery attempts one message has
// consumed without exposing the increment mutators.
type AttemptLister interface {
	// ListAttempts returns the current attempt count for one event, or zero
	// when no attempt was recorded.
	ListAttempts(ctx context.Context, tenantID, sessionKey, eventID string) (int, error)
}

type MemoryRetryTracker struct {
	mu       sync.Mutex
	attempts map[string]int
}

func NewMemoryRetryTracker() *MemoryRetryTracker {
	return &MemoryRetryTracker{attempts: map[string]int{}}
}
func retryKey(tenantID, sessionKey, eventID string) string {
	return tenantID + "\x00" + sessionKey + "\x00" + eventID
}
func (s *MemoryRetryTracker) Increment(_ context.Context, tenantID, sessionKey, eventID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := retryKey(tenantID, sessionKey, eventID)
	s.attempts[key]++
	return s.attempts[key], nil
}
func (s *MemoryRetryTracker) Clear(_ context.Context, tenantID, sessionKey, eventID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.attempts, retryKey(tenantID, sessionKey, eventID))
	return nil
}

// ListAttempts implements AttemptLister for the in-process tracker.
func (s *MemoryRetryTracker) ListAttempts(_ context.Context, tenantID, sessionKey, eventID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts[retryKey(tenantID, sessionKey, eventID)], nil
}

type PostgresRetryTracker struct{ database *sql.DB }

func NewPostgresRetryTracker(database *sql.DB) (*PostgresRetryTracker, error) {
	if database == nil {
		return nil, fmt.Errorf("PostgreSQL database is required")
	}
	return &PostgresRetryTracker{database: database}, nil
}
func (s *PostgresRetryTracker) Increment(ctx context.Context, tenantID, sessionKey, eventID string) (int, error) {
	var attempt int
	err := s.database.QueryRowContext(ctx, `
INSERT INTO message_retry_attempts (tenant_id, session_key, event_id, attempts)
VALUES ($1, $2, $3, 1)
ON CONFLICT (tenant_id, session_key, event_id) DO UPDATE SET attempts = message_retry_attempts.attempts + 1, updated_at = NOW()
RETURNING attempts`, tenantID, sessionKey, eventID).Scan(&attempt)
	if err != nil {
		return 0, fmt.Errorf("increment retry attempt: %w", err)
	}
	return attempt, nil
}
func (s *PostgresRetryTracker) Clear(ctx context.Context, tenantID, sessionKey, eventID string) error {
	if _, err := s.database.ExecContext(ctx, `DELETE FROM message_retry_attempts WHERE tenant_id=$1 AND session_key=$2 AND event_id=$3`, tenantID, sessionKey, eventID); err != nil {
		return fmt.Errorf("clear retry attempt: %w", err)
	}
	return nil
}

// ListAttempts implements AttemptLister for the durable tracker.
func (s *PostgresRetryTracker) ListAttempts(ctx context.Context, tenantID, sessionKey, eventID string) (int, error) {
	var attempts int
	err := s.database.QueryRowContext(ctx, `
SELECT attempts FROM message_retry_attempts
WHERE tenant_id = $1 AND session_key = $2 AND event_id = $3`,
		tenantID, sessionKey, eventID).Scan(&attempts)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("list retry attempts: %w", err)
	}
	return attempts, nil
}

var _ AttemptLister = (*PostgresRetryTracker)(nil)
var _ AttemptLister = (*MemoryRetryTracker)(nil)
