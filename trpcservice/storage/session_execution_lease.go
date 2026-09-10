package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrSessionExecutionLeaseLost = errors.New("session execution lease lost")

// SessionExecutionLease is the exclusive ownership proof for one session run.
// FencingToken increases whenever ownership moves to a new execution attempt.
type SessionExecutionLease struct {
	TenantID     string
	SessionKey   string
	OwnerID      string
	FencingToken uint64
	LeaseUntil   time.Time
}

// SessionExecutionLeaser serializes executions for one canonical Session and
// supplies the monotonic fence checked again by the final storage transaction.
type SessionExecutionLeaser interface {
	AcquireSessionExecutionLease(context.Context, string, string, string, time.Duration) (SessionExecutionLease, error)
	RenewSessionExecutionLease(context.Context, SessionExecutionLease, time.Duration) (SessionExecutionLease, error)
	ReleaseSessionExecutionLease(context.Context, SessionExecutionLease) error
}

func validateSessionExecutionLeaseInput(tenantID, sessionKey, ownerID string, ttl time.Duration) error {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(sessionKey) == "" || strings.TrimSpace(ownerID) == "" {
		return errors.New("session execution lease identity is incomplete")
	}
	if !strings.HasPrefix(sessionKey, tenantID+"/") {
		return errors.New("session execution lease is not tenant scoped")
	}
	if ttl <= 0 {
		return errors.New("session execution lease TTL must be positive")
	}
	return nil
}

func (s *MemoryStateStore) AcquireSessionExecutionLease(ctx context.Context, tenantID, sessionKey, ownerID string, ttl time.Duration) (SessionExecutionLease, error) {
	if err := validateSessionExecutionLeaseInput(tenantID, sessionKey, ownerID, ttl); err != nil {
		return SessionExecutionLease{}, err
	}
	key := tenantSessionKey(tenantID, sessionKey)
	for {
		if err := ctx.Err(); err != nil {
			return SessionExecutionLease{}, err
		}
		now := s.now().UTC()
		s.mu.Lock()
		current := s.sessionLeases[key]
		if current.FencingToken == 0 || !current.LeaseUntil.After(now) {
			current = SessionExecutionLease{
				TenantID: tenantID, SessionKey: sessionKey, OwnerID: ownerID,
				FencingToken: current.FencingToken + 1, LeaseUntil: now.Add(ttl),
			}
			s.sessionLeases[key] = current
			s.mu.Unlock()
			return current, nil
		}
		s.mu.Unlock()

		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return SessionExecutionLease{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *MemoryStateStore) RenewSessionExecutionLease(ctx context.Context, lease SessionExecutionLease, ttl time.Duration) (SessionExecutionLease, error) {
	if err := validateSessionExecutionLeaseInput(lease.TenantID, lease.SessionKey, lease.OwnerID, ttl); err != nil {
		return SessionExecutionLease{}, err
	}
	if err := ctx.Err(); err != nil {
		return SessionExecutionLease{}, err
	}
	key := tenantSessionKey(lease.TenantID, lease.SessionKey)
	now := s.now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.sessionLeases[key]
	if current.FencingToken != lease.FencingToken || current.OwnerID != lease.OwnerID || !current.LeaseUntil.After(now) {
		return SessionExecutionLease{}, ErrSessionExecutionLeaseLost
	}
	current.LeaseUntil = now.Add(ttl)
	s.sessionLeases[key] = current
	return current, nil
}

func (s *MemoryStateStore) ReleaseSessionExecutionLease(ctx context.Context, lease SessionExecutionLease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := tenantSessionKey(lease.TenantID, lease.SessionKey)
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.sessionLeases[key]
	if current.FencingToken != lease.FencingToken || current.OwnerID != lease.OwnerID {
		return ErrSessionExecutionLeaseLost
	}
	current.LeaseUntil = s.now().UTC()
	s.sessionLeases[key] = current
	return nil
}

func (s *PostgresStateStore) AcquireSessionExecutionLease(ctx context.Context, tenantID, sessionKey, ownerID string, ttl time.Duration) (SessionExecutionLease, error) {
	if err := validateSessionExecutionLeaseInput(tenantID, sessionKey, ownerID, ttl); err != nil {
		return SessionExecutionLease{}, err
	}
	ttlMilliseconds := ttl.Milliseconds()
	if ttlMilliseconds <= 0 {
		ttlMilliseconds = 1
	}
	for {
		var lease SessionExecutionLease
		lease.TenantID, lease.SessionKey, lease.OwnerID = tenantID, sessionKey, ownerID
		err := s.database.QueryRowContext(ctx, `
INSERT INTO session_execution_leases (tenant_id, session_key, owner_id, fencing_token, lease_until, updated_at)
VALUES ($1, $2, $3, 1, NOW() + ($4 * INTERVAL '1 millisecond'), NOW())
ON CONFLICT (tenant_id, session_key) DO UPDATE
SET owner_id = EXCLUDED.owner_id,
    fencing_token = session_execution_leases.fencing_token + 1,
    lease_until = NOW() + ($4 * INTERVAL '1 millisecond'),
    updated_at = NOW()
WHERE session_execution_leases.lease_until <= NOW()
RETURNING fencing_token, lease_until`, tenantID, sessionKey, ownerID, ttlMilliseconds).Scan(&lease.FencingToken, &lease.LeaseUntil)
		if err == nil {
			return lease, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return SessionExecutionLease{}, fmt.Errorf("acquire session execution lease: %w", err)
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return SessionExecutionLease{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *PostgresStateStore) RenewSessionExecutionLease(ctx context.Context, lease SessionExecutionLease, ttl time.Duration) (SessionExecutionLease, error) {
	if err := validateSessionExecutionLeaseInput(lease.TenantID, lease.SessionKey, lease.OwnerID, ttl); err != nil {
		return SessionExecutionLease{}, err
	}
	ttlMilliseconds := ttl.Milliseconds()
	if ttlMilliseconds <= 0 {
		ttlMilliseconds = 1
	}
	var renewed SessionExecutionLease
	renewed.TenantID, renewed.SessionKey, renewed.OwnerID, renewed.FencingToken = lease.TenantID, lease.SessionKey, lease.OwnerID, lease.FencingToken
	err := s.database.QueryRowContext(ctx, `
UPDATE session_execution_leases
SET lease_until = NOW() + ($5 * INTERVAL '1 millisecond'), updated_at = NOW()
WHERE tenant_id = $1 AND session_key = $2 AND owner_id = $3 AND fencing_token = $4
  AND lease_until > NOW()
RETURNING lease_until`, lease.TenantID, lease.SessionKey, lease.OwnerID, lease.FencingToken, ttlMilliseconds).Scan(&renewed.LeaseUntil)
	if errors.Is(err, sql.ErrNoRows) {
		return SessionExecutionLease{}, ErrSessionExecutionLeaseLost
	}
	if err != nil {
		return SessionExecutionLease{}, fmt.Errorf("renew session execution lease: %w", err)
	}
	return renewed, nil
}

func (s *PostgresStateStore) ReleaseSessionExecutionLease(ctx context.Context, lease SessionExecutionLease) error {
	result, err := s.database.ExecContext(ctx, `
UPDATE session_execution_leases
SET lease_until = NOW(), updated_at = NOW()
WHERE tenant_id = $1 AND session_key = $2 AND owner_id = $3 AND fencing_token = $4`,
		lease.TenantID, lease.SessionKey, lease.OwnerID, lease.FencingToken)
	if err != nil {
		return fmt.Errorf("release session execution lease: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read session execution lease release result: %w", err)
	}
	if rows != 1 {
		return ErrSessionExecutionLeaseLost
	}
	return nil
}

var _ SessionExecutionLeaser = (*MemoryStateStore)(nil)
var _ SessionExecutionLeaser = (*PostgresStateStore)(nil)
