package platform

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"
)

type SessionExecutionLease struct {
	FencingToken uint64
	Lost         <-chan struct{}
	release      func()
}

func (l SessionExecutionLease) Release() {
	if l.release != nil {
		l.release()
	}
}

type SessionLeaseManager interface {
	Acquire(context.Context, string, string) (SessionExecutionLease, error)
	Close() error
}

type PostgresSessionLeaseManager struct {
	db            *sql.DB
	owner         string
	ttl           time.Duration
	renewInterval time.Duration
}

func NewPostgresSessionLeaseManager(dsn, owner string, timing ...time.Duration) (*PostgresSessionLeaseManager, error) {
	if dsn == "" || owner == "" {
		return nil, errors.New("PostgreSQL DSN and Gateway owner are required")
	}
	ttl, renewInterval := 30*time.Second, 10*time.Second
	if len(timing) != 0 {
		if len(timing) != 2 || timing[0] <= 0 || timing[1] <= 0 || timing[1] >= timing[0] {
			return nil, errors.New("Session Execution Lease timing is invalid")
		}
		ttl, renewInterval = timing[0], timing[1]
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, err
	}
	return &PostgresSessionLeaseManager{db: db, owner: owner, ttl: ttl, renewInterval: renewInterval}, nil
}

func (m *PostgresSessionLeaseManager) Acquire(ctx context.Context, tenantID, sessionID string) (SessionExecutionLease, error) {
	if tenantID == "" || sessionID == "" {
		return SessionExecutionLease{}, errors.New("invalid Session Execution Lease scope")
	}
	for {
		now := time.Now().UTC()
		var token uint64
		err := m.db.QueryRowContext(ctx, `INSERT INTO session_execution_leases(tenant_id, session_id, owner_id, fencing_token, expires_at)
			VALUES ($1,$2,$3,1,$4)
			ON CONFLICT(tenant_id, session_id) DO UPDATE SET owner_id=excluded.owner_id,
			fencing_token=session_execution_leases.fencing_token+1, expires_at=excluded.expires_at
			WHERE session_execution_leases.expires_at <= $5
			RETURNING fencing_token`, tenantID, sessionID, m.owner, now.Add(m.ttl), now).Scan(&token)
		if err == nil {
			return m.maintain(ctx, tenantID, sessionID, token), nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return SessionExecutionLease{}, fmt.Errorf("acquire Session Execution Lease: %w", err)
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

func (m *PostgresSessionLeaseManager) maintain(parent context.Context, tenantID, sessionID string, token uint64) SessionExecutionLease {
	lost := make(chan struct{})
	ctx, cancel := context.WithCancel(parent)
	var once sync.Once
	release := func() {
		once.Do(func() {
			cancel()
			releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer releaseCancel()
			_, _ = m.db.ExecContext(releaseCtx, `UPDATE session_execution_leases SET expires_at=$1 WHERE tenant_id=$2 AND session_id=$3 AND owner_id=$4 AND fencing_token=$5`, time.Unix(0, 0).UTC(), tenantID, sessionID, m.owner, token)
		})
	}
	go func() {
		defer close(lost)
		ticker := time.NewTicker(m.renewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				result, err := m.db.ExecContext(ctx, `UPDATE session_execution_leases SET expires_at=$1 WHERE tenant_id=$2 AND session_id=$3 AND owner_id=$4 AND fencing_token=$5`, time.Now().UTC().Add(m.ttl), tenantID, sessionID, m.owner, token)
				if err != nil {
					return
				}
				rows, _ := result.RowsAffected()
				if rows != 1 {
					return
				}
			}
		}
	}()
	return SessionExecutionLease{FencingToken: token, Lost: lost, release: release}
}

func (m *PostgresSessionLeaseManager) Close() error { return m.db.Close() }
