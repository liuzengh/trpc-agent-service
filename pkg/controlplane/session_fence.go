package controlplane

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/fence"
)

const sessionFenceCleanupTimeout = 5 * time.Second

// PostgresSessionFence is the backend-native fence used by production
// Session/Memory adapters. It pins one database connection, takes a
// session-level advisory lock, validates the current guard, and keeps the
// lock alive until the delegated backend operation returns. StartWithRequest
// uses the matching transaction-scoped lock, so generation admission and
// every fenced operation have one total order.
type PostgresSessionFence struct {
	db               *sql.DB
	minRenewInterval time.Duration
	maxRenewInterval time.Duration
}

// NewPostgresSessionFence uses the dedicated execution pool shared with
// ExecutionRecorder. It must point at the same PostgreSQL authority as the
// metadata and migration gate pools, but must not share their pool capacity:
// the delegated Session operation can need both while holding this fence.
func NewPostgresSessionFence(db *sql.DB) *PostgresSessionFence {
	return &PostgresSessionFence{
		db:               db,
		minRenewInterval: 100 * time.Millisecond,
		maxRenewInterval: time.Minute,
	}
}

// Acquire implements fence.Authorizer.
func (f *PostgresSessionFence) Acquire(ctx context.Context, token fence.Token) (func() error, error) {
	if f == nil || f.db == nil {
		return nil, fmt.Errorf("%w: control-plane database is unavailable", fence.ErrMismatch)
	}
	if err := token.Validate(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	conn, err := f.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire session fence connection: %w", err)
	}
	if _, err := conn.ExecContext(ctx,
		`SELECT pg_advisory_lock(hashtextextended($1, 0))`, token.Scope()); err != nil {
		// The server may have acquired the session-level lock before the
		// client observed a transport or cancellation error. A normal Close
		// would return that physical connection to the pool with unknown
		// session state, so discard it unconditionally.
		discardErr := discardPostgresConn(conn)
		return nil, errors.Join(fmt.Errorf("acquire session fence lock: %w", err), discardErr)
	}

	var (
		status       string
		generation   int64
		currentID    sql.NullInt64
		executionTok sql.NullString
		executionSt  sql.NullString
		leaseUntil   sql.NullTime
		heartbeatAt  time.Time
		leaseValid   bool
	)
	err = conn.QueryRowContext(ctx, `
		SELECT g.status, g.generation, g.current_execution_id,
		       e.execution_token, e.status, e.lease_until, e.heartbeat_at,
		       (e.lease_until > clock_timestamp())
		FROM session_execution_guards g
		LEFT JOIN execution_records e ON e.id=g.current_execution_id
		WHERE g.tenant_id=$1 AND g.agent_app_id=$2 AND g.session_id=$3`,
		token.TenantID, token.AgentAppID, token.SessionID).Scan(
		&status, &generation, &currentID, &executionTok, &executionSt, &leaseUntil, &heartbeatAt, &leaseValid,
	)
	if err != nil {
		cleanupErr := unlockAndClosePostgresConn(conn, token.Scope())
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.Join(
				fmt.Errorf("%w: session guard is missing", fence.ErrMismatch), cleanupErr,
			)
		}
		return nil, errors.Join(fmt.Errorf("check session fence: %w", err), cleanupErr)
	}
	if status != "RUNNING" || generation != token.Generation ||
		!currentID.Valid || currentID.Int64 != token.ExecutionID ||
		!executionTok.Valid || executionTok.String != token.Value ||
		!executionSt.Valid || executionSt.String != "RUNNING" ||
		!leaseUntil.Valid || !leaseValid {
		cleanupErr := unlockAndClosePostgresConn(conn, token.Scope())
		return nil, errors.Join(
			fmt.Errorf("%w: session generation=%d execution=%d status=%s",
				fence.ErrMismatch, generation, currentID.Int64, status),
			cleanupErr,
		)
	}

	leaseTTL, err := executionLeaseTTL(leaseUntil.Time, heartbeatAt)
	if err != nil {
		cleanupErr := unlockAndClosePostgresConn(conn, token.Scope())
		return nil, errors.Join(err, cleanupErr)
	}
	minRenewInterval := f.minRenewInterval
	if minRenewInterval <= 0 {
		minRenewInterval = 100 * time.Millisecond
	}
	maxRenewInterval := f.maxRenewInterval
	if maxRenewInterval <= 0 {
		maxRenewInterval = time.Minute
	}
	renewInterval := leaseTTL / 3
	if renewInterval < minRenewInterval {
		renewInterval = minRenewInterval
	}
	if renewInterval > maxRenewInterval {
		renewInterval = maxRenewInterval
	}

	// Session-level advisory locks are connection-owned. Serialize keepalive,
	// final validation, unlock, and close on this connection so a renewal can
	// never race release and accidentally renew a released authority.
	connMu := new(sync.Mutex)
	stopRenew := make(chan struct{})
	renewDone := make(chan struct{})
	var lostMu sync.Mutex
	var lostErr error
	setLost := func(err error) {
		if err == nil {
			err = fence.ErrLeaseLost
		}
		lostMu.Lock()
		if lostErr == nil {
			lostErr = errors.Join(fence.ErrLeaseLost, err)
		}
		lostMu.Unlock()
	}
	getLost := func() error {
		lostMu.Lock()
		defer lostMu.Unlock()
		return lostErr
	}
	go func() {
		defer close(renewDone)
		ticker := time.NewTicker(renewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopRenew:
				return
			case <-ticker.C:
				renewCtx, cancel := context.WithTimeout(context.Background(), minDuration(renewInterval/2, 5*time.Second))
				connMu.Lock()
				result, renewErr := conn.ExecContext(renewCtx, `
					UPDATE execution_records
					SET heartbeat_at=clock_timestamp(),
					    lease_until=clock_timestamp() + ($3 * INTERVAL '1 millisecond')
					WHERE id=$1 AND tenant_id=$2 AND execution_token=$4
					  AND status='RUNNING' AND lease_until > clock_timestamp()`,
					token.ExecutionID, token.TenantID, leaseTTL.Milliseconds(), token.Value)
				connMu.Unlock()
				cancel()
				if renewErr != nil {
					setLost(renewErr)
					return
				}
				rows, rowsErr := result.RowsAffected()
				if rowsErr != nil || rows != 1 {
					if rowsErr == nil {
						rowsErr = fmt.Errorf("renewed %d records", rows)
					}
					setLost(rowsErr)
					return
				}
			}
		}
	}()

	var once sync.Once
	var releaseResult error
	release := func() error {
		once.Do(func() {
			var releaseErr error
			close(stopRenew)
			<-renewDone
			connMu.Lock()
			if err := getLost(); err != nil {
				releaseErr = err
			} else {
				// Re-check under the advisory lock. This catches an expired or
				// replaced execution even when no keepalive tick happened after
				// the backend operation returned.
				var finalStatus string
				var finalGeneration int64
				var finalExecutionID sql.NullInt64
				var finalToken sql.NullString
				var finalLease sql.NullTime
				var finalLeaseValid bool
				finalCtx, cancelFinal := context.WithTimeout(context.Background(), sessionFenceCleanupTimeout)
				if err := conn.QueryRowContext(finalCtx, `
					SELECT g.status, g.generation, g.current_execution_id,
					       e.execution_token, e.lease_until,
					       (e.lease_until > clock_timestamp())
					FROM session_execution_guards g
					LEFT JOIN execution_records e ON e.id=g.current_execution_id
					WHERE g.tenant_id=$1 AND g.agent_app_id=$2 AND g.session_id=$3`,
					token.TenantID, token.AgentAppID, token.SessionID).Scan(
					&finalStatus, &finalGeneration, &finalExecutionID, &finalToken, &finalLease, &finalLeaseValid,
				); err != nil {
					releaseErr = fmt.Errorf("%w: final session fence check: %v", fence.ErrLeaseLost, err)
				} else if finalStatus != "RUNNING" || finalGeneration != token.Generation ||
					!finalExecutionID.Valid || finalExecutionID.Int64 != token.ExecutionID ||
					!finalToken.Valid || finalToken.String != token.Value ||
					!finalLease.Valid || !finalLeaseValid {
					releaseErr = fmt.Errorf("%w: final session fence check rejected execution %d", fence.ErrLeaseLost, token.ExecutionID)
				}
				cancelFinal()
			}
			releaseErr = errors.Join(releaseErr, unlockAndClosePostgresConn(conn, token.Scope()))
			connMu.Unlock()
			releaseResult = releaseErr
		})
		return releaseResult
	}
	return release, nil
}

func executionLeaseTTL(leaseUntil, heartbeatAt time.Time) (time.Duration, error) {
	leaseTTL := leaseUntil.Sub(heartbeatAt)
	if leaseTTL < time.Minute || leaseTTL > 24*time.Hour {
		return 0, fmt.Errorf("%w: execution lease window %s is outside 1m..24h",
			fence.ErrMismatch, leaseTTL)
	}
	return leaseTTL, nil
}

// unlockAndClosePostgresConn removes the exact session-level lock before the
// connection can re-enter database/sql's pool. If PostgreSQL cannot prove that
// this caller released a held lock, discard the physical connection so its
// unknown session state cannot poison later requests.
func unlockAndClosePostgresConn(conn *sql.Conn, scope string) error {
	if conn == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), sessionFenceCleanupTimeout)
	defer cancel()
	var unlocked bool
	err := conn.QueryRowContext(ctx,
		`SELECT pg_advisory_unlock(hashtextextended($1, 0))`, scope).Scan(&unlocked)
	if err == nil && unlocked {
		return conn.Close()
	}
	if err == nil {
		err = errors.New("PostgreSQL reported that the session fence lock was not held")
	}
	return errors.Join(fmt.Errorf("release session fence lock: %w", err), discardPostgresConn(conn))
}

func discardPostgresConn(conn *sql.Conn) error {
	if conn == nil {
		return nil
	}
	err := conn.Raw(func(any) error { return driver.ErrBadConn })
	if errors.Is(err, driver.ErrBadConn) {
		err = nil
	}
	closeErr := conn.Close()
	if errors.Is(closeErr, sql.ErrConnDone) {
		closeErr = nil
	}
	return errors.Join(err, closeErr)
}

func minDuration(a, b time.Duration) time.Duration {
	if a <= 0 {
		return time.Millisecond
	}
	if a < b {
		return a
	}
	return b
}

var _ fence.Authorizer = (*PostgresSessionFence)(nil)
