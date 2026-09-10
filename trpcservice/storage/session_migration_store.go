package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/dbscope"
)

type SessionMigrationPhase string

const (
	SessionMigrationPrepared     SessionMigrationPhase = "prepared"
	SessionMigrationDualWrite    SessionMigrationPhase = "dual_write"
	SessionMigrationBackfill     SessionMigrationPhase = "backfill"
	SessionMigrationVerify       SessionMigrationPhase = "verify"
	SessionMigrationCutRead      SessionMigrationPhase = "cut_read"
	SessionMigrationStopOldWrite SessionMigrationPhase = "stop_old_write"
	SessionMigrationDone         SessionMigrationPhase = "done"
	SessionMigrationRolledBack   SessionMigrationPhase = "rolled_back"
)

var (
	ErrSessionMigrationNotFound = errors.New("Session migration not found")
	ErrSessionMigrationConflict = errors.New("Session migration conflict")
)

type SessionBackendRef struct {
	Driver        string `json:"driver"`
	ConnectionRef string `json:"connection_ref,omitempty"`
}

func (b SessionBackendRef) Normalize(defaultDriver string) SessionBackendRef {
	b.Driver = strings.ToLower(strings.TrimSpace(b.Driver))
	if b.Driver == "" {
		b.Driver = defaultDriver
	}
	b.ConnectionRef = strings.TrimSpace(b.ConnectionRef)
	return b
}

func (b SessionBackendRef) Equal(other SessionBackendRef) bool {
	return b.Driver == other.Driver && b.ConnectionRef == other.ConnectionRef
}

type SessionMigrationStatus struct {
	ID                 string                `json:"migration_id"`
	TenantID           string                `json:"tenant_id"`
	AppCode            string                `json:"app_code"`
	Generation         uint64                `json:"generation"`
	SourceProfileID    string                `json:"source_profile_id"`
	TargetProfileID    string                `json:"target_profile_id"`
	Source             SessionBackendRef     `json:"-"`
	Target             SessionBackendRef     `json:"-"`
	Phase              SessionMigrationPhase `json:"phase"`
	BackfilledSessions int                   `json:"backfilled_sessions"`
	VerifiedSessions   int                   `json:"verified_sessions"`
	LastError          string                `json:"last_error,omitempty"`
	CreatedAt          time.Time             `json:"created_at"`
	UpdatedAt          time.Time             `json:"updated_at"`
}

type SessionMigrationRoute struct {
	Reader  SessionBackendRef
	Writers []SessionBackendRef
}

func (s SessionMigrationStatus) Route() (SessionMigrationRoute, error) {
	switch s.Phase {
	case SessionMigrationPrepared, SessionMigrationRolledBack:
		return SessionMigrationRoute{Reader: s.Source, Writers: []SessionBackendRef{s.Source}}, nil
	case SessionMigrationDualWrite, SessionMigrationBackfill, SessionMigrationVerify:
		return SessionMigrationRoute{Reader: s.Source, Writers: []SessionBackendRef{s.Source, s.Target}}, nil
	case SessionMigrationCutRead:
		return SessionMigrationRoute{Reader: s.Target, Writers: []SessionBackendRef{s.Target, s.Source}}, nil
	case SessionMigrationStopOldWrite, SessionMigrationDone:
		return SessionMigrationRoute{Reader: s.Target, Writers: []SessionBackendRef{s.Target}}, nil
	default:
		return SessionMigrationRoute{}, fmt.Errorf("unsupported Session migration phase %q", s.Phase)
	}
}

type SessionMigrationStore interface {
	CreateSessionMigration(context.Context, SessionMigrationStatus) (SessionMigrationStatus, error)
	GetSessionMigration(context.Context, string, string, string) (SessionMigrationStatus, error)
	UpdateSessionMigration(context.Context, SessionMigrationStatus, uint64) (SessionMigrationStatus, error)
	ActiveSessionMigration(context.Context, string, string) (SessionMigrationStatus, bool, error)
	ListSessionMigrations(context.Context, string, string, int) ([]SessionMigrationStatus, error)
}

type SessionMigrationRouteSource interface {
	ActiveSessionMigrationRoute(context.Context, string, string) (SessionMigrationRoute, bool, error)
}

type PostgresSessionMigrationStore struct {
	database *sql.DB
}

func NewPostgresSessionMigrationStore(database *sql.DB) (*PostgresSessionMigrationStore, error) {
	if database == nil {
		return nil, errors.New("Session migration database is required")
	}
	return &PostgresSessionMigrationStore{database: database}, nil
}

func (s *PostgresSessionMigrationStore) CreateSessionMigration(ctx context.Context, status SessionMigrationStatus) (SessionMigrationStatus, error) {
	if strings.TrimSpace(status.TenantID) == "" || strings.TrimSpace(status.AppCode) == "" ||
		strings.TrimSpace(status.SourceProfileID) == "" || strings.TrimSpace(status.TargetProfileID) == "" ||
		status.Source.Driver == "" || status.Target.Driver == "" {
		return SessionMigrationStatus{}, errors.New("Session migration identity and backends are required")
	}
	if status.Source.Equal(status.Target) {
		return SessionMigrationStatus{}, errors.New("Session migration source and target must differ")
	}
	if status.ID == "" {
		status.ID = uuid.NewString()
	}
	status.Generation = 1
	status.Phase = SessionMigrationPrepared
	status.CreatedAt = time.Now().UTC()
	status.UpdatedAt = status.CreatedAt
	tx, err := dbscope.BeginTenantTransaction(ctx, s.database, status.TenantID)
	if err != nil {
		return SessionMigrationStatus{}, fmt.Errorf("begin Session migration: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", status.TenantID+"/"+status.AppCode+"/session-migration"); err != nil {
		return SessionMigrationStatus{}, fmt.Errorf("lock Session migration: %w", err)
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
  SELECT 1 FROM session_backend_migrations
  WHERE tenant_id=$1 AND app_code=$2 AND phase NOT IN ('done','rolled_back')
)`, status.TenantID, status.AppCode).Scan(&exists); err != nil {
		return SessionMigrationStatus{}, fmt.Errorf("check active Session migration: %w", err)
	}
	if exists {
		return SessionMigrationStatus{}, ErrSessionMigrationConflict
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO session_backend_migrations (
    migration_id, tenant_id, app_code, generation,
    source_profile_id, target_profile_id,
    source_driver, source_connection_ref, target_driver, target_connection_ref,
    phase, backfilled_sessions, verified_sessions, last_error, created_at, updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,0,0,'',$12,$12)`,
		status.ID, status.TenantID, status.AppCode, status.Generation,
		status.SourceProfileID, status.TargetProfileID,
		status.Source.Driver, status.Source.ConnectionRef, status.Target.Driver, status.Target.ConnectionRef,
		status.Phase, status.CreatedAt)
	if err != nil {
		return SessionMigrationStatus{}, fmt.Errorf("insert Session migration: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return SessionMigrationStatus{}, fmt.Errorf("commit Session migration: %w", err)
	}
	return status, nil
}

func (s *PostgresSessionMigrationStore) GetSessionMigration(ctx context.Context, tenantID, appCode, migrationID string) (SessionMigrationStatus, error) {
	tx, err := dbscope.BeginTenantTransaction(ctx, s.database, tenantID)
	if err != nil {
		return SessionMigrationStatus{}, err
	}
	defer func() { _ = tx.Rollback() }()
	status, err := scanSessionMigration(tx.QueryRowContext(ctx, `
SELECT migration_id, tenant_id, app_code, generation,
       source_profile_id, target_profile_id,
       source_driver, source_connection_ref, target_driver, target_connection_ref,
       phase, backfilled_sessions, verified_sessions, last_error, created_at, updated_at
FROM session_backend_migrations
WHERE tenant_id=$1 AND app_code=$2 AND migration_id=$3`, tenantID, appCode, migrationID))
	if err != nil {
		return SessionMigrationStatus{}, err
	}
	if err := tx.Commit(); err != nil {
		return SessionMigrationStatus{}, err
	}
	return status, nil
}

func (s *PostgresSessionMigrationStore) UpdateSessionMigration(ctx context.Context, status SessionMigrationStatus, expectedGeneration uint64) (SessionMigrationStatus, error) {
	if expectedGeneration == 0 || status.Generation != expectedGeneration {
		return SessionMigrationStatus{}, ErrSessionMigrationConflict
	}
	tx, err := dbscope.BeginTenantTransaction(ctx, s.database, status.TenantID)
	if err != nil {
		return SessionMigrationStatus{}, err
	}
	defer func() { _ = tx.Rollback() }()
	nextGeneration := expectedGeneration + 1
	updatedAt := time.Now().UTC()
	result, err := tx.ExecContext(ctx, `
UPDATE session_backend_migrations
SET generation=$1, phase=$2, backfilled_sessions=$3, verified_sessions=$4,
    last_error=$5, updated_at=$6
WHERE tenant_id=$7 AND app_code=$8 AND migration_id=$9 AND generation=$10`,
		nextGeneration, status.Phase, status.BackfilledSessions, status.VerifiedSessions,
		status.LastError, updatedAt, status.TenantID, status.AppCode, status.ID, expectedGeneration)
	if err != nil {
		return SessionMigrationStatus{}, fmt.Errorf("update Session migration: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return SessionMigrationStatus{}, err
	}
	if rows != 1 {
		return SessionMigrationStatus{}, ErrSessionMigrationConflict
	}
	if err := tx.Commit(); err != nil {
		return SessionMigrationStatus{}, err
	}
	status.Generation = nextGeneration
	status.UpdatedAt = updatedAt
	return status, nil
}

func (s *PostgresSessionMigrationStore) ActiveSessionMigration(ctx context.Context, tenantID, appCode string) (SessionMigrationStatus, bool, error) {
	tx, err := dbscope.BeginTenantTransaction(ctx, s.database, tenantID)
	if err != nil {
		return SessionMigrationStatus{}, false, err
	}
	defer func() { _ = tx.Rollback() }()
	status, err := scanSessionMigration(tx.QueryRowContext(ctx, `
SELECT migration_id, tenant_id, app_code, generation,
       source_profile_id, target_profile_id,
       source_driver, source_connection_ref, target_driver, target_connection_ref,
       phase, backfilled_sessions, verified_sessions, last_error, created_at, updated_at
FROM session_backend_migrations
WHERE tenant_id=$1 AND app_code=$2 AND phase NOT IN ('done','rolled_back')
ORDER BY updated_at DESC
LIMIT 1`, tenantID, appCode))
	if errors.Is(err, ErrSessionMigrationNotFound) {
		return SessionMigrationStatus{}, false, nil
	}
	if err != nil {
		return SessionMigrationStatus{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return SessionMigrationStatus{}, false, err
	}
	return status, true, nil
}

func (s *PostgresSessionMigrationStore) ListSessionMigrations(ctx context.Context, tenantID, appCode string, limit int) ([]SessionMigrationStatus, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(appCode) == "" {
		return nil, errors.New("Session migration list requires tenant and application")
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	tx, err := dbscope.BeginTenantTransaction(ctx, s.database, tenantID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `
SELECT migration_id, tenant_id, app_code, generation,
       source_profile_id, target_profile_id,
       source_driver, source_connection_ref, target_driver, target_connection_ref,
       phase, backfilled_sessions, verified_sessions, last_error, created_at, updated_at
FROM session_backend_migrations
WHERE tenant_id=$1 AND app_code=$2
ORDER BY updated_at DESC
LIMIT $3`, tenantID, appCode, limit)
	if err != nil {
		return nil, fmt.Errorf("list Session migrations: %w", err)
	}
	defer rows.Close()
	result := make([]SessionMigrationStatus, 0, limit)
	for rows.Next() {
		status, err := scanSessionMigration(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, status)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate Session migrations: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *PostgresSessionMigrationStore) ActiveSessionMigrationRoute(ctx context.Context, tenantID, appCode string) (SessionMigrationRoute, bool, error) {
	status, ok, err := s.ActiveSessionMigration(ctx, tenantID, appCode)
	if err != nil || !ok {
		return SessionMigrationRoute{}, ok, err
	}
	route, err := status.Route()
	return route, true, err
}

type sessionMigrationScanner interface{ Scan(...any) error }

func scanSessionMigration(row sessionMigrationScanner) (SessionMigrationStatus, error) {
	var status SessionMigrationStatus
	err := row.Scan(
		&status.ID, &status.TenantID, &status.AppCode, &status.Generation,
		&status.SourceProfileID, &status.TargetProfileID,
		&status.Source.Driver, &status.Source.ConnectionRef, &status.Target.Driver, &status.Target.ConnectionRef,
		&status.Phase, &status.BackfilledSessions, &status.VerifiedSessions, &status.LastError,
		&status.CreatedAt, &status.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return SessionMigrationStatus{}, ErrSessionMigrationNotFound
	}
	if err != nil {
		return SessionMigrationStatus{}, fmt.Errorf("scan Session migration: %w", err)
	}
	return status, nil
}

var _ SessionMigrationStore = (*PostgresSessionMigrationStore)(nil)
var _ SessionMigrationRouteSource = (*PostgresSessionMigrationStore)(nil)
