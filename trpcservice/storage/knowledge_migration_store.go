package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/dbscope"
)

var ErrKnowledgeMigrationConflict = errors.New("knowledge migration conflict")

type KnowledgeMigrationPhase string

const (
	KnowledgeMigrationPrepared   KnowledgeMigrationPhase = "prepared"
	KnowledgeMigrationReindexed  KnowledgeMigrationPhase = "reindexed"
	KnowledgeMigrationVerified   KnowledgeMigrationPhase = "verified"
	KnowledgeMigrationDone       KnowledgeMigrationPhase = "done"
	KnowledgeMigrationRolledBack KnowledgeMigrationPhase = "rolled_back"
)

type KnowledgeMigrationStatus struct {
	ID                 string                  `json:"migration_id"`
	TenantID           string                  `json:"tenant_id"`
	AppCode            string                  `json:"app_code"`
	Generation         uint64                  `json:"generation"`
	SourceProfileID    string                  `json:"source_profile_id"`
	TargetProfileID    string                  `json:"target_profile_id"`
	Source             config.BackendConfig    `json:"-"`
	Target             config.BackendConfig    `json:"-"`
	Phase              KnowledgeMigrationPhase `json:"phase"`
	ReindexedDocuments int                     `json:"reindexed_documents"`
	VerifiedDocuments  int                     `json:"verified_documents"`
	LastError          string                  `json:"last_error,omitempty"`
	CreatedAt          time.Time               `json:"created_at"`
	UpdatedAt          time.Time               `json:"updated_at"`
}

type KnowledgeMigrationStore interface {
	CreateKnowledgeMigration(context.Context, KnowledgeMigrationStatus) (KnowledgeMigrationStatus, error)
	GetKnowledgeMigration(context.Context, string, string, string) (KnowledgeMigrationStatus, error)
	UpdateKnowledgeMigration(context.Context, KnowledgeMigrationStatus, uint64) (KnowledgeMigrationStatus, error)
	ActiveKnowledgeMigration(context.Context, string, string) (KnowledgeMigrationStatus, bool, error)
	ListKnowledgeMigrations(context.Context, string, string, int) ([]KnowledgeMigrationStatus, error)
	WithKnowledgeMigrationLock(context.Context, string, string, func() error) error
}

type PostgresKnowledgeMigrationStore struct{ database *sql.DB }

func NewPostgresKnowledgeMigrationStore(database *sql.DB) (*PostgresKnowledgeMigrationStore, error) {
	if database == nil {
		return nil, errors.New("knowledge migration database is required")
	}
	return &PostgresKnowledgeMigrationStore{database: database}, nil
}

func (s *PostgresKnowledgeMigrationStore) CreateKnowledgeMigration(ctx context.Context, status KnowledgeMigrationStatus) (KnowledgeMigrationStatus, error) {
	if strings.TrimSpace(status.TenantID) == "" || strings.TrimSpace(status.AppCode) == "" {
		return KnowledgeMigrationStatus{}, errors.New("knowledge migration tenant and application are required")
	}
	status.ID = uuid.NewString()
	status.Generation = 1
	status.Phase = KnowledgeMigrationPrepared
	tx, err := dbscope.BeginTenantTransaction(ctx, s.database, status.TenantID)
	if err != nil {
		return KnowledgeMigrationStatus{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", "knowledge-migration:"+status.TenantID+"/"+status.AppCode); err != nil {
		return KnowledgeMigrationStatus{}, fmt.Errorf("lock knowledge migration creation: %w", err)
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
  SELECT 1 FROM knowledge_backend_migrations
  WHERE tenant_id=$1 AND app_code=$2 AND phase NOT IN ('done','rolled_back')
)`, status.TenantID, status.AppCode).Scan(&exists); err != nil {
		return KnowledgeMigrationStatus{}, fmt.Errorf("check active knowledge migration: %w", err)
	}
	if exists {
		return KnowledgeMigrationStatus{}, ErrKnowledgeMigrationConflict
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO knowledge_backend_migrations (
    migration_id, tenant_id, app_code, generation,
    source_profile_id, target_profile_id,
    source_driver, source_connection_ref, target_driver, target_connection_ref, phase
) VALUES ($1,$2,$3,1,$4,$5,$6,$7,$8,$9,'prepared')`,
		status.ID, status.TenantID, status.AppCode,
		status.SourceProfileID, status.TargetProfileID,
		status.Source.Driver, status.Source.ConnectionRef, status.Target.Driver, status.Target.ConnectionRef)
	if err != nil {
		return KnowledgeMigrationStatus{}, fmt.Errorf("create knowledge migration: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return KnowledgeMigrationStatus{}, err
	}
	return s.GetKnowledgeMigration(ctx, status.TenantID, status.AppCode, status.ID)
}

func (s *PostgresKnowledgeMigrationStore) GetKnowledgeMigration(ctx context.Context, tenantID, appCode, id string) (KnowledgeMigrationStatus, error) {
	tx, err := dbscope.BeginTenantTransaction(ctx, s.database, tenantID)
	if err != nil {
		return KnowledgeMigrationStatus{}, err
	}
	defer func() { _ = tx.Rollback() }()
	status, err := scanKnowledgeMigration(tx.QueryRowContext(ctx, `
SELECT migration_id, tenant_id, app_code, generation,
       source_profile_id, target_profile_id,
       source_driver, source_connection_ref, target_driver, target_connection_ref,
       phase, reindexed_documents, verified_documents, last_error, created_at, updated_at
FROM knowledge_backend_migrations
WHERE tenant_id=$1 AND app_code=$2 AND migration_id=$3`, tenantID, appCode, id))
	if err != nil {
		return KnowledgeMigrationStatus{}, err
	}
	if err := tx.Commit(); err != nil {
		return KnowledgeMigrationStatus{}, err
	}
	return status, nil
}

func (s *PostgresKnowledgeMigrationStore) UpdateKnowledgeMigration(ctx context.Context, status KnowledgeMigrationStatus, expected uint64) (KnowledgeMigrationStatus, error) {
	tx, err := dbscope.BeginTenantTransaction(ctx, s.database, status.TenantID)
	if err != nil {
		return KnowledgeMigrationStatus{}, err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `
UPDATE knowledge_backend_migrations
SET generation=generation+1, phase=$5, reindexed_documents=$6,
    verified_documents=$7, last_error=$8, updated_at=NOW()
WHERE tenant_id=$1 AND app_code=$2 AND migration_id=$3 AND generation=$4`,
		status.TenantID, status.AppCode, status.ID, expected, status.Phase,
		status.ReindexedDocuments, status.VerifiedDocuments, status.LastError)
	if err != nil {
		return KnowledgeMigrationStatus{}, err
	}
	affected, err := result.RowsAffected()
	if err != nil || affected != 1 {
		return KnowledgeMigrationStatus{}, ErrKnowledgeMigrationConflict
	}
	if err := tx.Commit(); err != nil {
		return KnowledgeMigrationStatus{}, err
	}
	return s.GetKnowledgeMigration(ctx, status.TenantID, status.AppCode, status.ID)
}

func (s *PostgresKnowledgeMigrationStore) ActiveKnowledgeMigration(ctx context.Context, tenantID, appCode string) (KnowledgeMigrationStatus, bool, error) {
	tx, err := dbscope.BeginTenantTransaction(ctx, s.database, tenantID)
	if err != nil {
		return KnowledgeMigrationStatus{}, false, err
	}
	defer func() { _ = tx.Rollback() }()
	status, err := scanKnowledgeMigration(tx.QueryRowContext(ctx, `
SELECT migration_id, tenant_id, app_code, generation,
       source_profile_id, target_profile_id,
       source_driver, source_connection_ref, target_driver, target_connection_ref,
       phase, reindexed_documents, verified_documents, last_error, created_at, updated_at
FROM knowledge_backend_migrations
WHERE tenant_id=$1 AND app_code=$2 AND phase NOT IN ('done','rolled_back')
ORDER BY updated_at DESC LIMIT 1`, tenantID, appCode))
	if errors.Is(err, sql.ErrNoRows) {
		return KnowledgeMigrationStatus{}, false, nil
	}
	if err != nil {
		return KnowledgeMigrationStatus{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return KnowledgeMigrationStatus{}, false, err
	}
	return status, true, nil
}

func (s *PostgresKnowledgeMigrationStore) ListKnowledgeMigrations(ctx context.Context, tenantID, appCode string, limit int) ([]KnowledgeMigrationStatus, error) {
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
       phase, reindexed_documents, verified_documents, last_error, created_at, updated_at
FROM knowledge_backend_migrations
WHERE tenant_id=$1 AND app_code=$2 ORDER BY updated_at DESC LIMIT $3`, tenantID, appCode, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]KnowledgeMigrationStatus, 0, limit)
	for rows.Next() {
		status, err := scanKnowledgeMigration(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, status)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

// WithKnowledgeMigrationLock serializes a long-running reindex/verify step
// across Gateway replicas without keeping a SQL transaction open.
func (s *PostgresKnowledgeMigrationStore) WithKnowledgeMigrationLock(ctx context.Context, tenantID, appCode string, operation func() error) error {
	if operation == nil {
		return errors.New("knowledge migration operation is required")
	}
	connection, err := s.database.Conn(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	key := "knowledge-migration:" + tenantID + "/" + appCode
	if _, err := connection.ExecContext(ctx, `SELECT pg_advisory_lock(hashtextextended($1, 0))`, key); err != nil {
		return fmt.Errorf("lock knowledge migration: %w", err)
	}
	defer func() {
		_, _ = connection.ExecContext(context.Background(), `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, key)
	}()
	return operation()
}

type knowledgeMigrationScanner interface{ Scan(...any) error }

func scanKnowledgeMigration(scanner knowledgeMigrationScanner) (KnowledgeMigrationStatus, error) {
	var status KnowledgeMigrationStatus
	if err := scanner.Scan(
		&status.ID, &status.TenantID, &status.AppCode, &status.Generation,
		&status.SourceProfileID, &status.TargetProfileID,
		&status.Source.Driver, &status.Source.ConnectionRef, &status.Target.Driver, &status.Target.ConnectionRef,
		&status.Phase, &status.ReindexedDocuments, &status.VerifiedDocuments, &status.LastError,
		&status.CreatedAt, &status.UpdatedAt,
	); err != nil {
		return KnowledgeMigrationStatus{}, err
	}
	return status, nil
}

var _ KnowledgeMigrationStore = (*PostgresKnowledgeMigrationStore)(nil)
