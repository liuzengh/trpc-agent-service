package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	platformartifact "github.com/liuzengh/trpc-agent-service/trpcservice/artifact"
)

var _ platformartifact.MetadataStore = (*Store)(nil)

// ReserveArtifact atomically allocates one immutable artifact version before
// an object is uploaded. Pending reservations never authorize reads.
func (s *Store) ReserveArtifact(
	ctx context.Context,
	access platformartifact.Access,
	filename string,
	mimeType string,
	size int64,
) (platformartifact.Record, error) {
	if err := s.validate(); err != nil {
		return platformartifact.Record{}, err
	}
	if err := validateArtifactAccess(access, filename); err != nil {
		return platformartifact.Record{}, err
	}
	if size < 0 {
		return platformartifact.Record{}, errors.New("artifact size must not be negative")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return platformartifact.Record{}, fmt.Errorf("begin reserve artifact: %w", err)
	}
	defer func() { rollback(tx) }()
	lockKey := strings.Join([]string{
		access.Scope.TenantID, access.Scope.AppID, access.SessionPrincipalID, access.SessionID, filename,
	}, "\x1f")
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockKey); err != nil {
		return platformartifact.Record{}, fmt.Errorf("lock artifact version: %w", err)
	}
	var version int
	if err := tx.QueryRow(ctx, `
SELECT COALESCE(MAX(version), -1) + 1
FROM platform.artifact
WHERE tenant_id = $1 AND app_id = $2
  AND session_principal_id = $3 AND session_id = $4 AND filename = $5`,
		access.Scope.TenantID, access.Scope.AppID, access.SessionPrincipalID, access.SessionID, filename,
	).Scan(&version); err != nil {
		return platformartifact.Record{}, fmt.Errorf("reserve artifact version: %w", err)
	}
	record := platformartifact.Record{
		ID:                 uuid.NewString(),
		TenantID:           access.Scope.TenantID,
		AppID:              access.Scope.AppID,
		ConfigVersion:      access.ConfigVersion,
		SessionPrincipalID: access.SessionPrincipalID,
		SessionID:          access.SessionID,
		Filename:           filename,
		Version:            version,
		MIMEType:           mimeType,
		Size:               size,
		Status:             platformartifact.StatusPending,
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO platform.artifact (
    artifact_id, tenant_id, app_id, session_principal_id, session_id,
    filename, version, object_key, mime_type, size_bytes, status, config_version
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		record.ID,
		record.TenantID,
		record.AppID,
		record.SessionPrincipalID,
		record.SessionID,
		record.Filename,
		record.Version,
		"",
		record.MIMEType,
		record.Size,
		record.Status,
		record.ConfigVersion,
	); err != nil {
		return platformartifact.Record{}, fmt.Errorf("reserve artifact metadata: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return platformartifact.Record{}, fmt.Errorf("commit reserve artifact: %w", err)
	}
	return record, nil
}

// BindArtifactObject associates a pending reservation with its deterministic
// storage key before any object write occurs.
func (s *Store) BindArtifactObject(ctx context.Context, record platformartifact.Record, objectKey string) error {
	if err := s.validate(); err != nil {
		return err
	}
	if record.ID == "" || objectKey == "" {
		return errors.New("artifact reservation and object key are required")
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE platform.artifact
SET object_key = $2, updated_at = clock_timestamp()
WHERE artifact_id = $1 AND status = 'PENDING' AND object_key = ''`, record.ID, objectKey)
	if err != nil {
		return fmt.Errorf("bind artifact object key: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("artifact reservation is not pending")
	}
	return nil
}

// PublishArtifact makes a successfully uploaded pending artifact readable.
func (s *Store) PublishArtifact(ctx context.Context, record platformartifact.Record) error {
	return s.setReservedArtifactStatus(ctx, record, platformartifact.StatusAvailable)
}

// AbandonArtifact permanently closes a failed reservation. It never makes an
// object readable and is safe to repeat after a caller-side failure.
func (s *Store) AbandonArtifact(ctx context.Context, record platformartifact.Record) error {
	return s.setReservedArtifactStatus(ctx, record, platformartifact.StatusDeleted)
}

func (s *Store) setReservedArtifactStatus(ctx context.Context, record platformartifact.Record, status platformartifact.Status) error {
	if err := s.validate(); err != nil {
		return err
	}
	if record.ID == "" || (status != platformartifact.StatusAvailable && status != platformartifact.StatusDeleted) {
		return errors.New("artifact reservation transition is invalid")
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE platform.artifact
SET status = $2, cleanup_next_attempt_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE artifact_id = $1 AND status = 'PENDING'`, record.ID, status)
	if err != nil {
		return fmt.Errorf("transition artifact reservation: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("artifact reservation is not pending")
	}
	return nil
}

// FindArtifact returns the requested available version, or the latest
// available version when version is nil.
func (s *Store) FindArtifact(
	ctx context.Context,
	access platformartifact.Access,
	filename string,
	version *int,
) (platformartifact.Record, error) {
	if err := s.validate(); err != nil {
		return platformartifact.Record{}, err
	}
	if err := validateArtifactAccess(access, filename); err != nil {
		return platformartifact.Record{}, err
	}
	if version != nil && *version < 0 {
		return platformartifact.Record{}, errors.New("artifact version must not be negative")
	}
	var requestedVersion any
	if version != nil {
		requestedVersion = *version
	}
	var record platformartifact.Record
	err := s.pool.QueryRow(ctx, `
SELECT artifact_id, tenant_id, app_id, session_principal_id, session_id,
       filename, version, object_key, mime_type, size_bytes, status, config_version,
       created_at, updated_at
FROM platform.artifact
WHERE tenant_id = $1 AND app_id = $2
  AND session_principal_id = $3 AND session_id = $4
  AND filename = $5 AND config_version = $6
  AND ($7::integer IS NULL OR version = $7)
  AND status = 'AVAILABLE'
ORDER BY version DESC
LIMIT 1`,
		access.Scope.TenantID,
		access.Scope.AppID,
		access.SessionPrincipalID,
		access.SessionID,
		filename,
		access.ConfigVersion,
		requestedVersion,
	).Scan(
		&record.ID,
		&record.TenantID,
		&record.AppID,
		&record.SessionPrincipalID,
		&record.SessionID,
		&record.Filename,
		&record.Version,
		&record.ObjectKey,
		&record.MIMEType,
		&record.Size,
		&record.Status,
		&record.ConfigVersion,
		&record.CreatedAt,
		&record.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return platformartifact.Record{}, artifactNotFoundError()
	}
	if err != nil {
		return platformartifact.Record{}, fmt.Errorf("find artifact metadata: %w", err)
	}
	return record, nil
}

// ListArtifactKeys returns available artifact filenames in stable order.
func (s *Store) ListArtifactKeys(ctx context.Context, access platformartifact.Access) ([]string, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if err := validateArtifactAccess(access, "artifact"); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
SELECT filename
FROM platform.artifact
WHERE tenant_id = $1 AND app_id = $2
  AND session_principal_id = $3 AND session_id = $4
  AND config_version = $5 AND status = 'AVAILABLE'
GROUP BY filename
ORDER BY filename`,
		access.Scope.TenantID,
		access.Scope.AppID,
		access.SessionPrincipalID,
		access.SessionID,
		access.ConfigVersion,
	)
	if err != nil {
		return nil, fmt.Errorf("list artifact metadata keys: %w", err)
	}
	defer rows.Close()

	var filenames []string
	for rows.Next() {
		var filename string
		if err := rows.Scan(&filename); err != nil {
			return nil, fmt.Errorf("scan artifact metadata key: %w", err)
		}
		filenames = append(filenames, filename)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list artifact metadata keys: %w", err)
	}
	return filenames, nil
}

// ListArtifactVersions returns available versions in ascending order.
func (s *Store) ListArtifactVersions(
	ctx context.Context,
	access platformartifact.Access,
	filename string,
) ([]int, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if err := validateArtifactAccess(access, filename); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
SELECT version
FROM platform.artifact
WHERE tenant_id = $1 AND app_id = $2
  AND session_principal_id = $3 AND session_id = $4
  AND filename = $5 AND config_version = $6 AND status = 'AVAILABLE'
ORDER BY version`,
		access.Scope.TenantID,
		access.Scope.AppID,
		access.SessionPrincipalID,
		access.SessionID,
		filename,
		access.ConfigVersion,
	)
	if err != nil {
		return nil, fmt.Errorf("list artifact metadata versions: %w", err)
	}
	defer rows.Close()

	var versions []int
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("scan artifact metadata version: %w", err)
		}
		versions = append(versions, version)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list artifact metadata versions: %w", err)
	}
	return versions, nil
}

// MarkArtifactsDeleted prevents further SQL-authorized reads and returns each
// exact immutable object that must be removed from storage.
func (s *Store) MarkArtifactsDeleted(
	ctx context.Context,
	access platformartifact.Access,
	filename string,
) ([]platformartifact.Record, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if err := validateArtifactAccess(access, filename); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
UPDATE platform.artifact
SET status = 'DELETED', cleanup_next_attempt_at = clock_timestamp(), updated_at = now()
WHERE tenant_id = $1 AND app_id = $2
  AND session_principal_id = $3 AND session_id = $4
  AND filename = $5 AND config_version = $6 AND status = 'AVAILABLE'
	RETURNING artifact_id, tenant_id, app_id, session_principal_id, session_id,
          filename, version, object_key, mime_type, size_bytes, status, config_version,
          created_at, updated_at`,
		access.Scope.TenantID,
		access.Scope.AppID,
		access.SessionPrincipalID,
		access.SessionID,
		filename,
		access.ConfigVersion,
	)
	if err != nil {
		return nil, fmt.Errorf("mark artifact metadata deleted: %w", err)
	}
	defer rows.Close()
	records := make([]platformartifact.Record, 0)
	for rows.Next() {
		var record platformartifact.Record
		if err := rows.Scan(
			&record.ID,
			&record.TenantID,
			&record.AppID,
			&record.SessionPrincipalID,
			&record.SessionID,
			&record.Filename,
			&record.Version,
			&record.ObjectKey,
			&record.MIMEType,
			&record.Size,
			&record.Status,
			&record.ConfigVersion,
			&record.CreatedAt,
			&record.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("read deleted artifact metadata: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate deleted artifact metadata: %w", err)
	}
	if len(records) == 0 {
		return nil, artifactNotFoundError()
	}
	return records, nil
}

func validateArtifactAccess(access platformartifact.Access, filename string) error {
	if err := access.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(filename) == "" {
		return errors.New("artifact filename is required")
	}
	return nil
}

func artifactNotFoundError() error {
	return fmt.Errorf("artifact metadata: %w: %w", platformartifact.ErrNotFound, ErrNotFound)
}
