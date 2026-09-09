package postgres

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/artifact"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// ArtifactMetadataRepository owns only PostgreSQL artifact metadata. Object
// bytes remain in the separately owned ObjectStore and are reconciled through
// explicit status transitions.
type ArtifactMetadataRepository struct {
	pool        *pgxpool.Pool
	objectStore storage.ObjectStore
}

var _ storage.ArtifactRepository = (*ArtifactMetadataRepository)(nil)
var _ storage.ArtifactMetadataRepository = (*ArtifactMetadataRepository)(nil)

func NewArtifactMetadataRepository(pool *pgxpool.Pool, objectStore storage.ObjectStore) (*ArtifactMetadataRepository, error) {
	if pool == nil {
		return nil, errors.New("postgres: artifact metadata pool is required")
	}
	return &ArtifactMetadataRepository{pool: pool, objectStore: objectStore}, nil
}

func (r *ArtifactMetadataRepository) Create(ctx context.Context, tc tenant.TenantContext, value artifact.Artifact) error {
	if err := validateArtifactContext(ctx, tc); err != nil {
		return err
	}
	if err := value.Validate(); err != nil {
		return fmt.Errorf("%w: artifact metadata is invalid", storage.ErrInvalidArgument)
	}
	if value.TenantID != tc.TenantID {
		return storage.ErrTenantMismatch
	}
	canonical, err := storage.CanonicalObjectKey(value.TenantID, value.ID)
	if err != nil || value.ObjectKey != canonical {
		return fmt.Errorf("%w: object key is not server-canonical", storage.ErrInvalidArgument)
	}
	if value.Status != artifact.StatusPending {
		return fmt.Errorf("%w: new artifact metadata must be pending", storage.ErrInvalidArgument)
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return artifactDBError(err)
	}
	if err = SetTenantContext(ctx, tx, tc.TenantID); err != nil {
		return artifactDBError(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='15s'`); err != nil {
		return artifactDBError(err)
	}
	var eventExists bool
	if err := tx.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM session_event
    WHERE tenant_id=$1 AND session_id=$2 AND message_id=$3
)`, value.TenantID, value.SessionID, value.MessageID).Scan(&eventExists); err != nil {
		return artifactDBError(err)
	}
	if !eventExists {
		return fmt.Errorf("%w: artifact message is not bound to its session", storage.ErrInvalidArgument)
	}
	_, err = tx.Exec(ctx, `
INSERT INTO artifact (
    tenant_id, artifact_id, session_id, message_id, object_key, mime_type,
    size_bytes, sha256, status, expires_at, created_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::timestamptz,COALESCE($11::timestamptz, now()))`,
		value.TenantID, value.ID, value.SessionID, value.MessageID, value.ObjectKey,
		value.MIMEType, value.SizeBytes, nullableSHA(value.SHA256), string(value.Status), nullableTime(value.ExpiresAt), nullableTime(value.CreatedAt))
	if err != nil {
		return artifactDBError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return artifactDBError(err)
	}
	return nil
}

func (r *ArtifactMetadataRepository) Get(ctx context.Context, tc tenant.TenantContext, id string) (artifact.Artifact, error) {
	if err := validateArtifactContext(ctx, tc); err != nil {
		return artifact.Artifact{}, err
	}
	if err := validateArtifactID(id); err != nil {
		return artifact.Artifact{}, err
	}
	return r.query(ctx, tc.TenantID, id)
}

func (r *ArtifactMetadataRepository) PresignedURL(ctx context.Context, tc tenant.TenantContext, id string, ttl time.Duration) (string, error) {
	value, err := r.Get(ctx, tc, id)
	if err != nil {
		return "", err
	}
	if value.Status != artifact.StatusReady {
		return "", fmt.Errorf("%w: artifact object is not ready", storage.ErrConflict)
	}
	if !value.ExpiresAt.IsZero() && !time.Now().UTC().Before(value.ExpiresAt) {
		return "", storage.ErrNotFound
	}
	if r.objectStore == nil {
		return "", storage.ErrObjectUnavailable
	}
	return r.objectStore.PresignedURL(ctx, tc, id, ttl)
}

func (r *ArtifactMetadataRepository) MarkReady(ctx context.Context, tc tenant.TenantContext, id string, info storage.ObjectInfo) error {
	if err := validateArtifactContext(ctx, tc); err != nil {
		return err
	}
	if err := validateArtifactID(id); err != nil {
		return err
	}
	if !objectInfoMatches(tc.TenantID, id, info) {
		return fmt.Errorf("%w: object metadata does not match artifact", storage.ErrConflict)
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return artifactDBError(err)
	}
	if err = SetTenantContext(ctx, tx, tc.TenantID); err != nil {
		return artifactDBError(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='15s'`); err != nil {
		return artifactDBError(err)
	}
	current, err := scanArtifact(ctx, tx.QueryRow(ctx, artifactSelect+` FOR UPDATE`, tc.TenantID, id))
	if err != nil {
		return err
	}
	if current.Status == artifact.StatusReady && current.SizeBytes == info.SizeBytes && current.MIMEType == info.MIMEType && strings.EqualFold(current.SHA256, info.SHA256) {
		if err := tx.Commit(ctx); err != nil {
			return artifactDBError(err)
		}
		return nil
	}
	if current.Status != artifact.StatusPending || current.SizeBytes != info.SizeBytes || current.MIMEType != info.MIMEType {
		return fmt.Errorf("%w: artifact is not pending or does not match object", storage.ErrConflict)
	}
	if current.SHA256 != "" && !strings.EqualFold(current.SHA256, info.SHA256) {
		return fmt.Errorf("%w: artifact checksum does not match object", storage.ErrConflict)
	}
	if _, err := tx.Exec(ctx, `UPDATE artifact SET sha256=$3, status='ready' WHERE tenant_id=$1 AND artifact_id=$2`, tc.TenantID, id, strings.ToLower(info.SHA256)); err != nil {
		return artifactDBError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return artifactDBError(err)
	}
	return nil
}

func (r *ArtifactMetadataRepository) MarkFailed(ctx context.Context, tc tenant.TenantContext, id string) error {
	if err := validateArtifactContext(ctx, tc); err != nil {
		return err
	}
	if err := validateArtifactID(id); err != nil {
		return err
	}
	var affected int64
	err := WithTenantContext(ctx, r.pool, tc.TenantID, "artifact mark failed", func(ctx context.Context, tx pgx.Tx) error {
		command, execErr := tx.Exec(ctx, `
UPDATE artifact SET status='failed'
WHERE tenant_id=$1 AND artifact_id=$2 AND status='pending'`, tc.TenantID, id)
		affected = command.RowsAffected()
		return execErr
	})
	if err != nil {
		return artifactDBError(err)
	}
	if affected == 0 {
		value, getErr := r.Get(ctx, tc, id)
		if getErr != nil {
			return getErr
		}
		if value.Status == artifact.StatusFailed {
			return nil
		}
		return storage.ErrConflict
	}
	return nil
}

func (r *ArtifactMetadataRepository) markExpired(ctx context.Context, tc tenant.TenantContext, id string) error {
	var affected int64
	err := WithTenantContext(ctx, r.pool, tc.TenantID, "artifact mark expired", func(ctx context.Context, tx pgx.Tx) error {
		command, execErr := tx.Exec(ctx, "UPDATE artifact SET status='expired' WHERE tenant_id=$1 AND artifact_id=$2 AND status='ready'", tc.TenantID, id)
		affected = command.RowsAffected()
		return execErr
	})
	if err != nil {
		return artifactDBError(err)
	}
	if affected == 0 {
		value, getErr := r.Get(ctx, tc, id)
		if getErr != nil {
			return getErr
		}
		if value.Status == artifact.StatusExpired {
			return nil
		}
		return storage.ErrConflict
	}
	return nil
}

func (r *ArtifactMetadataRepository) Reconcile(ctx context.Context, tc tenant.TenantContext, id string) (artifact.Artifact, error) {
	value, err := r.Get(ctx, tc, id)
	if err != nil {
		return artifact.Artifact{}, err
	}
	if value.Status != artifact.StatusPending && value.Status != artifact.StatusReady {
		return value, nil
	}
	if r.objectStore == nil {
		return value, storage.ErrObjectUnavailable
	}
	info, err := r.objectStore.Head(ctx, tc, id)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			if value.Status == artifact.StatusPending {
				if markErr := r.MarkFailed(ctx, tc, id); markErr != nil {
					return value, markErr
				}
			} else if markErr := r.markExpired(ctx, tc, id); markErr != nil {
				return value, markErr
			}
			return r.Get(ctx, tc, id)
		}
		return value, err
	}
	if !objectInfoMatches(tc.TenantID, id, info) {
		if value.Status == artifact.StatusPending {
			if markErr := r.MarkFailed(ctx, tc, id); markErr != nil {
				return value, markErr
			}
		} else if markErr := r.markExpired(ctx, tc, id); markErr != nil {
			return value, markErr
		}
		return r.Get(ctx, tc, id)
	}
	if value.Status == artifact.StatusPending {
		if err := r.MarkReady(ctx, tc, id, info); err != nil {
			return value, err
		}
	}
	return r.Get(ctx, tc, id)
}

const artifactSelect = `
SELECT tenant_id, artifact_id, session_id, message_id, object_key, mime_type,
       size_bytes, sha256, status, expires_at, created_at
FROM artifact
WHERE tenant_id=$1 AND artifact_id=$2`

func (r *ArtifactMetadataRepository) query(ctx context.Context, tenantID, id string) (artifact.Artifact, error) {
	var value artifact.Artifact
	err := WithTenantContext(ctx, r.pool, tenantID, "artifact query", func(ctx context.Context, tx pgx.Tx) error {
		var scanErr error
		value, scanErr = scanArtifact(ctx, tx.QueryRow(ctx, artifactSelect, tenantID, id))
		return scanErr
	})
	if err != nil {
		return artifact.Artifact{}, err
	}
	return value, nil
}

type artifactRow interface {
	Scan(...any) error
}

func scanArtifact(ctx context.Context, row artifactRow) (artifact.Artifact, error) {
	if err := ctx.Err(); err != nil {
		return artifact.Artifact{}, err
	}
	var value artifact.Artifact
	var sha *string
	var status string
	var expiresAt *time.Time
	if err := row.Scan(&value.TenantID, &value.ID, &value.SessionID, &value.MessageID, &value.ObjectKey, &value.MIMEType, &value.SizeBytes, &sha, &status, &expiresAt, &value.CreatedAt); err != nil {
		return artifact.Artifact{}, artifactScanError(err)
	}
	if sha != nil {
		value.SHA256 = *sha
	}
	value.Status = artifact.Status(status)
	if expiresAt != nil {
		value.ExpiresAt = expiresAt.UTC()
	}
	return value, nil
}

func validateArtifactContext(ctx context.Context, tc tenant.TenantContext) error {
	if ctx == nil {
		return storage.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tc.Validate(); err != nil {
		return fmt.Errorf("%w: tenant context is invalid", storage.ErrInvalidArgument)
	}
	return nil
}

func validateArtifactID(id string) error {
	if _, err := storage.CanonicalObjectKey("tenant", id); err != nil {
		return fmt.Errorf("%w: artifact id is invalid", storage.ErrInvalidArgument)
	}
	return nil
}

func objectInfoMatches(tenantID, artifactID string, info storage.ObjectInfo) bool {
	key, err := storage.CanonicalObjectKey(tenantID, artifactID)
	if err != nil || info.TenantID != tenantID || info.ArtifactID != artifactID || info.ObjectKey != key || info.MIMEType == "" || info.SizeBytes < 0 {
		return false
	}
	decoded, err := hex.DecodeString(info.SHA256)
	return err == nil && len(decoded) == 32
}

func nullableSHA(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

func artifactScanError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.ErrNotFound
	}
	return artifactDBError(err)
}

func artifactDBError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505", "40001":
			return storage.ErrConflict
		case "23503", "23514", "22P02":
			return storage.ErrInvalidArgument
		}
	}
	return errors.Join(storage.ErrBackendUnavailable, errors.New("artifact metadata operation failed"))
}
