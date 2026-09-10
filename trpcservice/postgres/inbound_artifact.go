package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

const (
	inboundArtifactUploading   = "UPLOADING"
	inboundArtifactPending     = "PENDING"
	inboundArtifactAttached    = "ATTACHED"
	inboundArtifactDeleted     = "DELETED"
	inboundArtifactUploadLease = 5 * time.Minute
)

// StagedInboundArtifact is durable media written before channel admission.
// The provider reference never enters this record; ArtifactRef is an
// internally generated name that can be atomically attached to a session.
type StagedInboundArtifact struct {
	TenantID          string
	AppID             string
	BindingID         string
	ExternalMessageID string
	ItemNo            int
	ArtifactRef       string
	ConfigVersion     string
	Filename          string
	ObjectKey         string
	MIMEType          string
	Size              int64
	Status            string
	// UploadToken fences the uploader that owns an UPLOADING row. It is an
	// internal lease token and must never enter API, audit, or log payloads.
	UploadToken string
	// Created reports whether StageInboundArtifact inserted this row for the
	// current call rather than returning an existing idempotent stage.
	Created            bool
	CreatedAt          time.Time
	UpdatedAt          time.Time
	CleanupCompletedAt *time.Time
}

// ReserveInboundArtifact records the object identity before the blob upload.
// The UPLOADING row is the durable recovery ledger for a process crash between
// object-store success and the admission staging transaction.
func (s *Store) ReserveInboundArtifact(ctx context.Context, record StagedInboundArtifact) (StagedInboundArtifact, error) {
	if err := s.validate(); err != nil {
		return StagedInboundArtifact{}, err
	}
	if err := validateStagedInboundArtifact(record); err != nil {
		return StagedInboundArtifact{}, err
	}
	uploadToken := uuid.NewString()
	tag, err := s.pool.Exec(ctx, `
INSERT INTO platform.inbound_artifact (
    tenant_id, app_id, binding_id, external_message_id, item_no,
    artifact_ref, config_version, filename, object_key, mime_type, size_bytes,
    status, upload_token, upload_lease_until
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'UPLOADING', $12, clock_timestamp() + $13::interval)
ON CONFLICT (tenant_id, app_id, binding_id, external_message_id, item_no) DO NOTHING`,
		record.TenantID, record.AppID, record.BindingID, record.ExternalMessageID,
		record.ItemNo, record.ArtifactRef, record.ConfigVersion, record.Filename,
		record.ObjectKey, record.MIMEType, record.Size, uploadToken,
		intervalLiteral(inboundArtifactUploadLease),
	)
	if err != nil {
		return StagedInboundArtifact{}, fmt.Errorf("reserve inbound artifact: %w", err)
	}
	if tag.RowsAffected() == 0 {
		existing, findErr := s.findStagedInboundArtifact(ctx, nil, record.TenantID, record.AppID, record.BindingID, record.ExternalMessageID, record.ItemNo)
		if findErr != nil {
			return StagedInboundArtifact{}, findErr
		}
		if existing.CleanupCompletedAt != nil && existing.Status != inboundArtifactAttached {
			resetTag, resetErr := s.pool.Exec(ctx, `
UPDATE platform.inbound_artifact
SET artifact_ref = $6, config_version = $7, filename = $8, object_key = $9,
    mime_type = $10, size_bytes = $11, status = 'UPLOADING',
    upload_token = $12, upload_lease_until = clock_timestamp() + $13::interval,
    cleanup_attempts = 0, cleanup_next_attempt_at = clock_timestamp(),
    cleanup_owner = NULL, cleanup_lease_until = NULL, cleanup_last_error = '',
    cleanup_completed_at = NULL, updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND external_message_id = $4 AND item_no = $5
  AND status IN ('DELETED', 'PENDING', 'UPLOADING')
  AND cleanup_completed_at IS NOT NULL`,
				record.TenantID, record.AppID, record.BindingID, record.ExternalMessageID,
				record.ItemNo, record.ArtifactRef, record.ConfigVersion, record.Filename,
				record.ObjectKey, record.MIMEType, record.Size, uploadToken,
				intervalLiteral(inboundArtifactUploadLease),
			)
			if resetErr != nil {
				return StagedInboundArtifact{}, fmt.Errorf("reopen inbound artifact reservation: %w", resetErr)
			}
			if resetTag.RowsAffected() == 1 {
				existing, findErr = s.findStagedInboundArtifact(ctx, nil, record.TenantID, record.AppID, record.BindingID, record.ExternalMessageID, record.ItemNo)
				if findErr != nil {
					return StagedInboundArtifact{}, findErr
				}
				existing.Created = true
				return existing, nil
			}
		}
		existing.Created = false
		return existing, nil
	}
	reserved, err := s.findStagedInboundArtifact(ctx, nil, record.TenantID, record.AppID, record.BindingID, record.ExternalMessageID, record.ItemNo)
	if err != nil {
		return StagedInboundArtifact{}, err
	}
	reserved.Created = true
	return reserved, nil
}

// FinalizeInboundArtifactUpload publishes a successfully uploaded object to
// the normal pre-admission PENDING state.
func (s *Store) FinalizeInboundArtifactUpload(ctx context.Context, record StagedInboundArtifact) (StagedInboundArtifact, error) {
	if err := s.validate(); err != nil {
		return StagedInboundArtifact{}, err
	}
	if err := validateStagedInboundArtifact(record); err != nil {
		return StagedInboundArtifact{}, err
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE platform.inbound_artifact
SET status = 'PENDING', updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND external_message_id = $4 AND item_no = $5
  AND artifact_ref = $6 AND config_version = $7 AND object_key = $8
  AND upload_token = $9 AND upload_lease_until > clock_timestamp()
  AND status = 'UPLOADING'`,
		record.TenantID, record.AppID, record.BindingID, record.ExternalMessageID,
		record.ItemNo, record.ArtifactRef, record.ConfigVersion,
		record.ObjectKey, record.UploadToken,
	)
	if err != nil {
		return StagedInboundArtifact{}, fmt.Errorf("finalize inbound artifact upload: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return StagedInboundArtifact{}, errors.New("inbound artifact upload reservation is not owned")
	}
	return s.findStagedInboundArtifact(ctx, nil, record.TenantID, record.AppID, record.BindingID, record.ExternalMessageID, record.ItemNo)
}

// RenewInboundArtifactUpload keeps the current uploader's reservation alive.
// The token and live lease are both required so an uploader that lost the
// reservation cannot revive it after cleanup has reclaimed the row.
func (s *Store) RenewInboundArtifactUpload(ctx context.Context, record StagedInboundArtifact) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := validateStagedInboundArtifact(record); err != nil {
		return err
	}
	if record.UploadToken == "" {
		return errors.New("inbound artifact upload token is required")
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE platform.inbound_artifact
SET upload_lease_until = clock_timestamp() + $9::interval,
    updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND external_message_id = $4 AND item_no = $5
  AND artifact_ref = $6 AND config_version = $7 AND object_key = $8
  AND status = 'UPLOADING'
  AND upload_token = $10
  AND upload_lease_until > clock_timestamp()`,
		record.TenantID, record.AppID, record.BindingID, record.ExternalMessageID,
		record.ItemNo, record.ArtifactRef, record.ConfigVersion, record.ObjectKey,
		intervalLiteral(inboundArtifactUploadLease), record.UploadToken,
	)
	if err != nil {
		return fmt.Errorf("renew inbound artifact upload: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("inbound artifact upload reservation is not owned")
	}
	return nil
}

// StageInboundArtifact records one uploaded provider attachment using the
// binding/message/item idempotency tuple. A concurrent duplicate returns the
// original durable object instead of replacing it.
func (s *Store) StageInboundArtifact(
	ctx context.Context,
	record StagedInboundArtifact,
) (StagedInboundArtifact, error) {
	if err := s.validate(); err != nil {
		return StagedInboundArtifact{}, err
	}
	if err := validateStagedInboundArtifact(record); err != nil {
		return StagedInboundArtifact{}, err
	}
	tag, err := s.pool.Exec(ctx, `
INSERT INTO platform.inbound_artifact (
	    tenant_id, app_id, binding_id, external_message_id, item_no,
	    artifact_ref, config_version, filename, object_key, mime_type, size_bytes,
	    status
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'PENDING')
ON CONFLICT (tenant_id, app_id, binding_id, external_message_id, item_no) DO NOTHING`,
		record.TenantID,
		record.AppID,
		record.BindingID,
		record.ExternalMessageID,
		record.ItemNo,
		record.ArtifactRef,
		record.ConfigVersion,
		record.Filename,
		record.ObjectKey,
		record.MIMEType,
		record.Size,
	)
	if err != nil {
		return StagedInboundArtifact{}, fmt.Errorf("stage inbound artifact: %w", err)
	}
	staged, err := s.findStagedInboundArtifact(ctx, nil, record.TenantID, record.AppID, record.BindingID, record.ExternalMessageID, record.ItemNo)
	if err != nil {
		return StagedInboundArtifact{}, err
	}
	if staged.Status == inboundArtifactDeleted {
		return StagedInboundArtifact{}, errors.New("staged inbound artifact was deleted")
	}
	staged.Created = tag.RowsAffected() == 1
	return staged, nil
}

func validateStagedInboundArtifact(record StagedInboundArtifact) error {
	if record.TenantID == "" || record.AppID == "" || record.BindingID == "" ||
		record.ExternalMessageID == "" || record.ConfigVersion == "" {
		return errors.New("staged inbound artifact scope and config are required")
	}
	if record.ItemNo < 0 {
		return errors.New("staged inbound artifact item number must not be negative")
	}
	if _, err := inboundArtifactName(record.ArtifactRef); err != nil {
		return errors.New("staged inbound artifact ref is invalid")
	}
	if record.Filename == "" || record.ObjectKey == "" {
		return errors.New("staged inbound artifact filename and object key are required")
	}
	if strings.ContainsAny(record.Filename+record.MIMEType, "\r\n\x00") {
		return errors.New("staged inbound artifact metadata is invalid")
	}
	if record.Size <= 0 {
		return errors.New("staged inbound artifact size must be positive")
	}
	return nil
}

func (s *Store) findStagedInboundArtifact(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, appID, bindingID, externalMessageID string, itemNo int,
) (StagedInboundArtifact, error) {
	query := `
SELECT tenant_id, app_id, binding_id, external_message_id, item_no,
       artifact_ref, config_version, filename, object_key, mime_type, size_bytes,
       status, upload_token, created_at, updated_at, cleanup_completed_at
FROM platform.inbound_artifact
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND external_message_id = $4 AND item_no = $5`
	var row pgx.Row
	if tx != nil {
		row = tx.QueryRow(ctx, query+" FOR UPDATE", tenantID, appID, bindingID, externalMessageID, itemNo)
	} else {
		row = s.pool.QueryRow(ctx, query, tenantID, appID, bindingID, externalMessageID, itemNo)
	}
	record, err := scanStagedInboundArtifact(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return StagedInboundArtifact{}, fmt.Errorf("staged inbound artifact: %w", ErrNotFound)
	}
	if err != nil {
		return StagedInboundArtifact{}, fmt.Errorf("find staged inbound artifact: %w", err)
	}
	return record, nil
}

// MarkInboundArtifactDeleted moves a staged object out of PENDING under a row
// lock. It is idempotent for a previously deleted row and deliberately does
// nothing for ATTACHED rows, which are now owned by a committed execution.
func (s *Store) MarkInboundArtifactDeleted(
	ctx context.Context,
	record StagedInboundArtifact,
) (StagedInboundArtifact, bool, error) {
	if err := s.validate(); err != nil {
		return StagedInboundArtifact{}, false, err
	}
	if record.TenantID == "" || record.AppID == "" || record.BindingID == "" ||
		record.ExternalMessageID == "" || record.ArtifactRef == "" || record.ConfigVersion == "" {
		return StagedInboundArtifact{}, false, errors.New("inbound artifact compensation scope is required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return StagedInboundArtifact{}, false, fmt.Errorf("begin inbound artifact compensation: %w", err)
	}
	defer func() { rollback(tx) }()
	staged, err := s.findStagedInboundArtifact(ctx, tx, record.TenantID, record.AppID, record.BindingID, record.ExternalMessageID, record.ItemNo)
	if errors.Is(err, ErrNotFound) {
		return StagedInboundArtifact{}, false, nil
	}
	if err != nil {
		return StagedInboundArtifact{}, false, err
	}
	if staged.ArtifactRef != record.ArtifactRef || staged.ConfigVersion != record.ConfigVersion {
		return StagedInboundArtifact{}, false, errors.New("inbound artifact compensation identity mismatch")
	}
	if staged.Status == inboundArtifactAttached {
		if err := tx.Commit(ctx); err != nil {
			return StagedInboundArtifact{}, false, fmt.Errorf("commit attached artifact compensation check: %w", err)
		}
		return staged, false, nil
	}
	if staged.Status != inboundArtifactUploading && staged.Status != inboundArtifactPending && staged.Status != inboundArtifactDeleted {
		return StagedInboundArtifact{}, false, fmt.Errorf("inbound artifact status %q cannot be compensated", staged.Status)
	}
	if staged.Status == inboundArtifactUploading || staged.Status == inboundArtifactPending {
		if staged.Status == inboundArtifactUploading && record.UploadToken != staged.UploadToken {
			return StagedInboundArtifact{}, false, errors.New("inbound artifact upload reservation is not owned")
		}
		if _, err := tx.Exec(ctx, `
UPDATE platform.inbound_artifact
SET status = 'DELETED', upload_token = NULL, upload_lease_until = NULL,
    updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND external_message_id = $4 AND item_no = $5
	  AND artifact_ref = $6 AND status IN ('UPLOADING', 'PENDING')`,
			staged.TenantID, staged.AppID, staged.BindingID, staged.ExternalMessageID,
			staged.ItemNo, staged.ArtifactRef,
		); err != nil {
			return StagedInboundArtifact{}, false, fmt.Errorf("mark inbound artifact deleted: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return StagedInboundArtifact{}, false, fmt.Errorf("commit inbound artifact compensation: %w", err)
	}
	return staged, true, nil
}

func scanStagedInboundArtifact(row interface{ Scan(...any) error }) (StagedInboundArtifact, error) {
	var record StagedInboundArtifact
	var uploadToken pgtype.Text
	err := row.Scan(
		&record.TenantID,
		&record.AppID,
		&record.BindingID,
		&record.ExternalMessageID,
		&record.ItemNo,
		&record.ArtifactRef,
		&record.ConfigVersion,
		&record.Filename,
		&record.ObjectKey,
		&record.MIMEType,
		&record.Size,
		&record.Status,
		&uploadToken,
		&record.CreatedAt,
		&record.UpdatedAt,
		&record.CleanupCompletedAt,
	)
	if err != nil {
		return StagedInboundArtifact{}, err
	}
	if uploadToken.Valid {
		record.UploadToken = uploadToken.String
	}
	return record, nil
}

func attachStagedInboundArtifacts(
	ctx context.Context,
	tx pgx.Tx,
	input channels.ChannelInput,
	configVersion, sessionPrincipalID, sessionID string,
) error {
	if len(input.ArtifactRefs) == 0 {
		return nil
	}
	for _, artifactRef := range input.ArtifactRefs {
		name, err := inboundArtifactName(artifactRef)
		if err != nil {
			return err
		}
		staged, err := findStagedInboundArtifactTx(
			ctx,
			tx,
			input.TenantID,
			input.AppID,
			input.BindingID,
			input.ExternalMessageID,
			artifactRef,
		)
		if err != nil {
			return err
		}
		if staged.ConfigVersion != configVersion {
			return errors.New("inbound artifact config version changed before admission")
		}
		if staged.Status != inboundArtifactPending {
			return fmt.Errorf("staged inbound artifact status %q is not pending", staged.Status)
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO platform.artifact (
    artifact_id, tenant_id, app_id, session_principal_id, session_id,
    filename, version, object_key, mime_type, size_bytes, status, config_version
) VALUES (gen_random_uuid()::text, $1, $2, $3, $4, $5, 0, $6, $7, $8, 'AVAILABLE', $9)`,
			input.TenantID,
			input.AppID,
			sessionPrincipalID,
			sessionID,
			name,
			staged.ObjectKey,
			staged.MIMEType,
			staged.Size,
			staged.ConfigVersion,
		); err != nil {
			return fmt.Errorf("attach inbound artifact metadata: %w", err)
		}
		result, err := tx.Exec(ctx, `
UPDATE platform.inbound_artifact
SET status = 'ATTACHED', updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND external_message_id = $4 AND artifact_ref = $5 AND status = 'PENDING'`,
			input.TenantID, input.AppID, input.BindingID, input.ExternalMessageID, artifactRef,
		)
		if err != nil {
			return fmt.Errorf("attach inbound artifact stage: %w", err)
		}
		if result.RowsAffected() != 1 {
			return errors.New("staged inbound artifact changed during admission")
		}
	}
	return nil
}

func findStagedInboundArtifactTx(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, appID, bindingID, externalMessageID, artifactRef string,
) (StagedInboundArtifact, error) {
	var record StagedInboundArtifact
	var uploadToken pgtype.Text
	err := tx.QueryRow(ctx, `
	SELECT tenant_id, app_id, binding_id, external_message_id, item_no,
	       artifact_ref, config_version, filename, object_key, mime_type, size_bytes,
	       status, upload_token, created_at, updated_at, cleanup_completed_at
FROM platform.inbound_artifact
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND external_message_id = $4 AND artifact_ref = $5
FOR UPDATE`, tenantID, appID, bindingID, externalMessageID, artifactRef).Scan(
		&record.TenantID,
		&record.AppID,
		&record.BindingID,
		&record.ExternalMessageID,
		&record.ItemNo,
		&record.ArtifactRef,
		&record.ConfigVersion,
		&record.Filename,
		&record.ObjectKey,
		&record.MIMEType,
		&record.Size,
		&record.Status,
		&uploadToken,
		&record.CreatedAt,
		&record.UpdatedAt,
		&record.CleanupCompletedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return StagedInboundArtifact{}, errors.New("inbound artifact was not staged by the verified binding")
	}
	if err != nil {
		return StagedInboundArtifact{}, fmt.Errorf("find inbound artifact for admission: %w", err)
	}
	if uploadToken.Valid {
		record.UploadToken = uploadToken.String
	}
	return record, nil
}

func inboundArtifactName(artifactRef string) (string, error) {
	name, version, err := gateway.ParseArtifactRef(artifactRef)
	if err != nil {
		return "", fmt.Errorf("inbound artifact ref: %w", err)
	}
	if version != 0 {
		return "", errors.New("inbound artifact ref version must be zero")
	}
	return name, nil
}
