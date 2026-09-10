package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

const attachmentColumns = "tenant_id,attachment_id,kind,mime_type,name,size,sha256,provider,provider_id,event_id,expires_at"

type storedAttachment struct {
	reference attachment.Reference
	eventID   sql.NullString
	expiresAt time.Time
}

// PutAttachment writes immutable bytes and their lifecycle metadata in one transaction.
func (s *Store) PutAttachment(ctx context.Context, tenantID string, upload attachment.Upload, content io.Reader) (attachment.Reference, error) {
	if err := checkCapability(ctx, s); err != nil {
		return attachment.Reference{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || content == nil {
		return attachment.Reference{}, runtimestorage.ErrInvalid
	}
	reference, data, expiresAt, err := prepareAttachment(upload, content)
	if err != nil {
		return attachment.Reference{}, err
	}
	return s.persistAttachment(ctx, tenantID, reference, data, expiresAt)
}

func prepareAttachment(upload attachment.Upload, content io.Reader) (attachment.Reference, []byte, time.Time, error) {
	normalized, err := upload.Normalize(time.Now().UTC())
	if err != nil {
		return attachment.Reference{}, nil, time.Time{}, err
	}
	data, err := io.ReadAll(io.LimitReader(content, normalized.Size+1))
	if err != nil {
		return attachment.Reference{}, nil, time.Time{}, runtimestorage.ErrStorage
	}
	if int64(len(data)) != normalized.Size {
		return attachment.Reference{}, nil, time.Time{}, attachment.ErrInvalid
	}
	digest := sha256.Sum256(data)
	reference := attachment.Reference{ID: normalized.ID, Kind: normalized.Kind, MIMEType: normalized.MIMEType, Name: normalized.Name, Size: normalized.Size, SHA256: hex.EncodeToString(digest[:]), Provider: normalized.Provider, ProviderID: normalized.ProviderID}
	if _, err := reference.Normalize(); err != nil {
		return attachment.Reference{}, nil, time.Time{}, err
	}
	return reference, data, normalized.ExpiresAt, nil
}

func (s *Store) persistAttachment(ctx context.Context, tenantID string, reference attachment.Reference, data []byte, expiresAt time.Time) (attachment.Reference, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return attachment.Reference{}, runtimestorage.ErrStorage
	}
	defer func() { _ = tx.Rollback() }()
	stored, found, err := loadAttachment(ctx, tx, tenantID, reference.ID, true)
	if err != nil {
		return attachment.Reference{}, err
	}
	if found {
		if stored.reference != reference {
			return attachment.Reference{}, runtimestorage.ErrConflict
		}
		return commitAttachment(tx, stored.reference)
	}
	if err := ensureAttachmentObject(ctx, tx, tenantID, reference, data); err != nil {
		return attachment.Reference{}, err
	}
	if err := insertAttachmentMetadata(ctx, tx, tenantID, reference, expiresAt); err != nil {
		return attachment.Reference{}, err
	}
	return commitAttachment(tx, reference)
}

func ensureAttachmentObject(ctx context.Context, tx *sql.Tx, tenantID string, reference attachment.Reference, data []byte) error {
	if _, err := tx.ExecContext(ctx, "INSERT INTO public.runtime_object (tenant_id,object_key,content_type,content,size,etag) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (tenant_id,object_key) DO NOTHING", tenantID, reference.ID, reference.MIMEType, data, reference.Size, reference.SHA256); err != nil {
		return mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	var objectType, objectETag string
	var objectSize int64
	if err := tx.QueryRowContext(ctx, "SELECT content_type,size,etag FROM public.runtime_object WHERE tenant_id=$1 AND object_key=$2", tenantID, reference.ID).Scan(&objectType, &objectSize, &objectETag); err != nil {
		return mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if objectType != reference.MIMEType || objectSize != reference.Size || objectETag != reference.SHA256 {
		return runtimestorage.ErrConflict
	}
	return nil
}

func insertAttachmentMetadata(ctx context.Context, tx *sql.Tx, tenantID string, reference attachment.Reference, expiresAt time.Time) error {
	result, err := tx.ExecContext(ctx, "INSERT INTO public.runtime_attachment (tenant_id,attachment_id,kind,mime_type,name,size,sha256,provider,provider_id,expires_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (tenant_id,attachment_id) DO NOTHING", tenantID, reference.ID, reference.Kind, reference.MIMEType, reference.Name, reference.Size, reference.SHA256, reference.Provider, reference.ProviderID, expiresAt)
	if err != nil {
		return mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	created, err := result.RowsAffected()
	if err != nil {
		return runtimestorage.ErrStorage
	}
	if created != 0 {
		return nil
	}
	stored, found, err := loadAttachment(ctx, tx, tenantID, reference.ID, true)
	if err != nil {
		return err
	}
	if !found || stored.reference != reference {
		return runtimestorage.ErrConflict
	}
	return nil
}

func commitAttachment(tx *sql.Tx, reference attachment.Reference) (attachment.Reference, error) {
	if err := tx.Commit(); err != nil {
		return attachment.Reference{}, runtimestorage.ErrStorage
	}
	return reference, nil
}

// BindAttachments atomically binds references to an existing message event.
func (s *Store) BindAttachments(ctx context.Context, tenantID, eventID string, references []attachment.Reference) error {
	if err := checkCapability(ctx, s); err != nil {
		return err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || eventID == "" {
		return runtimestorage.ErrInvalid
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return runtimestorage.ErrStorage
	}
	defer func() { _ = tx.Rollback() }()
	var present bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM public.message_event WHERE tenant_id=$1 AND event_id=$2)", tenantID, eventID).Scan(&present); err != nil || !present {
		if err == nil {
			return runtimestorage.ErrNotFound
		}
		return mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	for _, reference := range references {
		normalized, err := reference.Normalize()
		if err != nil {
			return err
		}
		stored, found, err := loadAttachment(ctx, tx, tenantID, normalized.ID, true)
		if err != nil {
			return err
		}
		if !found {
			return runtimestorage.ErrNotFound
		}
		if stored.reference != normalized || !stored.expiresAt.After(time.Now().UTC()) || stored.eventID.Valid && stored.eventID.String != eventID {
			return runtimestorage.ErrConflict
		}
	}
	for _, reference := range references {
		if _, err := tx.ExecContext(ctx, "UPDATE public.runtime_attachment SET event_id=$3 WHERE tenant_id=$1 AND attachment_id=$2 AND (event_id IS NULL OR event_id=$3)", tenantID, reference.ID, eventID); err != nil {
			return mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
		}
	}
	if err := tx.Commit(); err != nil {
		return runtimestorage.ErrStorage
	}
	return nil
}

// Load returns data only for a reference previously bound to eventID.
func (s *Store) Load(ctx context.Context, tenantID, eventID string, reference attachment.Reference) (attachment.Content, error) {
	if err := checkCapability(ctx, s); err != nil {
		return attachment.Content{}, err
	}
	normalized, err := reference.Normalize()
	if err != nil {
		return attachment.Content{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || eventID == "" {
		return attachment.Content{}, runtimestorage.ErrInvalid
	}
	stored, found, err := loadAttachment(ctx, s.db, tenantID, normalized.ID, false)
	if err != nil {
		return attachment.Content{}, err
	}
	if !found || stored.reference != normalized || !stored.eventID.Valid || stored.eventID.String != eventID || !stored.expiresAt.After(time.Now().UTC()) {
		return attachment.Content{}, runtimestorage.ErrNotFound
	}
	var data []byte
	if err := s.db.QueryRowContext(ctx, "SELECT content FROM public.runtime_object WHERE tenant_id=$1 AND object_key=$2", tenantID, normalized.ID).Scan(&data); err != nil {
		return attachment.Content{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	content := attachment.Content{Data: bytes.Clone(data)}
	if err := content.Validate(normalized); err != nil {
		return attachment.Content{}, err
	}
	return content, nil
}

// CleanupAttachments deletes expired unbound attachments and attachments whose
// bound message has reached a terminal state.
func (s *Store) CleanupAttachments(ctx context.Context, tenantID string, before time.Time) (int, error) {
	if err := checkCapability(ctx, s); err != nil {
		return 0, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || before.IsZero() {
		return 0, runtimestorage.ErrInvalid
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, runtimestorage.ErrStorage
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, "DELETE FROM public.runtime_attachment AS a WHERE a.tenant_id=$1 AND a.expires_at <= $2 AND (a.event_id IS NULL OR EXISTS (SELECT 1 FROM public.message_event AS e WHERE e.tenant_id=a.tenant_id AND e.event_id=a.event_id AND e.status IN ('completed','failed'))) RETURNING a.attachment_id", tenantID, before.UTC())
	if err != nil {
		return 0, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	defer func() { _ = rows.Close() }()
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return 0, runtimestorage.ErrStorage
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return 0, runtimestorage.ErrStorage
	}
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, "DELETE FROM public.runtime_object WHERE tenant_id=$1 AND object_key=$2", tenantID, id); err != nil {
			return 0, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, runtimestorage.ErrStorage
	}
	return len(ids), nil
}

type attachmentQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadAttachment(ctx context.Context, query attachmentQuerier, tenantID, id string, lock bool) (storedAttachment, bool, error) {
	var stored storedAttachment
	var storedTenantID string
	var kind string
	statement := "SELECT " + attachmentColumns + " FROM public.runtime_attachment WHERE tenant_id=$1 AND attachment_id=$2"
	if lock {
		statement += " FOR UPDATE"
	}
	err := query.QueryRowContext(ctx, statement, tenantID, id).Scan(&storedTenantID, &stored.reference.ID, &kind, &stored.reference.MIMEType, &stored.reference.Name, &stored.reference.Size, &stored.reference.SHA256, &stored.reference.Provider, &stored.reference.ProviderID, &stored.eventID, &stored.expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return storedAttachment{}, false, nil
	}
	if err != nil {
		return storedAttachment{}, false, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if storedTenantID != tenantID {
		return storedAttachment{}, false, runtimestorage.ErrStorage
	}
	stored.reference.Kind = attachment.Kind(kind)
	if _, err := stored.reference.Normalize(); err != nil {
		return storedAttachment{}, false, fmt.Errorf("%w: stored attachment metadata is invalid", runtimestorage.ErrStorage)
	}
	return stored, true, nil
}

var _ runtimestorage.AttachmentStore = (*Store)(nil)
