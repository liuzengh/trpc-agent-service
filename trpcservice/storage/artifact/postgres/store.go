// Package postgres implements durable tenant-scoped Artifact metadata. New
// production instances should use NewWithObjectStore so PostgreSQL stores only
// immutable metadata; New retains the bytea reference backend for compatibility.
package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore"
)

const (
	storagePostgres = "postgres_bytea"
	storageObject   = "object"
)

type Store struct {
	db               *sql.DB
	objects          objectstore.Store
	objectBacked     bool
	objectPutTimeout time.Duration
	uploadProtection time.Duration
}

type ObjectStoreOptions struct {
	PutTimeout       time.Duration
	UploadProtection time.Duration
}

// New returns the PostgreSQL bytea reference backend. It remains available for
// small deployments and rolling upgrades, but distributed production wiring
// should use NewWithObjectStore.
func New(db *sql.DB) *Store { return &Store{db: db} }

func NewWithObjectStore(db *sql.DB, objects objectstore.Store) *Store {
	return NewWithObjectStoreOptions(db, objects, ObjectStoreOptions{})
}

func NewWithObjectStoreOptions(db *sql.DB, objects objectstore.Store, options ObjectStoreOptions) *Store {
	return &Store{db: db, objects: objects, objectBacked: true,
		objectPutTimeout: options.PutTimeout, uploadProtection: options.UploadProtection}
}

func (s *Store) PutArtifact(ctx context.Context, in artifact.Record) (artifact.Record, error) {
	if s == nil || s.db == nil {
		return artifact.Record{}, runtime.ErrCapabilityUnsupported
	}
	if err := validateInput(in); err != nil {
		return artifact.Record{}, err
	}
	if s.objectBacked {
		if s.objects == nil {
			return artifact.Record{}, runtime.ErrCapabilityUnsupported
		}
		existing, err := s.GetArtifact(ctx, in.TenantID, in.ArtifactID)
		switch {
		case err == nil && sameArtifact(existing, in):
			return existing, nil
		case err == nil:
			return artifact.Record{}, runtime.ErrIdempotencyCollision
		case !errors.Is(err, runtime.ErrNotFound):
			return artifact.Record{}, err
		}
	}
	storageKind := storagePostgres
	content, objectKey := in.Content, ""
	var upload artifact.ObjectUpload
	if s.objectBacked {
		var err error
		objectKey, err = objectstore.StableKey(in.TenantID, in.ArtifactID)
		if err != nil {
			return artifact.Record{}, err
		}
		upload, err = s.protectObjectUpload(ctx, in, objectKey)
		if err != nil {
			return artifact.Record{}, err
		}
		putCtx, cancel := context.WithTimeout(ctx, s.putTimeout())
		storedObject, err := s.objects.PutObject(putCtx, objectstore.Object{TenantID: in.TenantID, ObjectKey: objectKey,
			ContentDigest: in.ContentDigest, Content: in.Content})
		cancel()
		if err != nil {
			return artifact.Record{}, err
		}
		if !sameObject(storedObject, in.TenantID, objectKey, in.ContentDigest, in.Content) {
			return artifact.Record{}, runtime.ErrVersionMismatch
		}
		upload, err = s.renewObjectUpload(ctx, upload)
		if err != nil {
			if existing, resolved, resolveErr := s.resolveConcurrentArtifact(ctx, in); resolved || resolveErr != nil {
				return existing, resolveErr
			}
			return artifact.Record{}, err
		}
		storageKind, content = storageObject, nil
	}
	var stored storedMetadata
	var err error
	if storageKind == storageObject {
		stored, err = s.commitObjectMetadata(ctx, in, upload)
	} else {
		stored, err = s.insertMetadata(ctx, in, storageKind, objectKey, content)
	}
	if err != nil {
		if errors.Is(err, runtime.ErrVersionConflict) {
			if existing, resolved, resolveErr := s.resolveConcurrentArtifact(ctx, in); resolved || resolveErr != nil {
				return existing, resolveErr
			}
		}
		return artifact.Record{}, err
	}
	storedRecord, err := s.hydrate(ctx, stored)
	if err != nil {
		return artifact.Record{}, err
	}
	if !sameArtifact(storedRecord, in) {
		return artifact.Record{}, runtime.ErrIdempotencyCollision
	}
	return storedRecord, nil
}

func (s *Store) GetArtifact(ctx context.Context, tenantID, artifactID string) (artifact.Record, error) {
	if s == nil || s.db == nil {
		return artifact.Record{}, runtime.ErrCapabilityUnsupported
	}
	if tenantID == "" || artifactID == "" {
		return artifact.Record{}, runtime.ErrTenantScope
	}
	stored, err := scanMetadata(s.db.QueryRowContext(ctx, `SELECT tenant_id,request_id,artifact_id,artifact_ref,ordinal,source_digest,content_digest,media_type,kind,
content,malware_scan_version,dlp_version,created_at,storage_kind,object_key,content_size
FROM media_artifact WHERE tenant_id=$1 AND artifact_id=$2`, tenantID, artifactID))
	if errors.Is(err, sql.ErrNoRows) {
		return artifact.Record{}, runtime.ErrNotFound
	}
	if err != nil {
		return artifact.Record{}, translate(err)
	}
	return s.hydrate(ctx, stored)
}

type storedMetadata struct {
	record      artifact.Record
	storageKind string
	objectKey   string
	contentSize int64
}

func (s *Store) insertMetadata(ctx context.Context, in artifact.Record, storageKind, objectKey string, content []byte) (storedMetadata, error) {
	return insertMetadata(ctx, s.db, in, storageKind, objectKey, content)
}

type queryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func insertMetadata(ctx context.Context, query queryRower, in artifact.Record, storageKind, objectKey string, content []byte) (storedMetadata, error) {
	var nullableObjectKey any
	if objectKey != "" {
		nullableObjectKey = objectKey
	}
	stored, err := scanMetadata(query.QueryRowContext(ctx, `INSERT INTO media_artifact(
tenant_id,request_id,artifact_id,artifact_ref,ordinal,source_digest,content_digest,media_type,kind,content,malware_scan_version,dlp_version,storage_kind,object_key,content_size,retention_managed)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,true)
ON CONFLICT (tenant_id,artifact_id) DO UPDATE SET artifact_id=EXCLUDED.artifact_id
RETURNING tenant_id,request_id,artifact_id,artifact_ref,ordinal,source_digest,content_digest,media_type,kind,
content,malware_scan_version,dlp_version,created_at,storage_kind,object_key,content_size`,
		in.TenantID, in.RequestID, in.ArtifactID, in.ArtifactRef, in.Ordinal, in.SourceDigest, in.ContentDigest, in.MediaType, in.Kind,
		content, in.MalwareScanVersion, in.DLPVersion, storageKind, nullableObjectKey, len(in.Content)))
	if errors.Is(err, sql.ErrNoRows) {
		return storedMetadata{}, runtime.ErrVersionConflict
	}
	if err != nil {
		return storedMetadata{}, translate(err)
	}
	return stored, nil
}

type rowScanner interface{ Scan(...any) error }

func scanMetadata(row rowScanner) (storedMetadata, error) {
	var stored storedMetadata
	var objectKey sql.NullString
	err := row.Scan(&stored.record.TenantID, &stored.record.RequestID, &stored.record.ArtifactID, &stored.record.ArtifactRef,
		&stored.record.Ordinal, &stored.record.SourceDigest, &stored.record.ContentDigest, &stored.record.MediaType, &stored.record.Kind,
		&stored.record.Content, &stored.record.MalwareScanVersion, &stored.record.DLPVersion, &stored.record.CreatedAt,
		&stored.storageKind, &objectKey, &stored.contentSize)
	if objectKey.Valid {
		stored.objectKey = objectKey.String
	}
	return stored, err
}

func (s *Store) hydrate(ctx context.Context, stored storedMetadata) (artifact.Record, error) {
	if stored.contentSize <= 0 {
		return artifact.Record{}, runtime.ErrVersionMismatch
	}
	switch stored.storageKind {
	case storagePostgres:
		if stored.objectKey != "" || int64(len(stored.record.Content)) != stored.contentSize || !contentMatches(stored.record.Content, stored.record.ContentDigest) {
			return artifact.Record{}, runtime.ErrVersionMismatch
		}
	case storageObject:
		if stored.objectKey == "" || len(stored.record.Content) != 0 {
			return artifact.Record{}, runtime.ErrVersionMismatch
		}
		if s.objects == nil {
			return artifact.Record{}, runtime.ErrCapabilityUnsupported
		}
		value, err := s.objects.GetObject(ctx, stored.record.TenantID, stored.objectKey)
		if errors.Is(err, runtime.ErrNotFound) {
			return artifact.Record{}, runtime.ErrBackendUnavailable
		}
		if err != nil {
			return artifact.Record{}, err
		}
		if !sameObject(value, stored.record.TenantID, stored.objectKey, stored.record.ContentDigest, value.Content) ||
			int64(len(value.Content)) != stored.contentSize {
			return artifact.Record{}, runtime.ErrVersionMismatch
		}
		stored.record.Content = append([]byte(nil), value.Content...)
	default:
		return artifact.Record{}, runtime.ErrVersionMismatch
	}
	return stored.record, nil
}

func validateInput(in artifact.Record) error {
	id, ref, err := artifact.StableIdentity(in.TenantID, in.RequestID, in.Ordinal, in.SourceDigest)
	if err != nil {
		return err
	}
	if in.ArtifactID != id || in.ArtifactRef != ref || !contentMatches(in.Content, in.ContentDigest) || in.MediaType == "" ||
		(in.Kind != "image" && in.Kind != "file") || len(in.Content) == 0 || in.MalwareScanVersion == "" || in.DLPVersion == "" {
		return runtime.ErrInvalidEnvelope
	}
	return nil
}

func contentMatches(content []byte, expected string) bool {
	digest := sha256.Sum256(content)
	return len(content) > 0 && expected == hex.EncodeToString(digest[:])
}

func sameArtifact(left, right artifact.Record) bool {
	return left.TenantID == right.TenantID && left.RequestID == right.RequestID && left.ArtifactID == right.ArtifactID &&
		left.ArtifactRef == right.ArtifactRef && left.Ordinal == right.Ordinal && left.SourceDigest == right.SourceDigest &&
		left.ContentDigest == right.ContentDigest && left.MediaType == right.MediaType && left.Kind == right.Kind &&
		left.MalwareScanVersion == right.MalwareScanVersion && left.DLPVersion == right.DLPVersion && bytes.Equal(left.Content, right.Content)
}

func sameObject(value objectstore.Object, tenantID, objectKey, contentDigest string, content []byte) bool {
	return value.TenantID == tenantID && value.ObjectKey == objectKey && value.ContentDigest == contentDigest &&
		bytes.Equal(value.Content, content) && objectstore.Validate(value) == nil
}

type sqlStater interface{ SQLState() string }

func translate(err error) error {
	if err == nil {
		return nil
	}
	var state sqlStater
	if errors.As(err, &state) {
		switch state.SQLState() {
		case "23505":
			return runtime.ErrIdempotencyCollision
		case "23503", "23514":
			return runtime.ErrInvalidEnvelope
		case "40001":
			return runtime.ErrVersionConflict
		}
	}
	return err
}

var _ artifact.Store = (*Store)(nil)
