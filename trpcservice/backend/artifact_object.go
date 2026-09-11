package backend

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cyl6/trpc-agent-service/migrations"
	"github.com/cyl6/trpc-agent-service/trpcservice/observability"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	s3storage "trpc.group/trpc-go/trpc-agent-go/storage/s3"
)

const (
	artifactDefaultMediaType = "application/octet-stream"
	artifactRecoveryDelay    = 30 * time.Second
	artifactRecoveryBatch    = 100
)

var (
	ErrArtifactInvalid       = errors.New("artifact object: invalid request")
	ErrArtifactUnavailable   = errors.New("artifact object: backend unavailable")
	ErrArtifactInconsistent  = errors.New("artifact object: metadata and blob are inconsistent")
	ErrArtifactSchemaMissing = errors.New("artifact object: metadata schema is unavailable")
)

type artifactScope struct {
	TenantID  string
	AppName   string
	UserID    string
	SessionID string
}

type artifactRecord struct {
	Scope     artifactScope
	Filename  string
	Revision  int
	ObjectKey string
	MediaType string
	Size      int64
	SHA256    string
	State     string
	UpdatedAt time.Time
}

type artifactReservation struct {
	Scope        artifactScope
	Filename     string
	ObjectPrefix string
	MediaType    string
	Size         int64
	SHA256       string
}

type artifactMetadata interface {
	Reserve(context.Context, artifactReservation) (artifactRecord, error)
	MarkReady(context.Context, artifactRecord) (bool, error)
	MarkUploadFailed(context.Context, artifactRecord, string) error
	MarkDeleteRetry(context.Context, artifactRecord) error
	Find(context.Context, artifactScope, string, *int) (artifactRecord, bool, error)
	ListKeys(context.Context, artifactScope) ([]string, error)
	ListVersions(context.Context, artifactScope, string) ([]int, error)
	MarkDeletePending(context.Context, artifactScope, string) ([]artifactRecord, error)
	MarkDeleted(context.Context, []artifactRecord) error
	ListRecoverable(context.Context, string, string, time.Time, int) ([]artifactRecord, error)
}

// ObjectArtifact couples an S3-compatible blob store to a PostgreSQL recovery
// ledger. PostgreSQL is authoritative for revision allocation and visibility;
// object listings are never used to allocate a revision.
type ObjectArtifact struct {
	metadata artifactMetadata
	blobs    s3storage.Client
	tenantID string
	appName  string

	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

var _ artifact.Service = (*ObjectArtifact)(nil)

// NewPostgresObjectArtifact verifies the independently migrated metadata
// schema and starts bounded reconciliation for cross-store partial failures.
// The returned service owns both pool and blobs.
func NewPostgresObjectArtifact(
	ctx context.Context,
	pool *pgxpool.Pool,
	blobs s3storage.Client,
	tenantID string,
	appName string,
	reconcileInterval time.Duration,
) (*ObjectArtifact, error) {
	if pool == nil || blobs == nil || tenantID == "" || appName == "" {
		return nil, ErrArtifactInvalid
	}
	if err := migrations.VerifyArtifactObjects(ctx, pool); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrArtifactSchemaMissing, stableArtifactSchemaError(err))
	}
	metadata := &postgresArtifactMetadata{pool: pool}
	if err := metadata.verifyAccess(ctx, tenantID); err != nil {
		return nil, ErrArtifactSchemaMissing
	}
	service, err := newObjectArtifact(metadata, blobs, tenantID, appName, reconcileInterval)
	if err != nil {
		return nil, err
	}
	return service, nil
}

func newObjectArtifact(
	metadata artifactMetadata,
	blobs s3storage.Client,
	tenantID string,
	appName string,
	reconcileInterval time.Duration,
) (*ObjectArtifact, error) {
	if metadata == nil || blobs == nil || tenantID == "" || appName == "" {
		return nil, ErrArtifactInvalid
	}
	s := &ObjectArtifact{metadata: metadata, blobs: blobs, tenantID: tenantID, appName: appName}
	if reconcileInterval > 0 {
		ctx, cancel := context.WithCancel(context.Background())
		s.cancel = cancel
		s.done = make(chan struct{})
		go s.reconcileLoop(ctx, reconcileInterval)
	}
	return s, nil
}

func (s *ObjectArtifact) SaveArtifact(ctx context.Context, info artifact.SessionInfo, filename string, value *artifact.Artifact) (revision int, err error) {
	ctx, finish := observability.StartStorage(ctx, "artifact.put", "object", s.tenantID, "")
	defer func() { finish(err) }()
	scope, err := s.validateScope(info, filename)
	if err != nil || value == nil {
		return 0, ErrArtifactInvalid
	}
	mediaType := value.MimeType
	if mediaType == "" {
		mediaType = artifactDefaultMediaType
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(value.Data))
	prefix := buildArtifactObjectPrefix(scope, filename)
	record, err := s.metadata.Reserve(ctx, artifactReservation{
		Scope: scope, Filename: filename, ObjectPrefix: prefix, MediaType: mediaType,
		Size: int64(len(value.Data)), SHA256: digest,
	})
	if err != nil {
		return 0, ErrArtifactUnavailable
	}
	if err := s.blobs.PutObject(ctx, record.ObjectKey, value.Data, mediaType); err != nil {
		// A timed-out PutObject can still have committed remotely. Keep the row
		// pending so reconciliation can resolve the ambiguous outcome.
		_ = s.metadata.MarkUploadFailed(ctx, record, "put_ambiguous")
		return 0, ErrArtifactUnavailable
	}
	ready, err := s.metadata.MarkReady(ctx, record)
	if err != nil {
		return 0, ErrArtifactUnavailable
	}
	if !ready {
		// A delete may have won after the revision was reserved. Compensate the
		// just-uploaded blob so a deleted metadata row cannot leave an orphan.
		if deleteErr := s.blobs.DeleteObjects(ctx, []string{record.ObjectKey}); deleteErr != nil {
			_ = s.metadata.MarkDeleteRetry(ctx, record)
		}
		return 0, ErrArtifactInconsistent
	}
	return record.Revision, nil
}

func (s *ObjectArtifact) LoadArtifact(ctx context.Context, info artifact.SessionInfo, filename string, version *int) (result *artifact.Artifact, err error) {
	ctx, finish := observability.StartStorage(ctx, "artifact.get", "object", s.tenantID, "")
	defer func() { finish(err) }()
	scope, err := s.validateScope(info, filename)
	if err != nil || (version != nil && *version < 0) {
		return nil, ErrArtifactInvalid
	}
	record, found, err := s.metadata.Find(ctx, scope, filename, version)
	if err != nil {
		return nil, ErrArtifactUnavailable
	}
	if !found {
		return nil, nil
	}
	data, mediaType, err := s.blobs.GetObject(ctx, record.ObjectKey)
	if err != nil {
		if errors.Is(err, s3storage.ErrNotFound) {
			return nil, ErrArtifactInconsistent
		}
		return nil, ErrArtifactUnavailable
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(data))
	if int64(len(data)) != record.Size || digest != record.SHA256 {
		return nil, ErrArtifactInconsistent
	}
	if mediaType == "" {
		mediaType = record.MediaType
	}
	return &artifact.Artifact{Data: data, MimeType: mediaType, Name: filename}, nil
}

func (s *ObjectArtifact) ListArtifactKeys(ctx context.Context, info artifact.SessionInfo) ([]string, error) {
	scope, err := s.validateScope(info, "placeholder")
	if err != nil {
		return nil, ErrArtifactInvalid
	}
	keys, err := s.metadata.ListKeys(ctx, scope)
	if err != nil {
		return nil, ErrArtifactUnavailable
	}
	slices.Sort(keys)
	return keys, nil
}

func (s *ObjectArtifact) DeleteArtifact(ctx context.Context, info artifact.SessionInfo, filename string) (err error) {
	ctx, finish := observability.StartStorage(ctx, "artifact.delete", "object", s.tenantID, "")
	defer func() { finish(err) }()
	scope, err := s.validateScope(info, filename)
	if err != nil {
		return ErrArtifactInvalid
	}
	records, err := s.metadata.MarkDeletePending(ctx, scope, filename)
	if err != nil {
		return ErrArtifactUnavailable
	}
	if len(records) == 0 {
		return nil
	}
	keys := make([]string, len(records))
	for i := range records {
		keys[i] = records[i].ObjectKey
	}
	if err := s.blobs.DeleteObjects(ctx, keys); err != nil {
		return ErrArtifactUnavailable
	}
	if err := s.metadata.MarkDeleted(ctx, records); err != nil {
		return ErrArtifactUnavailable
	}
	return nil
}

func (s *ObjectArtifact) ListVersions(ctx context.Context, info artifact.SessionInfo, filename string) ([]int, error) {
	scope, err := s.validateScope(info, filename)
	if err != nil {
		return nil, ErrArtifactInvalid
	}
	versions, err := s.metadata.ListVersions(ctx, scope, filename)
	if err != nil {
		return nil, ErrArtifactUnavailable
	}
	slices.Sort(versions)
	return versions, nil
}

// Reconcile repairs bounded, stale cross-store operations. It is public so an
// operational command can run an immediate pass without waiting for a ticker.
func (s *ObjectArtifact) Reconcile(ctx context.Context, limit int) (repaired int, err error) {
	ctx, finish := observability.StartStorage(ctx, "artifact.recovery", "object", s.tenantID, "")
	defer func() { finish(err) }()
	if limit <= 0 || limit > artifactRecoveryBatch {
		limit = artifactRecoveryBatch
	}
	records, err := s.metadata.ListRecoverable(ctx, s.tenantID, s.appName, time.Now().Add(-artifactRecoveryDelay), limit)
	if err != nil {
		return 0, ErrArtifactUnavailable
	}
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return repaired, err
		}
		switch record.State {
		case "delete_pending":
			if err := s.blobs.DeleteObjects(ctx, []string{record.ObjectKey}); err != nil {
				continue
			}
			if err := s.metadata.MarkDeleted(ctx, []artifactRecord{record}); err == nil {
				repaired++
			}
		case "upload_pending":
			data, _, getErr := s.blobs.GetObject(ctx, record.ObjectKey)
			if getErr != nil {
				if errors.Is(getErr, s3storage.ErrNotFound) {
					_ = s.metadata.MarkUploadFailed(ctx, record, "blob_missing")
				}
				continue
			}
			digest := fmt.Sprintf("%x", sha256.Sum256(data))
			if int64(len(data)) != record.Size || digest != record.SHA256 {
				_ = s.metadata.MarkUploadFailed(ctx, record, "checksum_mismatch")
				continue
			}
			if ok, markErr := s.metadata.MarkReady(ctx, record); markErr == nil && ok {
				repaired++
			}
		}
	}
	return repaired, nil
}

func (s *ObjectArtifact) reconcileLoop(ctx context.Context, interval time.Duration) {
	defer close(s.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			passCtx, cancel := context.WithTimeout(ctx, interval)
			_, _ = s.Reconcile(passCtx, artifactRecoveryBatch)
			cancel()
		}
	}
}

func (s *ObjectArtifact) Close() error {
	var result error
	s.once.Do(func() {
		if s.cancel != nil {
			s.cancel()
			<-s.done
		}
		result = s.blobs.Close()
		if postgres, ok := s.metadata.(*postgresArtifactMetadata); ok {
			postgres.pool.Close()
		}
	})
	return result
}

func (s *ObjectArtifact) validateScope(info artifact.SessionInfo, filename string) (artifactScope, error) {
	if s == nil || s.tenantID == "" || s.appName == "" || info.AppName != s.appName ||
		info.UserID == "" || info.SessionID == "" || !validArtifactFilename(filename) {
		return artifactScope{}, ErrArtifactInvalid
	}
	sessionID := info.SessionID
	if strings.HasPrefix(filename, "user:") {
		sessionID = ""
	}
	return artifactScope{TenantID: s.tenantID, AppName: s.appName, UserID: info.UserID, SessionID: sessionID}, nil
}

func validArtifactFilename(filename string) bool {
	return filename != "" && !strings.Contains(filename, "/") && !strings.Contains(filename, "\\") &&
		!strings.Contains(filename, "..") && !strings.ContainsRune(filename, '\x00')
}

func buildArtifactObjectPrefix(scope artifactScope, filename string) string {
	encode := func(value string) string { return base64.RawURLEncoding.EncodeToString([]byte(value)) }
	parts := []string{"artifacts", "v1", encode(scope.TenantID), encode(scope.AppName), encode(scope.UserID)}
	if scope.SessionID == "" {
		parts = append(parts, "user")
	} else {
		parts = append(parts, "session", encode(scope.SessionID))
	}
	return strings.Join(append(parts, encode(filename)), "/")
}

func stableArtifactSchemaError(err error) error {
	if errors.Is(err, migrations.ErrArtifactObjectsNotApplied) {
		return migrations.ErrArtifactObjectsNotApplied
	}
	if errors.Is(err, migrations.ErrArtifactObjectsChecksumMismatch) {
		return migrations.ErrArtifactObjectsChecksumMismatch
	}
	return migrations.ErrArtifactObjectsVerification
}

type postgresArtifactMetadata struct{ pool *pgxpool.Pool }

func (p *postgresArtifactMetadata) verifyAccess(ctx context.Context, tenantID string) error {
	tx, err := p.begin(ctx, tenantID)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var rowSecurity, forceRowSecurity, privileges bool
	if err := tx.QueryRow(ctx, `
		SELECT c.relrowsecurity, c.relforcerowsecurity,
		       has_table_privilege(current_user, c.oid, 'SELECT,INSERT,UPDATE,DELETE')
		FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		WHERE n.nspname=current_schema() AND c.relname='artifact_blob_versions'`).Scan(
		&rowSecurity, &forceRowSecurity, &privileges,
	); err != nil || !rowSecurity || !forceRowSecurity || !privileges {
		return ErrArtifactSchemaMissing
	}
	required := []string{
		"tenant_id", "app_name", "user_id", "session_id", "filename", "revision",
		"object_key", "media_type", "size_bytes", "content_sha256", "state",
		"last_error_type", "attempt_count", "created_at", "updated_at",
	}
	var columnCount int
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM pg_attribute a
		JOIN pg_class c ON c.oid=a.attrelid
		JOIN pg_namespace n ON n.oid=c.relnamespace
		WHERE n.nspname=current_schema() AND c.relname='artifact_blob_versions'
		  AND a.attname=ANY($1::text[]) AND NOT a.attisdropped`, required).Scan(&columnCount); err != nil || columnCount != len(required) {
		return ErrArtifactSchemaMissing
	}
	return tx.Commit(ctx)
}

func (p *postgresArtifactMetadata) begin(ctx context.Context, tenantID string) (pgx.Tx, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id', $1, true)`, tenantID); err != nil {
		_ = tx.Rollback(context.Background())
		return nil, err
	}
	return tx, nil
}

func artifactLockKey(scope artifactScope, filename string) string {
	return strings.Join([]string{scope.TenantID, scope.AppName, scope.UserID, scope.SessionID, filename}, "\x1f")
}

func (p *postgresArtifactMetadata) Reserve(ctx context.Context, req artifactReservation) (artifactRecord, error) {
	tx, err := p.begin(ctx, req.Scope.TenantID)
	if err != nil {
		return artifactRecord{}, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, artifactLockKey(req.Scope, req.Filename)); err != nil {
		return artifactRecord{}, err
	}
	var revision int
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(MAX(revision), -1) + 1 FROM artifact_blob_versions
		WHERE tenant_id=$1 AND app_name=$2 AND user_id=$3 AND session_id=$4 AND filename=$5`,
		req.Scope.TenantID, req.Scope.AppName, req.Scope.UserID, req.Scope.SessionID, req.Filename).Scan(&revision); err != nil {
		return artifactRecord{}, err
	}
	record := artifactRecord{Scope: req.Scope, Filename: req.Filename, Revision: revision,
		ObjectKey: req.ObjectPrefix + "/" + strconv.Itoa(revision), MediaType: req.MediaType,
		Size: req.Size, SHA256: req.SHA256, State: "upload_pending"}
	if _, err := tx.Exec(ctx, `
		INSERT INTO artifact_blob_versions
			(tenant_id, app_name, user_id, session_id, filename, revision, object_key,
			 media_type, size_bytes, content_sha256, state)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'upload_pending')`,
		record.Scope.TenantID, record.Scope.AppName, record.Scope.UserID, record.Scope.SessionID,
		record.Filename, record.Revision, record.ObjectKey, record.MediaType, record.Size, record.SHA256); err != nil {
		return artifactRecord{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return artifactRecord{}, err
	}
	return record, nil
}

func (p *postgresArtifactMetadata) MarkReady(ctx context.Context, record artifactRecord) (bool, error) {
	tx, err := p.begin(ctx, record.Scope.TenantID)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	tag, err := tx.Exec(ctx, `
		UPDATE artifact_blob_versions SET state='ready', last_error_type='', attempt_count=attempt_count+1
		WHERE tenant_id=$1 AND object_key=$2 AND state='upload_pending' AND content_sha256=$3`,
		record.Scope.TenantID, record.ObjectKey, record.SHA256)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (p *postgresArtifactMetadata) MarkUploadFailed(ctx context.Context, record artifactRecord, errorType string) error {
	tx, err := p.begin(ctx, record.Scope.TenantID)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	// put_ambiguous remains recoverable; a reconciliation read decides whether
	// the object exists. Definitive missing/corruption states become terminal.
	state := "upload_failed"
	if errorType == "put_ambiguous" {
		state = "upload_pending"
	}
	_, err = tx.Exec(ctx, `
		UPDATE artifact_blob_versions SET state=$3, last_error_type=$4, attempt_count=attempt_count+1
		WHERE tenant_id=$1 AND object_key=$2 AND state='upload_pending'`,
		record.Scope.TenantID, record.ObjectKey, state, errorType)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (p *postgresArtifactMetadata) MarkDeleteRetry(ctx context.Context, record artifactRecord) error {
	tx, err := p.begin(ctx, record.Scope.TenantID)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	_, err = tx.Exec(ctx, `
		UPDATE artifact_blob_versions
		SET state='delete_pending', last_error_type='delete_compensation_failed', attempt_count=attempt_count+1
		WHERE tenant_id=$1 AND object_key=$2 AND state='deleted'`, record.Scope.TenantID, record.ObjectKey)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (p *postgresArtifactMetadata) Find(ctx context.Context, scope artifactScope, filename string, version *int) (artifactRecord, bool, error) {
	tx, err := p.begin(ctx, scope.TenantID)
	if err != nil {
		return artifactRecord{}, false, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	query := `SELECT revision, object_key, media_type, size_bytes, content_sha256, state, updated_at
		FROM artifact_blob_versions WHERE tenant_id=$1 AND app_name=$2 AND user_id=$3 AND session_id=$4
		AND filename=$5 AND state='ready'`
	args := []any{scope.TenantID, scope.AppName, scope.UserID, scope.SessionID, filename}
	if version != nil {
		query += ` AND revision=$6`
		args = append(args, *version)
	} else {
		query += ` ORDER BY revision DESC LIMIT 1`
	}
	record := artifactRecord{Scope: scope, Filename: filename}
	err = tx.QueryRow(ctx, query, args...).Scan(&record.Revision, &record.ObjectKey, &record.MediaType, &record.Size, &record.SHA256, &record.State, &record.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return artifactRecord{}, false, commitErr
		}
		return artifactRecord{}, false, nil
	}
	if err != nil {
		return artifactRecord{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return artifactRecord{}, false, err
	}
	return record, true, nil
}

func (p *postgresArtifactMetadata) ListKeys(ctx context.Context, scope artifactScope) ([]string, error) {
	tx, err := p.begin(ctx, scope.TenantID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	rows, err := tx.Query(ctx, `
		SELECT DISTINCT filename FROM artifact_blob_versions
		WHERE tenant_id=$1 AND app_name=$2 AND user_id=$3 AND session_id IN ('', $4) AND state='ready'
		ORDER BY filename`, scope.TenantID, scope.AppName, scope.UserID, scope.SessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return keys, nil
}

func (p *postgresArtifactMetadata) ListVersions(ctx context.Context, scope artifactScope, filename string) ([]int, error) {
	tx, err := p.begin(ctx, scope.TenantID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	rows, err := tx.Query(ctx, `SELECT revision FROM artifact_blob_versions
		WHERE tenant_id=$1 AND app_name=$2 AND user_id=$3 AND session_id=$4 AND filename=$5 AND state='ready'
		ORDER BY revision`, scope.TenantID, scope.AppName, scope.UserID, scope.SessionID, filename)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var versions []int
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		versions = append(versions, version)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return versions, nil
}

func (p *postgresArtifactMetadata) MarkDeletePending(ctx context.Context, scope artifactScope, filename string) ([]artifactRecord, error) {
	tx, err := p.begin(ctx, scope.TenantID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, artifactLockKey(scope, filename)); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		UPDATE artifact_blob_versions SET state='delete_pending', last_error_type='', attempt_count=attempt_count+1
		WHERE tenant_id=$1 AND app_name=$2 AND user_id=$3 AND session_id=$4 AND filename=$5
		  AND state IN ('upload_pending','upload_failed','ready','delete_pending')
		RETURNING revision, object_key, media_type, size_bytes, content_sha256, state, updated_at`,
		scope.TenantID, scope.AppName, scope.UserID, scope.SessionID, filename)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []artifactRecord
	for rows.Next() {
		record := artifactRecord{Scope: scope, Filename: filename}
		if err := rows.Scan(&record.Revision, &record.ObjectKey, &record.MediaType, &record.Size, &record.SHA256, &record.State, &record.UpdatedAt); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return records, nil
}

func (p *postgresArtifactMetadata) MarkDeleted(ctx context.Context, records []artifactRecord) error {
	if len(records) == 0 {
		return nil
	}
	tx, err := p.begin(ctx, records[0].Scope.TenantID)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	for _, record := range records {
		if record.Scope.TenantID != records[0].Scope.TenantID {
			return ErrArtifactInvalid
		}
		if _, err := tx.Exec(ctx, `UPDATE artifact_blob_versions SET state='deleted', last_error_type=''
			WHERE tenant_id=$1 AND object_key=$2 AND state='delete_pending'`, record.Scope.TenantID, record.ObjectKey); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (p *postgresArtifactMetadata) ListRecoverable(ctx context.Context, tenantID, appName string, before time.Time, limit int) ([]artifactRecord, error) {
	tx, err := p.begin(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	rows, err := tx.Query(ctx, `SELECT app_name, user_id, session_id, filename, revision, object_key,
		media_type, size_bytes, content_sha256, state, updated_at FROM artifact_blob_versions
		WHERE tenant_id=$1 AND app_name=$2 AND state IN ('upload_pending','delete_pending') AND updated_at < $3
		ORDER BY updated_at, object_key LIMIT $4`, tenantID, appName, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []artifactRecord
	for rows.Next() {
		record := artifactRecord{Scope: artifactScope{TenantID: tenantID}}
		if err := rows.Scan(&record.Scope.AppName, &record.Scope.UserID, &record.Scope.SessionID, &record.Filename,
			&record.Revision, &record.ObjectKey, &record.MediaType, &record.Size, &record.SHA256, &record.State, &record.UpdatedAt); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return records, nil
}
