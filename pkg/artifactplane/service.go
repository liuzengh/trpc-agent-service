// Package artifactplane implements tRPC-Agent-Go artifact.Service with an
// S3-compatible object body plane and PostgreSQL version metadata.
package artifactplane

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"mime"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

var (
	ErrInvalidArtifact          = errors.New("invalid artifact request")
	ErrArtifactCorrupt          = errors.New("artifact body failed integrity verification")
	ErrArtifactVersionConflict  = errors.New("artifact version already contains different content")
	ErrArtifactStoreUnavailable = errors.New("artifact store unavailable")
)

type ObjectStore interface {
	Put(context.Context, string, string, []byte) error
	Get(context.Context, string) ([]byte, error)
	Delete(context.Context, string) error
}

type Service struct {
	tenantID string
	db       *sql.DB
	objects  ObjectStore
	maxBytes int64
}

func NewService(tenantID string, db *sql.DB, objects ObjectStore, maxBytes int64) (*Service, error) {
	if err := tenant.ValidateTenantID(tenantID); err != nil || db == nil || objects == nil || maxBytes <= 0 || maxBytes > 16<<20 {
		return nil, ErrArtifactStoreUnavailable
	}
	return &Service{tenantID: tenantID, db: db, objects: objects, maxBytes: maxBytes}, nil
}

func (s *Service) SaveArtifact(ctx context.Context, info artifact.SessionInfo, filename string, value *artifact.Artifact) (savedVersion int, err error) {
	if err := s.validate(info, filename); err != nil {
		return 0, err
	}
	if value == nil || len(value.Data) == 0 || int64(len(value.Data)) > s.maxBytes {
		return 0, ErrInvalidArtifact
	}
	mediaType, _, err := mime.ParseMediaType(value.MimeType)
	if err != nil || !strings.Contains(mediaType, "/") || len(mediaType) > 255 {
		return 0, ErrInvalidArtifact
	}
	ctx = nonNilContext(ctx)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin artifact version allocation: %w", err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, artifactScope(s.tenantID, info, filename)); err != nil {
		return 0, fmt.Errorf("lock artifact version: %w", err)
	}
	var version int
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), -1) + 1 FROM artifact_versions
		WHERE tenant_id=$1 AND app_name=$2 AND user_id=$3 AND session_id=$4 AND filename=$5`,
		s.tenantID, info.AppName, info.UserID, info.SessionID, filename).Scan(&version); err != nil {
		return 0, fmt.Errorf("allocate artifact version: %w", err)
	}
	key := newObjectKey(s.tenantID, info, filename, version)
	body := append([]byte(nil), value.Data...)
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
			err = errors.Join(err, s.cleanupUnreferencedObject(ctx, info, filename, version, key))
		}
	}()
	if err = s.objects.Put(ctx, key, mediaType, body); err != nil {
		return 0, fmt.Errorf("store artifact body: %w", err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO artifact_versions
		(tenant_id,app_name,user_id,session_id,filename,version,object_key,mime_type,size_bytes,content_sha256)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		s.tenantID, info.AppName, info.UserID, info.SessionID, filename, version, key, mediaType, int64(len(body)), hashBytes(body)); err != nil {
		return 0, fmt.Errorf("persist artifact metadata: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit artifact version: %w", err)
	}
	committed = true
	return version, nil
}

// ProjectVersion writes an exact immutable artifact version. It is intended
// for migration replay, where allocating a new version would corrupt source
// history. Replaying identical bytes repairs the recorded object body;
// reusing a version for different content fails closed.
func (s *Service) ProjectVersion(ctx context.Context, info artifact.SessionInfo, filename string, version int, value *artifact.Artifact) (err error) {
	if err := s.validate(info, filename); err != nil {
		return err
	}
	mediaType, body, err := s.normalizedValue(value)
	if err != nil || version < 0 {
		return ErrInvalidArtifact
	}
	ctx = nonNilContext(ctx)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin artifact projection: %w", err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, artifactScope(s.tenantID, info, filename)); err != nil {
		return fmt.Errorf("lock artifact projection: %w", err)
	}

	var existingMIME, existingHash, key string
	var existingSize int64
	var deletedAt sql.NullTime
	err = tx.QueryRowContext(ctx, `SELECT mime_type,size_bytes,content_sha256,deleted_at,object_key FROM artifact_versions
		WHERE tenant_id=$1 AND app_name=$2 AND user_id=$3 AND session_id=$4 AND filename=$5 AND version=$6
		FOR UPDATE`, s.tenantID, info.AppName, info.UserID, info.SessionID, filename, version).Scan(
		&existingMIME, &existingSize, &existingHash, &deletedAt, &key,
	)
	exists := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read projected artifact version: %w", err)
	}
	wantHash := hashBytes(body)
	if exists && (deletedAt.Valid || existingMIME != mediaType || existingSize != int64(len(body)) || !strings.EqualFold(existingHash, wantHash)) {
		return ErrArtifactVersionConflict
	}

	if !exists {
		key = newObjectKey(s.tenantID, info, filename, version)
	}
	removeOnFailure := !exists
	committed := false
	defer func() {
		if removeOnFailure && !committed {
			_ = tx.Rollback()
			err = errors.Join(err, s.cleanupUnreferencedObject(ctx, info, filename, version, key))
		}
	}()
	if err = s.objects.Put(ctx, key, mediaType, body); err != nil {
		return fmt.Errorf("store projected artifact body: %w", err)
	}
	if !exists {
		if _, err = tx.ExecContext(ctx, `INSERT INTO artifact_versions
			(tenant_id,app_name,user_id,session_id,filename,version,object_key,mime_type,size_bytes,content_sha256)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			s.tenantID, info.AppName, info.UserID, info.SessionID, filename, version, key, mediaType, int64(len(body)), wantHash); err != nil {
			return fmt.Errorf("persist projected artifact metadata: %w", err)
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit artifact projection: %w", err)
	}
	committed = true
	return nil
}

// DeleteProjectedVersion tombstones one exact immutable version. It remains
// idempotent when a migration crashes after the metadata commit but before the
// object delete: a replay returns the same key and retries body cleanup.
func (s *Service) DeleteProjectedVersion(ctx context.Context, info artifact.SessionInfo, filename string, version int) error {
	if err := s.validate(info, filename); err != nil || version < 0 {
		return ErrInvalidArtifact
	}
	ctx = nonNilContext(ctx)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin projected artifact delete: %w", err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, artifactScope(s.tenantID, info, filename)); err != nil {
		return fmt.Errorf("lock projected artifact delete: %w", err)
	}
	var key string
	err = tx.QueryRowContext(ctx, `UPDATE artifact_versions SET deleted_at=COALESCE(deleted_at,clock_timestamp())
		WHERE tenant_id=$1 AND app_name=$2 AND user_id=$3 AND session_id=$4 AND filename=$5 AND version=$6
		RETURNING object_key`, s.tenantID, info.AppName, info.UserID, info.SessionID, filename, version).Scan(&key)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("tombstone projected artifact version: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit projected artifact delete: %w", err)
	}
	if key == "" {
		return nil
	}
	if err = s.objects.Delete(ctx, key); err != nil {
		return fmt.Errorf("delete projected artifact body: %w", err)
	}
	return nil
}

func (s *Service) LoadArtifact(ctx context.Context, info artifact.SessionInfo, filename string, version *int) (*artifact.Artifact, error) {
	if err := s.validate(info, filename); err != nil {
		return nil, err
	}
	query := `SELECT object_key,mime_type,size_bytes,content_sha256 FROM artifact_versions
		WHERE tenant_id=$1 AND app_name=$2 AND user_id=$3 AND session_id=$4 AND filename=$5 AND deleted_at IS NULL`
	args := []any{s.tenantID, info.AppName, info.UserID, info.SessionID, filename}
	if version == nil {
		query += ` ORDER BY version DESC LIMIT 1`
	} else {
		if *version < 0 {
			return nil, ErrInvalidArtifact
		}
		query += ` AND version=$6`
		args = append(args, *version)
	}
	var key, mimeType, hash string
	var size int64
	err := s.db.QueryRowContext(nonNilContext(ctx), query, args...).Scan(&key, &mimeType, &size, &hash)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load artifact metadata: %w", err)
	}
	body, err := s.objects.Get(nonNilContext(ctx), key)
	if err != nil {
		return nil, fmt.Errorf("load artifact body: %w", err)
	}
	if int64(len(body)) != size || !strings.EqualFold(hashBytes(body), hash) {
		return nil, ErrArtifactCorrupt
	}
	return &artifact.Artifact{Data: body, MimeType: mimeType, Name: filename}, nil
}

func (s *Service) ListArtifactKeys(ctx context.Context, info artifact.SessionInfo) ([]string, error) {
	if err := s.validate(info, "placeholder"); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(nonNilContext(ctx), `SELECT DISTINCT filename FROM artifact_versions
		WHERE tenant_id=$1 AND app_name=$2 AND user_id=$3 AND session_id=$4 AND deleted_at IS NULL ORDER BY filename`,
		s.tenantID, info.AppName, info.UserID, info.SessionID)
	if err != nil {
		return nil, fmt.Errorf("list artifact keys: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

func (s *Service) DeleteArtifact(ctx context.Context, info artifact.SessionInfo, filename string) error {
	if err := s.validate(info, filename); err != nil {
		return err
	}
	ctx = nonNilContext(ctx)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, artifactScope(s.tenantID, info, filename)); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `UPDATE artifact_versions SET deleted_at=COALESCE(deleted_at,clock_timestamp())
		WHERE tenant_id=$1 AND app_name=$2 AND user_id=$3 AND session_id=$4 AND filename=$5 RETURNING object_key`,
		s.tenantID, info.AppName, info.UserID, info.SessionID, filename)
	if err != nil {
		return fmt.Errorf("tombstone artifact: %w", err)
	}
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			rows.Close()
			return err
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("read tombstoned artifact keys: %w", err)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit artifact tombstone: %w", err)
	}
	var deleteErrors []error
	for _, key := range keys {
		if err := s.objects.Delete(ctx, key); err != nil {
			deleteErrors = append(deleteErrors, fmt.Errorf("delete tombstoned artifact body: %w", err))
		}
	}
	return errors.Join(deleteErrors...)
}

func (s *Service) ListVersions(ctx context.Context, info artifact.SessionInfo, filename string) ([]int, error) {
	if err := s.validate(info, filename); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(nonNilContext(ctx), `SELECT version FROM artifact_versions
		WHERE tenant_id=$1 AND app_name=$2 AND user_id=$3 AND session_id=$4 AND filename=$5 AND deleted_at IS NULL ORDER BY version`,
		s.tenantID, info.AppName, info.UserID, info.SessionID, filename)
	if err != nil {
		return nil, fmt.Errorf("list artifact versions: %w", err)
	}
	defer rows.Close()
	var out []int
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		out = append(out, version)
	}
	sort.Ints(out)
	return out, rows.Err()
}

func (s *Service) validate(info artifact.SessionInfo, filename string) error {
	if s == nil || s.db == nil || s.objects == nil {
		return ErrArtifactStoreUnavailable
	}
	if err := tenant.ValidateAgentAppName(info.AppName); err != nil || !safeText(info.UserID, 255) || !safeText(info.SessionID, 512) ||
		!safeText(filename, 512) || strings.ContainsAny(filename, "/\\") || filename == "." || filename == ".." {
		return ErrInvalidArtifact
	}
	return nil
}

func (s *Service) normalizedValue(value *artifact.Artifact) (string, []byte, error) {
	if value == nil || len(value.Data) == 0 || int64(len(value.Data)) > s.maxBytes {
		return "", nil, ErrInvalidArtifact
	}
	mediaType, _, err := mime.ParseMediaType(value.MimeType)
	if err != nil || !strings.Contains(mediaType, "/") || len(mediaType) > 255 {
		return "", nil, ErrInvalidArtifact
	}
	return mediaType, append([]byte(nil), value.Data...), nil
}

func safeText(value string, max int) bool {
	if value == "" || len(value) > max || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf) {
			return false
		}
	}
	return true
}
func artifactScope(tenantID string, info artifact.SessionInfo, filename string) string {
	digest := sha256.Sum256([]byte(tenantID + "\x00" + info.AppName + "\x00" + info.UserID + "\x00" + info.SessionID + "\x00" + filename))
	// PostgreSQL text values cannot contain NUL. A fixed digest also keeps
	// user-controlled identifiers out of pg_stat_activity while preserving a
	// deterministic advisory-lock scope.
	return hex.EncodeToString(digest[:])
}
func objectKey(tenantID string, info artifact.SessionInfo, filename string, version int) string {
	encode := func(v string) string { return base64.RawURLEncoding.EncodeToString([]byte(v)) }
	return fmt.Sprintf("tsa-artifacts/v1/%s/%s/%s/%s/%s/%020d", encode(tenantID), encode(info.AppName), encode(info.UserID), encode(info.SessionID), encode(filename), version)
}

func newObjectKey(tenantID string, info artifact.SessionInfo, filename string, version int) string {
	// A delayed cleanup from an uncertain write must never target a later
	// writer that allocated the same logical version after a rollback.
	return objectKey(tenantID, info, filename, version) + "/" + uuid.NewString()
}

func (s *Service) cleanupUnreferencedObject(ctx context.Context, info artifact.SessionInfo, filename string, version int, key string) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(cleanup, nil)
	if err != nil {
		return fmt.Errorf("begin artifact cleanup verification: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(cleanup, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, artifactScope(s.tenantID, info, filename)); err != nil {
		return fmt.Errorf("lock artifact cleanup: %w", err)
	}
	var referenced int
	err = tx.QueryRowContext(cleanup, `SELECT 1 FROM artifact_versions
		WHERE tenant_id=$1 AND app_name=$2 AND user_id=$3 AND session_id=$4 AND filename=$5 AND version=$6 AND object_key=$7`,
		s.tenantID, info.AppName, info.UserID, info.SessionID, filename, version, key).Scan(&referenced)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("verify artifact cleanup reference: %w", err)
	}
	if err := s.objects.Delete(cleanup, key); err != nil {
		return fmt.Errorf("delete unreferenced artifact body: %w", err)
	}
	return nil
}

func hashBytes(value []byte) string { sum := sha256.Sum256(value); return hex.EncodeToString(sum[:]) }
func nonNilContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

var _ artifact.Service = (*Service)(nil)
