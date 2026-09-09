package artifactplane

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/minio/minio-go/v7"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
)

type MigrationEntry struct {
	AppName   string `json:"app"`
	UserID    string `json:"user"`
	SessionID string `json:"session"`
	Filename  string `json:"file"`
	Version   int    `json:"version"`
	MIMEType  string `json:"mime,omitempty"`
	Deleted   bool   `json:"deleted,omitempty"`
}

func (s *Service) MigrationTargetEmpty(ctx context.Context) (bool, error) {
	inspector, ok := s.objects.(interface {
		HasObjectPrefix(context.Context, string) (bool, error)
	})
	if !ok {
		return false, ErrArtifactStoreUnavailable
	}
	prefix := "tsa-artifacts/v1/" + base64.RawURLEncoding.EncodeToString([]byte(s.tenantID)) + "/"
	found, err := inspector.HasObjectPrefix(ctx, prefix)
	return !found, err
}

func (s *MinIOStore) HasObjectPrefix(ctx context.Context, prefix string) (bool, error) {
	if s == nil || s.client == nil || prefix == "" {
		return false, ErrArtifactStoreUnavailable
	}
	probe, cancel := context.WithCancel(nonNilContext(ctx))
	defer cancel()
	for object := range s.client.ListObjects(probe, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true, MaxKeys: 1}) {
		if object.Err != nil {
			return false, object.Err
		}
		return true, nil
	}
	return false, nil
}

func (e MigrationEntry) SessionInfo() artifact.SessionInfo {
	return artifact.SessionInfo{AppName: e.AppName, UserID: e.UserID, SessionID: e.SessionID}
}

// MigrationEntries includes every immutable version and tombstone. The tuple
// cursor remains valid when earlier files are deleted or new files are added.
func (s *Service) MigrationEntries(ctx context.Context, cursor string, limit int) ([]MigrationEntry, string, bool, error) {
	if s == nil || s.db == nil || limit < 1 || limit > 1000 {
		return nil, "", false, ErrInvalidArtifact
	}
	var after MigrationEntry
	if cursor != "" {
		body, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil || len(body) > 8192 || json.Unmarshal(body, &after) != nil || s.validate(after.SessionInfo(), after.Filename) != nil || after.Version < 0 {
			return nil, "", false, ErrInvalidArtifact
		}
	}
	query := `SELECT app_name,user_id,session_id,filename,version,mime_type,deleted_at IS NOT NULL
		FROM artifact_versions WHERE tenant_id=$1`
	args := []any{s.tenantID}
	if cursor != "" {
		query += ` AND (app_name,user_id,session_id,filename,version)>($2,$3,$4,$5,$6)`
		args = append(args, after.AppName, after.UserID, after.SessionID, after.Filename, after.Version)
	}
	query += fmt.Sprintf(` ORDER BY app_name,user_id,session_id,filename,version LIMIT $%d`, len(args)+1)
	args = append(args, limit+1)
	rows, err := s.db.QueryContext(nonNilContext(ctx), query, args...)
	if err != nil {
		return nil, "", false, err
	}
	defer rows.Close()
	entries := make([]MigrationEntry, 0, limit+1)
	for rows.Next() {
		var e MigrationEntry
		if err := rows.Scan(&e.AppName, &e.UserID, &e.SessionID, &e.Filename, &e.Version, &e.MIMEType, &e.Deleted); err != nil {
			return nil, "", false, err
		}
		if s.validate(e.SessionInfo(), e.Filename) != nil || e.Version < 0 {
			return nil, "", false, ErrInvalidArtifact
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, "", false, err
	}
	if len(entries) <= limit {
		return entries, "", true, nil
	}
	entries = entries[:limit]
	last := entries[len(entries)-1]
	last.MIMEType, last.Deleted = "", false
	body, err := json.Marshal(last)
	if err != nil {
		return nil, "", false, err
	}
	return entries, base64.RawURLEncoding.EncodeToString(body), false, nil
}

// MigrationFileEntries includes tombstoned versions so retried deletion can
// finish cleanup in both the source and destination bucket.
func (s *Service) MigrationFileEntries(ctx context.Context, info artifact.SessionInfo, filename string) ([]MigrationEntry, error) {
	if err := s.validate(info, filename); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(nonNilContext(ctx), `SELECT version,mime_type,deleted_at IS NOT NULL FROM artifact_versions
		WHERE tenant_id=$1 AND app_name=$2 AND user_id=$3 AND session_id=$4 AND filename=$5 ORDER BY version`, s.tenantID, info.AppName, info.UserID, info.SessionID, filename)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []MigrationEntry
	for rows.Next() {
		e := MigrationEntry{AppName: info.AppName, UserID: info.UserID, SessionID: info.SessionID, Filename: filename}
		if err := rows.Scan(&e.Version, &e.MIMEType, &e.Deleted); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

func (s *Service) NextMigrationVersion(ctx context.Context, info artifact.SessionInfo, filename string) (int, error) {
	if err := s.validate(info, filename); err != nil {
		return 0, err
	}
	var version int
	err := s.db.QueryRowContext(nonNilContext(ctx), `SELECT COALESCE(MAX(version),-1)+1 FROM artifact_versions
		WHERE tenant_id=$1 AND app_name=$2 AND user_id=$3 AND session_id=$4 AND filename=$5`, s.tenantID, info.AppName, info.UserID, info.SessionID, filename).Scan(&version)
	return version, err
}

// ReconcileMigrationVersion retries source cleanup after the deletion's SQL
// commit. Active versions and identities from failed allocations stay intact.
func (s *Service) ReconcileMigrationVersion(ctx context.Context, info artifact.SessionInfo, filename string, version int, mimeType string) error {
	key, actualMIME, deleted, err := s.migrationVersionState(ctx, info, filename, version)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if actualMIME != mimeType || !deleted {
		return nil
	}
	return s.objects.Delete(nonNilContext(ctx), key)
}

// DeleteMigrationVersion applies only a matching SQL tombstone. Both buckets
// share metadata, so a still-active row must never be deleted by projection.
func (s *Service) DeleteMigrationVersion(ctx context.Context, info artifact.SessionInfo, filename string, version int, mimeType string) error {
	key, actualMIME, deleted, err := s.migrationVersionState(ctx, info, filename, version)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if actualMIME != mimeType {
		return nil
	}
	if !deleted {
		return ErrArtifactVersionConflict
	}
	return s.objects.Delete(nonNilContext(ctx), key)
}

func (s *Service) migrationVersionState(ctx context.Context, info artifact.SessionInfo, filename string, version int) (key, mimeType string, deleted bool, err error) {
	if err := s.validate(info, filename); err != nil || version < 0 {
		return "", "", false, ErrInvalidArtifact
	}
	err = s.db.QueryRowContext(nonNilContext(ctx), `SELECT object_key,mime_type,deleted_at IS NOT NULL FROM artifact_versions
		WHERE tenant_id=$1 AND app_name=$2 AND user_id=$3 AND session_id=$4 AND filename=$5 AND version=$6`,
		s.tenantID, info.AppName, info.UserID, info.SessionID, filename, version).Scan(&key, &mimeType, &deleted)
	if err == nil && !strings.HasPrefix(key, objectKey(s.tenantID, info, filename, version)+"/") {
		err = ErrArtifactCorrupt
	}
	return
}

// ReadMigrationVersion verifies the actual bucket as well as shared metadata.
// A tombstone whose object remains in this bucket is not a verified deletion.
func (s *Service) ReadMigrationVersion(ctx context.Context, info artifact.SessionInfo, filename string, version int) (MigrationEntry, *artifact.Artifact, error) {
	if err := s.validate(info, filename); err != nil || version < 0 {
		return MigrationEntry{}, nil, ErrInvalidArtifact
	}
	entry := MigrationEntry{AppName: info.AppName, UserID: info.UserID, SessionID: info.SessionID, Filename: filename, Version: version}
	var key, hash string
	var size int64
	err := s.db.QueryRowContext(nonNilContext(ctx), `SELECT object_key,mime_type,size_bytes,content_sha256,deleted_at IS NOT NULL FROM artifact_versions
		WHERE tenant_id=$1 AND app_name=$2 AND user_id=$3 AND session_id=$4 AND filename=$5 AND version=$6`, s.tenantID, info.AppName, info.UserID, info.SessionID, filename, version).Scan(&key, &entry.MIMEType, &size, &hash, &entry.Deleted)
	if err != nil {
		return entry, nil, err
	}
	if !strings.HasPrefix(key, objectKey(s.tenantID, info, filename, version)+"/") {
		return entry, nil, ErrArtifactCorrupt
	}
	body, err := s.objects.Get(nonNilContext(ctx), key)
	if entry.Deleted {
		if err == nil {
			return entry, nil, fmt.Errorf("%w: tombstoned object still exists", ErrArtifactCorrupt)
		}
		if isMissingMigrationObject(err) {
			return entry, nil, nil
		}
		return entry, nil, err
	}
	if err != nil {
		return entry, nil, err
	}
	if int64(len(body)) != size || !strings.EqualFold(hashBytes(body), hash) {
		return entry, nil, ErrArtifactCorrupt
	}
	return entry, &artifact.Artifact{Name: filename, MimeType: entry.MIMEType, Data: body}, nil
}

func isMissingMigrationObject(err error) bool {
	if errors.Is(err, sql.ErrNoRows) {
		return true
	}
	response := minio.ToErrorResponse(err)
	return response.Code == "NoSuchKey" || response.Code == "NoSuchObject" || response.Code == "NotFound"
}
