package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"trpc.group/trpc-go/trpc-agent-go/artifact"
)

// CoordinatedArtifact serializes S3 revision allocation through the shared
// platform PostgreSQL database. The upstream S3 implementation intentionally
// cannot allocate concurrent revisions on its own.
type CoordinatedArtifact struct {
	DB       *sql.DB
	Delegate artifact.Service
}

func (service *CoordinatedArtifact) SaveArtifact(ctx context.Context, info artifact.SessionInfo, filename string, value *artifact.Artifact) (int, error) {
	if service == nil || service.DB == nil || service.Delegate == nil {
		return 0, errors.New("storage: coordinated artifact service is incomplete")
	}
	tx, err := service.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	digest := sha256.Sum256([]byte(info.AppName + "\x00" + info.UserID + "\x00" + info.SessionID + "\x00" + filename))
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, fmt.Sprintf("%x", digest)); err != nil {
		return 0, err
	}
	revision, err := service.Delegate.SaveArtifact(ctx, info, filename, value)
	if err != nil {
		return 0, err
	}
	tenantID, appID, sessionID, err := artifactScope(info, filename)
	if err != nil {
		return 0, err
	}
	checksum := ArtifactChecksum(value)
	var existing string
	err = tx.QueryRowContext(ctx, `INSERT INTO runtime_artifact_catalog (tenant_id,app_id,user_id,session_id,filename,revision,checksum) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (tenant_id,app_id,user_id,session_id,filename,revision) DO UPDATE SET checksum=runtime_artifact_catalog.checksum RETURNING checksum`, tenantID, appID, info.UserID, sessionID, filename, revision, checksum).Scan(&existing)
	if err != nil {
		return 0, err
	}
	if existing != checksum {
		return 0, errors.New("storage: artifact catalog checksum conflict")
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return revision, nil
}

func (service *CoordinatedArtifact) LoadArtifact(ctx context.Context, info artifact.SessionInfo, filename string, version *int) (*artifact.Artifact, error) {
	return service.Delegate.LoadArtifact(ctx, info, filename, version)
}
func (service *CoordinatedArtifact) ListArtifactKeys(ctx context.Context, info artifact.SessionInfo) ([]string, error) {
	return service.Delegate.ListArtifactKeys(ctx, info)
}
func (service *CoordinatedArtifact) DeleteArtifact(ctx context.Context, info artifact.SessionInfo, filename string) error {
	if service == nil || service.DB == nil || service.Delegate == nil {
		return errors.New("storage: coordinated artifact service is incomplete")
	}
	tx, err := service.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	digest := sha256.Sum256([]byte(info.AppName + "\x00" + info.UserID + "\x00" + info.SessionID + "\x00" + filename))
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, fmt.Sprintf("%x", digest)); err != nil {
		return err
	}
	if err := service.Delegate.DeleteArtifact(ctx, info, filename); err != nil {
		return err
	}
	tenantID, appID, sessionID, err := artifactScope(info, filename)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM runtime_artifact_catalog WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND session_id=$4 AND filename=$5`, tenantID, appID, info.UserID, sessionID, filename); err != nil {
		return err
	}
	return tx.Commit()
}
func (service *CoordinatedArtifact) ListVersions(ctx context.Context, info artifact.SessionInfo, filename string) ([]int, error) {
	return service.Delegate.ListVersions(ctx, info, filename)
}
func (service *CoordinatedArtifact) Close() error {
	if closer, ok := service.Delegate.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

var _ artifact.Service = (*CoordinatedArtifact)(nil)

// ArtifactChecksum is a stable digest of the full logical artifact value.
// It is safe to store and expose in migration diagnostics; content is not.
func ArtifactChecksum(value *artifact.Artifact) string {
	if value == nil {
		return ""
	}
	payload, _ := json.Marshal(struct {
		Data                []byte `json:"data"`
		MimeType, URL, Name string
	}{Data: value.Data, MimeType: value.MimeType, URL: value.URL, Name: value.Name})
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}
