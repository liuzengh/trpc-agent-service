package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
)

const userArtifactSession = "@user"

// PostgresArtifactService stores versioned artifact bytes with explicit tenant
// and application columns. The caller owns DB.
type PostgresArtifactService struct{ DB *sql.DB }

var _ artifact.Service = (*PostgresArtifactService)(nil)

func (service *PostgresArtifactService) SaveArtifact(ctx context.Context, info artifact.SessionInfo, filename string, value *artifact.Artifact) (int, error) {
	tenantID, appID, sessionID, err := artifactScope(info, filename)
	if err != nil {
		return 0, err
	}
	if service == nil || service.DB == nil || value == nil || value.MimeType == "" {
		return 0, errors.New("storage: PostgreSQL artifact database, value, and MIME type are required")
	}
	tx, err := service.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	lockDigest := sha256.Sum256([]byte(tenantID + "\x00" + appID + "\x00" + info.UserID + "\x00" + sessionID + "\x00" + filename))
	lockKey := fmt.Sprintf("%x", lockDigest)
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, lockKey); err != nil {
		return 0, err
	}
	var revision int
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(revision),-1)+1 FROM runtime_artifacts WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND session_id=$4 AND filename=$5`, tenantID, appID, info.UserID, sessionID, filename).Scan(&revision)
	if err != nil {
		return 0, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO runtime_artifacts (tenant_id,app_id,user_id,session_id,filename,revision,mime_type,artifact_url,display_name,data) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, tenantID, appID, info.UserID, sessionID, filename, revision, value.MimeType, value.URL, value.Name, value.Data)
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return revision, nil
}

func (service *PostgresArtifactService) LoadArtifact(ctx context.Context, info artifact.SessionInfo, filename string, version *int) (*artifact.Artifact, error) {
	tenantID, appID, sessionID, err := artifactScope(info, filename)
	if err != nil {
		return nil, err
	}
	if service == nil || service.DB == nil {
		return nil, errors.New("storage: nil PostgreSQL artifact database")
	}
	var row *sql.Row
	if version == nil {
		row = service.DB.QueryRowContext(ctx, `SELECT data,mime_type,artifact_url,display_name FROM runtime_artifacts WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND session_id=$4 AND filename=$5 ORDER BY revision DESC LIMIT 1`, tenantID, appID, info.UserID, sessionID, filename)
	} else {
		if *version < 0 {
			return nil, errors.New("storage: artifact revision must not be negative")
		}
		row = service.DB.QueryRowContext(ctx, `SELECT data,mime_type,artifact_url,display_name FROM runtime_artifacts WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND session_id=$4 AND filename=$5 AND revision=$6`, tenantID, appID, info.UserID, sessionID, filename, *version)
	}
	value := &artifact.Artifact{}
	if err := row.Scan(&value.Data, &value.MimeType, &value.URL, &value.Name); errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return value, nil
}

func (service *PostgresArtifactService) ListArtifactKeys(ctx context.Context, info artifact.SessionInfo) ([]string, error) {
	tenantID, appID, _, err := artifactScope(info, "probe")
	if err != nil {
		return nil, err
	}
	if service == nil || service.DB == nil {
		return nil, errors.New("storage: nil PostgreSQL artifact database")
	}
	rows, err := service.DB.QueryContext(ctx, `SELECT DISTINCT filename FROM runtime_artifacts WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND session_id IN ($4,$5) ORDER BY filename`, tenantID, appID, info.UserID, info.SessionID, userArtifactSession)
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
	return keys, rows.Err()
}

func (service *PostgresArtifactService) DeleteArtifact(ctx context.Context, info artifact.SessionInfo, filename string) error {
	tenantID, appID, sessionID, err := artifactScope(info, filename)
	if err != nil {
		return err
	}
	if service == nil || service.DB == nil {
		return errors.New("storage: nil PostgreSQL artifact database")
	}
	_, err = service.DB.ExecContext(ctx, `DELETE FROM runtime_artifacts WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND session_id=$4 AND filename=$5`, tenantID, appID, info.UserID, sessionID, filename)
	return err
}

func (service *PostgresArtifactService) ListVersions(ctx context.Context, info artifact.SessionInfo, filename string) ([]int, error) {
	tenantID, appID, sessionID, err := artifactScope(info, filename)
	if err != nil {
		return nil, err
	}
	if service == nil || service.DB == nil {
		return nil, errors.New("storage: nil PostgreSQL artifact database")
	}
	rows, err := service.DB.QueryContext(ctx, `SELECT revision FROM runtime_artifacts WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND session_id=$4 AND filename=$5 ORDER BY revision`, tenantID, appID, info.UserID, sessionID, filename)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	versions := make([]int, 0)
	for rows.Next() {
		var revision int
		if err := rows.Scan(&revision); err != nil {
			return nil, err
		}
		versions = append(versions, revision)
	}
	return versions, rows.Err()
}

func artifactScope(info artifact.SessionInfo, filename string) (string, string, string, error) {
	if info.UserID == "" || filename == "" {
		return "", "", "", errors.New("storage: artifact app, user, and filename are required")
	}
	tenantID, appID, err := tenant.ParseCanonicalAppName(info.AppName)
	if err != nil {
		return "", "", "", fmt.Errorf("storage: artifact scope: %w", err)
	}
	if strings.HasPrefix(filename, "user:") {
		return tenantID, appID, userArtifactSession, nil
	}
	if info.SessionID == "" {
		return "", "", "", errors.New("storage: session-scoped artifact requires session ID")
	}
	return tenantID, appID, info.SessionID, nil
}
