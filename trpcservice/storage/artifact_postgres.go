package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/dbscope"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
)

// PostgresArtifactService persists the framework artifact contract directly.
// The framework owns artifact identity; the adapter only maps AppName to the
// tenant/application columns required by the platform RLS boundary.
type PostgresArtifactService struct {
	database *sql.DB
}

func NewPostgresArtifactService(database *sql.DB) (*PostgresArtifactService, error) {
	if database == nil {
		return nil, errors.New("artifact PostgreSQL database is required")
	}
	return &PostgresArtifactService{database: database}, nil
}

func (s *PostgresArtifactService) SaveArtifact(ctx context.Context, info agentartifact.SessionInfo, filename string, artifact *agentartifact.Artifact) (int, error) {
	scope, err := newArtifactScope(info, filename)
	if err != nil {
		return 0, err
	}
	if err := validateFrameworkArtifact(artifact); err != nil {
		return 0, err
	}
	tx, err := s.beginTenantTransaction(ctx, scope.tenantID)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	lockKey := fmt.Sprintf("%q/%q/%q/%q/%q", scope.tenantID, scope.appCode, scope.userID, scope.sessionID, scope.filename)
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", lockKey); err != nil {
		return 0, fmt.Errorf("lock artifact version: %w", err)
	}
	var version int
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(MAX(version), -1) + 1
FROM artifacts
WHERE tenant_id=$1 AND app_code=$2 AND user_id=$3 AND session_id=$4 AND filename=$5`,
		scope.tenantID, scope.appCode, scope.userID, scope.sessionID, scope.filename).Scan(&version); err != nil {
		return 0, fmt.Errorf("resolve next artifact version: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO artifacts (
    tenant_id, app_code, user_id, session_id, filename, version,
    mime_type, content, artifact_url, display_name, created_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())`,
		scope.tenantID, scope.appCode, scope.userID, scope.sessionID, scope.filename, version,
		artifact.MimeType, artifact.Data, artifact.URL, artifact.Name); err != nil {
		return 0, fmt.Errorf("save PostgreSQL artifact: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit PostgreSQL artifact: %w", err)
	}
	return version, nil
}

func (s *PostgresArtifactService) LoadArtifact(ctx context.Context, info agentartifact.SessionInfo, filename string, version *int) (*agentartifact.Artifact, error) {
	scope, err := newArtifactScope(info, filename)
	if err != nil {
		return nil, err
	}
	if version != nil && *version < 0 {
		return nil, errors.New("artifact version cannot be negative")
	}
	tx, err := s.beginTenantTransaction(ctx, scope.tenantID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	query := `
SELECT mime_type, content, artifact_url, display_name
FROM artifacts
WHERE tenant_id=$1 AND app_code=$2 AND user_id=$3 AND session_id=$4 AND filename=$5`
	args := []any{scope.tenantID, scope.appCode, scope.userID, scope.sessionID, scope.filename}
	if version == nil {
		query += " ORDER BY version DESC LIMIT 1"
	} else {
		query += " AND version=$6"
		args = append(args, *version)
	}
	var artifact agentartifact.Artifact
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&artifact.MimeType, &artifact.Data, &artifact.URL, &artifact.Name); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("load PostgreSQL artifact: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit PostgreSQL artifact read: %w", err)
	}
	return &artifact, nil
}

func (s *PostgresArtifactService) ListArtifactKeys(ctx context.Context, info agentartifact.SessionInfo) ([]string, error) {
	tenantID, appCode, err := splitArtifactAppName(info.AppName)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(info.UserID) == "" || strings.TrimSpace(info.SessionID) == "" {
		return nil, errors.New("artifact user_id and session_id are required")
	}
	tx, err := s.beginTenantTransaction(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `
SELECT DISTINCT filename
FROM artifacts
WHERE tenant_id=$1 AND app_code=$2 AND user_id=$3
  AND (session_id=$4 OR (session_id='' AND filename LIKE 'user:%'))
ORDER BY filename`, tenantID, appCode, info.UserID, info.SessionID)
	if err != nil {
		return nil, fmt.Errorf("list PostgreSQL artifact keys: %w", err)
	}
	defer rows.Close()
	keys := make([]string, 0)
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("scan PostgreSQL artifact key: %w", err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate PostgreSQL artifact keys: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit PostgreSQL artifact list: %w", err)
	}
	return keys, nil
}

func (s *PostgresArtifactService) DeleteArtifact(ctx context.Context, info agentartifact.SessionInfo, filename string) error {
	scope, err := newArtifactScope(info, filename)
	if err != nil {
		return err
	}
	tx, err := s.beginTenantTransaction(ctx, scope.tenantID)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
DELETE FROM artifacts
WHERE tenant_id=$1 AND app_code=$2 AND user_id=$3 AND session_id=$4 AND filename=$5`,
		scope.tenantID, scope.appCode, scope.userID, scope.sessionID, scope.filename); err != nil {
		return fmt.Errorf("delete PostgreSQL artifact: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit PostgreSQL artifact delete: %w", err)
	}
	return nil
}

func (s *PostgresArtifactService) ListVersions(ctx context.Context, info agentartifact.SessionInfo, filename string) ([]int, error) {
	scope, err := newArtifactScope(info, filename)
	if err != nil {
		return nil, err
	}
	tx, err := s.beginTenantTransaction(ctx, scope.tenantID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `
SELECT version FROM artifacts
WHERE tenant_id=$1 AND app_code=$2 AND user_id=$3 AND session_id=$4 AND filename=$5
ORDER BY version`, scope.tenantID, scope.appCode, scope.userID, scope.sessionID, scope.filename)
	if err != nil {
		return nil, fmt.Errorf("list PostgreSQL artifact versions: %w", err)
	}
	defer rows.Close()
	versions := make([]int, 0)
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("scan PostgreSQL artifact version: %w", err)
		}
		versions = append(versions, version)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate PostgreSQL artifact versions: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit PostgreSQL artifact versions: %w", err)
	}
	return versions, nil
}

func (s *PostgresArtifactService) beginTenantTransaction(ctx context.Context, tenantID string) (*sql.Tx, error) {
	tx, err := dbscope.BeginTenantTransaction(ctx, s.database, tenantID)
	if err != nil {
		return nil, fmt.Errorf("begin artifact transaction: %w", err)
	}
	return tx, nil
}

type artifactScope struct {
	tenantID  string
	appCode   string
	userID    string
	sessionID string
	filename  string
}

func newArtifactScope(info agentartifact.SessionInfo, filename string) (artifactScope, error) {
	tenantID, appCode, err := splitArtifactAppName(info.AppName)
	if err != nil {
		return artifactScope{}, err
	}
	if strings.TrimSpace(info.UserID) == "" || strings.TrimSpace(info.SessionID) == "" {
		return artifactScope{}, errors.New("artifact user_id and session_id are required")
	}
	if strings.TrimSpace(filename) == "" || strings.Contains(filename, "\x00") {
		return artifactScope{}, errors.New("artifact filename is required and cannot contain NUL")
	}
	sessionID := info.SessionID
	if strings.HasPrefix(filename, "user:") {
		sessionID = ""
	}
	return artifactScope{tenantID: tenantID, appCode: appCode, userID: info.UserID, sessionID: sessionID, filename: filename}, nil
}

func splitArtifactAppName(appName string) (string, string, error) {
	tenantID, appCode, ok := strings.Cut(strings.TrimSpace(appName), "/")
	if !ok || !validObjectSegment(tenantID) || !validObjectSegment(appCode) {
		return "", "", errors.New("artifact app_name must be tenant_id/app_code")
	}
	return tenantID, appCode, nil
}

func validObjectSegment(value string) bool {
	value = strings.TrimSpace(value)
	return value != "" && value != "." && value != ".." && !strings.ContainsAny(value, "/\\")
}

func validateFrameworkArtifact(artifact *agentartifact.Artifact) error {
	if artifact == nil {
		return errors.New("artifact is required")
	}
	if artifact.Data == nil || strings.TrimSpace(artifact.MimeType) == "" {
		return errors.New("artifact data and mime_type are required")
	}
	if int64(len(artifact.Data)) > MaxArtifactBytes {
		return fmt.Errorf("artifact content exceeds limit %d", MaxArtifactBytes)
	}
	return nil
}

var _ agentartifact.Service = (*PostgresArtifactService)(nil)
