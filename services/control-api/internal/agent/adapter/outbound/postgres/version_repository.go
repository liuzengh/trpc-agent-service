package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
)

const versionColumns = `
	id, tenant_id, agent_id, version_number, source_draft_revision,
	schema_version, spec_jsonb, spec_digest, published_by, published_at
`

func (s *Store) PublishVersion(
	ctx context.Context,
	candidate domain.AgentVersion,
	expectedRevision int64,
) (domain.AgentVersion, bool, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return domain.AgentVersion{}, false, fmt.Errorf("begin publication: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	const lockQuery = `
		SELECT d.spec_revision, a.latest_version_number
		FROM agent_drafts AS d
		JOIN agents AS a
		  ON a.tenant_id = d.tenant_id AND a.id = d.agent_id
		WHERE d.tenant_id = $1 AND d.agent_id = $2
		FOR UPDATE OF d, a
	`
	var currentRevision int64
	var latestVersion *int64
	err = tx.QueryRow(ctx, lockQuery, candidate.TenantID, candidate.AgentID).Scan(
		&currentRevision, &latestVersion,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.AgentVersion{}, false, application.ErrAgentNotFound
	}
	if err != nil {
		return domain.AgentVersion{}, false, fmt.Errorf("lock publication source: %w", err)
	}
	if currentRevision != expectedRevision || candidate.SourceDraftRevision != expectedRevision {
		return domain.AgentVersion{}, false, application.ErrDraftRevisionConflict
	}

	const existingQuery = `
		SELECT ` + versionColumns + `
		FROM agent_versions
		WHERE tenant_id = $1 AND agent_id = $2 AND source_draft_revision = $3
	`
	existing, err := scanVersion(tx.QueryRow(
		ctx, existingQuery, candidate.TenantID, candidate.AgentID, expectedRevision,
	))
	if err == nil {
		if err := tx.Commit(ctx); err != nil {
			return domain.AgentVersion{}, false, fmt.Errorf("commit idempotent publication: %w", err)
		}
		return existing, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.AgentVersion{}, false, fmt.Errorf("query existing publication: %w", err)
	}

	candidate.VersionNumber = 1
	if latestVersion != nil {
		candidate.VersionNumber = *latestVersion + 1
	}
	const insertStatement = `
		INSERT INTO agent_versions (
			id, tenant_id, agent_id, version_number, source_draft_revision,
			schema_version, spec_jsonb, spec_digest, published_by, published_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10)
	`
	_, err = tx.Exec(
		ctx, insertStatement, candidate.ID, candidate.TenantID, candidate.AgentID,
		candidate.VersionNumber, candidate.SourceDraftRevision, candidate.SchemaVersion,
		[]byte(candidate.Spec), candidate.SpecDigest, candidate.PublishedBy,
		candidate.PublishedAt,
	)
	if err != nil {
		return domain.AgentVersion{}, false, fmt.Errorf("insert agent version: %w", err)
	}
	const updateAgent = `
		UPDATE agents SET latest_version_number = $3, updated_at = $4
		WHERE tenant_id = $1 AND id = $2
	`
	if _, err := tx.Exec(
		ctx, updateAgent, candidate.TenantID, candidate.AgentID,
		candidate.VersionNumber, candidate.PublishedAt,
	); err != nil {
		return domain.AgentVersion{}, false, fmt.Errorf("update latest agent version: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.AgentVersion{}, false, fmt.Errorf("commit publication: %w", err)
	}
	return candidate, true, nil
}

func (s *Store) GetVersion(
	ctx context.Context, tenantID, agentID string, versionNumber int64,
) (domain.AgentVersion, error) {
	const query = `
		SELECT ` + versionColumns + `
		FROM agent_versions
		WHERE tenant_id = $1 AND agent_id = $2 AND version_number = $3
	`
	version, err := scanVersion(s.db.QueryRow(ctx, query, tenantID, agentID, versionNumber))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.AgentVersion{}, application.ErrAgentVersionNotFound
	}
	if err != nil {
		return domain.AgentVersion{}, fmt.Errorf("query agent version: %w", err)
	}
	return version, nil
}

func (s *Store) ListVersions(
	ctx context.Context,
	tenantID, agentID string,
	page application.Page,
) (application.VersionPage, error) {
	const query = `
		SELECT
			EXISTS (SELECT 1 FROM agents WHERE tenant_id = $1 AND id = $2),
			COALESCE((
				SELECT jsonb_agg(
					jsonb_build_object(
						'id', p.id,
						'tenant_id', p.tenant_id,
						'agent_id', p.agent_id,
						'version_number', p.version_number,
						'source_draft_revision', p.source_draft_revision,
						'schema_version', p.schema_version,
						'spec', p.spec_jsonb,
						'spec_digest', p.spec_digest,
						'published_by', p.published_by,
						'published_at', p.published_at
					) ORDER BY p.version_number DESC
				)
				FROM (
					SELECT * FROM agent_versions
					WHERE tenant_id = $1 AND agent_id = $2
					ORDER BY version_number DESC
					LIMIT $3 OFFSET $4
				) AS p
			), '[]'::jsonb),
			(SELECT count(*)::int FROM agent_versions WHERE tenant_id = $1 AND agent_id = $2)
	`
	var exists bool
	var encoded []byte
	var total int
	if err := s.db.QueryRow(ctx, query, tenantID, agentID, page.Limit, page.Offset).Scan(
		&exists, &encoded, &total,
	); err != nil {
		return application.VersionPage{}, fmt.Errorf("query agent version page: %w", err)
	}
	if !exists {
		return application.VersionPage{}, application.ErrAgentNotFound
	}
	var records []versionRecord
	if err := json.Unmarshal(encoded, &records); err != nil {
		return application.VersionPage{}, fmt.Errorf("decode agent version page: %w", err)
	}
	versions := make([]domain.AgentVersion, 0, len(records))
	for _, record := range records {
		versions = append(versions, record.domain())
	}
	return application.VersionPage{Versions: versions, Total: total}, nil
}

var _ application.Store = (*Store)(nil)
