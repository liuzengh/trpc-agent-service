package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
)

const publishedRevisionColumns = `
	r.id, r.tenant_id, r.deployment_id, r.revision_number,
	r.schema_version, r.input_jsonb, r.input_digest,
	r.agent_id, r.agent_version_id, r.agent_version_number,
	r.agent_schema_version, r.agent_spec_digest,
	r.profile_id, r.profile_revision_id, r.profile_revision_number,
	r.profile_schema_version, r.profile_spec_digest,
	r.published_by, r.published_at,
	COALESCE(m.id, ''), COALESCE(m.schema_version, ''),
	COALESCE(m.compiler_version, ''), COALESCE(m.runtime_contract_version, ''),
	COALESCE(m.content_jsonb, '{}'::jsonb), COALESCE(m.content_digest, ''),
	COALESCE(m.published_at, r.published_at)
`

func (s *Store) GetPublishedRevision(
	ctx context.Context, tenantID, deploymentID string, revisionNumber int64,
) (domain.PublishedRevision, error) {
	const query = `
		SELECT ` + publishedRevisionColumns + `
		FROM deployment_revisions AS r
		LEFT JOIN runtime_manifests AS m
		  ON m.tenant_id = r.tenant_id AND m.deployment_revision_id = r.id
		WHERE r.tenant_id = $1 AND r.deployment_id = $2 AND r.revision_number = $3
	`
	value, err := scanPublishedRevision(s.db.QueryRow(
		ctx, query, tenantID, deploymentID, revisionNumber,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.PublishedRevision{}, application.ErrDeploymentRevisionNotFound
	}
	if err != nil {
		return domain.PublishedRevision{}, fmt.Errorf("query deployment revision: %w", err)
	}
	return value, nil
}

func (s *Store) ListRevisionSummaries(
	ctx context.Context, tenantID, deploymentID string, page application.Page,
) (application.RevisionSummaryPage, error) {
	const query = `
		SELECT
			EXISTS (
				SELECT 1 FROM deployments WHERE tenant_id = $1 AND id = $2
			),
			COALESCE((
				SELECT jsonb_agg(
					jsonb_build_object(
						'id', p.id,
						'tenant_id', p.tenant_id,
						'deployment_id', p.deployment_id,
						'revision_number', p.revision_number,
						'schema_version', p.schema_version,
						'agent_id', p.agent_id,
						'agent_version_number', p.agent_version_number,
						'profile_id', p.profile_id,
						'profile_revision_number', p.profile_revision_number,
						'input_digest', p.input_digest,
						'manifest_id', p.manifest_id,
						'manifest_digest', p.manifest_digest,
						'published_by', p.published_by,
						'published_at', p.published_at
					) ORDER BY p.revision_number DESC
				)
				FROM (
					SELECT
						r.id, r.tenant_id, r.deployment_id, r.revision_number,
						r.schema_version, r.agent_id, r.agent_version_number,
						r.profile_id, r.profile_revision_number, r.input_digest,
						m.id AS manifest_id, m.content_digest AS manifest_digest,
						r.published_by, r.published_at
					FROM deployment_revisions AS r
					LEFT JOIN runtime_manifests AS m
					  ON m.tenant_id = r.tenant_id
					 AND m.deployment_revision_id = r.id
					WHERE r.tenant_id = $1 AND r.deployment_id = $2
					ORDER BY r.revision_number DESC
					LIMIT $3 OFFSET $4
				) AS p
			), '[]'::jsonb),
			(SELECT count(*)::int FROM deployment_revisions
			 WHERE tenant_id = $1 AND deployment_id = $2)
	`
	var exists bool
	var encoded []byte
	var total int
	if err := s.db.QueryRow(
		ctx, query, tenantID, deploymentID, page.Limit, page.Offset,
	).Scan(&exists, &encoded, &total); err != nil {
		return application.RevisionSummaryPage{}, fmt.Errorf("query deployment revision summary page: %w", err)
	}
	if !exists {
		return application.RevisionSummaryPage{}, application.ErrDeploymentNotFound
	}
	var records []revisionSummaryRecord
	if err := json.Unmarshal(encoded, &records); err != nil {
		return application.RevisionSummaryPage{}, fmt.Errorf("decode deployment revision summary page: %w", err)
	}
	values := make([]domain.DeploymentRevisionSummary, 0, len(records))
	for _, record := range records {
		if record.ManifestID == "" || record.ManifestDigest == "" {
			return application.RevisionSummaryPage{}, application.ErrPublicationIntegrity
		}
		values = append(values, record.domain())
	}
	return application.RevisionSummaryPage{Revisions: values, Total: total}, nil
}
