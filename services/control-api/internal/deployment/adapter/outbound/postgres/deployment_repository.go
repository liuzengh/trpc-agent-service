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

const deploymentColumns = `
	id, tenant_id, name, description, metadata_revision,
	latest_revision_number, created_by, created_at, updated_at
`

func (s *Store) GetDeployment(
	ctx context.Context, tenantID, deploymentID string,
) (domain.Deployment, error) {
	const query = `
		SELECT ` + deploymentColumns + `
		FROM deployments
		WHERE tenant_id = $1 AND id = $2
	`
	value, err := scanDeployment(s.db.QueryRow(ctx, query, tenantID, deploymentID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Deployment{}, application.ErrDeploymentNotFound
	}
	if err != nil {
		return domain.Deployment{}, fmt.Errorf("query deployment: %w", err)
	}
	return value, nil
}

func (s *Store) ListDeployments(
	ctx context.Context, tenantID string, page application.Page,
) (application.DeploymentPage, error) {
	const query = `
		SELECT
			COALESCE((
				SELECT jsonb_agg(
					jsonb_build_object(
						'id', d.id,
						'tenant_id', d.tenant_id,
						'name', d.name,
						'description', d.description,
						'metadata_revision', d.metadata_revision,
						'latest_revision_number', d.latest_revision_number,
						'created_by', d.created_by,
						'created_at', d.created_at,
						'updated_at', d.updated_at
					) ORDER BY d.updated_at DESC, d.id
				)
				FROM (
					SELECT id, tenant_id, name, description, metadata_revision,
					       latest_revision_number, created_by, created_at, updated_at
					FROM deployments
					WHERE tenant_id = $1
					ORDER BY updated_at DESC, id
					LIMIT $2 OFFSET $3
				) AS d
			), '[]'::jsonb),
			(SELECT count(*)::int FROM deployments WHERE tenant_id = $1)
	`
	var encoded []byte
	var total int
	if err := s.db.QueryRow(ctx, query, tenantID, page.Limit, page.Offset).Scan(
		&encoded, &total,
	); err != nil {
		return application.DeploymentPage{}, fmt.Errorf("query deployment page: %w", err)
	}
	var records []deploymentRecord
	if err := json.Unmarshal(encoded, &records); err != nil {
		return application.DeploymentPage{}, fmt.Errorf("decode deployment page: %w", err)
	}
	values := make([]domain.Deployment, 0, len(records))
	for _, record := range records {
		values = append(values, record.domain())
	}
	return application.DeploymentPage{Deployments: values, Total: total}, nil
}

func (s *Store) UpdateDeploymentMetadata(
	ctx context.Context, candidate domain.Deployment, expected int64,
) (domain.Deployment, error) {
	if expected <= 0 || candidate.MetadataRevision != expected+1 ||
		candidate.Validate() != nil {
		return domain.Deployment{}, application.ErrInvalidDeployment
	}
	const statement = `
		UPDATE deployments
		SET name = $3, description = $4, metadata_revision = $5, updated_at = $6
		WHERE tenant_id = $1 AND id = $2 AND metadata_revision = $7
		RETURNING ` + deploymentColumns
	value, err := scanDeployment(s.db.QueryRow(
		ctx, statement, candidate.TenantID, candidate.ID, candidate.Name,
		candidate.Description, candidate.MetadataRevision, candidate.UpdatedAt, expected,
	))
	if err == nil {
		return value, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.Deployment{}, fmt.Errorf("compare and swap deployment metadata: %w", err)
	}
	const existsQuery = `SELECT EXISTS (
		SELECT 1 FROM deployments WHERE tenant_id = $1 AND id = $2
	)`
	var exists bool
	if err := s.db.QueryRow(ctx, existsQuery, candidate.TenantID, candidate.ID).Scan(&exists); err != nil {
		return domain.Deployment{}, fmt.Errorf("classify deployment metadata conflict: %w", err)
	}
	if !exists {
		return domain.Deployment{}, application.ErrDeploymentNotFound
	}
	return domain.Deployment{}, application.ErrMetadataRevisionConflict
}
