package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func (s *Store) CreateRuntimeProfile(
	ctx context.Context, profile domain.RuntimeProfile, draft domain.ProfileDraft,
) error {
	const statement = `
		WITH inserted_profile AS (
			INSERT INTO runtime_profiles (
				tenant_id, id, name, description, latest_revision_number,
				created_by, created_at, updated_at
			) VALUES ($1, $2, $3, $4, NULL, $5, $6, $7)
			RETURNING tenant_id, id
		)
		INSERT INTO runtime_profile_drafts (
			tenant_id, profile_id, spec_revision, spec_jsonb, updated_by, updated_at
		)
		SELECT tenant_id, id, $8, $9::jsonb, $10, $11 FROM inserted_profile
	`
	tag, err := s.db.Exec(
		ctx, statement, profile.TenantID, profile.ID, profile.Name,
		profile.Description, profile.CreatedBy, profile.CreatedAt, profile.UpdatedAt,
		draft.Revision, []byte(draft.Spec), draft.UpdatedBy, draft.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert runtime profile and draft: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("insert runtime profile and draft: affected %d rows", tag.RowsAffected())
	}
	return nil
}

func (s *Store) GetRuntimeProfile(
	ctx context.Context, tenantID, profileID string,
) (domain.RuntimeProfile, error) {
	const query = `
		SELECT id, tenant_id, name, description, latest_revision_number,
		       created_by, created_at, updated_at
		FROM runtime_profiles
		WHERE tenant_id = $1 AND id = $2
	`
	var profile domain.RuntimeProfile
	err := s.db.QueryRow(ctx, query, tenantID, profileID).Scan(
		&profile.ID, &profile.TenantID, &profile.Name, &profile.Description,
		&profile.LatestRevisionNumber, &profile.CreatedBy, &profile.CreatedAt,
		&profile.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.RuntimeProfile{}, application.ErrRuntimeProfileNotFound
	}
	if err != nil {
		return domain.RuntimeProfile{}, fmt.Errorf("query runtime profile: %w", err)
	}
	return profile, nil
}

func (s *Store) ListRuntimeProfiles(
	ctx context.Context,
	tenantID string,
	page application.Page,
) (application.RuntimeProfilePage, error) {
	const query = `
		SELECT
			COALESCE((
				SELECT jsonb_agg(
					jsonb_build_object(
						'id', p.id,
						'tenant_id', p.tenant_id,
						'name', p.name,
						'description', p.description,
						'latest_revision_number', p.latest_revision_number,
						'created_by', p.created_by,
						'created_at', p.created_at,
						'updated_at', p.updated_at
					) ORDER BY p.updated_at DESC, p.id
				)
				FROM (
					SELECT * FROM runtime_profiles
					WHERE tenant_id = $1
					ORDER BY updated_at DESC, id
					LIMIT $2 OFFSET $3
				) AS p
			), '[]'::jsonb),
			(SELECT count(*)::int FROM runtime_profiles WHERE tenant_id = $1)
	`
	var encoded []byte
	var total int
	if err := s.db.QueryRow(ctx, query, tenantID, page.Limit, page.Offset).Scan(
		&encoded, &total,
	); err != nil {
		return application.RuntimeProfilePage{}, fmt.Errorf("query runtime profile page: %w", err)
	}
	var records []profileRecord
	if err := json.Unmarshal(encoded, &records); err != nil {
		return application.RuntimeProfilePage{}, fmt.Errorf("decode runtime profile page: %w", err)
	}
	profiles := make([]domain.RuntimeProfile, 0, len(records))
	for _, record := range records {
		profiles = append(profiles, record.domain())
	}
	return application.RuntimeProfilePage{Profiles: profiles, Total: total}, nil
}

func (s *Store) UpdateRuntimeProfile(ctx context.Context, profile domain.RuntimeProfile) error {
	const statement = `
		UPDATE runtime_profiles
		SET name = $3, description = $4, updated_at = $5
		WHERE tenant_id = $1 AND id = $2
	`
	tag, err := s.db.Exec(
		ctx, statement, profile.TenantID, profile.ID, profile.Name,
		profile.Description, profile.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("update runtime profile metadata: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return application.ErrRuntimeProfileNotFound
	}
	return nil
}
