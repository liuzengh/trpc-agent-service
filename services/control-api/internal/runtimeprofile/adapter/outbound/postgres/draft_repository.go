package postgresadapter

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func (s *Store) GetProfileDraft(
	ctx context.Context, tenantID, profileID string,
) (domain.ProfileDraft, error) {
	const query = `
		SELECT tenant_id, profile_id, spec_revision, spec_jsonb, updated_by, updated_at
		FROM runtime_profile_drafts
		WHERE tenant_id = $1 AND profile_id = $2
	`
	var draft domain.ProfileDraft
	err := s.db.QueryRow(ctx, query, tenantID, profileID).Scan(
		&draft.TenantID, &draft.ProfileID, &draft.Revision, &draft.Spec,
		&draft.UpdatedBy, &draft.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ProfileDraft{}, application.ErrRuntimeProfileNotFound
	}
	if err != nil {
		return domain.ProfileDraft{}, fmt.Errorf("query runtime profile draft: %w", err)
	}
	return draft, nil
}

func (s *Store) SaveProfileDraft(
	ctx context.Context, draft domain.ProfileDraft, expectedRevision int64,
) error {
	const statement = `
		UPDATE runtime_profile_drafts
		SET spec_revision = $4, spec_jsonb = $5::jsonb, updated_by = $6, updated_at = $7
		WHERE tenant_id = $1 AND profile_id = $2 AND spec_revision = $3
	`
	tag, err := s.db.Exec(
		ctx, statement, draft.TenantID, draft.ProfileID, expectedRevision,
		draft.Revision, []byte(draft.Spec), draft.UpdatedBy, draft.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("compare and swap runtime profile draft: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	const existenceQuery = `
		SELECT EXISTS (
			SELECT 1 FROM runtime_profile_drafts
			WHERE tenant_id = $1 AND profile_id = $2
		)
	`
	var exists bool
	if err := s.db.QueryRow(ctx, existenceQuery, draft.TenantID, draft.ProfileID).Scan(&exists); err != nil {
		return fmt.Errorf("resolve runtime profile draft conflict: %w", err)
	}
	if !exists {
		return application.ErrRuntimeProfileNotFound
	}
	return application.ErrDraftRevisionConflict
}
