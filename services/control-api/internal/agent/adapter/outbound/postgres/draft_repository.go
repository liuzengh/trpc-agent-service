package postgresadapter

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
)

func (s *Store) GetDraft(
	ctx context.Context, tenantID, agentID string,
) (domain.AgentDraft, error) {
	const query = `
		SELECT tenant_id, agent_id, spec_revision, spec_jsonb, updated_by, updated_at
		FROM agent_drafts
		WHERE tenant_id = $1 AND agent_id = $2
	`
	var draft domain.AgentDraft
	err := s.db.QueryRow(ctx, query, tenantID, agentID).Scan(
		&draft.TenantID, &draft.AgentID, &draft.Revision, &draft.Spec,
		&draft.UpdatedBy, &draft.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.AgentDraft{}, application.ErrAgentNotFound
	}
	if err != nil {
		return domain.AgentDraft{}, fmt.Errorf("query agent draft: %w", err)
	}
	return draft, nil
}

func (s *Store) SaveDraft(
	ctx context.Context, draft domain.AgentDraft, expectedRevision int64,
) error {
	const statement = `
		UPDATE agent_drafts
		SET spec_revision = $4, spec_jsonb = $5::jsonb, updated_by = $6, updated_at = $7
		WHERE tenant_id = $1 AND agent_id = $2 AND spec_revision = $3
	`
	tag, err := s.db.Exec(
		ctx, statement, draft.TenantID, draft.AgentID, expectedRevision,
		draft.Revision, []byte(draft.Spec), draft.UpdatedBy, draft.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("compare and swap agent draft: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	const existenceQuery = `
		SELECT EXISTS (
			SELECT 1 FROM agent_drafts WHERE tenant_id = $1 AND agent_id = $2
		)
	`
	var exists bool
	if err := s.db.QueryRow(ctx, existenceQuery, draft.TenantID, draft.AgentID).Scan(&exists); err != nil {
		return fmt.Errorf("resolve agent draft conflict: %w", err)
	}
	if !exists {
		return application.ErrAgentNotFound
	}
	return application.ErrDraftRevisionConflict
}
