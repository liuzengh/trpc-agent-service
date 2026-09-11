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

func (s *Store) CreateAgent(ctx context.Context, agent domain.Agent, draft domain.AgentDraft) error {
	const statement = `
		WITH inserted_agent AS (
			INSERT INTO agents (
				tenant_id, id, name, description, latest_version_number,
				created_by, created_at, updated_at
			) VALUES ($1, $2, $3, $4, NULL, $5, $6, $7)
			RETURNING tenant_id, id
		)
		INSERT INTO agent_drafts (
			tenant_id, agent_id, spec_revision, spec_jsonb, updated_by, updated_at
		)
		SELECT tenant_id, id, $8, $9::jsonb, $10, $11 FROM inserted_agent
	`
	_, err := s.db.Exec(
		ctx, statement, agent.TenantID, agent.ID, agent.Name, agent.Description,
		agent.CreatedBy, agent.CreatedAt, agent.UpdatedAt, draft.Revision,
		[]byte(draft.Spec), draft.UpdatedBy, draft.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert agent and draft: %w", err)
	}
	return nil
}

func (s *Store) GetAgent(ctx context.Context, tenantID, agentID string) (domain.Agent, error) {
	const query = `
		SELECT id, tenant_id, name, description, latest_version_number,
		       created_by, created_at, updated_at
		FROM agents
		WHERE tenant_id = $1 AND id = $2
	`
	var agent domain.Agent
	err := s.db.QueryRow(ctx, query, tenantID, agentID).Scan(
		&agent.ID, &agent.TenantID, &agent.Name, &agent.Description,
		&agent.LatestVersionNumber, &agent.CreatedBy, &agent.CreatedAt, &agent.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Agent{}, application.ErrAgentNotFound
	}
	if err != nil {
		return domain.Agent{}, fmt.Errorf("query agent: %w", err)
	}
	return agent, nil
}

func (s *Store) ListAgents(
	ctx context.Context,
	tenantID string,
	page application.Page,
) (application.AgentPage, error) {
	const query = `
		SELECT
			COALESCE((
				SELECT jsonb_agg(
					jsonb_build_object(
						'id', p.id,
						'tenant_id', p.tenant_id,
						'name', p.name,
						'description', p.description,
						'latest_version_number', p.latest_version_number,
						'created_by', p.created_by,
						'created_at', p.created_at,
						'updated_at', p.updated_at
					) ORDER BY p.updated_at DESC, p.id
				)
				FROM (
					SELECT * FROM agents
					WHERE tenant_id = $1
					ORDER BY updated_at DESC, id
					LIMIT $2 OFFSET $3
				) AS p
			), '[]'::jsonb),
			(SELECT count(*)::int FROM agents WHERE tenant_id = $1)
	`
	var encoded []byte
	var total int
	if err := s.db.QueryRow(ctx, query, tenantID, page.Limit, page.Offset).Scan(&encoded, &total); err != nil {
		return application.AgentPage{}, fmt.Errorf("query agent page: %w", err)
	}
	var records []agentRecord
	if err := json.Unmarshal(encoded, &records); err != nil {
		return application.AgentPage{}, fmt.Errorf("decode agent page: %w", err)
	}
	agents := make([]domain.Agent, 0, len(records))
	for _, record := range records {
		agents = append(agents, record.domain())
	}
	return application.AgentPage{Agents: agents, Total: total}, nil
}

func (s *Store) UpdateAgent(ctx context.Context, agent domain.Agent) error {
	const statement = `
		UPDATE agents
		SET name = $3, description = $4, updated_at = $5
		WHERE tenant_id = $1 AND id = $2
	`
	tag, err := s.db.Exec(
		ctx, statement, agent.TenantID, agent.ID, agent.Name,
		agent.Description, agent.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("update agent metadata: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return application.ErrAgentNotFound
	}
	return nil
}
