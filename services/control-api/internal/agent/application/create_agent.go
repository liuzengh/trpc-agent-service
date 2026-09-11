package application

import (
	"context"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
)

func (s *Service) CreateAgent(
	ctx context.Context,
	command CreateAgentCommand,
) (CreateAgentResult, error) {
	if err := s.authorize(ctx, command.TenantID, command.ActorUserID); err != nil {
		return CreateAgentResult{}, err
	}
	id, err := s.deps.NewAgentID()
	if err != nil {
		return CreateAgentResult{}, fmt.Errorf("generate agent id: %w", err)
	}
	now := s.deps.Now().UTC()
	agent, err := domain.NewAgent(
		id, command.TenantID, command.Name, command.Description,
		command.ActorUserID, now,
	)
	if err != nil {
		return CreateAgentResult{}, ErrInvalidAgent
	}
	draft := domain.AgentDraft{
		TenantID: command.TenantID, AgentID: id, Revision: 1,
		Spec: []byte(`{}`), UpdatedBy: command.ActorUserID, UpdatedAt: now,
	}
	if err := s.deps.Store.CreateAgent(ctx, agent, draft); err != nil {
		return CreateAgentResult{}, fmt.Errorf("create agent: %w", err)
	}
	return CreateAgentResult{Agent: agent, Draft: draft.Clone()}, nil
}
