package application

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
)

func (s *Service) UpdateAgent(
	ctx context.Context,
	command UpdateAgentCommand,
) (domain.Agent, error) {
	if err := s.authorize(ctx, command.TenantID, command.ActorUserID); err != nil {
		return domain.Agent{}, err
	}
	agent, err := s.deps.Store.GetAgent(ctx, command.TenantID, command.AgentID)
	if err != nil {
		if errors.Is(err, ErrAgentNotFound) {
			return domain.Agent{}, ErrAgentNotFound
		}
		return domain.Agent{}, fmt.Errorf("get agent: %w", err)
	}
	name, description := agent.Name, agent.Description
	if command.Name != nil {
		name = *command.Name
	}
	if command.Description != nil {
		description = *command.Description
	}
	if command.Name == nil && command.Description == nil {
		return domain.Agent{}, ErrInvalidAgent
	}
	if err := agent.UpdateMetadata(name, description, s.deps.Now()); err != nil {
		return domain.Agent{}, ErrInvalidAgent
	}
	if err := s.deps.Store.UpdateAgent(ctx, agent); err != nil {
		if errors.Is(err, ErrAgentNotFound) {
			return domain.Agent{}, ErrAgentNotFound
		}
		return domain.Agent{}, fmt.Errorf("update agent: %w", err)
	}
	return agent, nil
}
