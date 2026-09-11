package application

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
)

func (s *Service) GetAgent(
	ctx context.Context, tenantID, agentID, userID string,
) (domain.Agent, error) {
	if err := s.authorize(ctx, tenantID, userID); err != nil {
		return domain.Agent{}, err
	}
	agent, err := s.deps.Store.GetAgent(ctx, tenantID, agentID)
	if errors.Is(err, ErrAgentNotFound) {
		return domain.Agent{}, ErrAgentNotFound
	}
	if err != nil {
		return domain.Agent{}, fmt.Errorf("get agent: %w", err)
	}
	return agent, nil
}

func (s *Service) ListAgents(
	ctx context.Context, tenantID, userID string, page Page,
) (AgentPage, error) {
	if err := s.authorize(ctx, tenantID, userID); err != nil {
		return AgentPage{}, err
	}
	result, err := s.deps.Store.ListAgents(ctx, tenantID, normalizePage(page))
	if err != nil {
		return AgentPage{}, fmt.Errorf("list agents: %w", err)
	}
	if result.Agents == nil {
		result.Agents = []domain.Agent{}
	}
	return result, nil
}

func (s *Service) GetDraft(
	ctx context.Context, tenantID, agentID, userID string,
) (domain.AgentDraft, error) {
	if err := s.authorize(ctx, tenantID, userID); err != nil {
		return domain.AgentDraft{}, err
	}
	draft, err := s.deps.Store.GetDraft(ctx, tenantID, agentID)
	if errors.Is(err, ErrAgentNotFound) {
		return domain.AgentDraft{}, ErrAgentNotFound
	}
	if err != nil {
		return domain.AgentDraft{}, fmt.Errorf("get agent draft: %w", err)
	}
	return draft.Clone(), nil
}

func (s *Service) GetAgentVersion(
	ctx context.Context, tenantID, agentID, userID string, versionNumber int64,
) (domain.AgentVersion, error) {
	if err := s.authorize(ctx, tenantID, userID); err != nil {
		return domain.AgentVersion{}, err
	}
	if versionNumber <= 0 {
		return domain.AgentVersion{}, ErrAgentNotFound
	}
	version, err := s.deps.Store.GetVersion(ctx, tenantID, agentID, versionNumber)
	if errors.Is(err, ErrAgentVersionNotFound) {
		return domain.AgentVersion{}, ErrAgentVersionNotFound
	}
	if err != nil {
		return domain.AgentVersion{}, fmt.Errorf("get agent version: %w", err)
	}
	version, err = canonicalStoredVersion(version)
	if err != nil {
		return domain.AgentVersion{}, fmt.Errorf("verify agent version: %w", err)
	}
	return version.Clone(), nil
}

func (s *Service) ListAgentVersions(
	ctx context.Context, tenantID, agentID, userID string, page Page,
) (VersionPage, error) {
	if err := s.authorize(ctx, tenantID, userID); err != nil {
		return VersionPage{}, err
	}
	result, err := s.deps.Store.ListVersions(ctx, tenantID, agentID, normalizePage(page))
	if errors.Is(err, ErrAgentNotFound) {
		return VersionPage{}, ErrAgentNotFound
	}
	if err != nil {
		return VersionPage{}, fmt.Errorf("list agent versions: %w", err)
	}
	if result.Versions == nil {
		result.Versions = []domain.AgentVersion{}
	}
	for index, version := range result.Versions {
		canonical, verifyErr := canonicalStoredVersion(version)
		if verifyErr != nil {
			return VersionPage{}, fmt.Errorf("verify agent version page: %w", verifyErr)
		}
		result.Versions[index] = canonical
	}
	return result, nil
}
