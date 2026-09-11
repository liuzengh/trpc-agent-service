package application

import (
	"context"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func (s *Service) CreateRuntimeProfile(
	ctx context.Context,
	command CreateRuntimeProfileCommand,
) (CreateRuntimeProfileResult, error) {
	if err := s.authorize(ctx, command.TenantID, command.ActorUserID); err != nil {
		return CreateRuntimeProfileResult{}, err
	}
	id, err := s.deps.NewProfileID()
	if err != nil {
		return CreateRuntimeProfileResult{}, fmt.Errorf("generate runtime profile id: %w", err)
	}
	now := s.deps.Now().UTC()
	profile, err := domain.NewRuntimeProfile(
		id, command.TenantID, command.Name, command.Description,
		command.ActorUserID, now,
	)
	if err != nil {
		return CreateRuntimeProfileResult{}, ErrInvalidRuntimeProfile
	}
	draft := domain.ProfileDraft{
		TenantID: command.TenantID, ProfileID: id, Revision: 1,
		Spec: []byte(`{}`), UpdatedBy: command.ActorUserID, UpdatedAt: now,
	}
	if err := s.deps.Store.CreateRuntimeProfile(ctx, profile, draft); err != nil {
		return CreateRuntimeProfileResult{}, fmt.Errorf("create runtime profile: %w", err)
	}
	return CreateRuntimeProfileResult{Profile: profile, Draft: draft.Clone()}, nil
}
