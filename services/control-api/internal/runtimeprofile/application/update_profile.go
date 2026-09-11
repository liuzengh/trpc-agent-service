package application

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func (s *Service) UpdateRuntimeProfile(
	ctx context.Context,
	command UpdateRuntimeProfileCommand,
) (domain.RuntimeProfile, error) {
	if err := s.authorize(ctx, command.TenantID, command.ActorUserID); err != nil {
		return domain.RuntimeProfile{}, err
	}
	profile, err := s.deps.Store.GetRuntimeProfile(ctx, command.TenantID, command.ProfileID)
	if err != nil {
		if errors.Is(err, ErrRuntimeProfileNotFound) {
			return domain.RuntimeProfile{}, ErrRuntimeProfileNotFound
		}
		return domain.RuntimeProfile{}, fmt.Errorf("get runtime profile: %w", err)
	}
	name, description := profile.Name, profile.Description
	if command.Name != nil {
		name = *command.Name
	}
	if command.Description != nil {
		description = *command.Description
	}
	if command.Name == nil && command.Description == nil {
		return domain.RuntimeProfile{}, ErrInvalidRuntimeProfile
	}
	if err := profile.UpdateMetadata(name, description, s.deps.Now()); err != nil {
		return domain.RuntimeProfile{}, ErrInvalidRuntimeProfile
	}
	if err := s.deps.Store.UpdateRuntimeProfile(ctx, profile); err != nil {
		if errors.Is(err, ErrRuntimeProfileNotFound) {
			return domain.RuntimeProfile{}, ErrRuntimeProfileNotFound
		}
		return domain.RuntimeProfile{}, fmt.Errorf("update runtime profile: %w", err)
	}
	return profile, nil
}
