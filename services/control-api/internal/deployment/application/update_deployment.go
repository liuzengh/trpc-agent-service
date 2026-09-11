package application

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
)

func (s *Service) UpdateDeploymentMetadata(ctx context.Context, command UpdateDeploymentCommand) (result domain.Deployment, err error) {
	if err = s.authorizeMember(ctx, command.TenantID, command.ActorUserID); err != nil {
		return result, err
	}
	if command.ExpectedMetadataRevision <= 0 || (command.Name == nil && command.Description == nil) {
		return result, ErrInvalidDeploymentInput
	}
	current, err := s.deps.Queries.GetDeployment(ctx, command.TenantID, command.DeploymentID)
	if errors.Is(err, ErrDeploymentNotFound) {
		return result, ErrDeploymentNotFound
	}
	if err != nil {
		return result, fmt.Errorf("get deployment for update: %w", err)
	}
	if current.Validate() != nil || current.ID != command.DeploymentID || current.TenantID != command.TenantID {
		return result, ErrPublicationIntegrity
	}
	if current.MetadataRevision != command.ExpectedMetadataRevision {
		return result, ErrMetadataRevisionConflict
	}
	next := current
	changed, err := next.UpdateMetadata(command.Name, command.Description, s.deps.Now())
	if err != nil {
		return result, ErrInvalidDeployment
	}
	if !changed {
		return current, nil
	}
	updated, err := s.deps.Publications.UpdateDeploymentMetadata(ctx, next, command.ExpectedMetadataRevision)
	if err != nil {
		return result, err
	}
	if updated.Validate() != nil || !sameDeploymentMetadata(updated, next) {
		return result, ErrPublicationIntegrity
	}
	return updated, nil
}
