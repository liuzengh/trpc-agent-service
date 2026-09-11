package application

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
)

func (s *Service) GetDeployment(ctx context.Context, tenantID, deploymentID, actorUserID string) (domain.Deployment, error) {
	if err := s.authorizeMember(ctx, tenantID, actorUserID); err != nil {
		return domain.Deployment{}, err
	}
	value, err := s.deps.Queries.GetDeployment(ctx, tenantID, deploymentID)
	if errors.Is(err, ErrDeploymentNotFound) {
		return domain.Deployment{}, ErrDeploymentNotFound
	}
	if err != nil {
		return domain.Deployment{}, fmt.Errorf("get deployment: %w", err)
	}
	if value.Validate() != nil || value.TenantID != tenantID || value.ID != deploymentID {
		return domain.Deployment{}, ErrPublicationIntegrity
	}
	return value, nil
}

func (s *Service) ListDeployments(ctx context.Context, tenantID, actorUserID string, page Page) (DeploymentPage, error) {
	if err := s.authorizeMember(ctx, tenantID, actorUserID); err != nil {
		return DeploymentPage{}, err
	}
	result, err := s.deps.Queries.ListDeployments(ctx, tenantID, normalizePage(page))
	if err != nil {
		return DeploymentPage{}, fmt.Errorf("list deployments: %w", err)
	}
	if result.Deployments == nil {
		result.Deployments = []domain.Deployment{}
	}
	for _, value := range result.Deployments {
		if value.Validate() != nil || value.TenantID != tenantID {
			return DeploymentPage{}, ErrPublicationIntegrity
		}
	}
	return result, nil
}

func (s *Service) GetDeploymentRevision(ctx context.Context, tenantID, deploymentID, actorUserID string, number int64) (domain.PublishedRevision, error) {
	if err := s.authorizeMember(ctx, tenantID, actorUserID); err != nil {
		return domain.PublishedRevision{}, err
	}
	if number <= 0 {
		return domain.PublishedRevision{}, ErrDeploymentRevisionNotFound
	}
	value, err := s.deps.Queries.GetPublishedRevision(ctx, tenantID, deploymentID, number)
	if errors.Is(err, ErrDeploymentNotFound) || errors.Is(err, ErrDeploymentRevisionNotFound) {
		return domain.PublishedRevision{}, err
	}
	if err != nil {
		return domain.PublishedRevision{}, fmt.Errorf("get deployment revision: %w", err)
	}
	value, err = verifyPublished(value)
	if err != nil {
		return domain.PublishedRevision{}, err
	}
	if value.Revision.TenantID != tenantID || value.Revision.DeploymentID != deploymentID ||
		value.Revision.RevisionNumber != number {
		return domain.PublishedRevision{}, ErrPublicationIntegrity
	}
	return value, nil
}

func (s *Service) ListDeploymentRevisions(ctx context.Context, tenantID, deploymentID, actorUserID string, page Page) (RevisionSummaryPage, error) {
	if err := s.authorizeMember(ctx, tenantID, actorUserID); err != nil {
		return RevisionSummaryPage{}, err
	}
	result, err := s.deps.Queries.ListRevisionSummaries(ctx, tenantID, deploymentID, normalizePage(page))
	if errors.Is(err, ErrDeploymentNotFound) {
		return RevisionSummaryPage{}, ErrDeploymentNotFound
	}
	if err != nil {
		return RevisionSummaryPage{}, fmt.Errorf("list deployment revisions: %w", err)
	}
	if result.Revisions == nil {
		result.Revisions = []domain.DeploymentRevisionSummary{}
	}
	for _, value := range result.Revisions {
		if value.TenantID != tenantID || value.DeploymentID != deploymentID || value.RevisionNumber <= 0 ||
			value.ID == "" || value.ManifestID == "" || value.ManifestDigest == "" {
			return RevisionSummaryPage{}, ErrPublicationIntegrity
		}
	}
	return result, nil
}
