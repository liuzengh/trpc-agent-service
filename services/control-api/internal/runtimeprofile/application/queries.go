package application

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func (s *Service) GetRuntimeProfile(
	ctx context.Context, tenantID, profileID, userID string,
) (domain.RuntimeProfile, error) {
	if err := s.authorize(ctx, tenantID, userID); err != nil {
		return domain.RuntimeProfile{}, err
	}
	profile, err := s.deps.Store.GetRuntimeProfile(ctx, tenantID, profileID)
	if errors.Is(err, ErrRuntimeProfileNotFound) {
		return domain.RuntimeProfile{}, ErrRuntimeProfileNotFound
	}
	if err != nil {
		return domain.RuntimeProfile{}, fmt.Errorf("get runtime profile: %w", err)
	}
	return profile, nil
}

func (s *Service) ListRuntimeProfiles(
	ctx context.Context, tenantID, userID string, page Page,
) (RuntimeProfilePage, error) {
	if err := s.authorize(ctx, tenantID, userID); err != nil {
		return RuntimeProfilePage{}, err
	}
	result, err := s.deps.Store.ListRuntimeProfiles(ctx, tenantID, normalizePage(page))
	if err != nil {
		return RuntimeProfilePage{}, fmt.Errorf("list runtime profiles: %w", err)
	}
	if result.Profiles == nil {
		result.Profiles = []domain.RuntimeProfile{}
	}
	return result, nil
}

func (s *Service) GetProfileDraft(
	ctx context.Context, tenantID, profileID, userID string,
) (domain.ProfileDraft, error) {
	if err := s.authorize(ctx, tenantID, userID); err != nil {
		return domain.ProfileDraft{}, err
	}
	draft, err := s.deps.Store.GetProfileDraft(ctx, tenantID, profileID)
	if errors.Is(err, ErrRuntimeProfileNotFound) {
		return domain.ProfileDraft{}, ErrRuntimeProfileNotFound
	}
	if err != nil {
		return domain.ProfileDraft{}, fmt.Errorf("get runtime profile draft: %w", err)
	}
	return draft.Clone(), nil
}

func (s *Service) GetProfileRevision(
	ctx context.Context, tenantID, profileID, userID string, revisionNumber int64,
) (domain.ProfileRevision, error) {
	if err := s.authorize(ctx, tenantID, userID); err != nil {
		return domain.ProfileRevision{}, err
	}
	if revisionNumber <= 0 {
		return domain.ProfileRevision{}, ErrProfileRevisionNotFound
	}
	revision, err := s.deps.Store.GetProfileRevision(ctx, tenantID, profileID, revisionNumber)
	if errors.Is(err, ErrProfileRevisionNotFound) {
		return domain.ProfileRevision{}, ErrProfileRevisionNotFound
	}
	if err != nil {
		return domain.ProfileRevision{}, fmt.Errorf("get runtime profile revision: %w", err)
	}
	revision, err = canonicalStoredRevision(revision)
	if err != nil {
		return domain.ProfileRevision{}, fmt.Errorf("verify runtime profile revision: %w", err)
	}
	return revision.Clone(), nil
}

func (s *Service) ListProfileRevisions(
	ctx context.Context, tenantID, profileID, userID string, page Page,
) (ProfileRevisionSummaryPage, error) {
	if err := s.authorize(ctx, tenantID, userID); err != nil {
		return ProfileRevisionSummaryPage{}, err
	}
	result, err := s.deps.Store.ListProfileRevisionSummaries(
		ctx, tenantID, profileID, normalizePage(page),
	)
	if errors.Is(err, ErrRuntimeProfileNotFound) {
		return ProfileRevisionSummaryPage{}, ErrRuntimeProfileNotFound
	}
	if err != nil {
		return ProfileRevisionSummaryPage{}, fmt.Errorf("list runtime profile revisions: %w", err)
	}
	if result.Revisions == nil {
		result.Revisions = []domain.ProfileRevisionSummary{}
	}
	return result, nil
}
