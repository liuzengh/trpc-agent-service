package application

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func (s *Service) PublishProfileRevision(
	ctx context.Context,
	command PublishProfileRevisionCommand,
) (PublishProfileRevisionResult, error) {
	if err := s.authorize(ctx, command.TenantID, command.ActorUserID); err != nil {
		return PublishProfileRevisionResult{}, err
	}

	// The immutable source-draft key is the idempotency key. This lookup must
	// precede reading the mutable current draft so retries remain successful
	// after editing has advanced that draft.
	existing, found, err := s.deps.Store.FindRevisionBySourceDraft(
		ctx, command.TenantID, command.ProfileID, command.ExpectedRevision,
	)
	if err != nil {
		return PublishProfileRevisionResult{}, fmt.Errorf("find published runtime profile revision: %w", err)
	}
	if found {
		existing, err = canonicalStoredRevision(existing)
		if err != nil {
			return PublishProfileRevisionResult{}, fmt.Errorf("verify published runtime profile revision: %w", err)
		}
		return PublishProfileRevisionResult{Revision: existing.Clone(), Created: false}, nil
	}
	if command.ExpectedRevision <= 0 {
		return PublishProfileRevisionResult{}, ErrDraftRevisionConflict
	}

	draft, err := s.deps.Store.GetProfileDraft(ctx, command.TenantID, command.ProfileID)
	if err != nil {
		if errors.Is(err, ErrRuntimeProfileNotFound) {
			return PublishProfileRevisionResult{}, ErrRuntimeProfileNotFound
		}
		return PublishProfileRevisionResult{}, fmt.Errorf("get runtime profile draft: %w", err)
	}
	if draft.Revision != command.ExpectedRevision {
		return PublishProfileRevisionResult{}, ErrDraftRevisionConflict
	}
	canonical, report := domain.ValidateForPublication(draft.Spec, draft.Revision)
	if !report.Valid {
		return PublishProfileRevisionResult{Report: report}, ErrRuntimeProfileSpecInvalid
	}
	if err := s.checkManagedDocument(ctx, command.TenantID, canonical.Document); err != nil {
		return PublishProfileRevisionResult{}, err
	}
	id, err := s.deps.NewRevisionID()
	if err != nil {
		return PublishProfileRevisionResult{}, fmt.Errorf("generate runtime profile revision id: %w", err)
	}
	candidate := domain.ProfileRevision{
		ID: id, TenantID: command.TenantID, ProfileID: command.ProfileID,
		SourceDraftRevision: draft.Revision, SchemaVersion: canonical.SchemaVersion,
		Spec: canonical.Document, SpecDigest: canonical.Digest,
		PublishedBy: command.ActorUserID, PublishedAt: s.deps.Now().UTC(),
	}
	published, created, err := s.deps.Store.PublishProfileRevision(
		ctx, candidate, command.ExpectedRevision,
	)
	if err != nil {
		switch {
		case errors.Is(err, ErrDraftRevisionConflict):
			return PublishProfileRevisionResult{}, ErrDraftRevisionConflict
		case errors.Is(err, ErrRuntimeProfileNotFound):
			return PublishProfileRevisionResult{}, ErrRuntimeProfileNotFound
		default:
			return PublishProfileRevisionResult{}, fmt.Errorf("publish runtime profile revision: %w", err)
		}
	}
	published, err = canonicalStoredRevision(published)
	if err != nil {
		return PublishProfileRevisionResult{}, fmt.Errorf("verify published runtime profile revision: %w", err)
	}
	return PublishProfileRevisionResult{
		Revision: published.Clone(), Report: report, Created: created,
	}, nil
}
