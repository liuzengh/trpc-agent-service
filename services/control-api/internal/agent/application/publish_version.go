package application

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
)

func (s *Service) PublishAgentVersion(
	ctx context.Context,
	command PublishVersionCommand,
) (PublishVersionResult, error) {
	if err := s.authorize(ctx, command.TenantID, command.ActorUserID); err != nil {
		return PublishVersionResult{}, err
	}
	if command.ExpectedRevision <= 0 {
		return PublishVersionResult{}, ErrDraftRevisionConflict
	}
	draft, err := s.deps.Store.GetDraft(ctx, command.TenantID, command.AgentID)
	if err != nil {
		if errors.Is(err, ErrAgentNotFound) {
			return PublishVersionResult{}, ErrAgentNotFound
		}
		return PublishVersionResult{}, fmt.Errorf("get agent draft: %w", err)
	}
	if draft.Revision != command.ExpectedRevision {
		return PublishVersionResult{}, ErrDraftRevisionConflict
	}
	canonical, report := domain.ValidateForPublication(draft.Spec, draft.Revision)
	if !report.Valid {
		return PublishVersionResult{Report: report}, ErrAgentSpecInvalid
	}
	id, err := s.deps.NewVersionID()
	if err != nil {
		return PublishVersionResult{}, fmt.Errorf("generate agent version id: %w", err)
	}
	version := domain.AgentVersion{
		ID: id, TenantID: command.TenantID, AgentID: command.AgentID,
		SourceDraftRevision: draft.Revision, SchemaVersion: canonical.SchemaVersion,
		Spec: canonical.Document, SpecDigest: canonical.Digest,
		PublishedBy: command.ActorUserID, PublishedAt: s.deps.Now().UTC(),
	}
	published, created, err := s.deps.Store.PublishVersion(ctx, version, command.ExpectedRevision)
	if err != nil {
		switch {
		case errors.Is(err, ErrDraftRevisionConflict):
			return PublishVersionResult{}, ErrDraftRevisionConflict
		case errors.Is(err, ErrAgentNotFound):
			return PublishVersionResult{}, ErrAgentNotFound
		default:
			return PublishVersionResult{}, fmt.Errorf("publish agent version: %w", err)
		}
	}
	published, err = canonicalStoredVersion(published)
	if err != nil {
		return PublishVersionResult{}, fmt.Errorf("verify published agent version: %w", err)
	}
	return PublishVersionResult{Version: published.Clone(), Report: report, Created: created}, nil
}
