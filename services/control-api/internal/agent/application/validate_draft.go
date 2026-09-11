package application

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
)

func (s *Service) ValidateDraft(
	ctx context.Context,
	command ValidateDraftCommand,
) (domain.ValidationReport, error) {
	if err := s.authorize(ctx, command.TenantID, command.ActorUserID); err != nil {
		return domain.ValidationReport{}, err
	}
	if command.ExpectedRevision <= 0 {
		return domain.ValidationReport{}, ErrDraftRevisionConflict
	}
	draft, err := s.deps.Store.GetDraft(ctx, command.TenantID, command.AgentID)
	if err != nil {
		if errors.Is(err, ErrAgentNotFound) {
			return domain.ValidationReport{}, ErrAgentNotFound
		}
		return domain.ValidationReport{}, fmt.Errorf("get agent draft: %w", err)
	}
	if draft.Revision != command.ExpectedRevision {
		return domain.ValidationReport{}, ErrDraftRevisionConflict
	}
	_, report := domain.ValidateForPublication(draft.Spec, draft.Revision)
	return report, nil
}
