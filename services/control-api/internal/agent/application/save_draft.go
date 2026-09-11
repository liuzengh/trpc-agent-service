package application

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
)

func (s *Service) SaveDraft(
	ctx context.Context,
	command SaveDraftCommand,
) (domain.AgentDraft, domain.ValidationReport, error) {
	if err := s.authorize(ctx, command.TenantID, command.ActorUserID); err != nil {
		return domain.AgentDraft{}, domain.ValidationReport{}, err
	}
	if command.ExpectedRevision <= 0 {
		return domain.AgentDraft{}, domain.ValidationReport{}, ErrDraftRevisionConflict
	}
	report := domain.ValidateDraftForStorage(command.Spec, command.ExpectedRevision)
	if !report.Valid {
		return domain.AgentDraft{}, report, ErrAgentSpecInvalid
	}
	draft := domain.AgentDraft{
		TenantID: command.TenantID, AgentID: command.AgentID,
		Revision: command.ExpectedRevision + 1,
		Spec:     append([]byte(nil), command.Spec...), UpdatedBy: command.ActorUserID,
		UpdatedAt: s.deps.Now().UTC(),
	}
	if err := s.deps.Store.SaveDraft(ctx, draft, command.ExpectedRevision); err != nil {
		switch {
		case errors.Is(err, ErrDraftRevisionConflict):
			return domain.AgentDraft{}, domain.ValidationReport{}, ErrDraftRevisionConflict
		case errors.Is(err, ErrAgentNotFound):
			return domain.AgentDraft{}, domain.ValidationReport{}, ErrAgentNotFound
		default:
			return domain.AgentDraft{}, domain.ValidationReport{}, fmt.Errorf("save agent draft: %w", err)
		}
	}
	return draft.Clone(), report, nil
}
