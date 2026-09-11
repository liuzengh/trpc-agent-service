package application

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func (s *Service) ValidateProfileDraft(
	ctx context.Context,
	command ValidateProfileDraftCommand,
) (domain.ValidationReport, error) {
	if err := s.authorize(ctx, command.TenantID, command.ActorUserID); err != nil {
		return domain.ValidationReport{}, err
	}
	if command.ExpectedRevision <= 0 {
		return domain.ValidationReport{}, ErrDraftRevisionConflict
	}
	draft, err := s.deps.Store.GetProfileDraft(ctx, command.TenantID, command.ProfileID)
	if err != nil {
		if errors.Is(err, ErrRuntimeProfileNotFound) {
			return domain.ValidationReport{}, ErrRuntimeProfileNotFound
		}
		return domain.ValidationReport{}, fmt.Errorf("get runtime profile draft: %w", err)
	}
	if draft.Revision != command.ExpectedRevision {
		return domain.ValidationReport{}, ErrDraftRevisionConflict
	}
	_, report := domain.ValidateForPublication(draft.Spec, draft.Revision)
	if report.Valid {
		if err := s.checkManagedDocument(ctx, command.TenantID, draft.Spec); err != nil {
			report.Valid = false
			report.Diagnostics = append(report.Diagnostics, domain.Diagnostic{Code: "RUNTIME_PROFILE_BACKEND_UNAVAILABLE", Severity: domain.SeverityError, Pointer: "", Message: "one or more selected platform backends are unavailable"})
		}
	}
	return report, nil
}
