package application

import (
	"context"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
)

// ManagedBackendResolver is owned by Deployment. Implementations must authorize
// the trusted actor and tenant against the platform's immutable target catalog.
// It must never return credentials or substitute another revision.
type ManagedBackendResolver interface {
	ResolveDeploymentBackend(context.Context, string, string, domain.BackendRequest) (datav1.Snapshot, error)
}

func (s *Service) resolveManagedBackends(ctx context.Context, tenant, actor string, requests []domain.BackendRequest) (map[string]datav1.Snapshot, []domain.Diagnostic) {
	resolved := make(map[string]datav1.Snapshot, len(requests))
	var diagnostics []domain.Diagnostic
	for _, r := range requests {
		if s.deps.ManagedBackends == nil {
			diagnostics = append(diagnostics, domain.BackendDiagnostic(domain.DiagnosticBackendUnavailable, r))
			continue
		}
		if ctx.Err() != nil {
			diagnostics = append(diagnostics, domain.BackendDiagnostic(domain.DiagnosticBackendUnavailable, r))
			break
		}
		snapshot, err := s.deps.ManagedBackends.ResolveDeploymentBackend(ctx, tenant, actor, r)
		if err != nil {
			diagnostics = append(diagnostics, domain.BackendDiagnostic(domain.DiagnosticBackendUnavailable, r))
			continue
		}
		// Detach provider-owned pointers before the compiler receives fixed values.
		resolved[r.Key()] = snapshot.Clone()
	}
	return resolved, diagnostics
}
