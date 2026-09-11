package application

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"sort"
)

var ErrManagedBackend = errors.New("runtime profile managed backend is unavailable")

// BackendAccess checks only platform selection eligibility. It does not return
// connection targets or credentials and does not claim runtime adapter readiness.
type BackendAccess interface {
	CheckBackend(context.Context, string, string, uint64, string) error
}

func (s *Service) checkManaged(ctx context.Context, tenant string, spec domain.Spec) error {
	keys := make([]string, 0, len(spec.Storage))
	for k := range spec.Storage {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		r := spec.Storage[k]
		if !r.Kind.Managed() {
			continue
		}
		if k != r.Kind.Role() || s.deps.Backends == nil {
			return ErrManagedBackend
		}
		if err := s.deps.Backends.CheckBackend(ctx, tenant, r.BackendID, r.BackendRevision, r.Kind.Role()); err != nil {
			return ErrManagedBackend
		}
	}
	keys = keys[:0]
	for k := range spec.Knowledge {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		r := spec.Knowledge[k]
		if r.Kind != domain.KnowledgeKindManaged {
			continue
		}
		if s.deps.Backends == nil {
			return ErrManagedBackend
		}
		if err := s.deps.Backends.CheckBackend(ctx, tenant, r.BackendID, r.BackendRevision, "knowledge"); err != nil {
			return ErrManagedBackend
		}
	}
	return nil
}
func (s *Service) checkManagedDocument(ctx context.Context, tenant string, document []byte) error {
	var spec domain.Spec
	if err := json.Unmarshal(document, &spec); err != nil {
		return ErrManagedBackend
	}
	return s.checkManaged(ctx, tenant, spec)
}
