package application

import (
	"context"
	"errors"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/platformbackend/domain"
)

var ErrForbidden = errors.New("TENANT_FORBIDDEN")
var ErrDependency = errors.New("BACKEND_DIRECTORY_UNAVAILABLE")

type TenantAccess interface {
	IsActiveMember(context.Context, string, string) (bool, error)
}
type Service struct {
	catalog *domain.Catalog
	access  TenantAccess
}

func NewService(c *domain.Catalog, a TenantAccess) (*Service, error) {
	if c == nil || a == nil {
		return nil, ErrDependency
	}
	return &Service{c, a}, nil
}
func (s *Service) List(ctx context.Context, tenant, user string) ([]domain.View, error) {
	if tenant == "" || user == "" {
		return nil, ErrForbidden
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ok, err := s.access.IsActiveMember(ctx, tenant, user)
	if err != nil {
		return nil, ErrDependency
	}
	if !ok {
		return nil, ErrForbidden
	}
	vs, err := s.catalog.List(tenant)
	if err != nil {
		return nil, ErrForbidden
	}
	return vs, nil
}
