package application

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/domain"
)

var (
	ErrInvalidTenant         = errors.New("invalid tenant")
	ErrTenantSlugTaken       = errors.New("tenant slug already exists")
	ErrTenantNotFound        = errors.New("tenant not found")
	ErrMembershipNotFound    = errors.New("membership not found")
	ErrMembershipExists      = errors.New("membership already exists")
	ErrTenantForbidden       = errors.New("tenant action forbidden")
	ErrAccountUnavailable    = errors.New("account is unavailable")
	ErrOwnerRequiresTransfer = errors.New("owner membership requires ownership transfer")
	ErrInvalidCandidateQuery = errors.New("invalid member candidate query")
)

type Store interface {
	ProvisionTenant(context.Context, domain.Tenant, domain.Membership) error
	GetTenantMembership(context.Context, string, string) (domain.TenantMembership, error)
	GetMembership(context.Context, string, string) (domain.Membership, error)
	CreateMembership(context.Context, domain.Membership) error
	DeleteMembership(context.Context, string, string) error
	ListMyTenants(context.Context, string) ([]domain.TenantMembership, error)
	ListMembers(context.Context, string) ([]domain.Membership, error)
	ListTenants(context.Context, Page) (TenantPage, error)
}

// AccountLookup deliberately reveals only the fact Tenant needs.
type AccountLookup interface {
	IsActiveAccount(context.Context, string) (bool, error)
}

// MemberCandidateQuery is the Tenant-owned read port for finding global
// accounts that can be added to one Tenant. Its implementation may use a
// dedicated cross-module read model, but it must not expose Identity-owned
// credentials or permit writes to Identity state.
type MemberCandidateQuery interface {
	SearchMemberCandidates(context.Context, string, string, Page) (MemberCandidatePage, error)
}

type Dependencies struct {
	Store           Store
	Accounts        AccountLookup
	Candidates      MemberCandidateQuery
	NewTenantID     func() (string, error)
	NewMembershipID func() (string, error)
	Now             func() time.Time
}

type Service struct {
	deps Dependencies
}

func NewService(deps Dependencies) *Service {
	if deps.Now == nil {
		deps.Now = time.Now
	}
	return &Service{deps: deps}
}

type Page struct {
	Offset int
	Limit  int
}

type TenantPage struct {
	Tenants []domain.Tenant
	Total   int
}

type MemberCandidate struct {
	UserID      string
	Username    string
	DisplayName string
}

type MemberCandidatePage struct {
	Candidates []MemberCandidate
	Offset     int
	Limit      int
	Total      int
}

func (s *Service) validate() error {
	if s == nil || s.deps.Store == nil || s.deps.Accounts == nil ||
		s.deps.NewTenantID == nil || s.deps.NewMembershipID == nil {
		return errors.New("tenant service: incomplete dependencies")
	}
	return nil
}
