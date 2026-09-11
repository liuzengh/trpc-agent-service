// Package application coordinates Runtime Profile use cases without depending
// on HTTP or PostgreSQL. Its ports expose only authorization facts and
// persistence operations owned by this subdomain.
package application

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

var (
	ErrInvalidRuntimeProfile     = errors.New("invalid runtime profile")
	ErrRuntimeProfileNotFound    = errors.New("runtime profile not found")
	ErrProfileRevisionNotFound   = errors.New("runtime profile revision not found")
	ErrTenantForbidden           = errors.New("tenant action forbidden")
	ErrDraftRevisionConflict     = errors.New("runtime profile draft revision conflict")
	ErrRuntimeProfileSpecInvalid = errors.New("runtime profile spec is not publishable")
)

// TenantAccess deliberately exposes only the authorization fact Runtime
// Profile needs from the Tenant subdomain.
type TenantAccess interface {
	IsActiveMember(context.Context, string, string) (bool, error)
}

// Store is the persistence boundary owned by the Runtime Profile application
// layer. Every operation is tenant-scoped, including immutable revision reads.
type Store interface {
	CreateRuntimeProfile(context.Context, domain.RuntimeProfile, domain.ProfileDraft) error
	GetRuntimeProfile(context.Context, string, string) (domain.RuntimeProfile, error)
	ListRuntimeProfiles(context.Context, string, Page) (RuntimeProfilePage, error)
	UpdateRuntimeProfile(context.Context, domain.RuntimeProfile) error
	GetProfileDraft(context.Context, string, string) (domain.ProfileDraft, error)
	FindRevisionBySourceDraft(context.Context, string, string, int64) (domain.ProfileRevision, bool, error)
	PublishProfileRevision(context.Context, domain.ProfileRevision, int64) (domain.ProfileRevision, bool, error)
	GetProfileRevision(context.Context, string, string, int64) (domain.ProfileRevision, error)
	ListProfileRevisionSummaries(context.Context, string, string, Page) (ProfileRevisionSummaryPage, error)
}

type Dependencies struct {
	FinalArtifacts           FinalArtifactAuthorizer
	ManagedCredentialTargets ManagedCredentialTargetResolver
	Backends                 BackendAccess
	ExecutionVerifier        ExecutionAuthorizationVerifier
	Credentials              CredentialStore
	Cipher                   CredentialCipher
	OwnerAccess              OwnerAccess
	NewCredentialID          func() (string, error)
	Store                    Store
	TenantAccess             TenantAccess
	NewProfileID             func() (string, error)
	NewRevisionID            func() (string, error)
	Now                      func() time.Time
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

func (s *Service) validate() error {
	if s == nil || s.deps.Store == nil || s.deps.TenantAccess == nil ||
		s.deps.NewProfileID == nil || s.deps.NewRevisionID == nil {
		return errors.New("runtime profile service: incomplete dependencies")
	}
	return nil
}

func (s *Service) authorize(ctx context.Context, tenantID, userID string) error {
	if err := s.validate(); err != nil {
		return err
	}
	if tenantID == "" || userID == "" {
		return ErrTenantForbidden
	}
	allowed, err := s.deps.TenantAccess.IsActiveMember(ctx, tenantID, userID)
	if err != nil {
		return err
	}
	if !allowed {
		return ErrTenantForbidden
	}
	return nil
}

type Page struct {
	Offset int
	Limit  int
}

func normalizePage(page Page) Page {
	if page.Offset < 0 {
		page.Offset = 0
	}
	if page.Limit <= 0 {
		page.Limit = 20
	}
	if page.Limit > 100 {
		page.Limit = 100
	}
	return page
}

type RuntimeProfilePage struct {
	Profiles []domain.RuntimeProfile
	Total    int
}

type ProfileRevisionSummaryPage struct {
	Revisions []domain.ProfileRevisionSummary
	Total     int
}

type CreateRuntimeProfileCommand struct {
	TenantID    string
	ActorUserID string
	Name        string
	Description string
}

type CreateRuntimeProfileResult struct {
	Profile domain.RuntimeProfile
	Draft   domain.ProfileDraft
}

type UpdateRuntimeProfileCommand struct {
	TenantID    string
	ProfileID   string
	ActorUserID string
	Name        *string
	Description *string
}

type ValidateProfileDraftCommand struct {
	TenantID         string
	ProfileID        string
	ActorUserID      string
	ExpectedRevision int64
}

type PublishProfileRevisionCommand struct {
	TenantID         string
	ProfileID        string
	ActorUserID      string
	ExpectedRevision int64
}

type PublishProfileRevisionResult struct {
	Revision domain.ProfileRevision
	Report   domain.ValidationReport
	Created  bool
}
