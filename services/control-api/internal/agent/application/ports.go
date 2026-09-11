// Package application coordinates Agent use cases without depending on HTTP or
// PostgreSQL. Its ports expose only facts and persistence operations owned by
// this subdomain.
package application

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
)

var (
	ErrInvalidAgent          = errors.New("invalid agent")
	ErrAgentNotFound         = errors.New("agent not found")
	ErrAgentVersionNotFound  = errors.New("agent version not found")
	ErrTenantForbidden       = errors.New("tenant action forbidden")
	ErrDraftRevisionConflict = errors.New("agent draft revision conflict")
	ErrAgentSpecInvalid      = errors.New("agent spec is not publishable")
)

// TenantAccess deliberately exposes only the authorization fact Agent needs.
type TenantAccess interface {
	IsActiveMember(context.Context, string, string) (bool, error)
}

// Store is the persistence boundary owned by the Agent application layer.
type Store interface {
	CreateAgent(context.Context, domain.Agent, domain.AgentDraft) error
	GetAgent(context.Context, string, string) (domain.Agent, error)
	ListAgents(context.Context, string, Page) (AgentPage, error)
	UpdateAgent(context.Context, domain.Agent) error
	GetDraft(context.Context, string, string) (domain.AgentDraft, error)
	SaveDraft(context.Context, domain.AgentDraft, int64) error
	PublishVersion(context.Context, domain.AgentVersion, int64) (domain.AgentVersion, bool, error)
	GetVersion(context.Context, string, string, int64) (domain.AgentVersion, error)
	ListVersions(context.Context, string, string, Page) (VersionPage, error)
}

type Dependencies struct {
	Store        Store
	TenantAccess TenantAccess
	NewAgentID   func() (string, error)
	NewVersionID func() (string, error)
	Now          func() time.Time
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
		s.deps.NewAgentID == nil || s.deps.NewVersionID == nil {
		return errors.New("agent service: incomplete dependencies")
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

type AgentPage struct {
	Agents []domain.Agent
	Total  int
}

type VersionPage struct {
	Versions []domain.AgentVersion
	Total    int
}

type CreateAgentCommand struct {
	TenantID    string
	ActorUserID string
	Name        string
	Description string
}

type CreateAgentResult struct {
	Agent domain.Agent
	Draft domain.AgentDraft
}

type UpdateAgentCommand struct {
	TenantID    string
	AgentID     string
	ActorUserID string
	Name        *string
	Description *string
}

type SaveDraftCommand struct {
	TenantID         string
	AgentID          string
	ActorUserID      string
	ExpectedRevision int64
	Spec             json.RawMessage
}

type ValidateDraftCommand struct {
	TenantID         string
	AgentID          string
	ActorUserID      string
	ExpectedRevision int64
}

type PublishVersionCommand struct {
	TenantID         string
	AgentID          string
	ActorUserID      string
	ExpectedRevision int64
}

type PublishVersionResult struct {
	Version domain.AgentVersion
	Report  domain.ValidationReport
	Created bool
}
