// Package application coordinates Deployment V1 use cases. It reads immutable
// Agent and Runtime Profile publications through their owner interfaces and
// persists each Deployment publication and its Outbox event atomically.
package application

import (
	"context"
	"errors"
	"time"

	executionv1 "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	agentapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/application"
	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
	profileapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

var (
	ErrInvalidDeployment               = errors.New("invalid deployment")
	ErrInvalidDeploymentInput          = errors.New("invalid deployment input")
	ErrDeploymentNotFound              = errors.New("deployment not found")
	ErrDeploymentRevisionNotFound      = errors.New("deployment revision not found")
	ErrAgentVersionNotFound            = errors.New("agent version not found")
	ErrProfileRevisionNotFound         = errors.New("runtime profile revision not found")
	ErrTenantForbidden                 = errors.New("tenant action forbidden")
	ErrMetadataRevisionConflict        = errors.New("deployment metadata revision conflict")
	ErrLatestRevisionConflict          = errors.New("deployment latest revision conflict")
	ErrIdempotencyConflict             = errors.New("deployment idempotency conflict")
	ErrDeploymentRevisionInvalid       = errors.New("deployment revision is not publishable")
	ErrCredentialDependencyUnavailable = errors.New("profile credential dependency unavailable")
	ErrPublicationIntegrity            = errors.New("deployment publication integrity violation")
)

type TenantAccess interface {
	IsActiveMember(context.Context, string, string) (bool, error)
	IsActiveOwner(context.Context, string, string) (bool, error)
}

type AgentVersionReader interface {
	GetAgentVersion(context.Context, string, string, string, int64) (agentdomain.AgentVersion, error)
}

type ProfileRevisionReader interface {
	GetProfileRevision(context.Context, string, string, string, int64) (profiledomain.ProfileRevision, error)
}

type ProfileCredentialChecker interface {
	CheckUsable(context.Context, profileapp.CheckProfileCredentialsCommand) error
}

type StorageCredentialResolver interface {
	ResolveStorageForOwner(context.Context, profileapp.CheckProfileCredentialsCommand) (profileapp.CredentialBatch, error)
}

type BackendMigrator interface {
	Execute(context.Context, executionv1.BackendMigrationRequest) (executionv1.BackendMigrationResponse, error)
}

type CommandReceiptKey struct {
	TenantID  string
	Operation string
	ScopeID   string
	KeyHash   string
}

type CreateCommit struct {
	Deployment domain.Deployment
	Receipt    domain.CommandReceipt
}

type CreateCommitResult struct {
	Receipt domain.CommandReceipt
	Created bool
}

type PublicationCommit struct {
	ExpectedLatestRevisionNumber *int64
	Revision                     domain.DeploymentRevision
	Manifest                     domain.RuntimeManifest
	OutboxEvent                  domain.OutboxEvent
	Receipt                      domain.CommandReceipt
}

type PublicationCommitResult struct {
	Receipt domain.CommandReceipt
	Created bool
}

type PublicationStore interface {
	FindCommandReceipt(context.Context, CommandReceiptKey) (domain.CommandReceipt, bool, error)
	CreateDeployment(context.Context, CreateCommit) (CreateCommitResult, error)
	UpdateDeploymentMetadata(context.Context, domain.Deployment, int64) (domain.Deployment, error)
	CommitPublication(context.Context, PublicationCommit) (PublicationCommitResult, error)
}

type QueryStore interface {
	GetDeployment(context.Context, string, string) (domain.Deployment, error)
	ListDeployments(context.Context, string, Page) (DeploymentPage, error)
	GetPublishedRevision(context.Context, string, string, int64) (domain.PublishedRevision, error)
	ListRevisionSummaries(context.Context, string, string, Page) (RevisionSummaryPage, error)
}

type Dependencies struct {
	KnowledgeBackend     KnowledgeBackend
	KnowledgeCredentials KnowledgeCredentialResolver
	ArtifactBackend      ArtifactBackend
	ArtifactCredentials  ArtifactCredentialResolver
	ManagedBackends      ManagedBackendResolver
	Publications         PublicationStore
	Queries              QueryStore
	TenantAccess         TenantAccess
	AgentVersions        AgentVersionReader
	ProfileRevisions     ProfileRevisionReader
	ProfileCredentials   ProfileCredentialChecker
	StorageCredentials   StorageCredentialResolver
	BackendMigrations    BackendMigrator
	Platform             domain.PlatformExecutionContract
	NewDeploymentID      func() (string, error)
	NewRevisionID        func() (string, error)
	NewManifestID        func() (string, error)
	NewEventID           func() (string, error)
	Now                  func() time.Time
	MaxEventBytes        int
}

type Service struct{ deps Dependencies }

func NewService(deps Dependencies) *Service {
	deps.Platform = deps.Platform.Clone()
	if deps.Now == nil {
		deps.Now = time.Now
	}
	if deps.MaxEventBytes <= 0 {
		deps.MaxEventBytes = 1024 * 1024
	}
	return &Service{deps: deps}
}

func (s *Service) validate() error {
	if s == nil || s.deps.Publications == nil || s.deps.Queries == nil ||
		s.deps.TenantAccess == nil || s.deps.AgentVersions == nil ||
		s.deps.ProfileRevisions == nil || s.deps.ProfileCredentials == nil ||
		s.deps.NewDeploymentID == nil || s.deps.NewRevisionID == nil ||
		s.deps.NewManifestID == nil || s.deps.NewEventID == nil {
		return errors.New("deployment service: incomplete dependencies")
	}
	return s.deps.Platform.Validate()
}

func (s *Service) authorizeMember(ctx context.Context, tenantID, userID string) error {
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

func (s *Service) authorizeOwner(ctx context.Context, tenantID, userID string) error {
	if err := s.authorizeMember(ctx, tenantID, userID); err != nil {
		return err
	}
	allowed, err := s.deps.TenantAccess.IsActiveOwner(ctx, tenantID, userID)
	if err != nil {
		return err
	}
	if !allowed {
		return ErrTenantForbidden
	}
	return nil
}

// Keep owner-specific errors out of the Deployment HTTP adapter.
func translateAgentError(err error) error {
	if errors.Is(err, agentapp.ErrAgentNotFound) || errors.Is(err, agentapp.ErrAgentVersionNotFound) {
		return ErrAgentVersionNotFound
	}
	if errors.Is(err, agentapp.ErrTenantForbidden) {
		return ErrTenantForbidden
	}
	return err
}

func translateProfileError(err error) error {
	if errors.Is(err, profileapp.ErrRuntimeProfileNotFound) || errors.Is(err, profileapp.ErrProfileRevisionNotFound) {
		return ErrProfileRevisionNotFound
	}
	if errors.Is(err, profileapp.ErrTenantForbidden) {
		return ErrTenantForbidden
	}
	return err
}

type Page struct{ Offset, Limit int }

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

type DeploymentPage struct {
	Deployments []domain.Deployment
	Total       int
}

type RevisionSummaryPage struct {
	Revisions []domain.DeploymentRevisionSummary
	Total     int
}
