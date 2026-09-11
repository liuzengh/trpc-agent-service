// Package deployment composes Deployment authoring, deterministic publication,
// and immutable RuntimeManifest query capabilities.
package deployment

import (
	"errors"
	"time"

	"github.com/gin-gonic/gin"

	httpadapter "github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/adapter/inbound/http"
	postgresadapter "github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
)

// Dependencies lists process-owned dependencies required by the Deployment
// module. Agent and Runtime Profile are consumed through their published owner
// queries; the module never reads either owner's tables directly.
type Dependencies struct {
	KnowledgeBackend     application.KnowledgeBackend
	KnowledgeCredentials application.KnowledgeCredentialResolver
	ArtifactBackend      application.ArtifactBackend
	ArtifactCredentials  application.ArtifactCredentialResolver
	ManagedBackends      application.ManagedBackendResolver
	DB                   postgresadapter.DB
	Routes               gin.IRouter
	Authenticate         gin.HandlerFunc
	TenantAccess         application.TenantAccess
	AgentVersions        application.AgentVersionReader
	ProfileRevisions     application.ProfileRevisionReader
	ProfileCredentials   application.ProfileCredentialChecker
	StorageCredentials   application.StorageCredentialResolver
	BackendMigrations    application.BackendMigrator
	Platform             domain.PlatformExecutionContract
}

// Module is the assembled Deployment module.
type Module struct {
	Service *application.Service
}

// NewModule assembles the Deployment module without starting process
// resources.
func NewModule(deps Dependencies) (*Module, error) {
	if deps.DB == nil || deps.Routes == nil || deps.Authenticate == nil ||
		deps.TenantAccess == nil || deps.AgentVersions == nil ||
		deps.ProfileRevisions == nil || deps.ProfileCredentials == nil {
		return nil, errors.New("deployment: database, routes, authentication, tenant access, and source readers are required")
	}
	if err := deps.Platform.Validate(); err != nil {
		return nil, errors.New("deployment: valid platform execution contract is required")
	}

	store := postgresadapter.NewStore(deps.DB)
	service := application.NewService(application.Dependencies{
		ManagedBackends: deps.ManagedBackends,
		ArtifactBackend: deps.ArtifactBackend, ArtifactCredentials: deps.ArtifactCredentials,
		KnowledgeBackend: deps.KnowledgeBackend, KnowledgeCredentials: deps.KnowledgeCredentials,
		Publications:       store,
		Queries:            store,
		TenantAccess:       deps.TenantAccess,
		AgentVersions:      deps.AgentVersions,
		ProfileRevisions:   deps.ProfileRevisions,
		ProfileCredentials: deps.ProfileCredentials,
		StorageCredentials: deps.StorageCredentials,
		BackendMigrations:  deps.BackendMigrations,
		Platform:           deps.Platform,
		NewDeploymentID:    func() (string, error) { return generateID("dpl") },
		NewRevisionID:      func() (string, error) { return generateID("dpr") },
		NewManifestID:      func() (string, error) { return generateID("rmf") },
		NewEventID:         func() (string, error) { return generateID("evt") },
		Now:                time.Now,
	})
	protected := deps.Routes.Group("", deps.Authenticate)
	httpadapter.NewHandler(service).Register(protected)
	return &Module{Service: service}, nil
}
