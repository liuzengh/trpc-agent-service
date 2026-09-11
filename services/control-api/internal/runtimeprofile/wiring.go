// Package runtimeprofile composes Runtime Profile authoring, validation, and
// immutable publication capabilities.
package runtimeprofile

import (
	"errors"
	"time"

	"github.com/gin-gonic/gin"

	httpadapter "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/adapter/inbound/http"
	runtimehttp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/adapter/inbound/runtimehttp"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/adapter/outbound/credentialcrypto"
	postgresadapter "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
)

// Dependencies lists process-owned dependencies required by the Runtime
// Profile module.
// ExecutionVerifier and AuthenticateWorker opt into the internal runtime HTTP
// route as a pair. The current process bootstrap leaves both unset.
type Dependencies struct {
	FinalArtifacts           application.FinalArtifactAuthorizer
	ManagedCredentialTargets application.ManagedCredentialTargetResolver
	Backends                 application.BackendAccess
	ExecutionVerifier        application.ExecutionAuthorizationVerifier
	AuthenticateWorker       gin.HandlerFunc
	DB                       postgresadapter.DB
	Routes                   gin.IRouter
	RuntimeRoutes            gin.IRouter
	Authenticate             gin.HandlerFunc
	TenantAccess             application.TenantAccess
	OwnerAccess              application.OwnerAccess
	CredentialKey            []byte
}

// Module is the assembled Runtime Profile module.
type Module struct {
	Service *application.Service
}

// NewModule assembles the Runtime Profile module without starting process
// resources.
func NewModule(deps Dependencies) (*Module, error) {
	if (deps.ExecutionVerifier == nil) != (deps.AuthenticateWorker == nil) {
		return nil, errors.New("runtime profile: worker authentication and execution verifier must be configured together")
	}
	if deps.DB == nil || deps.Routes == nil || deps.Authenticate == nil || deps.TenantAccess == nil {
		return nil, errors.New("runtime profile: database, routes, authentication, and tenant access are required")
	}
	if deps.OwnerAccess == nil {
		return nil, errors.New("runtime profile: owner authorization is required")
	}
	cipher, err := credentialcrypto.New(deps.CredentialKey)
	if err != nil {
		return nil, err
	}
	store := postgresadapter.NewStore(deps.DB)
	service := application.NewService(application.Dependencies{
		ManagedCredentialTargets: deps.ManagedCredentialTargets,
		Store:                    store, TenantAccess: deps.TenantAccess, Backends: deps.Backends,
		Credentials: store, Cipher: cipher, OwnerAccess: deps.OwnerAccess,
		ExecutionVerifier: deps.ExecutionVerifier, FinalArtifacts: deps.FinalArtifacts,
		NewCredentialID: generateCredentialID,
		NewProfileID:    func() (string, error) { return generateID("rpf") },
		NewRevisionID:   func() (string, error) { return generateID("rpr") },
		Now:             time.Now,
	})
	protected := deps.Routes.Group("", deps.Authenticate)
	httpadapter.NewHandler(service).Register(protected)
	if deps.AuthenticateWorker != nil {
		// Apply before authentication so rejected requests are non-cacheable too.
		runtimeRoutes := deps.RuntimeRoutes
		if runtimeRoutes == nil {
			runtimeRoutes = deps.Routes
		}
		runtimeProtected := runtimeRoutes.Group("", func(c *gin.Context) {
			c.Header("Cache-Control", "no-store")
			c.Next()
		}, deps.AuthenticateWorker)
		runtimehttp.NewHandler(service).Register(runtimeProtected)
	}
	return &Module{Service: service}, nil
}
