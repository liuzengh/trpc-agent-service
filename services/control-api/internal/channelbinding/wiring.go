// Package channelbinding composes tenant channel accounts, precise deployment
// bindings and workload-authenticated configuration/credential supply.
package channelbinding

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"net/http"

	"github.com/gin-gonic/gin"
	httpadapter "github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/adapter/inbound/http"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/adapter/inbound/internalhttp"
	deploymentadapter "github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/adapter/outbound/deployment"
	postgresadapter "github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
)

type Dependencies struct {
	DB                    postgresadapter.DB
	Routes                gin.IRouter
	Authenticate          gin.HandlerFunc
	TenantAccess          application.TenantAccess
	TransactionAuthorizer postgresadapter.PreflightAuthorizer
	Deployments           deploymentadapter.OwnerReader
	Cipher                application.CredentialCipher
	Options               postgresadapter.Options
	Workloads             []application.WorkloadPrincipal
}
type Module struct {
	Service         *application.Service
	Queries         *application.QueryService
	Runtime         *application.RuntimeService
	Preflights      *application.PreflightService
	InternalHandler http.Handler
	store           *postgresadapter.Store
}

func NewModule(deps Dependencies) (*Module, error) {
	if deps.Routes == nil || deps.Authenticate == nil || deps.Deployments == nil || deps.Cipher == nil {
		return nil, application.ErrDependencyUnavailable
	}
	for _, p := range deps.Workloads {
		if p.ScopeID != deps.Options.ScopeID {
			return nil, application.ErrWorkloadDenied
		}
	}
	store, err := postgresadapter.NewStore(deps.DB, deps.TransactionAuthorizer, deps.Options)
	if err != nil {
		return nil, err
	}
	service, err := application.NewService(application.Dependencies{Commands: store, Queries: store, TenantAccess: deps.TenantAccess, Targets: deploymentadapter.New(deps.Deployments), Cipher: deps.Cipher, ScopeID: deps.Options.ScopeID, NewID: generateID})
	if err != nil {
		return nil, err
	}
	queries, err := application.NewQueryService(store, deps.TenantAccess)
	if err != nil {
		return nil, err
	}
	runtime, err := application.NewRuntimeService(store, deps.Cipher, deps.Options.ScopeID, deps.Options.SourceEpoch)
	if err != nil {
		return nil, err
	}
	preflightStore, err := postgresadapter.NewPreflightStore(deps.DB, deps.TransactionAuthorizer, deps.Options)
	if err != nil {
		return nil, err
	}
	preflights, err := application.NewPreflightService(application.PreflightDependencies{Store: preflightStore, Access: deps.TenantAccess, Accounts: store, Cipher: deps.Cipher, ScopeID: deps.Options.ScopeID, SourceEpoch: deps.Options.SourceEpoch, NewID: generateID})
	if err != nil {
		return nil, err
	}
	internal, err := internalhttp.NewHandler(runtime, deps.Workloads, preflights)
	if err != nil {
		return nil, err
	}
	routes := deps.Routes.Group("", deps.Authenticate)
	httpadapter.NewHandler(service, queries).Register(routes)
	// Preflight responses are non-cacheable even when Session authentication
	// aborts before the handler. Keep this policy off the normal Channel group.
	preflightRoutes := deps.Routes.Group("", func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		c.Next()
	}, deps.Authenticate)
	httpadapter.NewPreflightHandler(preflights, queries).Register(preflightRoutes)
	return &Module{Service: service, Queries: queries, Runtime: runtime, Preflights: preflights, InternalHandler: internal, store: store}, nil
}

// Initialize persists or verifies the configured immutable catalog source epoch.
func (m *Module) Initialize(ctx context.Context) error { return m.store.EnsureCatalog(ctx) }
func generateID(prefix string) (string, error) {
	value := make([]byte, 18)
	if _, err := rand.Read(value); err != nil {
		return "", application.ErrDependencyUnavailable
	}
	return prefix + "_" + base64.RawURLEncoding.EncodeToString(value), nil
}

// NewRouteRelay binds only the Channel-owned committed Outbox source.
func (m *Module) NewRouteRelay(publisher application.RoutePublisher) (*application.RouteRelay, error) {
	return application.NewRouteRelay(m.store, publisher)
}
