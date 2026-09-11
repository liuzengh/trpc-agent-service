// Package admin composes platform administration capabilities.
package admin

import (
	"errors"
	"time"

	"github.com/gin-gonic/gin"

	adminhttp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/admin/adapter/inbound/http"
	adminpostgres "github.com/liuzengh/trpc-agent-service/services/control-api/internal/admin/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/admin/application"
)

type Dependencies struct {
	DB           adminpostgres.DB
	Routes       gin.IRouter
	Authenticate gin.HandlerFunc
	Accounts     application.AccountService
	Tenants      application.TenantService
}

type Module struct {
	Service *application.Service
}

func NewModule(deps Dependencies) (*Module, error) {
	if deps.DB == nil || deps.Routes == nil || deps.Authenticate == nil ||
		deps.Accounts == nil || deps.Tenants == nil {
		return nil, errors.New("admin: database, routes, authentication, accounts, and tenants are required")
	}
	store := adminpostgres.NewStore(deps.DB)
	service := application.NewService(application.Dependencies{
		Operators: store, Accounts: deps.Accounts, Tenants: deps.Tenants, Now: time.Now,
	})
	handler := adminhttp.NewHandler(service)
	protected := deps.Routes.Group("", deps.Authenticate, handler.AuthorizationMiddleware())
	handler.Register(protected)
	return &Module{Service: service}, nil
}

// NewStartupService composes the same Admin bootstrap use case against
// transaction-bound stores. Tenant capabilities are not part of startup.
func NewStartupService(
	db adminpostgres.DB,
	accounts application.AccountService,
) *application.Service {
	return application.NewService(application.Dependencies{
		Operators: adminpostgres.NewStore(db), Accounts: accounts, Now: time.Now,
	})
}
