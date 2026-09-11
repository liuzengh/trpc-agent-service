// Package tenant composes tenant membership and authorization capabilities.
package tenant

import (
	"errors"
	"time"

	"github.com/gin-gonic/gin"

	httpadapter "github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/adapter/inbound/http"
	postgresadapter "github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/application"
)

// Dependencies lists process-owned dependencies required by the tenant module.
type Dependencies struct {
	DB           postgresadapter.DB
	Routes       gin.IRouter
	Authenticate gin.HandlerFunc
	Accounts     application.AccountLookup
}

// Module is the assembled tenant module.
type Module struct {
	Service *application.Service
}

// NewModule assembles the tenant module without starting process resources.
func NewModule(deps Dependencies) (*Module, error) {
	if deps.DB == nil || deps.Routes == nil || deps.Authenticate == nil || deps.Accounts == nil {
		return nil, errors.New("tenant: database, routes, authentication, and accounts are required")
	}
	store := postgresadapter.NewStore(deps.DB)
	service := application.NewService(application.Dependencies{
		Store:      store,
		Accounts:   deps.Accounts,
		Candidates: postgresadapter.NewMemberCandidateReader(deps.DB),
		NewTenantID: func() (string, error) {
			return generateID("tnt")
		},
		NewMembershipID: func() (string, error) {
			return generateID("mem")
		},
		Now: time.Now,
	})
	protected := deps.Routes.Group("", deps.Authenticate)
	httpadapter.NewHandler(service).Register(protected)
	return &Module{Service: service}, nil
}
