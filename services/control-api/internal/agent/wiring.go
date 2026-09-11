// Package agent composes Agent authoring, validation, and versioning capabilities.
package agent

import (
	"errors"
	"time"

	"github.com/gin-gonic/gin"

	httpadapter "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/adapter/inbound/http"
	postgresadapter "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/application"
)

// Dependencies lists process-owned dependencies required by the Agent module.
type Dependencies struct {
	DB           postgresadapter.DB
	Routes       gin.IRouter
	Authenticate gin.HandlerFunc
	TenantAccess application.TenantAccess
}

// Module is the assembled Agent module.
type Module struct {
	Service *application.Service
}

// NewModule assembles the Agent module without starting process resources.
func NewModule(deps Dependencies) (*Module, error) {
	if deps.DB == nil || deps.Routes == nil || deps.Authenticate == nil || deps.TenantAccess == nil {
		return nil, errors.New("agent: database, routes, authentication, and tenant access are required")
	}
	store := postgresadapter.NewStore(deps.DB)
	service := application.NewService(application.Dependencies{
		Store: store, TenantAccess: deps.TenantAccess,
		NewAgentID:   func() (string, error) { return generateID("agt") },
		NewVersionID: func() (string, error) { return generateID("agv") },
		Now:          time.Now,
	})
	protected := deps.Routes.Group("", deps.Authenticate)
	httpadapter.NewHandler(service).Register(protected)
	return &Module{Service: service}, nil
}
