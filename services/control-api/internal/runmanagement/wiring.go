// Package runmanagement composes tenant-authorized runtime and audit reads.
package runmanagement

import (
	"errors"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	httpadapter "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runmanagement/adapter/inbound/http"
	auditpostgres "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runmanagement/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runmanagement/application"
)

type Dependencies struct {
	DB           *pgxpool.Pool
	Routes       gin.IRouter
	Authenticate gin.HandlerFunc
	TenantAccess application.TenantAccess
	Runtime      application.RuntimeReader
}

type Module struct{ Service *application.Service }

func NewModule(deps Dependencies) (*Module, error) {
	if deps.DB == nil || deps.Routes == nil || deps.Authenticate == nil || deps.TenantAccess == nil || deps.Runtime == nil {
		return nil, errors.New("run management: incomplete dependencies")
	}
	service, err := application.New(deps.TenantAccess, deps.Runtime, auditpostgres.NewAuditReader(deps.DB))
	if err != nil {
		return nil, err
	}
	httpadapter.New(service).Register(deps.Routes.Group("", deps.Authenticate))
	return &Module{Service: service}, nil
}
