package usagepolicy

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	channelapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	httpadapter "github.com/liuzengh/trpc-agent-service/services/control-api/internal/usagepolicy/adapter/inbound/http"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/usagepolicy/adapter/inbound/internalhttp"
	postgresadapter "github.com/liuzengh/trpc-agent-service/services/control-api/internal/usagepolicy/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/usagepolicy/application"
)

type Dependencies struct {
	DB           *pgxpool.Pool
	Routes       gin.IRouter
	Authenticate gin.HandlerFunc
	Access       application.Access
	Workloads    []channelapp.WorkloadPrincipal
}
type Module struct {
	Service         *application.Service
	InternalHandler http.Handler
}

func NewModule(d Dependencies) (*Module, error) {
	if d.DB == nil || d.Routes == nil || d.Authenticate == nil || d.Access == nil {
		return nil, errors.New("usage policy dependencies unavailable")
	}
	s, e := application.New(d.Access, postgresadapter.New(d.DB))
	if e != nil {
		return nil, e
	}
	httpadapter.New(s).Register(d.Routes.Group("", d.Authenticate))
	m := &Module{Service: s}
	if len(d.Workloads) > 0 {
		m.InternalHandler, e = internalhttp.New(s, d.Workloads)
		if e != nil {
			return nil, e
		}
	}
	return m, nil
}
