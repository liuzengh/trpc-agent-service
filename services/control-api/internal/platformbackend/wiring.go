package platformbackend

import (
	"errors"
	"github.com/gin-gonic/gin"
	httpadapter "github.com/liuzengh/trpc-agent-service/services/control-api/internal/platformbackend/adapter/inbound/http"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/platformbackend/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/platformbackend/domain"
)

type Dependencies struct {
	Routes       gin.IRouter
	Authenticate gin.HandlerFunc
	TenantAccess application.TenantAccess
	Catalog      *domain.Catalog
}
type Module struct{ Service *application.Service }

func NewModule(d Dependencies) (*Module, error) {
	if d.Routes == nil || d.Authenticate == nil {
		return nil, errors.New("platform backend routes and authentication are required")
	}
	s, err := application.NewService(d.Catalog, d.TenantAccess)
	if err != nil {
		return nil, err
	}
	httpadapter.NewHandler(s).Register(d.Routes.Group("", d.Authenticate))
	return &Module{Service: s}, nil
}
