// Package httpadapter exposes Agent authoring and immutable publication through
// authenticated tenant-scoped HTTP routes.
package httpadapter

import (
	"context"

	"github.com/gin-gonic/gin"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
)

// AgentService is the application surface consumed by the HTTP adapter.
type AgentService interface {
	CreateAgent(context.Context, application.CreateAgentCommand) (application.CreateAgentResult, error)
	UpdateAgent(context.Context, application.UpdateAgentCommand) (domain.Agent, error)
	GetAgent(context.Context, string, string, string) (domain.Agent, error)
	ListAgents(context.Context, string, string, application.Page) (application.AgentPage, error)
	GetDraft(context.Context, string, string, string) (domain.AgentDraft, error)
	SaveDraft(context.Context, application.SaveDraftCommand) (domain.AgentDraft, domain.ValidationReport, error)
	ValidateDraft(context.Context, application.ValidateDraftCommand) (domain.ValidationReport, error)
	PublishAgentVersion(context.Context, application.PublishVersionCommand) (application.PublishVersionResult, error)
	GetAgentVersion(context.Context, string, string, string, int64) (domain.AgentVersion, error)
	ListAgentVersions(context.Context, string, string, string, application.Page) (application.VersionPage, error)
}

type Handler struct {
	service AgentService
}

func NewHandler(service AgentService) *Handler {
	return &Handler{service: service}
}

// Register adds the complete Agent V1 HTTP surface.
func (h *Handler) Register(routes gin.IRoutes) {
	routes.POST("/v1/tenants/:tenant_id/agents", h.createAgent)
	routes.GET("/v1/tenants/:tenant_id/agents", h.listAgents)
	routes.GET("/v1/tenants/:tenant_id/agents/:agent_id", h.getAgent)
	routes.PATCH("/v1/tenants/:tenant_id/agents/:agent_id", h.updateAgent)
	routes.GET("/v1/tenants/:tenant_id/agents/:agent_id/draft", h.getDraft)
	routes.PUT("/v1/tenants/:tenant_id/agents/:agent_id/draft", h.saveDraft)
	routes.POST("/v1/tenants/:tenant_id/agents/:agent_id/draft/validate", h.validateDraft)
	routes.POST("/v1/tenants/:tenant_id/agents/:agent_id/versions", h.publishVersion)
	routes.GET("/v1/tenants/:tenant_id/agents/:agent_id/versions", h.listVersions)
	routes.GET("/v1/tenants/:tenant_id/agents/:agent_id/versions/:version_number", h.getVersion)
}
