package httpadapter

import (
	"context"

	"github.com/gin-gonic/gin"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
)

// DeploymentService is the complete application surface consumed by the
// tenant-scoped Deployment HTTP adapter.
type DeploymentService interface {
	CreateDeployment(context.Context, application.CreateDeploymentCommand) (application.CreateDeploymentResult, error)
	UpdateDeploymentMetadata(context.Context, application.UpdateDeploymentCommand) (domain.Deployment, error)
	ValidateDeploymentRevision(context.Context, application.ValidateDeploymentCommand) (domain.ValidationReport, error)
	PublishDeploymentRevision(context.Context, application.PublishDeploymentCommand) (application.PublishDeploymentResult, error)
	MigrateAndPublish(context.Context, application.MigrateAndPublishCommand) (application.MigrateAndPublishResult, error)
	GetDeployment(context.Context, string, string, string) (domain.Deployment, error)
	ListDeployments(context.Context, string, string, application.Page) (application.DeploymentPage, error)
	GetDeploymentRevision(context.Context, string, string, string, int64) (domain.PublishedRevision, error)
	ListDeploymentRevisions(context.Context, string, string, string, application.Page) (application.RevisionSummaryPage, error)
}

type Handler struct {
	service DeploymentService
}

func NewHandler(service DeploymentService) *Handler {
	return &Handler{service: service}
}

// Register adds the Deployment V1 management and explicit migration surface.
func (h *Handler) Register(routes gin.IRoutes) {
	routes.POST("/v1/tenants/:tenant_id/deployments/:deployment_id/revisions/:revision_number/knowledge/:resource/import", h.importKnowledge)
	routes.PUT("/v1/tenants/:tenant_id/deployments/:deployment_id/revisions/:revision_number/artifacts/:filename", h.artifact)
	routes.GET("/v1/tenants/:tenant_id/deployments/:deployment_id/revisions/:revision_number/artifacts/:filename", h.artifact)
	routes.POST("/v1/tenants/:tenant_id/deployments", h.createDeployment)
	routes.GET("/v1/tenants/:tenant_id/deployments", h.listDeployments)
	routes.GET("/v1/tenants/:tenant_id/deployments/:deployment_id", h.getDeployment)
	routes.PATCH("/v1/tenants/:tenant_id/deployments/:deployment_id", h.updateDeployment)
	routes.POST("/v1/tenants/:tenant_id/deployments/:deployment_id/validate", h.validateDeploymentRevision)
	routes.POST("/v1/tenants/:tenant_id/deployments/:deployment_id/revisions", h.publishDeploymentRevision)
	routes.POST("/v1/tenants/:tenant_id/deployments/:deployment_id/backend-migrations", h.migrateAndPublish)
	routes.GET("/v1/tenants/:tenant_id/deployments/:deployment_id/revisions", h.listDeploymentRevisions)
	routes.GET("/v1/tenants/:tenant_id/deployments/:deployment_id/revisions/:revision_number", h.getDeploymentRevision)
}
