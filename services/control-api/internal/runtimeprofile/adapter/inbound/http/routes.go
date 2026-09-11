// Package httpadapter exposes Runtime Profile authoring and immutable
// publication through authenticated Tenant-scoped HTTP routes.
package httpadapter

import (
	"context"

	"github.com/gin-gonic/gin"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

// RuntimeProfileService is the application surface consumed by this adapter.
type RuntimeProfileService interface {
	CreateRuntimeProfile(context.Context, application.CreateRuntimeProfileCommand) (application.CreateRuntimeProfileResult, error)
	UpdateRuntimeProfile(context.Context, application.UpdateRuntimeProfileCommand) (domain.RuntimeProfile, error)
	GetRuntimeProfile(context.Context, string, string, string) (domain.RuntimeProfile, error)
	ListRuntimeProfiles(context.Context, string, string, application.Page) (application.RuntimeProfilePage, error)
	GetCredentialDraft(context.Context, string, string, string) (application.ProfileRead, error)
	SaveCredentialDraft(context.Context, application.SaveCredentialDraftCommand) (application.DraftWriteResult, error)
	UpdateUsedProfileCredential(context.Context, application.UpdateUsedCredentialCommand) (application.CredentialUpdateResult, error)
	ValidateProfileDraft(context.Context, application.ValidateProfileDraftCommand) (domain.ValidationReport, error)
	PublishProfileRevision(context.Context, application.PublishProfileRevisionCommand) (application.PublishProfileRevisionResult, error)
	GetCredentialRevision(context.Context, string, string, string, int64) (application.ProfileRead, error)
	ListProfileRevisions(context.Context, string, string, string, application.Page) (application.ProfileRevisionSummaryPage, error)
}

type Handler struct {
	service RuntimeProfileService
}

func NewHandler(service RuntimeProfileService) *Handler {
	return &Handler{service: service}
}

// Register adds the complete Runtime Profile V1 HTTP surface.
func (h *Handler) Register(routes gin.IRoutes) {
	routes.POST("/v1/tenants/:tenant_id/runtime-profiles", h.createRuntimeProfile)
	routes.GET("/v1/tenants/:tenant_id/runtime-profiles", h.listRuntimeProfiles)
	routes.GET("/v1/tenants/:tenant_id/runtime-profiles/:profile_id", h.getRuntimeProfile)
	routes.PATCH("/v1/tenants/:tenant_id/runtime-profiles/:profile_id", h.updateRuntimeProfile)
	routes.GET("/v1/tenants/:tenant_id/runtime-profiles/:profile_id/draft", h.getProfileDraft)
	routes.PUT("/v1/tenants/:tenant_id/runtime-profiles/:profile_id/draft", h.saveProfileDraft)
	routes.POST("/v1/tenants/:tenant_id/runtime-profiles/:profile_id/credentials/update", h.updateUsedCredential)
	routes.POST("/v1/tenants/:tenant_id/runtime-profiles/:profile_id/draft/validate", h.validateProfileDraft)
	routes.POST("/v1/tenants/:tenant_id/runtime-profiles/:profile_id/revisions", h.publishProfileRevision)
	routes.GET("/v1/tenants/:tenant_id/runtime-profiles/:profile_id/revisions", h.listProfileRevisions)
	routes.GET("/v1/tenants/:tenant_id/runtime-profiles/:profile_id/revisions/:revision_number", h.getProfileRevision)
}
