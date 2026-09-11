package httpadapter

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	identityapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
)

type agentResponse struct {
	ID                  string    `json:"id"`
	TenantID            string    `json:"tenant_id"`
	Name                string    `json:"name"`
	Description         string    `json:"description"`
	LatestVersionNumber *int64    `json:"latest_version_number"`
	CreatedBy           string    `json:"created_by"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

func agentView(agent domain.Agent) agentResponse {
	return agentResponse{
		ID: agent.ID, TenantID: agent.TenantID, Name: agent.Name,
		Description: agent.Description, LatestVersionNumber: agent.LatestVersionNumber,
		CreatedBy: agent.CreatedBy, CreatedAt: agent.CreatedAt, UpdatedAt: agent.UpdatedAt,
	}
}

type draftResponse struct {
	AgentID   string          `json:"agent_id"`
	TenantID  string          `json:"tenant_id"`
	Revision  int64           `json:"revision"`
	Spec      json.RawMessage `json:"spec"`
	UpdatedBy string          `json:"updated_by"`
	UpdatedAt time.Time       `json:"updated_at"`
}

func draftView(draft domain.AgentDraft) draftResponse {
	return draftResponse{
		AgentID: draft.AgentID, TenantID: draft.TenantID, Revision: draft.Revision,
		Spec: draft.Spec, UpdatedBy: draft.UpdatedBy, UpdatedAt: draft.UpdatedAt,
	}
}

type versionResponse struct {
	ID                  string          `json:"id"`
	TenantID            string          `json:"tenant_id"`
	AgentID             string          `json:"agent_id"`
	VersionNumber       int64           `json:"version_number"`
	SourceDraftRevision int64           `json:"source_draft_revision"`
	SchemaVersion       string          `json:"schema_version"`
	Spec                json.RawMessage `json:"spec"`
	SpecDigest          string          `json:"spec_digest"`
	PublishedBy         string          `json:"published_by"`
	PublishedAt         time.Time       `json:"published_at"`
}

func versionView(version domain.AgentVersion) versionResponse {
	return versionResponse{
		ID: version.ID, TenantID: version.TenantID, AgentID: version.AgentID,
		VersionNumber:       version.VersionNumber,
		SourceDraftRevision: version.SourceDraftRevision,
		SchemaVersion:       version.SchemaVersion, Spec: version.Spec,
		SpecDigest: version.SpecDigest, PublishedBy: version.PublishedBy,
		PublishedAt: version.PublishedAt,
	}
}

type errorResponse struct {
	Error      errorBody                `json:"error"`
	Validation *domain.ValidationReport `json:"validation,omitempty"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(c *gin.Context, status int, code, message string) {
	c.JSON(status, errorResponse{Error: errorBody{Code: code, Message: message}})
}

func writeValidationError(c *gin.Context, report domain.ValidationReport) {
	c.JSON(http.StatusUnprocessableEntity, errorResponse{
		Error:      errorBody{Code: "AGENT_SPEC_INVALID", Message: "AgentSpec validation failed"},
		Validation: &report,
	})
}

func usableIdentity(c *gin.Context) (identityapp.IdentityContext, bool) {
	identity, ok := identityapp.IdentityFromContext(c.Request.Context())
	if !ok {
		writeError(c, http.StatusUnauthorized, "UNAUTHENTICATED", "authentication required")
		return identityapp.IdentityContext{}, false
	}
	if identity.Restricted {
		writeError(c, http.StatusForbidden, "PASSWORD_CHANGE_REQUIRED", "password change is required")
		return identityapp.IdentityContext{}, false
	}
	return identity, true
}

func handleApplicationError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, application.ErrInvalidAgent):
		writeError(c, http.StatusBadRequest, "INVALID_AGENT", "agent metadata is invalid")
	case errors.Is(err, application.ErrAgentNotFound):
		writeError(c, http.StatusNotFound, "AGENT_NOT_FOUND", "agent was not found")
	case errors.Is(err, application.ErrAgentVersionNotFound):
		writeError(c, http.StatusNotFound, "AGENT_VERSION_NOT_FOUND", "agent version was not found")
	case errors.Is(err, application.ErrTenantForbidden):
		writeError(c, http.StatusForbidden, "TENANT_FORBIDDEN", "tenant access is forbidden")
	case errors.Is(err, application.ErrDraftRevisionConflict):
		writeError(c, http.StatusConflict, "AGENT_DRAFT_REVISION_CONFLICT", "draft revision is stale")
	default:
		writeError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "request could not be completed")
	}
}
