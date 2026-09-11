package httpadapter

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/application"
)

func (h *Handler) getDraft(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	draft, err := h.service.GetDraft(
		c.Request.Context(), c.Param("tenant_id"), c.Param("agent_id"), identity.UserID,
	)
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	c.JSON(http.StatusOK, draftView(draft))
}

func (h *Handler) saveDraft(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	var request saveDraftRequest
	if err := decodeStrictJSON(c, &request, maxAgentSpecRequestBytes); err != nil ||
		request.ExpectedRevision <= 0 || len(request.Spec) == 0 {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "expected_revision and spec are required")
		return
	}
	draft, report, err := h.service.SaveDraft(c.Request.Context(), application.SaveDraftCommand{
		TenantID: c.Param("tenant_id"), AgentID: c.Param("agent_id"),
		ActorUserID: identity.UserID, ExpectedRevision: request.ExpectedRevision,
		Spec: request.Spec,
	})
	if errors.Is(err, application.ErrAgentSpecInvalid) {
		writeValidationError(c, report)
		return
	}
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	c.JSON(http.StatusOK, draftView(draft))
}

func (h *Handler) validateDraft(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	var request revisionRequest
	if err := decodeStrictJSON(c, &request, 4*1024); err != nil || request.ExpectedRevision <= 0 {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "expected_revision is required")
		return
	}
	report, err := h.service.ValidateDraft(c.Request.Context(), application.ValidateDraftCommand{
		TenantID: c.Param("tenant_id"), AgentID: c.Param("agent_id"),
		ActorUserID: identity.UserID, ExpectedRevision: request.ExpectedRevision,
	})
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	c.JSON(http.StatusOK, report)
}
