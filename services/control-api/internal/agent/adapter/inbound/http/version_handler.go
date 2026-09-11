package httpadapter

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/application"
)

func (h *Handler) publishVersion(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	var request revisionRequest
	if err := decodeStrictJSON(c, &request, 4*1024); err != nil || request.ExpectedRevision <= 0 {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "expected_revision is required")
		return
	}
	result, err := h.service.PublishAgentVersion(c.Request.Context(), application.PublishVersionCommand{
		TenantID: c.Param("tenant_id"), AgentID: c.Param("agent_id"),
		ActorUserID: identity.UserID, ExpectedRevision: request.ExpectedRevision,
	})
	if errors.Is(err, application.ErrAgentSpecInvalid) {
		writeValidationError(c, result.Report)
		return
	}
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	status := http.StatusOK
	if result.Created {
		status = http.StatusCreated
	}
	c.JSON(status, gin.H{"version": versionView(result.Version), "validation": result.Report})
}

func (h *Handler) getVersion(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	versionNumber, err := parseVersionNumber(c)
	if err != nil {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "version_number is invalid")
		return
	}
	version, err := h.service.GetAgentVersion(
		c.Request.Context(), c.Param("tenant_id"), c.Param("agent_id"),
		identity.UserID, versionNumber,
	)
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	c.JSON(http.StatusOK, versionView(version))
}

func (h *Handler) listVersions(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	page, err := parsePage(c)
	if err != nil {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "pagination is invalid")
		return
	}
	result, err := h.service.ListAgentVersions(
		c.Request.Context(), c.Param("tenant_id"), c.Param("agent_id"),
		identity.UserID, page,
	)
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	items := make([]versionResponse, 0, len(result.Versions))
	for _, version := range result.Versions {
		items = append(items, versionView(version))
	}
	c.JSON(http.StatusOK, gin.H{
		"versions": items, "total": result.Total,
		"offset": page.Offset, "limit": page.Limit,
	})
}
