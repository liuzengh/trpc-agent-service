package httpadapter

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
)

func (h *Handler) publishProfileRevision(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	var request profileDraftRevisionRequest
	if err := decodeStrictJSON(c, &request, 4*1024); err != nil || request.ExpectedRevision <= 0 {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "expected_revision is required")
		return
	}
	result, err := h.service.PublishProfileRevision(c.Request.Context(), application.PublishProfileRevisionCommand{
		TenantID: c.Param("tenant_id"), ProfileID: c.Param("profile_id"),
		ActorUserID: identity.UserID, ExpectedRevision: request.ExpectedRevision,
	})
	if errors.Is(err, application.ErrRuntimeProfileSpecInvalid) {
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
	view, err := application.PublishedProfileRead(result.Revision)
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	c.JSON(status, publishProfileRevisionResponse{Revision: view})
}

func (h *Handler) getProfileRevision(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	revisionNumber, err := parseRevisionNumber(c)
	if err != nil {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "revision_number is invalid")
		return
	}
	revision, err := h.service.GetCredentialRevision(
		c.Request.Context(), c.Param("tenant_id"), c.Param("profile_id"), identity.UserID, revisionNumber,
	)
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	c.JSON(http.StatusOK, revision)
}

func (h *Handler) listProfileRevisions(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	page, err := parsePage(c)
	if err != nil {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "pagination is invalid")
		return
	}
	result, err := h.service.ListProfileRevisions(
		c.Request.Context(), c.Param("tenant_id"), c.Param("profile_id"), identity.UserID, page,
	)
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	items := make([]runtimeProfileRevisionSummaryResponse, 0, len(result.Revisions))
	for _, revision := range result.Revisions {
		items = append(items, profileRevisionSummaryView(revision))
	}
	c.JSON(http.StatusOK, profileRevisionPageResponse{
		Revisions: items, Total: result.Total,
		Offset: page.Offset, Limit: page.Limit,
	})
}
