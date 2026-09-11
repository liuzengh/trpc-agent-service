package httpadapter

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
)

func (h *Handler) createRuntimeProfile(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	var request createRuntimeProfileRequest
	if err := decodeStrictJSON(c, &request, 32*1024); err != nil || strings.TrimSpace(request.Name) == "" {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "name and description must be valid")
		return
	}
	result, err := h.service.CreateRuntimeProfile(c.Request.Context(), application.CreateRuntimeProfileCommand{
		TenantID: c.Param("tenant_id"), ActorUserID: identity.UserID,
		Name: request.Name, Description: request.Description,
	})
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	c.JSON(http.StatusCreated, createRuntimeProfileResponse{
		Profile: runtimeProfileView(result.Profile),
		Draft:   profileDraftView(result.Draft),
	})
}

func (h *Handler) getRuntimeProfile(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	profile, err := h.service.GetRuntimeProfile(
		c.Request.Context(), c.Param("tenant_id"), c.Param("profile_id"), identity.UserID,
	)
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	c.JSON(http.StatusOK, runtimeProfileView(profile))
}

func (h *Handler) listRuntimeProfiles(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	page, err := parsePage(c)
	if err != nil {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "pagination is invalid")
		return
	}
	result, err := h.service.ListRuntimeProfiles(
		c.Request.Context(), c.Param("tenant_id"), identity.UserID, page,
	)
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	items := make([]runtimeProfileResponse, 0, len(result.Profiles))
	for _, profile := range result.Profiles {
		items = append(items, runtimeProfileView(profile))
	}
	c.JSON(http.StatusOK, runtimeProfilePageResponse{
		RuntimeProfiles: items, Total: result.Total,
		Offset: page.Offset, Limit: page.Limit,
	})
}

func (h *Handler) updateRuntimeProfile(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	var request updateRuntimeProfileRequest
	if err := decodeStrictJSON(c, &request, 32*1024); err != nil ||
		(request.Name == nil && request.Description == nil) {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "name or description is required")
		return
	}
	profile, err := h.service.UpdateRuntimeProfile(c.Request.Context(), application.UpdateRuntimeProfileCommand{
		TenantID: c.Param("tenant_id"), ProfileID: c.Param("profile_id"),
		ActorUserID: identity.UserID, Name: request.Name, Description: request.Description,
	})
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	c.JSON(http.StatusOK, runtimeProfileView(profile))
}
