package httpadapter

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
)

func (h *Handler) getProfileDraft(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	draft, err := h.service.GetCredentialDraft(
		c.Request.Context(), c.Param("tenant_id"), c.Param("profile_id"), identity.UserID,
	)
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	c.JSON(http.StatusOK, draft)
}

func (h *Handler) saveProfileDraft(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	data, err := readCredentialBody(c)
	if err != nil {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "credential draft request is invalid")
		return
	}
	defer clear(data)
	input, err := application.DecodeProfileWrite(data)
	if err != nil || c.GetHeader("Idempotency-Key") == "" {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "credential draft request is invalid")
		return
	}
	result, err := h.service.SaveCredentialDraft(c.Request.Context(), application.SaveCredentialDraftCommand{
		TenantID: c.Param("tenant_id"), ProfileID: c.Param("profile_id"), ActorUserID: identity.UserID, IdempotencyKey: c.GetHeader("Idempotency-Key"), Write: input,
	})
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

func (h *Handler) validateProfileDraft(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	var request profileDraftRevisionRequest
	if err := decodeStrictJSON(c, &request, 4*1024); err != nil || request.ExpectedRevision <= 0 {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "expected_revision is required")
		return
	}
	report, err := h.service.ValidateProfileDraft(c.Request.Context(), application.ValidateProfileDraftCommand{
		TenantID: c.Param("tenant_id"), ProfileID: c.Param("profile_id"),
		ActorUserID: identity.UserID, ExpectedRevision: request.ExpectedRevision,
	})
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	c.JSON(http.StatusOK, validationReportView(report))
}

func (h *Handler) updateUsedCredential(c *gin.Context) {
	identity, ok := usableIdentity(c)
	if !ok {
		return
	}
	data, err := readCredentialBody(c)
	if err != nil {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "credential update request is invalid")
		return
	}
	defer clear(data)
	input, err := application.DecodeCredentialUpdate(data)
	if err != nil || c.GetHeader("Idempotency-Key") == "" {
		writeError(c, http.StatusBadRequest, "INVALID_REQUEST", "credential update request is invalid")
		return
	}
	result, err := h.service.UpdateUsedProfileCredential(c.Request.Context(), application.UpdateUsedCredentialCommand{TenantID: c.Param("tenant_id"), ProfileID: c.Param("profile_id"), ActorUserID: identity.UserID, IdempotencyKey: c.GetHeader("Idempotency-Key"), Update: input})
	if err != nil {
		handleApplicationError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}
