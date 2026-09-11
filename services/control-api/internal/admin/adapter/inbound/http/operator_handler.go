package httpadapter

import (
	stdhttp "net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	identityapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
)

func (h *Handler) capabilities(c *gin.Context) {
	c.JSON(stdhttp.StatusOK, gin.H{"capabilities": []string{
		"users:manage", "operators:manage", "tenants:manage",
	}})
}

type operatorResponse struct {
	UserID      string    `json:"user_id"`
	Username    string    `json:"username"`
	DisplayName string    `json:"display_name"`
	GrantedBy   string    `json:"granted_by,omitempty"`
	GrantedAt   time.Time `json:"granted_at"`
}

func (h *Handler) listOperators(c *gin.Context) {
	operators, err := h.service.ListOperators(c.Request.Context())
	if err != nil {
		writeInternal(c)
		return
	}
	items := make([]operatorResponse, 0, len(operators))
	for _, operator := range operators {
		items = append(items, operatorResponse{
			UserID: operator.Grant.UserID, Username: operator.Username,
			DisplayName: operator.DisplayName, GrantedBy: operator.Grant.GrantedByUserID,
			GrantedAt: operator.Grant.GrantedAt,
		})
	}
	c.JSON(stdhttp.StatusOK, gin.H{"operators": items})
}

type grantOperatorRequest struct {
	UserID string `json:"user_id"`
}

func (h *Handler) grantOperator(c *gin.Context) {
	identity, _ := identityapp.IdentityFromContext(c.Request.Context())
	var request grantOperatorRequest
	if err := decodeStrictJSON(c, &request); err != nil || strings.TrimSpace(request.UserID) == "" {
		writeError(c, stdhttp.StatusBadRequest, "INVALID_REQUEST", "user_id is required")
		return
	}
	grant, err := h.service.GrantOperator(
		c.Request.Context(), identity.UserID, strings.TrimSpace(request.UserID),
	)
	if err != nil {
		handleError(c, err)
		return
	}
	c.JSON(stdhttp.StatusCreated, operatorResponse{
		UserID: grant.UserID, GrantedBy: grant.GrantedByUserID, GrantedAt: grant.GrantedAt,
	})
}

func (h *Handler) revokeOperator(c *gin.Context) {
	identity, _ := identityapp.IdentityFromContext(c.Request.Context())
	if err := h.service.RevokeOperator(
		c.Request.Context(), identity.UserID, c.Param("user_id"),
	); err != nil {
		handleError(c, err)
		return
	}
	c.Status(stdhttp.StatusNoContent)
}
