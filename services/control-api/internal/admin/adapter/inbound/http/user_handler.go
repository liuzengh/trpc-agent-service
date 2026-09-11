package httpadapter

import (
	stdhttp "net/http"
	"time"

	"github.com/gin-gonic/gin"

	identityapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
	identitydomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/domain"
)

type createUserRequest struct {
	Username          string `json:"username"`
	DisplayName       string `json:"display_name"`
	TemporaryPassword string `json:"temporary_password"`
}

func (h *Handler) createUser(c *gin.Context) {
	var request createUserRequest
	if err := decodeStrictJSON(c, &request); err != nil {
		writeError(c, stdhttp.StatusBadRequest, "INVALID_REQUEST", "request body is invalid")
		return
	}
	account, err := h.service.CreateUser(c.Request.Context(), identityapp.CreateManagedAccountCommand{
		Username: request.Username, DisplayName: request.DisplayName,
		TemporaryPassword: request.TemporaryPassword,
	})
	if err != nil {
		handleError(c, err)
		return
	}
	c.JSON(stdhttp.StatusCreated, userView(account))
}

func (h *Handler) listUsers(c *gin.Context) {
	page, ok := parsePage(c)
	if !ok {
		return
	}
	result, err := h.service.ListUsers(c.Request.Context(), identityapp.Page(page))
	if err != nil {
		writeInternal(c)
		return
	}
	users := make([]userResponse, 0, len(result.Accounts))
	for _, account := range result.Accounts {
		users = append(users, userView(account))
	}
	c.JSON(stdhttp.StatusOK, gin.H{
		"users": users, "offset": page.Offset, "limit": page.Limit, "total": result.Total,
	})
}

type userResponse struct {
	ID          string                       `json:"id"`
	Username    string                       `json:"username"`
	DisplayName string                       `json:"display_name"`
	Status      identitydomain.AccountStatus `json:"status"`
	CreatedAt   time.Time                    `json:"created_at"`
	UpdatedAt   time.Time                    `json:"updated_at"`
}

func userView(account identitydomain.UserAccount) userResponse {
	return userResponse{
		ID: account.ID, Username: account.Username, DisplayName: account.DisplayName,
		Status: account.Status, CreatedAt: account.CreatedAt, UpdatedAt: account.UpdatedAt,
	}
}
