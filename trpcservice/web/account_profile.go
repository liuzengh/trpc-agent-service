package web

import (
	"errors"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
)

type accountProfileResponse struct {
	PlatformUserID string                 `json:"platform_user_id"`
	DisplayName    string                 `json:"display_name"`
	Email          string                 `json:"email,omitempty"`
	LoginMethods   []identity.LoginMethod `json:"login_methods"`
}

func (c *consoleAPI) accountProfile(writer http.ResponseWriter, request *http.Request) {
	user, ok := sessionUser(request)
	if !ok {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	if c.dependencies.Identities == nil {
		serverError(writer, "account profile", errors.New("identity store is not configured"))
		return
	}
	switch request.Method {
	case http.MethodGet:
		c.writeAccountProfile(writer, request, user.PlatformUserID)
	case http.MethodPut:
		var body struct {
			DisplayName string `json:"display_name"`
		}
		if err := decodeJSONBody(request, &body); err != nil {
			badRequest(writer, "账号名称无效")
			return
		}
		if err := c.dependencies.Identities.UpdatePlatformUserProfile(request.Context(), user.PlatformUserID, strings.TrimSpace(body.DisplayName)); err != nil {
			badRequest(writer, err.Error())
			return
		}
		c.writeAccountProfile(writer, request, user.PlatformUserID)
	default:
		methodNotAllowed(writer, http.MethodGet+", "+http.MethodPut)
	}
}

func (c *consoleAPI) writeAccountProfile(writer http.ResponseWriter, request *http.Request, platformUserID string) {
	current, err := c.dependencies.Identities.ResolveSessionUser(request.Context(), platformUserID)
	if err != nil {
		serverError(writer, "resolve account profile", err)
		return
	}
	methods, err := c.dependencies.Identities.ListLoginMethods(request.Context(), platformUserID)
	if err != nil {
		serverError(writer, "list account login methods", err)
		return
	}
	if methods == nil {
		methods = make([]identity.LoginMethod, 0)
	}
	writeJSON(writer, http.StatusOK, accountProfileResponse{
		PlatformUserID: current.PlatformUserID,
		DisplayName:    current.DisplayName,
		Email:          current.Email,
		LoginMethods:   methods,
	})
}
