package admin

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/modelregistry"
)

func (s *Service) WithModelConnections(store *modelregistry.Store) *Service {
	s.models = store
	return s
}

func (h *Handler) handleModelConnections(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/admin/model-connections/list" {
		var in struct {
			TenantID string `json:"tenant_id"`
			After    string `json:"after"`
		}
		if !decodeAdmin(w, r, &in) || !h.require(w, r, in.TenantID, PermissionRead) {
			return
		}
		if len(in.After) > 128 {
			h.writeResult(w, 0, nil, invalidf("invalid cursor"))
			return
		}
		items, next, err := h.service.models.List(r.Context(), in.TenantID, in.After)
		h.writeResult(w, 200, map[string]any{"items": items, "next": next, "enabled": h.service.models != nil, "allowed_origins": h.service.models.AllowedOrigins()}, err)
		return
	}
	// Credential provisioning belongs to the deployment administrator, not a
	// tenant editing an Agent. Cookies still require same-origin + CSRF above.
	p, _ := r.Context().Value(principalContextKey{}).(Principal)
	if p.Role != RoleSuperAdmin {
		adminJSON(w, 403, map[string]string{"error": "只有平台管理员可新增模型凭据"})
		return
	}
	var in struct {
		TenantID string `json:"tenant_id"`
		ID       string `json:"connection_id"`
		Name     string `json:"name"`
		Model    string `json:"model_name"`
		BaseURL  string `json:"base_url"`
		APIKey   string `json:"api_key"`
	}
	if !decodeAdmin(w, r, &in) || !h.require(w, r, in.TenantID, PermissionWrite) {
		return
	}
	if !identifierPattern.MatchString(in.TenantID) || !identifierPattern.MatchString(in.ID) {
		h.writeResult(w, 0, nil, invalidf("invalid connection identity"))
		return
	}
	if _, err := h.service.repository.GetTenant(r.Context(), in.TenantID); err != nil {
		h.writeResult(w, 0, nil, err)
		return
	}
	var value modelregistry.Connection
	err := h.service.consoleStore.Transaction(r.Context(), func(ctx context.Context) error {
		var err error
		value, err = h.service.models.Create(ctx, modelregistry.Connection{TenantID: in.TenantID, ID: in.ID, Name: strings.TrimSpace(in.Name), Model: strings.TrimSpace(in.Model), BaseURL: strings.TrimRight(strings.TrimSpace(in.BaseURL), "/"), CreatedBy: p.Name}, strings.TrimSpace(in.APIKey))
		if err != nil {
			return err
		}
		return h.service.record(ctx, in.TenantID, "admin_model_connection_created", map[string]any{"connection_id": value.ID})
	})
	in.APIKey = ""
	if errors.Is(err, modelregistry.ErrInvalid) || errors.Is(err, modelregistry.ErrEndpoint) {
		err = invalidf("%s", err.Error())
	}
	if errors.Is(err, modelregistry.ErrUnavailable) {
		adminJSON(w, 503, map[string]string{"error": "模型连接存储未启用或不可用；请检查 schema 28、角色权限及部署加密密钥"})
		return
	}
	h.writeResult(w, http.StatusCreated, value, err)
}
