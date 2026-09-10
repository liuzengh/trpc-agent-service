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
	if r.URL.Path == "/admin/model-connections/get" {
		var in struct {
			TenantID string `json:"tenant_id"`
			ID       string `json:"connection_id"`
			After    string `json:"after"`
		}
		if !decodeAdmin(w, r, &in) || !h.require(w, r, in.TenantID, PermissionRead) {
			return
		}
		if !identifierPattern.MatchString(in.ID) || len(in.After) > 128 {
			h.writeResult(w, 0, nil, invalidf("invalid model connection identity"))
			return
		}
		c, err := h.service.models.Get(r.Context(), in.TenantID, in.ID)
		if err != nil {
			h.writeResult(w, 0, nil, err)
			return
		}
		usage, next, err := h.service.models.Usage(r.Context(), in.TenantID, in.ID, in.After)
		h.writeResult(w, 200, map[string]any{"connection": c, "usage": usage, "next": next}, err)
		return
	}
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
		adminJSON(w, 403, map[string]string{"error": "只有平台管理员可管理模型连接与凭据"})
		return
	}
	if r.URL.Path == "/admin/model-connections/update" || r.URL.Path == "/admin/model-connections/rotate-key" {
		h.editModelConnection(w, r, p)
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
		adminJSON(w, 503, map[string]string{"error": "模型连接存储未启用或不可用；请检查 schema 29、角色权限及部署加密密钥"})
		return
	}
	h.writeResult(w, http.StatusCreated, value, err)
}

func (h *Handler) editModelConnection(w http.ResponseWriter, r *http.Request, p Principal) {
	var in struct {
		TenantID string `json:"tenant_id"`
		ID       string `json:"connection_id"`
		Version  int64  `json:"expected_version"`
		Name     string `json:"name"`
		Model    string `json:"model_name"`
		BaseURL  string `json:"base_url"`
		APIKey   string `json:"api_key"`
		NewID    string `json:"new_connection_id"`
	}
	if !decodeAdmin(w, r, &in) || !h.require(w, r, in.TenantID, PermissionWrite) {
		return
	}
	if !identifierPattern.MatchString(in.TenantID) || !identifierPattern.MatchString(in.ID) || in.Version < 1 || (in.NewID != "" && !identifierPattern.MatchString(in.NewID)) {
		h.writeResult(w, 0, nil, invalidf("连接标识或版本无效，请刷新后重新编辑"))
		return
	}
	rotate := r.URL.Path == "/admin/model-connections/rotate-key"
	var result modelregistry.EditResult
	err := h.service.consoleStore.Transaction(r.Context(), func(ctx context.Context) error {
		var err error
		result, err = h.service.models.Edit(ctx, modelregistry.Edit{TenantID: in.TenantID, ID: in.ID, ExpectedVersion: in.Version, Name: in.Name, Model: in.Model, BaseURL: in.BaseURL, APIKey: in.APIKey, NewID: in.NewID, Actor: p.Name}, rotate)
		if err != nil {
			return err
		}
		decision := "admin_model_connection_updated"
		if result.NewConfig {
			decision = "admin_model_connection_version_created"
		} else if result.KeyChanged {
			decision = "admin_model_connection_key_rotated"
		}
		return h.service.record(ctx, in.TenantID, decision, map[string]any{"connection_id": result.Connection.ID, "previous_connection_id": result.PreviousID, "config_version": result.Connection.ConfigVersion, "credential_version": result.Connection.CredentialVersion, "version": result.Connection.Version, "key_changed": result.KeyChanged})
	})
	in.APIKey = ""
	if errors.Is(err, modelregistry.ErrInvalid) || errors.Is(err, modelregistry.ErrEndpoint) || errors.Is(err, modelregistry.ErrAddressKey) {
		err = invalidf("%s", err.Error())
	}
	if errors.Is(err, modelregistry.ErrSuperseded) {
		adminJSON(w, 409, map[string]string{"error": err.Error()})
		return
	}
	if errors.Is(err, modelregistry.ErrUnavailable) {
		adminJSON(w, 503, map[string]string{"error": "模型连接更新不可用，请检查迁移、数据库权限和加密主密钥"})
		return
	}
	h.writeResult(w, 200, result, err)
}
