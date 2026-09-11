package admin

import (
	"context"
	"errors"
	"net/http"

	"github.com/liuzengh/trpc-agent-service/trpcservice/backendregistry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

func (s *Service) WithBackendConnections(store *backendregistry.Store) *Service {
	s.backends = store
	return s
}

func (h *Handler) handleBackendConnections(w http.ResponseWriter, r *http.Request) {
	var in struct {
		TenantID    string                   `json:"tenant_id"`
		ID          string                   `json:"connection_id"`
		After       string                   `json:"after"`
		Name        string                   `json:"name"`
		Resource    string                   `json:"resource_type"`
		Backend     string                   `json:"backend_type"`
		Settings    backendregistry.Settings `json:"settings"`
		Secrets     backendregistry.Secrets  `json:"credentials"`
		ExistingRef string                   `json:"credential_ref"`
		AppID       string                   `json:"app_id"`
	}
	if !decodeAdmin(w, r, &in) || !h.require(w, r, in.TenantID, PermissionRead) {
		return
	}
	if !identifierPattern.MatchString(in.TenantID) || len(in.After) > 128 {
		h.writeResult(w, 0, nil, invalidf("invalid storage connection identity"))
		return
	}
	if r.URL.Path == "/admin/backend-connections/list" {
		items, next, err := h.service.backends.List(r.Context(), in.TenantID, in.After)
		h.backendResult(w, http.StatusOK, map[string]any{"items": items, "next": next, "enabled": h.service.backends != nil}, err)
		return
	}
	if !h.require(w, r, in.TenantID, PermissionWrite) {
		return
	}
	if h.service.backends == nil {
		h.backendResult(w, 0, nil, backendregistry.ErrUnavailable)
		return
	}
	if _, err := h.service.repository.GetTenant(r.Context(), in.TenantID); err != nil {
		h.writeResult(w, 0, nil, err)
		return
	}
	if r.URL.Path == "/admin/backend-connections/bind" {
		if !identifierPattern.MatchString(in.ID) || (in.AppID != "" && !identifierPattern.MatchString(in.AppID)) {
			h.writeResult(w, 0, nil, invalidf("invalid storage binding identity"))
			return
		}
		var binding controlplane.BackendBinding
		err := h.service.consoleStore.Transaction(r.Context(), func(ctx context.Context) error {
			c, err := h.service.backends.Get(ctx, in.TenantID, in.ID)
			if err != nil {
				return err
			}
			if in.AppID != "" {
				if _, err = h.service.repository.GetAgentApp(ctx, in.TenantID, in.AppID); err != nil {
					return err
				}
			}
			binding, err = h.service.CreateBackendBinding(ctx, controlplane.BackendBinding{TenantID: in.TenantID, AppID: in.AppID, ResourceType: c.Resource, BackendType: c.Backend, Config: c.Config, SecretRef: c.SecretRef})
			return err
		})
		h.backendResult(w, http.StatusCreated, map[string]string{"binding_id": binding.ID, "connection_id": in.ID}, err)
		return
	}
	// A credential grant alone must not authorize a tenant to redirect that
	// credential to a new destination. Provision destinations as superadmin.
	p := r.Context().Value(principalContextKey{}).(Principal)
	if p.Role != RoleSuperAdmin && (in.Backend != "inmemory" || in.ExistingRef != "" || !in.Secrets.Empty()) {
		h.backendResult(w, 0, nil, secret.ErrForbidden)
		return
	}
	var connection backendregistry.Connection
	err := h.service.consoleStore.Transaction(r.Context(), func(ctx context.Context) error {
		var err error
		connection, err = h.service.backends.Create(ctx, backendregistry.Connection{TenantID: in.TenantID, ID: in.ID, Name: in.Name, Resource: in.Resource, Backend: in.Backend, Settings: in.Settings, CreatedBy: p.Name}, in.Secrets, in.ExistingRef)
		if err != nil {
			return err
		}
		return h.service.record(ctx, in.TenantID, "admin_backend_connection_created", map[string]any{"connection_id": connection.ID, "resource_type": connection.Resource, "backend_type": connection.Backend})
	})
	in.Secrets = backendregistry.Secrets{}
	h.backendResult(w, http.StatusCreated, connection, err)
}

func (h *Handler) backendResult(w http.ResponseWriter, status int, value any, err error) {
	if errors.Is(err, backendregistry.ErrUnavailable) {
		adminJSON(w, http.StatusServiceUnavailable, map[string]string{"error": backendregistry.ErrUnavailable.Error()})
		return
	}
	if errors.Is(err, backendregistry.ErrInvalid) {
		err = invalidf("%s", err.Error())
	}
	if errors.Is(err, controlplane.ErrConflict) {
		adminJSON(w, http.StatusConflict, map[string]string{"error": "该范围已有后端绑定或连接标识重复。不要覆盖已有数据，请使用新的标识或受控迁移流程。"})
		return
	}
	h.writeResult(w, status, value, err)
}
