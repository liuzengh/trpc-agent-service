package web

import (
	"errors"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

type tenantBackendPolicyResponse struct {
	TenantID   string                         `json:"tenant_id"`
	Profiles   []storage.TenantBackendProfile `json:"profiles"`
	ProfileIDs []string                       `json:"profile_ids,omitempty"`
}

type backendProfileCreateRequest struct {
	ProfileID     string   `json:"profile_id"`
	DisplayName   string   `json:"display_name"`
	Driver        string   `json:"driver"`
	ConnectionRef string   `json:"connection_ref,omitempty"`
	Domains       []string `json:"domains"`
}

type backendProfileUpdateRequest struct {
	DisplayName string `json:"display_name"`
	Status      string `json:"status"`
}

func (c *consoleAPI) backendDrivers(writer http.ResponseWriter, request *http.Request) {
	if !requireSystemAdmin(writer, request) {
		return
	}
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"drivers": storage.BackendDriverCatalog()})
}

func (c *consoleAPI) backendProfiles(writer http.ResponseWriter, request *http.Request) {
	if c.dependencies.BackendProfiles == nil {
		writeJSON(writer, http.StatusServiceUnavailable, map[string]any{"error": "backend profile store is not configured"})
		return
	}
	if !requireSystemAdmin(writer, request) {
		return
	}
	switch request.Method {
	case http.MethodGet:
		profiles, err := c.dependencies.BackendProfiles.ListBackendProfiles(request.Context())
		if err != nil {
			serverError(writer, "list backend profiles", err)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"profiles": profiles})
	case http.MethodPost:
		var body backendProfileCreateRequest
		if err := decodeJSONBody(request, &body); err != nil {
			badRequest(writer, err.Error())
			return
		}
		profile := storage.BackendProfile{
			ProfileID: body.ProfileID, DisplayName: body.DisplayName, Driver: body.Driver,
			ConnectionRef: body.ConnectionRef, Domains: body.Domains, Status: storage.BackendProfileActive,
		}
		created, err := c.dependencies.BackendProfiles.CreateBackendProfile(request.Context(), profile)
		if err != nil {
			badRequest(writer, err.Error())
			return
		}
		writeJSON(writer, http.StatusCreated, created)
	default:
		methodNotAllowed(writer, http.MethodGet+", "+http.MethodPost)
	}
}

func (c *consoleAPI) backendProfile(writer http.ResponseWriter, request *http.Request) {
	if c.dependencies.BackendProfiles == nil {
		writeJSON(writer, http.StatusServiceUnavailable, map[string]any{"error": "backend profile store is not configured"})
		return
	}
	if !requireSystemAdmin(writer, request) {
		return
	}
	profileID := strings.TrimSpace(strings.TrimPrefix(request.URL.Path, "/api/v1/backend-profiles/"))
	if profileID == "" || strings.Contains(profileID, "/") {
		badRequest(writer, "backend profile ID is required")
		return
	}
	switch request.Method {
	case http.MethodPut:
		var body backendProfileUpdateRequest
		if err := decodeJSONBody(request, &body); err != nil {
			badRequest(writer, err.Error())
			return
		}
		updated, err := c.dependencies.BackendProfiles.UpdateBackendProfile(request.Context(), profileID, storage.BackendProfileUpdate{
			DisplayName: body.DisplayName,
			Status:      body.Status,
		})
		if err != nil {
			if errors.Is(err, storage.ErrBackendProfileNotFound) {
				notFound(writer, "backend profile does not exist")
				return
			}
			badRequest(writer, err.Error())
			return
		}
		writeJSON(writer, http.StatusOK, updated)
	case http.MethodDelete:
		if err := c.dependencies.BackendProfiles.DeleteBackendProfile(request.Context(), profileID); err != nil {
			if errors.Is(err, storage.ErrBackendProfileNotFound) {
				notFound(writer, "backend profile does not exist")
				return
			}
			badRequest(writer, err.Error())
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(writer, http.MethodPut+", "+http.MethodDelete)
	}
}

func (c *consoleAPI) tenantBackendPolicy(writer http.ResponseWriter, request *http.Request) {
	if c.dependencies.BackendProfiles == nil {
		writeJSON(writer, http.StatusServiceUnavailable, map[string]any{"error": "backend profile store is not configured"})
		return
	}
	tenantID := resolveTenantParam(request)
	if tenantID == "" {
		badRequest(writer, "tenant is required")
		return
	}
	switch request.Method {
	case http.MethodGet:
		user, ok := sessionUser(request)
		if !ok {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		if !user.IsSystemAdmin && !requireTenantWrite(writer, request, tenantID) {
			return
		}
		profiles, err := c.dependencies.BackendProfiles.ListTenantBackendProfiles(request.Context(), tenantID)
		if err != nil {
			serverError(writer, "list tenant backend policy", err)
			return
		}
		writeJSON(writer, http.StatusOK, tenantBackendPolicyResponse{TenantID: tenantID, Profiles: profiles})
	case http.MethodPut:
		if !requireSystemAdmin(writer, request) {
			return
		}
		var body tenantBackendPolicyResponse
		if err := decodeJSONBody(request, &body); err != nil {
			badRequest(writer, err.Error())
			return
		}
		if strings.TrimSpace(body.TenantID) != "" && strings.TrimSpace(body.TenantID) != tenantID {
			badRequest(writer, "tenant_id does not match selected tenant")
			return
		}
		if err := c.dependencies.BackendProfiles.ReplaceTenantBackendProfiles(request.Context(), tenantID, body.ProfileIDs); err != nil {
			badRequest(writer, err.Error())
			return
		}
		profiles, err := c.dependencies.BackendProfiles.ListTenantBackendProfiles(request.Context(), tenantID)
		if err != nil {
			serverError(writer, "list updated tenant backend policy", err)
			return
		}
		writeJSON(writer, http.StatusOK, tenantBackendPolicyResponse{TenantID: tenantID, Profiles: profiles})
	default:
		methodNotAllowed(writer, http.MethodGet+", "+http.MethodPut)
	}
}
