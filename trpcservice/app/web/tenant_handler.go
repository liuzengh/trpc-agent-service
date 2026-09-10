// Package web serves the admin REST API and chat pages for the Agent platform.
package web

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/tenant"
)

// TenantAPI exposes tenant CRUD over HTTP.
type TenantAPI struct {
	mgr *tenant.Manager
}

// NewTenantAPI returns a tenant CRUD API backed by the given manager.
func NewTenantAPI(mgr *tenant.Manager) *TenantAPI {
	return &TenantAPI{mgr: mgr}
}

// Register mounts tenant routes on the mux.
func (a *TenantAPI) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /tenants", a.create)
	mux.HandleFunc("GET /tenants", a.list)
	mux.HandleFunc("GET /tenants/{id}", a.get)
	mux.HandleFunc("PUT /tenants/{id}", a.update)
	mux.HandleFunc("DELETE /tenants/{id}", a.delete)
	mux.HandleFunc("GET /tenants/{id}/config-versions", a.configVersions)
	mux.HandleFunc("POST /tenants/{id}/config-rollback", a.configRollback)
}

// configVersions returns the tenant's configuration history, newest first.
func (a *TenantAPI) configVersions(w http.ResponseWriter, r *http.Request) {
	vs, err := a.mgr.ConfigVersions(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, vs)
}

// configRollback restores a tenant to a recorded configuration version.
func (a *TenantAPI) configRollback(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Version int `json:"version"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if body.Version <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("tenant: version must be positive"))
		return
	}
	got, err := a.mgr.RollbackConfig(r.Context(), r.PathValue("id"), body.Version)
	if err != nil {
		if errors.Is(err, tenant.ErrConfigVersionNotFound) || errors.Is(err, tenant.ErrNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, got)
}

func (a *TenantAPI) create(w http.ResponseWriter, r *http.Request) {
	var t tenant.Tenant
	if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := a.mgr.Create(r.Context(), &t); err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusCreated, t)
}

func (a *TenantAPI) get(w http.ResponseWriter, r *http.Request) {
	if claims := GetClaims(r.Context()); claims != nil && claims.TenantID != r.PathValue("id") &&
		!HasPermission(claims.Role, PermTenantManage) {
		writeError(w, http.StatusNotFound, tenant.ErrNotFound)
		return
	}
	t, err := a.mgr.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		if errors.Is(err, tenant.ErrNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (a *TenantAPI) list(w http.ResponseWriter, r *http.Request) {
	all, err := a.mgr.List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if claims := GetClaims(r.Context()); claims != nil && !HasPermission(claims.Role, PermTenantManage) {
		filtered := make([]*tenant.Tenant, 0, 1)
		for _, item := range all {
			if item.ID == claims.TenantID {
				filtered = append(filtered, item)
				break
			}
		}
		all = filtered
	}
	writeJSON(w, http.StatusOK, all)
}

func (a *TenantAPI) update(w http.ResponseWriter, r *http.Request) {
	var t tenant.Tenant
	if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	t.ID = r.PathValue("id")
	if err := a.mgr.Update(r.Context(), &t); err != nil {
		if errors.Is(err, tenant.ErrNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusBadRequest, err)
		return
	}
	// Echo the stored tenant (fresh read) so clients sync the server's
	// normalized copy of governance JSON rather than their own payload.
	got, err := a.mgr.Get(r.Context(), t.ID)
	if err != nil {
		writeJSON(w, http.StatusOK, t)
		return
	}
	writeJSON(w, http.StatusOK, got)
}

func (a *TenantAPI) delete(w http.ResponseWriter, r *http.Request) {
	if err := a.mgr.Delete(r.Context(), r.PathValue("id")); err != nil {
		if errors.Is(err, tenant.ErrNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
