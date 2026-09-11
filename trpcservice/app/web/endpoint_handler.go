package web

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/asset"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/llm"
)

// EndpointAPI exposes model-endpoint CRUD over HTTP. It operates directly on
// the model registry, which is both the endpoint store and the model cache.
type EndpointAPI struct {
	reg     *llm.Registry
	auditor assetAuditor // optional: asset changes are audited
}

// NewEndpointAPI returns an endpoint API backed by the given registry.
func NewEndpointAPI(reg *llm.Registry) *EndpointAPI {
	return &EndpointAPI{reg: reg}
}

// SetAuditor wires asset-change auditing. May be nil.
func (a *EndpointAPI) SetAuditor(rec assetAuditor) { a.auditor = rec }

// Register mounts endpoint routes on the mux.
func (a *EndpointAPI) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /endpoints", a.create)
	mux.HandleFunc("GET /endpoints", a.list)
	mux.HandleFunc("GET /endpoints/{id}", a.get)
	mux.HandleFunc("PUT /endpoints/{id}", a.update)
	mux.HandleFunc("DELETE /endpoints/{id}", a.delete)
}

func (a *EndpointAPI) create(w http.ResponseWriter, r *http.Request) {
	var ep llm.Endpoint
	if err := json.NewDecoder(r.Body).Decode(&ep); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	claims := GetClaims(r.Context())
	if ep.Scope == "" {
		ep.Scope = llm.ScopeTenant
	}
	// A tenant endpoint belongs to its creator's tenant; only the owner may mint
	// a platform-wide (global) endpoint or place one in another tenant.
	if ep.Scope != llm.ScopeGlobal {
		ep.TenantID = ClaimTenant(claims, ep.TenantID)
		if ep.TenantID == "" {
			writeError(w, http.StatusBadRequest, errors.New("tenant_id is required for a tenant endpoint"))
			return
		}
	} else if !GlobalAssetWritable(claims) {
		writeError(w, http.StatusForbidden, errors.New("only the platform owner may create a global endpoint"))
		return
	}
	ep.Visibility = asset.VisibilityOrDefault(ep.Visibility)
	if claims != nil {
		ep.CreatedBy = claims.UserID
	}
	if err := a.reg.Create(r.Context(), ep); err != nil {
		if errors.Is(err, llm.ErrEndpointExists) {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	recordAssetAllowed(claims, a.auditor, assetKindEndpoint, ep.ID, ep.TenantID)
	writeJSON(w, http.StatusCreated, a.persisted(r, ep))
}

func (a *EndpointAPI) list(w http.ResponseWriter, r *http.Request) {
	claims := GetClaims(r.Context())
	eps, err := a.reg.List(r.Context(), ScopeTenant(claims, r.URL.Query().Get("tenant_id")))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	// A global endpoint is platform-shared infrastructure and stays visible to
	// everyone; a tenant endpoint is private to its author until shared.
	visible := make([]llm.Endpoint, 0, len(eps))
	for _, ep := range eps {
		if ep.Scope == llm.ScopeGlobal || CanReadAsset(claims, ep.CreatedBy, ep.Visibility) {
			visible = append(visible, ep)
		}
	}
	writeJSON(w, http.StatusOK, visible)
}

func (a *EndpointAPI) get(w http.ResponseWriter, r *http.Request) {
	ep, ok := a.readable(w, r, r.PathValue("id"))
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, ep)
}

// readable resolves the endpoint a caller may read: its tenant must match and,
// unless the caller manages the tenant, it must be global, its own, or shared.
func (a *EndpointAPI) readable(w http.ResponseWriter, r *http.Request, id string) (llm.Endpoint, bool) {
	ep, err := a.reg.Get(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return llm.Endpoint{}, false
	}
	claims := GetClaims(r.Context())
	if !EndpointAccessible(claims, ep) {
		WriteCrossTenant(w)
		return llm.Endpoint{}, false
	}
	if !CanReadAsset(claims, ep.CreatedBy, ep.Visibility) {
		WriteAssetDenied(claims, w, a.auditor, assetKindEndpoint, id, ep.TenantID, "not the author and not shared")
		return llm.Endpoint{}, false
	}
	return ep, true
}

// writable resolves the endpoint a caller may change: a global endpoint is
// platform-owned (owner only), a tenant endpoint follows the asset rule.
func (a *EndpointAPI) writable(w http.ResponseWriter, r *http.Request, id string) (llm.Endpoint, bool) {
	ep, ok := a.readable(w, r, id)
	if !ok {
		return llm.Endpoint{}, false
	}
	claims := GetClaims(r.Context())
	if ep.Scope == llm.ScopeGlobal {
		if !GlobalAssetWritable(claims) {
			WriteAssetDenied(claims, w, a.auditor, assetKindEndpoint, id, "", "global endpoint is platform-owned")
			return llm.Endpoint{}, false
		}
		return ep, true
	}
	if !CanManageAsset(claims, ep.CreatedBy) {
		WriteAssetDenied(claims, w, a.auditor, assetKindEndpoint, id, ep.TenantID, "shared but not authored")
		return llm.Endpoint{}, false
	}
	return ep, true
}

func (a *EndpointAPI) update(w http.ResponseWriter, r *http.Request) {
	var ep llm.Endpoint
	if err := json.NewDecoder(r.Body).Decode(&ep); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	id := r.PathValue("id")
	existing, ok := a.writable(w, r, id)
	if !ok {
		return
	}
	ep.ID = id
	// Scope, tenant and author are fixed at creation: a write may rotate
	// credentials or model settings and publish/unpublish the endpoint, but it
	// never moves it into another tenant (or promotes it to global).
	ep.Scope = existing.Scope
	ep.TenantID = existing.TenantID
	ep.CreatedBy = existing.CreatedBy
	if err := a.reg.Update(r.Context(), ep); err != nil {
		if errors.Is(err, llm.ErrEndpointNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusBadRequest, err)
		return
	}
	claims := GetClaims(r.Context())
	recordAssetAllowed(claims, a.auditor, assetKindEndpoint, id, existing.TenantID)
	writeJSON(w, http.StatusOK, a.persisted(r, ep))
}

// persisted returns the stored endpoint after a write, so the response
// carries the credential reference rather than the plaintext key the client
// sent. If the read-back fails, it clears the plaintext key and returns the
// submitted endpoint.
func (a *EndpointAPI) persisted(r *http.Request, ep llm.Endpoint) llm.Endpoint {
	got, err := a.reg.Get(r.Context(), ep.ID)
	if err != nil {
		ep.APIKey = ""
		return ep
	}
	return got
}

func (a *EndpointAPI) delete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	existing, ok := a.writable(w, r, id)
	if !ok {
		return
	}
	if err := a.reg.Delete(r.Context(), id); err != nil {
		if errors.Is(err, llm.ErrEndpointNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	claims := GetClaims(r.Context())
	recordAssetAllowed(claims, a.auditor, assetKindEndpoint, id, existing.TenantID)
	w.WriteHeader(http.StatusNoContent)
}
