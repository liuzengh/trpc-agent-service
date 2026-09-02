package web

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/liuzengh/trpc-agent-service/trpcservice/llm"
)

// EndpointAPI exposes model-endpoint CRUD over HTTP. It operates directly on
// the model registry, which is both the endpoint store and the model cache.
type EndpointAPI struct {
	reg *llm.Registry
}

// NewEndpointAPI returns an endpoint API backed by the given registry.
func NewEndpointAPI(reg *llm.Registry) *EndpointAPI {
	return &EndpointAPI{reg: reg}
}

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
	if ep.Scope == "" {
		ep.Scope = llm.ScopeTenant
	}
	if err := a.reg.Create(r.Context(), ep); err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusCreated, ep)
}

func (a *EndpointAPI) list(w http.ResponseWriter, r *http.Request) {
	eps, err := a.reg.List(r.Context(), r.URL.Query().Get("tenant_id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, eps)
}

func (a *EndpointAPI) get(w http.ResponseWriter, r *http.Request) {
	ep, err := a.reg.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, ep)
}

func (a *EndpointAPI) update(w http.ResponseWriter, r *http.Request) {
	var ep llm.Endpoint
	if err := json.NewDecoder(r.Body).Decode(&ep); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	ep.ID = r.PathValue("id")
	if err := a.reg.Update(r.Context(), ep); err != nil {
		if errors.Is(err, llm.ErrEndpointNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, ep)
}

func (a *EndpointAPI) delete(w http.ResponseWriter, r *http.Request) {
	if err := a.reg.Delete(r.Context(), r.PathValue("id")); err != nil {
		if errors.Is(err, llm.ErrEndpointNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
