package web

import (
	"errors"
	"net/http"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

// ToolAPI exposes the registered tool catalog and its per-agent RBAC grants.
type ToolAPI struct {
	reg *tool.Registry
}

// NewToolAPI returns a tool catalog API.
func NewToolAPI(reg *tool.Registry) *ToolAPI {
	return &ToolAPI{reg: reg}
}

// Register mounts tool routes on the mux.
func (a *ToolAPI) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /tools", a.list)
	mux.HandleFunc("GET /tools/{id}/grants", a.listGrants)
	mux.HandleFunc("PUT /tools/{id}/grants/{agentID}", a.grant)
	mux.HandleFunc("DELETE /tools/{id}/grants/{agentID}", a.revoke)
}

func (a *ToolAPI) list(w http.ResponseWriter, r *http.Request) {
	defs, err := a.reg.List(r.Context(), r.URL.Query().Get("tenant_id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, defs)
}

// listGrants returns the agent ids a tool is granted to.
func (a *ToolAPI) listGrants(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := a.reg.Get(r.Context(), id); err != nil {
		if errors.Is(err, tool.ErrNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	agents, err := a.reg.GrantedAgents(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, agents)
}

// grant whitelists a tool for an agent (idempotent).
func (a *ToolAPI) grant(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := a.reg.Get(r.Context(), id); err != nil {
		if errors.Is(err, tool.ErrNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if err := a.reg.Grant(r.Context(), r.PathValue("agentID"), id); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// revoke removes an agent's grant (no-op when absent).
func (a *ToolAPI) revoke(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := a.reg.Get(r.Context(), id); err != nil {
		if errors.Is(err, tool.ErrNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if err := a.reg.Revoke(r.Context(), r.PathValue("agentID"), id); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
