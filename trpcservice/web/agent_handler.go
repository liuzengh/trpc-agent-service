package web

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
)

// AgentAPI exposes agent CRUD + version publish/rollback over HTTP.
type AgentAPI struct {
	mgr *agent.Manager
}

// NewAgentAPI returns an agent API backed by the given manager.
func NewAgentAPI(mgr *agent.Manager) *AgentAPI {
	return &AgentAPI{mgr: mgr}
}

// Register mounts agent routes on the mux.
func (a *AgentAPI) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /agents", a.create)
	mux.HandleFunc("GET /agents", a.list)
	mux.HandleFunc("GET /agents/{id}", a.get)
	mux.HandleFunc("PUT /agents/{id}", a.update)
	mux.HandleFunc("DELETE /agents/{id}", a.delete)
	mux.HandleFunc("POST /agents/{id}/publish", a.publish)
	mux.HandleFunc("POST /agents/{id}/rollback", a.rollback)
	mux.HandleFunc("GET /agents/{id}/versions", a.versions)
	mux.HandleFunc("GET /agents/{id}/profile", a.profile)
}

// profile returns the current runtime profile, used to pre-fill the publish form.
func (a *AgentAPI) profile(w http.ResponseWriter, r *http.Request) {
	p, err := a.mgr.Resolve(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (a *AgentAPI) create(w http.ResponseWriter, r *http.Request) {
	var ag agent.Agent
	if err := json.NewDecoder(r.Body).Decode(&ag); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if ag.Status == "" {
		ag.Status = agent.StatusDraft
	}
	if err := a.mgr.Create(r.Context(), ag); err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusCreated, ag)
}

func (a *AgentAPI) list(w http.ResponseWriter, r *http.Request) {
	all, err := a.mgr.List(r.Context(), r.URL.Query().Get("tenant_id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, all)
}

func (a *AgentAPI) get(w http.ResponseWriter, r *http.Request) {
	ag, err := a.mgr.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, ag)
}

func (a *AgentAPI) update(w http.ResponseWriter, r *http.Request) {
	var ag agent.Agent
	if err := json.NewDecoder(r.Body).Decode(&ag); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	ag.ID = r.PathValue("id")
	if err := a.mgr.Update(r.Context(), ag); err != nil {
		if errors.Is(err, agent.ErrNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, ag)
}

func (a *AgentAPI) delete(w http.ResponseWriter, r *http.Request) {
	if err := a.mgr.Delete(r.Context(), r.PathValue("id")); err != nil {
		if errors.Is(err, agent.ErrNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *AgentAPI) publish(w http.ResponseWriter, r *http.Request) {
	var p agent.RuntimeProfile
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	v, err := a.mgr.Publish(r.Context(), r.PathValue("id"), p)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"version": v})
}

func (a *AgentAPI) rollback(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Version int `json:"version"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := a.mgr.Rollback(r.Context(), r.PathValue("id"), req.Version); err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"version": req.Version})
}

func (a *AgentAPI) versions(w http.ResponseWriter, r *http.Request) {
	vs, err := a.mgr.Versions(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, vs)
}
