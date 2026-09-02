package web

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/skill"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

// AgentAPI exposes agent CRUD + version publish/rollback over HTTP.
type AgentAPI struct {
	mgr    *agent.Manager
	tools  *tool.Registry  // optional: tool grants reconciled on publish
	skills *skill.Manager  // optional: skill bindings reconciled on publish
}

// NewAgentAPI returns an agent API backed by the given manager.
func NewAgentAPI(mgr *agent.Manager) *AgentAPI {
	return &AgentAPI{mgr: mgr}
}

// SetGrants wires the tool + skill managers so publishing an agent persists
// its tool grants (agent_tool_grants) and skill bindings (agent_skills). May
// be nil — publish then only writes the frozen profile.
func (a *AgentAPI) SetGrants(tools *tool.Registry, skills *skill.Manager) {
	a.tools = tools
	a.skills = skills
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
	agentID := r.PathValue("id")
	v, err := a.mgr.Publish(r.Context(), agentID, p)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	a.syncGrants(r.Context(), agentID, p)
	writeJSON(w, http.StatusOK, map[string]int{"version": v})
}

// syncGrants persists the profile's tool grants and skill bindings after a
// successful publish, so the worker's RBAC check (agent_tool_grants) and the
// skill version lock (agent_skills) reflect the frozen profile. Best-effort:
// a grant failure is logged, not fatal, since the profile itself is already
// the source of truth for resolution.
func (a *AgentAPI) syncGrants(ctx context.Context, agentID string, p agent.RuntimeProfile) {
	if a.tools != nil {
		for _, tid := range p.ToolIDs {
			if err := a.tools.Grant(ctx, agentID, tid); err != nil {
				slog.Warn("agent: grant tool failed", "agent", agentID, "tool", tid, "err", err)
			}
		}
	}
	if a.skills != nil {
		for i, sid := range p.SkillIDs {
			sk, err := a.skills.Get(ctx, sid)
			if err != nil {
				slog.Warn("agent: bind skill failed (lookup)", "skill", sid, "err", err)
				continue
			}
			if err := a.skills.BindAgentSkill(ctx, agentID, sid, sk.CurrentVersion, i); err != nil {
				slog.Warn("agent: bind skill failed", "agent", agentID, "skill", sid, "err", err)
			}
		}
	}
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
