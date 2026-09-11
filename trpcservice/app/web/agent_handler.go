package web

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/asset"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/skill"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/tool"
)

// AgentAPI exposes agent CRUD + version publish/rollback over HTTP.
type AgentAPI struct {
	mgr     *agent.Manager
	agents  agentSource    // optional: tool grants record the agent's tenant
	tools   *tool.Registry // optional: tool grants reconciled on publish
	skills  *skill.Manager // optional: skill bindings reconciled on publish
	auditor assetAuditor   // optional: asset changes are audited
}

// agentSource is the narrow read side of the agent store the API needs to
// resolve an agent's tenant when it records tool grants.
type agentSource interface {
	Get(ctx context.Context, id string) (*agent.Agent, error)
}

// NewAgentAPI returns an agent API backed by the given manager.
func NewAgentAPI(mgr *agent.Manager) *AgentAPI {
	return &AgentAPI{mgr: mgr}
}

// SetAuditor wires asset-change auditing. May be nil.
func (a *AgentAPI) SetAuditor(rec assetAuditor) { a.auditor = rec }

// SetGrants wires the tool + skill managers so publishing an agent persists
// its tool grants (agent_tool_grants) and skill bindings (agent_skills). May
// be nil — publish then only writes the frozen profile.
func (a *AgentAPI) SetGrants(tools *tool.Registry, skills *skill.Manager) {
	a.tools = tools
	a.skills = skills
	// The manager answers the tenant lookup used when recording tool grants.
	if a.agents == nil {
		a.agents = a.mgr
	}
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
	mux.HandleFunc("PUT /agents/{id}/gray", a.setGray)
	mux.HandleFunc("DELETE /agents/{id}/gray", a.clearGray)
	mux.HandleFunc("GET /agents/{id}/profile", a.profile)
}

// profile returns the current runtime profile, used to pre-fill the publish form.
func (a *AgentAPI) profile(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := a.readable(w, r, id); !ok {
		return
	}
	p, err := a.mgr.Resolve(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

// readable resolves the agent a caller may read: its tenant must match and,
// unless the caller manages the tenant, the agent must be its own or shared.
func (a *AgentAPI) readable(w http.ResponseWriter, r *http.Request, id string) (*agent.Agent, bool) {
	ag, err := a.mgr.Get(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return nil, false
	}
	claims := GetClaims(r.Context())
	if !TenantAccessible(claims, ag.TenantID) {
		WriteCrossTenant(w)
		return nil, false
	}
	if !CanReadAsset(claims, ag.CreatedBy, ag.Visibility) {
		WriteAssetDenied(claims, w, a.auditor, assetKindAgent, id, ag.TenantID, "not the author and not shared")
		return nil, false
	}
	return ag, true
}

// writable resolves the agent a caller may change: tenant managers may change
// any agent of their tenant, everyone else only the ones they authored.
func (a *AgentAPI) writable(w http.ResponseWriter, r *http.Request, id string) (*agent.Agent, bool) {
	ag, ok := a.readable(w, r, id)
	if !ok {
		return nil, false
	}
	claims := GetClaims(r.Context())
	if !CanManageAsset(claims, ag.CreatedBy) {
		WriteAssetDenied(claims, w, a.auditor, assetKindAgent, id, ag.TenantID, "shared but not authored")
		return nil, false
	}
	return ag, true
}

func (a *AgentAPI) create(w http.ResponseWriter, r *http.Request) {
	var ag agent.Agent
	if err := json.NewDecoder(r.Body).Decode(&ag); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	claims := GetClaims(r.Context())
	// An agent is a tenant asset: only the owner may place it in another tenant,
	// everyone else creates inside their own whatever the body claims, and the
	// author is always the authenticated member.
	ag.TenantID = ClaimTenant(claims, ag.TenantID)
	if ag.TenantID == "" {
		writeError(w, http.StatusBadRequest, errors.New("tenant_id is required"))
		return
	}
	if ag.Status == "" {
		ag.Status = agent.StatusDraft
	}
	if claims != nil {
		ag.CreatedBy = claims.UserID
	}
	// The manager normalizes the stored row; echo the normalized value so the
	// client sees the visibility it actually got rather than an empty string.
	ag.Visibility = asset.VisibilityOrDefault(ag.Visibility)
	if err := a.mgr.Create(r.Context(), ag); err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	recordAssetAllowed(claims, a.auditor, assetKindAgent, ag.ID, ag.TenantID)
	writeJSON(w, http.StatusCreated, ag)
}

func (a *AgentAPI) list(w http.ResponseWriter, r *http.Request) {
	claims := GetClaims(r.Context())
	all, err := a.mgr.List(r.Context(), ScopeTenant(claims, r.URL.Query().Get("tenant_id")))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	visible := FilterReadable(claims, all,
		func(ag agent.Agent) string { return ag.CreatedBy },
		func(ag agent.Agent) string { return ag.Visibility })
	writeJSON(w, http.StatusOK, visible)
}

func (a *AgentAPI) get(w http.ResponseWriter, r *http.Request) {
	ag, ok := a.readable(w, r, r.PathValue("id"))
	if !ok {
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
	id := r.PathValue("id")
	existing, ok := a.writable(w, r, id)
	if !ok {
		return
	}
	ag.ID = id
	// The tenant and author of an existing agent are fixed at creation, not by
	// the payload: otherwise a writer could move a tenant asset out of its
	// tenant or claim somebody else's agent. Visibility stays writer-controlled.
	ag.TenantID = existing.TenantID
	ag.CreatedBy = existing.CreatedBy
	if err := a.mgr.Update(r.Context(), ag); err != nil {
		if errors.Is(err, agent.ErrNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusBadRequest, err)
		return
	}
	claims := GetClaims(r.Context())
	recordAssetAllowed(claims, a.auditor, assetKindAgent, id, existing.TenantID)
	writeJSON(w, http.StatusOK, ag)
}

func (a *AgentAPI) delete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	existing, ok := a.writable(w, r, id)
	if !ok {
		return
	}
	if err := a.mgr.Delete(r.Context(), id); err != nil {
		if errors.Is(err, agent.ErrNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	claims := GetClaims(r.Context())
	recordAssetAllowed(claims, a.auditor, assetKindAgent, id, existing.TenantID)
	w.WriteHeader(http.StatusNoContent)
}

func (a *AgentAPI) publish(w http.ResponseWriter, r *http.Request) {
	var p agent.RuntimeProfile
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	agentID := r.PathValue("id")
	existing, ok := a.writable(w, r, agentID)
	if !ok {
		return
	}
	v, err := a.mgr.Publish(r.Context(), agentID, p)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	a.syncGrants(r.Context(), agentID, p)
	claims := GetClaims(r.Context())
	recordAssetAllowed(claims, a.auditor, assetKindAgent, agentID, existing.TenantID)
	writeJSON(w, http.StatusOK, map[string]int{"version": v})
}

// syncGrants persists the profile's tool grants and skill bindings after a
// successful publish, so the worker's RBAC check (agent_tool_grants) and the
// skill version lock (agent_skills) reflect the frozen profile. The agent's own
// tenant is recorded on each grant, which is what the tenant-aware RBAC check
// at run time compares against. Best-effort: a grant failure is logged, not
// fatal, since the profile itself is already the source of truth for
// resolution.
func (a *AgentAPI) syncGrants(ctx context.Context, agentID string, p agent.RuntimeProfile) {
	if a.tools != nil {
		tenantID := ""
		if a.agents != nil {
			if ag, err := a.agents.Get(ctx, agentID); err == nil && ag != nil {
				tenantID = ag.TenantID
			} else if err != nil {
				slog.Warn("agent: grant tenant lookup failed", "agent", agentID, "err", err)
			}
		}
		for _, tid := range p.ToolIDs {
			if err := a.tools.Grant(ctx, tenantID, agentID, tid); err != nil {
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
	id := r.PathValue("id")
	if _, ok := a.writable(w, r, id); !ok {
		return
	}
	if err := a.mgr.Rollback(r.Context(), id, req.Version); err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"version": req.Version})
}

func (a *AgentAPI) versions(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := a.readable(w, r, id); !ok {
		return
	}
	vs, err := a.mgr.Versions(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, vs)
}

// setGray installs or clears the agent's canary release. Clearing is the fast
// rollback of a bad rollout: no publish, no restart, all traffic back on the
// current version.
func (a *AgentAPI) setGray(w http.ResponseWriter, r *http.Request) {
	var req agent.GrayRelease
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	id := r.PathValue("id")
	existing, ok := a.writable(w, r, id)
	if !ok {
		return
	}
	if err := a.mgr.SetGray(r.Context(), id, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	recordAssetAllowed(GetClaims(r.Context()), a.auditor, assetKindAgent, id, existing.TenantID)
	writeJSON(w, http.StatusOK, req)
}

// clearGray removes the canary release.
func (a *AgentAPI) clearGray(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	existing, ok := a.writable(w, r, id)
	if !ok {
		return
	}
	if err := a.mgr.ClearGray(r.Context(), id); err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	recordAssetAllowed(GetClaims(r.Context()), a.auditor, assetKindAgent, id, existing.TenantID)
	w.WriteHeader(http.StatusNoContent)
}
