package web

import (
	"context"
	"errors"
	"net/http"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/tool"
)

// toolAgentSource is the narrow slice of the agent manager the tool API needs to
// keep grants inside one tenant. May be nil (the checks are then skipped).
type toolAgentSource interface {
	Get(ctx context.Context, id string) (*agent.Agent, error)
}

// ToolAPI exposes the registered tool catalog and its per-agent RBAC grants.
type ToolAPI struct {
	reg    *tool.Registry
	agents toolAgentSource
}

// NewToolAPI returns a tool catalog API.
func NewToolAPI(reg *tool.Registry) *ToolAPI {
	return &ToolAPI{reg: reg}
}

// SetAgentSource wires the agent manager so a grant can only connect a tool to
// an agent of the same tenant: without it a tenant admin could grant their tool
// (or a platform tool) to another tenant's agent and silently change what that
// tenant's agent is allowed to do.
func (a *ToolAPI) SetAgentSource(src toolAgentSource) { a.agents = src }

// Register mounts tool routes on the mux.
func (a *ToolAPI) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /tools", a.list)
	mux.HandleFunc("GET /tools/{id}/grants", a.listGrants)
	mux.HandleFunc("PUT /tools/{id}/grants/{agentID}", a.grant)
	mux.HandleFunc("DELETE /tools/{id}/grants/{agentID}", a.revoke)
}

func (a *ToolAPI) list(w http.ResponseWriter, r *http.Request) {
	defs, err := a.reg.List(r.Context(), ScopeTenant(GetClaims(r.Context()), r.URL.Query().Get("tenant_id")))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, defs)
}

// canAccess loads the tool definition and reports whether the caller may touch
// it. Builtin tools carry no scope and stay platform-wide; a tenant tool follows
// the usual tenant rule. On denial it writes 404 and returns false.
func (a *ToolAPI) canAccess(w http.ResponseWriter, r *http.Request, id string) bool {
	def, err := a.reg.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, tool.ErrNotFound) {
			writeError(w, http.StatusNotFound, err)
			return false
		}
		writeError(w, http.StatusInternalServerError, err)
		return false
	}
	if def.Scope == "" || def.Scope == tool.ScopeGlobal {
		return true
	}
	if !TenantAccessible(GetClaims(r.Context()), def.TenantID) {
		WriteCrossTenant(w)
		return false
	}
	return true
}

// listGrants returns the agent ids a tool is granted to. Only agents the caller
// may see are returned, so a shared tool does not leak another tenant's agent
// ids into an admin's UI.
func (a *ToolAPI) listGrants(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !a.canAccess(w, r, id) {
		return
	}
	agents, err := a.reg.GrantedAgents(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, a.visibleAgents(r.Context(), agents))
}

// visibleAgents filters granted agent ids down to the ones the caller can
// reach. With no agent source the list is returned unchanged (the API is then
// running without tenant wiring, e.g. in a unit test).
func (a *ToolAPI) visibleAgents(ctx context.Context, ids []string) []string {
	if a.agents == nil {
		return ids
	}
	claims := GetClaims(ctx)
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		ag, err := a.agents.Get(ctx, id)
		if err != nil || ag == nil {
			continue
		}
		if TenantAccessible(claims, ag.TenantID) {
			out = append(out, id)
		}
	}
	return out
}

// grant whitelists a tool for an agent (idempotent). The agent must be in the
// same tenant as the tool: a tenant's tool is not a lever on another tenant's
// agent, and a grant is only meaningful inside one tenant's boundary.
func (a *ToolAPI) grant(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !a.canAccess(w, r, id) {
		return
	}
	agentID := r.PathValue("agentID")
	if !a.agentBindable(w, r, id, agentID) {
		return
	}
	if err := a.reg.Grant(r.Context(), agentID, id); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// revoke removes an agent's grant (no-op when absent).
func (a *ToolAPI) revoke(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !a.canAccess(w, r, id) {
		return
	}
	agentID := r.PathValue("agentID")
	if !a.agentBindable(w, r, id, agentID) {
		return
	}
	if err := a.reg.Revoke(r.Context(), agentID, id); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// agentBindable reports whether agentID may be bound to toolID. A cross-tenant
// target is reported as 404 so the response does not confirm that the other
// tenant's agent exists.
func (a *ToolAPI) agentBindable(w http.ResponseWriter, r *http.Request, toolID, agentID string) bool {
	if a.agents == nil {
		return true
	}
	ag, err := a.agents.Get(r.Context(), agentID)
	if err != nil {
		if errors.Is(err, agent.ErrNotFound) {
			writeError(w, http.StatusNotFound, err)
			return false
		}
		writeError(w, http.StatusInternalServerError, err)
		return false
	}
	if !TenantAccessible(GetClaims(r.Context()), ag.TenantID) {
		WriteCrossTenant(w)
		return false
	}
	def, err := a.reg.Get(r.Context(), toolID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return false
	}
	// A tenant-scoped tool only ever belongs to agents of that tenant, even for
	// the owner: the pair would otherwise be incoherent (the agent's tenant
	// could never legitimately use it).
	if def.Scope == tool.ScopeTenant && def.TenantID != "" && def.TenantID != ag.TenantID {
		WriteCrossTenant(w)
		return false
	}
	return true
}
