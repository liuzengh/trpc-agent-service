// Package web serves the admin REST API for the Agent platform.
package web

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/audit"
)

// AuditAPI exposes the audit log (read side) to the admin front end.
type AuditAPI struct {
	rec *audit.MySQLRecorder
}

// NewAuditAPI returns an audit list API backed by the given recorder.
func NewAuditAPI(rec *audit.MySQLRecorder) *AuditAPI {
	return &AuditAPI{rec: rec}
}

// Register mounts audit routes on the mux.
func (a *AuditAPI) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /audit", a.list)
}

// allowedAuditKinds is the vocabulary of asset kinds an operator may filter on.
// Runs leave agent_name empty, so they are excluded by any kind filter.
var allowedAuditKinds = map[string]bool{
	assetKindAgent:    true,
	assetKindKB:       true,
	assetKindSkill:    true,
	assetKindBinding:  true,
	assetKindEndpoint: true,
}

func (a *AuditAPI) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := 100
	if raw := q.Get("limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			limit = n
		}
	}
	// Audit is a tenant asset: everyone but the platform owner only ever sees
	// their own tenant, whatever the query string asks for.
	query := audit.ListQuery{
		TenantID: ScopeTenant(GetClaims(r.Context()), q.Get("tenant_id")),
		Channel:  q.Get("channel"),
		Kind:     q.Get("kind"),
		Decision: q.Get("decision"),
		UserID:   q.Get("user_id"),
		Limit:    limit,
	}
	// An unknown kind would silently return nothing (no row carries it), which
	// reads as "no matches" rather than "bad filter". Reject it instead.
	if query.Kind != "" && !allowedAuditKinds[query.Kind] {
		writeError(w, http.StatusBadRequest,
			fmt.Errorf("unknown audit kind %q: want agent|kb|skill|binding|endpoint", query.Kind))
		return
	}
	logs, err := a.rec.List(r.Context(), query)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, logs)
}
