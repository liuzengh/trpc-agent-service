// Package web serves the admin REST API for the Agent platform.
package web

import (
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

func (a *AuditAPI) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := 100
	if raw := q.Get("limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			limit = n
		}
	}
	logs, err := a.rec.List(r.Context(), q.Get("tenant_id"), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, logs)
}
