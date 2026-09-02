package web

import (
	"net/http"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
)

// UsageAPI exposes the usage metering read side (cost attribution, amounts
// only — no pricing). It is registered only when the MySQL recorder (and thus
// the usage_records table) is available.
type UsageAPI struct {
	rec *audit.MySQLRecorder
}

// NewUsageAPI returns the usage metering API.
func NewUsageAPI(rec *audit.MySQLRecorder) *UsageAPI {
	return &UsageAPI{rec: rec}
}

// Register mounts usage routes.
func (a *UsageAPI) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /usage", a.get)
}

// usageResponse bundles the per-dimension summary with the recent rows.
type usageResponse struct {
	Summary []audit.UsageSummary `json:"summary"`
	Rows    []audit.UsageRow     `json:"rows"`
}

func (a *UsageAPI) get(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	query := audit.UsageQuery{
		TenantID:  q.Get("tenant_id"),
		AgentID:   q.Get("agent_id"),
		Dimension: q.Get("dimension"),
	}
	if v := q.Get("from"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			query.From = t
		}
	}
	if v := q.Get("to"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			query.To = t
		}
	}
	query.Limit = 100

	summary, err := a.rec.UsageSummary(r.Context(), query)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	rows, err := a.rec.UsageRows(r.Context(), query)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, usageResponse{Summary: summary, Rows: rows})
}
