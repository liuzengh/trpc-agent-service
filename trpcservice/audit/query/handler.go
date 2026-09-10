package query

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
)

// QueryObserver observes query outcomes for low-cardinality metrics.
type QueryObserver interface {
	ObserveQuery(decision string, crossTenant bool)
}

// Handler serves the read-only audit query API with mandatory tenant scope,
// time range and pagination, recording every access as a secondary audit fact.
type Handler struct {
	Store      Store
	Principals PrincipalResolver
	MaxWindow  time.Duration
	MaxPage    int
	Now        func() time.Time
	Observer   QueryObserver
}

func (h Handler) observe(decision string, crossTenant bool) {
	if h.Observer != nil {
		h.Observer.ObserveQuery(decision, crossTenant)
	}
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	principal, err := h.Principals.Resolve(r)
	if err != nil {
		_, crossTenant, _ := auditPath(r.URL.Path)
		record := h.deniedRecord(Principal{TenantID: "_unknown", SubjectID: "_anonymous"},
			"_unknown", crossTenant, h.now(), "unauthenticated")
		record.TraceID = r.Header.Get("X-Trace-ID")
		if recErr := h.Store.RecordAccess(r.Context(), record); recErr != nil {
			writeError(w, http.StatusInternalServerError, "audit_record_failed")
			return
		}
		h.observe("denied", crossTenant)
		writeError(w, http.StatusUnauthorized, "unauthenticated")
		return
	}
	filter, record, status := h.authorize(principal, r)
	if status != 0 {
		if status == http.StatusNotFound {
			writeError(w, status, "not_found")
			return
		}
		// Secondary audit is mandatory: a failed access record is a fail-closed
		// condition, never a silent success.
		if err := h.Store.RecordAccess(r.Context(), record); err != nil {
			writeError(w, http.StatusInternalServerError, "audit_record_failed")
			return
		}
		h.observe("denied", record.CrossTenant)
		writeError(w, status, record.ReasonCode)
		return
	}
	page, err := h.Store.Query(r.Context(), filter)
	if err != nil {
		record.Decision = "denied"
		record.ReasonCode = "query_failed"
		record.OccurredAt = h.now()
		if recErr := h.Store.RecordAccess(r.Context(), record); recErr != nil {
			writeError(w, http.StatusInternalServerError, "audit_record_failed")
			return
		}
		h.observe("denied", record.CrossTenant)
		writeError(w, http.StatusInternalServerError, "query_failed")
		return
	}
	record.ResultCount = int64(len(page.Events))
	record.ResultDigest = Digest(page.Events)
	record.Decision = "allowed"
	record.OccurredAt = h.now()
	if recErr := h.Store.RecordAccess(r.Context(), record); recErr != nil {
		// Do not return results whose access was not durably recorded.
		writeError(w, http.StatusInternalServerError, "audit_record_failed")
		return
	}
	h.observe("allowed", record.CrossTenant)
	writeJSON(w, http.StatusOK, struct {
		Events     []audit.Event `json:"events"`
		NextCursor string        `json:"next_cursor,omitempty"`
	}{page.Events, page.NextCursor})
}

// authorize validates the principal against the requested scope and returns the
// effective filter and the durable access record.
func (h Handler) authorize(principal Principal, r *http.Request) (Filter, QueryRecord, int) {
	now := h.now()
	parts, crossTenant, singleTenant := auditPath(r.URL.Path)
	if !crossTenant && !singleTenant {
		return Filter{}, QueryRecord{}, http.StatusNotFound
	}
	if !principal.Authenticated || !principal.CanReadAudit {
		return Filter{}, h.deniedRecord(principal, "", crossTenant, now, "forbidden"), http.StatusForbidden
	}
	tenantID := principal.TenantID
	if singleTenant {
		tenantID = parts[2]
		if !principal.CanReadAuditCrossTenant && tenantID != principal.TenantID {
			return Filter{}, h.deniedRecord(principal, tenantID, crossTenant, now, "forbidden"), http.StatusForbidden
		}
	}
	if crossTenant && !principal.CanReadAuditCrossTenant {
		return Filter{}, h.deniedRecord(principal, tenantID, crossTenant, now, "forbidden"), http.StatusForbidden
	}
	filter, err := h.filterFromRequest(r, tenantID, crossTenant)
	if err != nil {
		return Filter{}, h.deniedRecord(principal, tenantID, crossTenant, now, "invalid_filter"), http.StatusBadRequest
	}
	record := QueryRecord{QueryID: NewQueryID(), TenantID: tenantID, Subject: principal.SubjectID,
		CrossTenant: crossTenant, From: filter.From, To: filter.To, FilterDigest: FilterDigest(filter),
		ResultDigest: Digest(nil), Decision: "allowed", TraceID: r.Header.Get("X-Trace-ID"), OccurredAt: now}
	return filter, record, 0
}

func auditPath(path string) (parts []string, crossTenant, singleTenant bool) {
	parts = strings.Split(strings.Trim(path, "/"), "/")
	crossTenant = len(parts) == 3 && parts[0] == "v1" && parts[1] == "audit" && parts[2] == "events"
	singleTenant = len(parts) == 5 && parts[0] == "v1" && parts[1] == "tenants" && parts[3] == "audit" && parts[4] == "events"
	return parts, crossTenant, singleTenant
}

func (h Handler) filterFromRequest(r *http.Request, tenantID string, crossTenant bool) (Filter, error) {
	from, err := time.Parse(time.RFC3339, r.URL.Query().Get("from"))
	if err != nil {
		return Filter{}, ErrInvalidFilter
	}
	to, err := time.Parse(time.RFC3339, r.URL.Query().Get("to"))
	if err != nil {
		return Filter{}, ErrInvalidFilter
	}
	pageSize := 20
	if raw := r.URL.Query().Get("page_size"); raw != "" {
		pageSize, err = strconv.Atoi(raw)
		if err != nil {
			return Filter{}, ErrInvalidFilter
		}
	}
	filter := Filter{TenantID: tenantID, CrossTenant: crossTenant, From: from, To: to,
		PageSize: pageSize, Cursor: r.URL.Query().Get("cursor")}
	if err := ValidateFilter(filter, h.MaxWindow, h.MaxPage); err != nil {
		return Filter{}, err
	}
	return filter, nil
}

func (h Handler) now() time.Time {
	if h.Now != nil {
		return h.Now().UTC()
	}
	return time.Now().UTC()
}

func (h Handler) deniedRecord(principal Principal, tenantID string, crossTenant bool, at time.Time, reason string) QueryRecord {
	if tenantID == "" {
		tenantID = principal.TenantID
	}
	filter := Filter{TenantID: tenantID, CrossTenant: crossTenant, From: at, To: at}
	return QueryRecord{QueryID: NewQueryID(), TenantID: tenantID, Subject: principal.SubjectID,
		CrossTenant: crossTenant, From: at, To: at, FilterDigest: FilterDigest(filter),
		ResultDigest: Digest(nil), Decision: "denied", ReasonCode: reason, OccurredAt: at}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, reason string) {
	writeJSON(w, status, struct {
		Error  string `json:"error"`
		Reason string `json:"reason,omitempty"`
	}{http.StatusText(status), reason})
}
