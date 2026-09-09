// Package operations serves tenant-scoped operational metadata without business content.
package operations

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/adminauth"
)

// Item is the explicit metadata allowlist shared by operation views. Database
// payloads, credentials, configuration snapshots and raw errors never enter it.
type Item struct {
	ID            string     `json:"id"`
	TenantID      string     `json:"tenantId"`
	Status        string     `json:"status"`
	CreatedAt     time.Time  `json:"createdAt"`
	Name          string     `json:"name,omitempty"`
	AppID         string     `json:"appId,omitempty"`
	AppName       string     `json:"appName,omitempty"`
	VersionID     string     `json:"versionId,omitempty"`
	DeploymentID  string     `json:"deploymentId,omitempty"`
	VersionNumber *int64     `json:"versionNumber,omitempty"`
	Kind          string     `json:"kind,omitempty"`
	TrafficBps    *int       `json:"trafficBps,omitempty"`
	ChannelType   string     `json:"channelType,omitempty"`
	AttemptCount  *int       `json:"attemptCount,omitempty"`
	MaxAttempts   *int       `json:"maxAttempts,omitempty"`
	AttemptNumber *int       `json:"attemptNumber,omitempty"`
	RetrySafe     *bool      `json:"retrySafe,omitempty"`
	InboxID       string     `json:"inboxId,omitempty"`
	LeaseVersion  string     `json:"leaseVersion,omitempty"`
	TraceID       string     `json:"traceId,omitempty"`
	Action        string     `json:"action,omitempty"`
	Actor         string     `json:"actor,omitempty"`
	ResourceType  string     `json:"resourceType,omitempty"`
	ResourceID    string     `json:"resourceId,omitempty"`
	Domain        string     `json:"domain,omitempty"`
	Phase         string     `json:"phase,omitempty"`
	Paused        *bool      `json:"paused,omitempty"`
	UpdatedAt     *time.Time `json:"updatedAt,omitempty"`
	PublishedAt   *time.Time `json:"publishedAt,omitempty"`
	StartedAt     *time.Time `json:"startedAt,omitempty"`
	CompletedAt   *time.Time `json:"completedAt,omitempty"`
	DeliveredAt   *time.Time `json:"deliveredAt,omitempty"`
}

// Page uses an opaque continuation token. IDs are strings to preserve BIGINTs
// through JavaScript clients.
type Page struct {
	Items      []Item `json:"items"`
	NextCursor string `json:"nextCursor,omitempty"`
}

type Counts struct {
	Apps        int64 `json:"apps"`
	Versions    int64 `json:"versions"`
	Deployments int64 `json:"deployments"`
	Inbox       int64 `json:"inbox"`
	Outbox      int64 `json:"outbox"`
	Executions  int64 `json:"executions"`
}
type StateCount struct {
	Status string `json:"status"`
	Count  int64  `json:"count"`
}
type Overview struct {
	Counts          Counts       `json:"counts"`
	InboxStates     []StateCount `json:"inboxStates"`
	OutboxStates    []StateCount `json:"outboxStates"`
	ExecutionStates []StateCount `json:"executionStates"`
}
type RequestDetail struct {
	Inbox      Item   `json:"inbox"`
	Executions []Item `json:"executions"`
	Outbox     []Item `json:"outbox"`
	Audit      []Item `json:"audit"`
	Truncated  bool   `json:"truncated"`
}

type handler struct{ db *sql.DB }

// NewHandler serves complete /api/v1/operations/... paths. The composition
// root authenticates principals; this handler independently enforces read
// permission and tenant scope before any database query.
func NewHandler(db *sql.DB) http.Handler { return &handler{db: db} }

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	principal, err := adminauth.PrincipalFromContext(r.Context())
	if err != nil {
		fail(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if !principal.Has(adminauth.PermissionTenantRead) {
		fail(w, http.StatusForbidden, "forbidden")
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		fail(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	const prefix = "/api/v1/operations/"
	if !strings.HasPrefix(r.URL.Path, prefix) {
		fail(w, http.StatusNotFound, "not found")
		return
	}
	view := strings.TrimPrefix(r.URL.Path, prefix)
	requestID := ""
	if strings.HasPrefix(view, "requests/") {
		requestID = strings.TrimPrefix(view, "requests/")
		if !validID(requestID, true) {
			fail(w, http.StatusBadRequest, "invalid request id")
			return
		}
		view = "requests"
	} else if view != "overview" {
		if _, ok := views[view]; !ok {
			fail(w, http.StatusNotFound, "not found")
			return
		}
	}
	query, err := parseQuery(r.URL.RawQuery, view)
	if err != nil {
		fail(w, http.StatusBadRequest, "invalid query")
		return
	}
	if _, err := adminauth.RequireTenant(r.Context(), query.tenant); err != nil {
		fail(w, http.StatusForbidden, "forbidden")
		return
	}
	// Cursors are pagination positions, never authority. Tenant authorization
	// and explicit tenant predicates apply even to a forged same-scope token.
	if err := query.parseCursor(view); err != nil {
		fail(w, http.StatusBadRequest, "invalid cursor")
		return
	}
	if h.db == nil {
		fail(w, http.StatusServiceUnavailable, "operations unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	var body any
	switch view {
	case "overview":
		body, err = h.overview(ctx, query.tenant)
	case "requests":
		body, err = h.detail(ctx, query.tenant, requestID)
	default:
		body, err = list(ctx, h.db, view, query, "", nil)
	}
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			fail(w, http.StatusNotFound, "not found")
		} else {
			fail(w, http.StatusServiceUnavailable, "operations unavailable")
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}
func fail(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Error string `json:"error"`
	}{message})
}

type query struct {
	tenant, status, cursor string
	limit                  int
	after                  *position
}

var errQuery = errors.New("invalid query")

func parseQuery(raw, view string) (query, error) {
	q := query{limit: 50}
	values, err := url.ParseQuery(raw)
	if err != nil {
		return q, errQuery
	}
	for key, vals := range values {
		if len(vals) != 1 || (key != "tenantId" && key != "status" && key != "cursor" && key != "limit") {
			return q, errQuery
		}
		if view == "overview" || view == "requests" {
			if key != "tenantId" {
				return q, errQuery
			}
		}
		if vals[0] == "" {
			return q, errQuery
		}
	}
	q.tenant = values.Get("tenantId")
	if q.tenant == "" || len(q.tenant) > 64 || strings.TrimSpace(q.tenant) != q.tenant || strings.ContainsAny(q.tenant, "/\\\r\n\x00") {
		return q, errQuery
	}
	q.status = values.Get("status")
	if len(q.status) > 64 {
		return q, errQuery
	}
	for _, c := range q.status {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '.' || c == '-') {
			return q, errQuery
		}
	}
	q.cursor = values.Get("cursor")
	if len(q.cursor) > 2048 {
		return q, errQuery
	}
	if text := values.Get("limit"); text != "" {
		q.limit, err = strconv.Atoi(text)
		if err != nil || q.limit < 1 || q.limit > 100 {
			return q, errQuery
		}
	}
	return q, nil
}
