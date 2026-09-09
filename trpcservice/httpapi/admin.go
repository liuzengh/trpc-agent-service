package httpapi

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/control"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type AdminTask struct {
	TaskID            string    `json:"task_id"`
	TenantID          string    `json:"tenant_id"`
	AgentAppID        string    `json:"agent_app_id"`
	Channel           string    `json:"channel"`
	BindingID         string    `json:"binding_id"`
	PlatformMessageID string    `json:"platform_message_id"`
	State             string    `json:"state"`
	Attempt           int       `json:"attempt"`
	PersistAttempt    int       `json:"persist_attempt"`
	NodeID            string    `json:"node_id,omitempty"`
	AssignmentState   string    `json:"assignment_state,omitempty"`
	ErrorCode         string    `json:"error_code,omitempty"`
	RequestID         string    `json:"request_id,omitempty"`
	TraceID           string    `json:"trace_id,omitempty"`
	OutboundState     string    `json:"outbound_state,omitempty"`
	ReceivedAt        time.Time `json:"received_at"`
}

type AdminConfig struct {
	Repository          control.Repository
	Token               string
	AssignmentOverrider interface {
		Override(context.Context, string, string) (control.NodeAssignment, error)
	}
	TaskLookup       func(context.Context, string, string) (map[string]any, error)
	TaskList         func(context.Context, int) ([]AdminTask, error)
	OutboundLookup   func(context.Context, string) (map[string]any, error)
	ReconcilerStatus func(context.Context) (map[string]any, error)
	Catalog          tenant.Catalog
}

func NewHandlerWithAdmin(backend Backend, admin AdminConfig) http.Handler {
	if admin.Repository == nil {
		return NewHandler(backend)
	}
	root := http.NewServeMux()
	registerHealth(root, backend)
	digestV2 := traceDigestV2Enabled(backend)
	root.HandleFunc("/api/v1/demo/messages", messageHandler(backend, digestV2))
	if async, ok := backend.(asyncBackend); ok {
		root.HandleFunc("/api/v1/web/messages", webMessageHandler(async, digestV2))
		root.HandleFunc("/api/v1/web/messages/", webSnapshotHandler(async))
		root.Handle("/", webUIHandler())
	}
	root.Handle("/api/v1/admin/", adminHandlerWithLookups(admin.Repository, admin.AssignmentOverrider, control.NewAdminAuthenticator(admin.Token), admin.TaskLookup, admin.TaskList, admin.OutboundLookup, admin.ReconcilerStatus, admin.Catalog))
	root.Handle("/admin/", adminUIHandler())
	return root
}

func adminUIHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		spanName := "admin.query"
		if r.Method != http.MethodGet {
			spanName = "admin.mutation"
		}
		ctx, span := telemetry.Start(r.Context(), spanName)
		defer span.End()
		r = r.WithContext(ctx)
		if r.URL.Path != "/admin/" && r.URL.Path != "/admin" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(adminUIHTML))
	})
}

//go:embed static/admin.html
var adminUIHTML string

func adminHandler(repository control.Repository, overrider interface {
	Override(context.Context, string, string) (control.NodeAssignment, error)
}, authenticator control.AdminAuthenticator) http.Handler {
	return adminHandlerWithLookups(repository, overrider, authenticator, nil, nil, nil, nil, tenant.Catalog{})
}

func adminHandlerWithLookups(repository control.Repository, overrider interface {
	Override(context.Context, string, string) (control.NodeAssignment, error)
}, authenticator control.AdminAuthenticator, taskLookup func(context.Context, string, string) (map[string]any, error), taskList func(context.Context, int) ([]AdminTask, error), outboundLookup func(context.Context, string) (map[string]any, error), reconcilerStatus func(context.Context) (map[string]any, error), catalog tenant.Catalog) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := authenticator.Authenticate(r.Header.Get("Authorization")); err != nil {
			if errors.Is(err, control.ErrAdminAPIDisabled) {
				writeError(w, http.StatusServiceUnavailable, "admin_api_disabled", "admin API is disabled", "", "")
			} else {
				writeError(w, http.StatusUnauthorized, "admin_unauthorized", "admin authorization failed", "", "")
			}
			return
		}
		parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/admin/"), "/"), "/")
		if len(parts) == 1 && parts[0] == "catalog" && r.Method == http.MethodGet {
			writeAdminCatalog(w, catalog)
			return
		}
		if len(parts) == 1 && parts[0] == "tasks" && r.Method == http.MethodGet {
			if taskList == nil {
				adminUnavailable(w)
				return
			}
			values, err := taskList(r.Context(), parseLimit(r.URL.Query().Get("limit")))
			if err != nil {
				adminUnavailable(w)
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"items": values})
			return
		}
		if len(parts) == 1 && parts[0] == "reconciler" && r.Method == http.MethodGet {
			if reconcilerStatus == nil {
				adminUnavailable(w)
				return
			}
			value, err := reconcilerStatus(r.Context())
			if err != nil {
				adminUnavailable(w)
				return
			}
			writeJSON(w, http.StatusOK, value)
			return
		}
		if len(parts) == 3 && parts[0] == "tasks" && r.Method == http.MethodGet {
			if taskLookup == nil {
				adminUnavailable(w)
				return
			}
			value, err := taskLookup(r.Context(), parts[1], parts[2])
			if err != nil {
				adminUnavailable(w)
				return
			}
			writeJSON(w, http.StatusOK, value)
			return
		}
		if len(parts) == 2 && parts[0] == "outbound" && r.Method == http.MethodGet {
			if outboundLookup == nil {
				adminUnavailable(w)
				return
			}
			value, err := outboundLookup(r.Context(), parts[1])
			if err != nil {
				adminUnavailable(w)
				return
			}
			writeJSON(w, http.StatusOK, value)
			return
		}
		if len(parts) == 1 && parts[0] == "nodes" && r.Method == http.MethodGet {
			nodes, err := repository.ListNodes(r.Context())
			if err != nil {
				adminUnavailable(w)
				return
			}
			writeJSON(w, http.StatusOK, nodes)
			return
		}
		if len(parts) == 1 && parts[0] == "audit" && r.Method == http.MethodGet {
			queryAudit(w, r, repository)
			return
		}
		if len(parts) == 1 && parts[0] == "metrics" && r.Method == http.MethodGet {
			queryMetrics(w, r, repository)
			return
		}
		if len(parts) == 2 && parts[0] == "assignments" {
			if r.Method == http.MethodGet {
				assignmentGet(w, r, repository, parts[1])
				return
			}
			if r.Method == http.MethodPut {
				assignmentPut(w, r, repository, overrider, parts[1])
				return
			}
		}
		if len(parts) == 3 && parts[0] == "tenants" {
			switch parts[2] {
			case "placement":
				tenantPlacement(w, r, repository, parts[1])
			case "policy":
				tenantPolicy(w, r, repository, parts[1])
			case "redaction":
				tenantRedaction(w, r, repository, parts[1])
			default:
				http.NotFound(w, r)
			}
			return
		}
		http.NotFound(w, r)
	})
}

func writeAdminCatalog(w http.ResponseWriter, catalog tenant.Catalog) {
	type bindingSummary struct {
		ID                string `json:"id"`
		Channel           string `json:"channel"`
		ExternalAccountID string `json:"external_account_id"`
		TenantID          string `json:"tenant_id"`
		AgentAppID        string `json:"agent_app_id"`
		Enabled           bool   `json:"enabled"`
	}
	bindings := make([]bindingSummary, 0, len(catalog.ChannelBindings))
	for _, binding := range catalog.ChannelBindings {
		bindings = append(bindings, bindingSummary{
			ID: binding.ID, Channel: binding.Channel, ExternalAccountID: binding.ExternalAccountID,
			TenantID: binding.TenantID, AgentAppID: binding.AgentAppID, Enabled: binding.Enabled,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"tenants": catalog.Tenants, "bindings": bindings})
}

func adminUnavailable(w http.ResponseWriter) {
	writeError(w, http.StatusServiceUnavailable, "control_plane_unavailable", "control plane unavailable", "", "")
}

func tenantPlacement(w http.ResponseWriter, r *http.Request, repo control.Repository, tenantID string) {
	switch r.Method {
	case http.MethodGet:
		value, err := repo.GetPlacement(r.Context(), tenantID)
		if err != nil {
			adminUnavailable(w)
			return
		}
		writeJSON(w, http.StatusOK, value)
	case http.MethodPut:
		var input struct {
			Mode             control.PlacementMode `json:"mode"`
			NodeID           string                `json:"node_id"`
			ExpectedRevision int64                 `json:"expected_revision"`
		}
		if !decodeAdminJSON(w, r, &input) {
			return
		}
		value := control.TenantPlacement{TenantID: tenantID, Mode: input.Mode, NodeID: input.NodeID}
		updated, err := repo.PutPlacement(r.Context(), value, input.ExpectedRevision)
		if err != nil {
			writeAdminMutationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, updated)
	default:
		methodNotAllowed(w)
	}
}

func tenantPolicy(w http.ResponseWriter, r *http.Request, repo control.Repository, tenantID string) {
	switch r.Method {
	case http.MethodGet:
		value, err := repo.GetPolicy(r.Context(), tenantID)
		if err != nil {
			adminUnavailable(w)
			return
		}
		writeJSON(w, http.StatusOK, value)
	case http.MethodPut:
		var body struct {
			control.TenantPolicy
			ExpectedRevision int64 `json:"expected_revision"`
		}
		if !decodeAdminJSON(w, r, &body) {
			return
		}
		body.TenantPolicy.TenantID = tenantID
		updated, err := repo.PutPolicy(r.Context(), body.TenantPolicy, body.ExpectedRevision)
		if err != nil {
			writeAdminMutationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, updated)
	default:
		methodNotAllowed(w)
	}
}

func tenantRedaction(w http.ResponseWriter, r *http.Request, repo control.Repository, tenantID string) {
	if r.Method == http.MethodGet {
		policy, err := repo.GetPolicy(r.Context(), tenantID)
		if err != nil {
			adminUnavailable(w)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"tenant_id": tenantID, "revision": policy.Revision, "patterns": policy.RedactionPatterns})
		return
	}
	if r.Method != http.MethodPut {
		methodNotAllowed(w)
		return
	}
	var input struct {
		Patterns         []string `json:"patterns"`
		ExpectedRevision int64    `json:"expected_revision"`
	}
	if !decodeAdminJSON(w, r, &input) {
		return
	}
	current, err := repo.GetPolicy(r.Context(), tenantID)
	if err != nil {
		adminUnavailable(w)
		return
	}
	current.RedactionPatterns = append(current.RedactionPatterns, input.Patterns...)
	updated, err := repo.PutPolicy(r.Context(), current, input.ExpectedRevision)
	if err != nil {
		writeAdminMutationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tenant_id": tenantID, "revision": updated.Revision, "patterns": updated.RedactionPatterns})
}

func assignmentGet(w http.ResponseWriter, r *http.Request, repo control.Repository, inboxID string) {
	value, err := repo.GetAssignment(r.Context(), inboxID)
	if err != nil {
		adminUnavailable(w)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func assignmentPut(w http.ResponseWriter, r *http.Request, repo control.Repository, overrider interface {
	Override(context.Context, string, string) (control.NodeAssignment, error)
}, inboxID string) {
	var input struct {
		NodeID           string `json:"node_id"`
		ExpectedRevision int64  `json:"expected_revision"`
	}
	if !decodeAdminJSON(w, r, &input) {
		return
	}
	if overrider != nil {
		updated, err := overrider.Override(r.Context(), inboxID, input.NodeID)
		if err != nil {
			writeAdminMutationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, updated)
		return
	}
	current, err := repo.GetAssignment(r.Context(), inboxID)
	if err != nil {
		adminUnavailable(w)
		return
	}
	if current.State != control.AssignmentPlanned && current.State != control.AssignmentNodeWait && current.State != control.AssignmentBlocked {
		writeError(w, http.StatusConflict, "assignment_not_overridable", "assignment is not overridable", "", "")
		return
	}
	node, err := repo.GetNode(r.Context(), input.NodeID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "node_not_found", "target node is not registered", "", "")
		return
	}
	placement, err := repo.GetPlacement(r.Context(), current.TenantID)
	if err != nil {
		adminUnavailable(w)
		return
	}
	if placement.Mode == control.PlacementDedicated && placement.NodeID != input.NodeID {
		writeError(w, http.StatusConflict, "assignment_not_overridable", "dedicated placement requires its node", "", "")
		return
	}
	current.NodeID = input.NodeID
	current.State = control.AssignmentPlanned
	current.BlockedReason = ""
	if node.State == control.NodeOffline || !node.LeaseUntil.After(time.Now()) {
		current.State = control.AssignmentBlocked
		current.BlockedReason = "dedicated_node_offline"
	}
	updated, err := repo.PutAssignment(r.Context(), current, input.ExpectedRevision)
	if err != nil {
		writeAdminMutationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

func queryAudit(w http.ResponseWriter, r *http.Request, repo control.Repository) {
	from, err := parseTime(r.URL.Query().Get("from"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid from time", "", "")
		return
	}
	to, err := parseTime(r.URL.Query().Get("to"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid to time", "", "")
		return
	}
	q := control.AuditQuery{TenantID: r.URL.Query().Get("tenant_id"), Decision: r.URL.Query().Get("decision"), ToolName: r.URL.Query().Get("tool_name"), ErrorType: r.URL.Query().Get("error_type"), Cursor: r.URL.Query().Get("cursor"), Limit: parseLimit(r.URL.Query().Get("limit"))}
	q.From, q.To = from, to
	values, cursor, err := repo.QueryAudit(r.Context(), q)
	if err != nil {
		adminUnavailable(w)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": values, "cursor": cursor})
}

func queryMetrics(w http.ResponseWriter, r *http.Request, repo control.Repository) {
	from, err := parseTime(r.URL.Query().Get("from"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid from time", "", "")
		return
	}
	to, err := parseTime(r.URL.Query().Get("to"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid to time", "", "")
		return
	}
	q := control.MetricQuery{TenantID: r.URL.Query().Get("tenant_id"), AgentAppID: r.URL.Query().Get("agent_app_id"), Cursor: r.URL.Query().Get("cursor"), Limit: parseLimit(r.URL.Query().Get("limit"))}
	q.From, q.To = from, to
	values, cursor, err := repo.QueryMetrics(r.Context(), q)
	if err != nil {
		adminUnavailable(w)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": values, "cursor": cursor})
}

func parseLimit(raw string) int {
	if raw == "" {
		return control.MaxQueryLimit
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 || value > control.MaxQueryLimit {
		return control.MaxQueryLimit
	}
	return value
}
func parseTime(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, raw)
}
func methodNotAllowed(w http.ResponseWriter) {
	writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "", "")
}
func decodeAdminJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	if err := decodeBody(r, target); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid request", "", "")
		return false
	}
	return true
}
func decodeBody(r *http.Request, target any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("request must contain one JSON object")
		}
		return err
	}
	return nil
}
func writeAdminMutationError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, control.ErrRevisionConflict):
		writeError(w, http.StatusConflict, "revision_conflict", "revision conflict", "", "")
	case errors.Is(err, control.ErrAssignmentNotOverridable):
		writeError(w, http.StatusConflict, "assignment_not_overridable", "assignment is not overridable", "", "")
	default:
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid control-plane update", "", "")
	}
}
