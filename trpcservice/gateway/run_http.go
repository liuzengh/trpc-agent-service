package gateway

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/redaction"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const defaultRunBodyLimit = 1 << 20

// RunRoute is returned by trusted application/session authorization. The
// canonical user and session values are server-owned, never body-owned.
type RunRoute struct {
	Tenant    tenant.Context
	UserID    string
	SessionID string
}

type RunRouteResolver interface {
	ResolveRunRoute(r *http.Request, principal Principal, appRoute, requestedSessionID string) (RunRoute, error)
}

type RunHandler struct {
	Submitter  RunSubmitter
	Principals PrincipalResolver
	Routes     RunRouteResolver
	MaxBody    int64
}

type createRunPayload struct {
	AgentAppID string `json:"agent_app_id"`
	SessionID  string `json:"session_id"`
	Text       string `json:"text"`
}

func (h RunHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || strings.Trim(r.URL.Path, "/") != "v1/agent-runs" {
		http.NotFound(w, r)
		return
	}
	if h.Principals == nil || h.Routes == nil {
		writeControlError(w, runtime.ErrCapabilityUnsupported)
		return
	}
	principal, err := h.Principals.Resolve(r)
	if err != nil || !principal.Authenticated || principal.TenantID == "" || principal.SubjectID == "" {
		writeControlError(w, ErrUnauthenticated)
		return
	}
	if !principal.CanRun {
		writeControlError(w, ErrForbidden)
		return
	}
	if principal.RedactionProgram != nil {
		r = r.WithContext(redaction.ContextWithProgram(r.Context(), principal.RedactionProgram))
	}
	limit := h.MaxBody
	if limit <= 0 {
		limit = defaultRunBodyLimit
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, limit))
	decoder.DisallowUnknownFields()
	var payload createRunPayload
	if err := decoder.Decode(&payload); err != nil {
		writeControlError(w, runtime.ErrInvalidEnvelope)
		return
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeControlError(w, runtime.ErrInvalidEnvelope)
		return
	}
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if idempotencyKey == "" || payload.AgentAppID == "" || payload.Text == "" {
		writeControlError(w, runtime.ErrInvalidEnvelope)
		return
	}
	route, err := h.Routes.ResolveRunRoute(r, principal, payload.AgentAppID, payload.SessionID)
	if err != nil {
		writeControlError(w, err)
		return
	}
	if route.Tenant.TenantID != principal.TenantID || route.Tenant.SubjectID != principal.SubjectID ||
		route.Tenant.AgentAppID != payload.AgentAppID ||
		route.UserID == "" || route.SessionID == "" {
		writeControlError(w, ErrForbidden)
		return
	}
	handle, err := h.Submitter.Submit(r.Context(), RunSubmission{Tenant: route.Tenant, UserID: route.UserID,
		SessionID: route.SessionID, IdempotencyKey: idempotencyKey, Protocol: "http", Text: payload.Text,
		TraceParent: firstNonEmpty(principal.TraceParent, r.Header.Get("traceparent"))})
	if err != nil {
		writeControlError(w, err)
		return
	}
	writeControlJSON(w, http.StatusAccepted, handle)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
