package gateway

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/redaction"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

var (
	ErrUnauthenticated = errors.New("gateway principal is not authenticated")
	ErrForbidden       = errors.New("gateway principal is not authorized for tenant")
)

// Principal is the already-authenticated identity supplied by the service's
// auth middleware. The gateway never derives tenant scope from request JSON.
type Principal struct {
	Authenticated bool
	TenantID      string
	TenantVersion int64
	SubjectID     string
	UserID        string
	AgentAppID    string
	SessionID     string
	CanRead       bool
	CanCancel     bool
	CanRun        bool
	TraceParent   string
	// RedactionProgram is derived only by a trusted principal resolver from
	// the tenant version it has just authorized. HTTP handlers attach it to
	// downstream context; request payloads never choose a logging policy.
	RedactionProgram *redaction.Program
}

type PrincipalResolver interface {
	Resolve(*http.Request) (Principal, error)
}

// ControlHandler exposes the minimum operator/user control plane for an
// execution: authoritative status reads and durable cancellation intent.
// Authentication and authorization remain injected so the package does not
// couple storage to a particular identity provider.
type ControlHandler struct {
	Tasks      TaskStore
	Principals PrincipalResolver
}

// ExecutionHandler is kept as a descriptive alias for callers that mount the
// status/cancel routes under an execution-oriented router.
type ExecutionHandler = ControlHandler

type cancelPayload struct {
	ExpectedVersion int64  `json:"expected_version"`
	ReasonCode      string `json:"reason_code"`
}

func (h ControlHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.Tasks == nil || h.Principals == nil {
		writeControlError(w, runtime.ErrCapabilityUnsupported)
		return
	}
	principal, err := h.Principals.Resolve(r)
	if err != nil {
		writeControlError(w, ErrUnauthenticated)
		return
	}
	if !principal.Authenticated || principal.TenantID == "" || principal.SubjectID == "" {
		writeControlError(w, ErrUnauthenticated)
		return
	}
	tenantID, requestID, action, ok := parseControlPath(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if tenantID == "" {
		tenantID = principal.TenantID
	}
	if tenantID != principal.TenantID {
		writeControlError(w, ErrForbidden)
		return
	}
	key := ExecutionKey{TenantID: tenantID, RequestID: requestID}
	switch {
	case r.Method == http.MethodGet && action == "":
		if !principal.CanRead {
			writeControlError(w, ErrForbidden)
			return
		}
		status, err := h.Tasks.GetExecution(r.Context(), key)
		if err != nil {
			writeControlError(w, err)
			return
		}
		writeControlJSON(w, http.StatusOK, status)
	case r.Method == http.MethodPost && action == "cancel":
		if !principal.CanCancel {
			writeControlError(w, ErrForbidden)
			return
		}
		var payload cancelPayload
		body := http.MaxBytesReader(w, r.Body, 64<<10)
		defer body.Close()
		decoder := json.NewDecoder(body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&payload); err != nil {
			if !errors.Is(err, io.EOF) {
				writeControlError(w, runtime.ErrInvalidEnvelope)
				return
			}
			if value := r.URL.Query().Get("expected_version"); value != "" {
				parsed, parseErr := strconv.ParseInt(value, 10, 64)
				if parseErr != nil {
					writeControlError(w, runtime.ErrVersionConflict)
					return
				}
				payload.ExpectedVersion = parsed
			}
		}
		var extra any
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			writeControlError(w, runtime.ErrInvalidEnvelope)
			return
		}
		if payload.ExpectedVersion < 0 {
			writeControlError(w, runtime.ErrVersionConflict)
			return
		}
		if payload.ReasonCode == "" {
			payload.ReasonCode = r.Header.Get("X-Reason-Code")
		}
		if strings.TrimSpace(payload.ReasonCode) == "" {
			writeControlError(w, runtime.ErrCommitConflict)
			return
		}
		traceParent := principal.TraceParent
		if traceParent == "" {
			traceParent = r.Header.Get("traceparent")
		}
		result, err := h.Tasks.RequestCancel(r.Context(), CancelRequest{
			TenantID: tenantID, RequestID: requestID, ExpectedVersion: payload.ExpectedVersion,
			ActorID: principal.SubjectID, ReasonCode: payload.ReasonCode, TraceParent: traceParent,
		})
		if err != nil {
			writeControlError(w, err)
			return
		}
		status := http.StatusAccepted
		if !result.Accepted {
			status = http.StatusOK
		}
		writeControlJSON(w, status, result)
	default:
		http.NotFound(w, r)
	}
}

func parseControlPath(path string) (tenantID, requestID, action string, ok bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	switch {
	case len(parts) == 3 && parts[0] == "v1" && parts[1] == "agent-runs":
		requestID, action := splitControlAction(parts[2])
		return "", requestID, action, requestID != ""
	case len(parts) == 4 && parts[0] == "v1" && parts[1] == "agent-runs" && parts[3] == "cancel":
		return "", parts[2], "cancel", parts[2] != ""
	case len(parts) == 5 && parts[0] == "v1" && parts[1] == "tenants" && parts[3] == "agent-runs":
		requestID, action := splitControlAction(parts[4])
		return parts[2], requestID, action, parts[2] != "" && requestID != ""
	case len(parts) == 6 && parts[0] == "v1" && parts[1] == "tenants" && parts[3] == "agent-runs" && parts[5] == "cancel":
		return parts[2], parts[4], "cancel", parts[2] != "" && parts[4] != ""
	default:
		return "", "", "", false
	}
}

func splitControlAction(value string) (string, string) {
	if strings.HasSuffix(value, ":cancel") {
		return strings.TrimSuffix(value, ":cancel"), "cancel"
	}
	return value, ""
}

func writeControlJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeControlError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	switch {
	case errors.Is(err, ErrUnauthenticated):
		status = http.StatusUnauthorized
	case errors.Is(err, ErrForbidden), errors.Is(err, runtime.ErrTenantScope):
		status = http.StatusForbidden
	case errors.Is(err, runtime.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, runtime.ErrVersionConflict), errors.Is(err, runtime.ErrCommitConflict), errors.Is(err, runtime.ErrIdempotencyCollision):
		status = http.StatusConflict
	case errors.Is(err, runtime.ErrCancelRequested):
		status = http.StatusConflict
	case errors.Is(err, runtime.ErrCapabilityUnsupported), errors.Is(err, runtime.ErrBackendUnavailable):
		status = http.StatusServiceUnavailable
	}
	http.Error(w, http.StatusText(status), status)
}
