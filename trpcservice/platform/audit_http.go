package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

type auditResponseWriter struct {
	http.ResponseWriter
	status   int
	tenant   TenantContext
	buffered bool
	body     bytes.Buffer
}

func (w *auditResponseWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	if !w.buffered {
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *auditResponseWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if w.buffered {
		return w.body.Write(data)
	}
	return w.ResponseWriter.Write(data)
}
func (w *auditResponseWriter) Flush() {
	if w.buffered {
		return
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *auditResponseWriter) commit() {
	if !w.buffered {
		return
	}
	status := w.status
	if status == 0 {
		status = http.StatusOK
	}
	w.ResponseWriter.WriteHeader(status)
	_, _ = w.ResponseWriter.Write(w.body.Bytes())
}

func (w *auditResponseWriter) auditUnavailable() {
	w.status = http.StatusServiceUnavailable
	w.body.Reset()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(&w.body).Encode(errorResponse{Error: errorBody{Code: "audit_unavailable", Message: "audit service is unavailable"}})
}
func markAuditIdentity(w http.ResponseWriter, tenant TenantContext) {
	if audit, ok := w.(*auditResponseWriter); ok {
		audit.tenant = tenant
	}
}

func (h *AdminHandler) auditHTTPRequest(ctx context.Context, r *http.Request, response *auditResponseWriter, started time.Time) error {
	status := response.status
	if status == 0 {
		status = http.StatusOK
	}
	if r.Method == http.MethodGet && status < http.StatusBadRequest && r.URL.Path != "/api/v1/auth/me" {
		return nil
	}
	if r.Method != http.MethodGet && status < http.StatusBadRequest && strings.HasPrefix(r.URL.Path, "/api/v1/admin/governance/") {
		return nil
	}
	decision := "http.allowed"
	errorType := ""
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		decision, errorType = "authorization.denied", "forbidden"
	} else if status >= http.StatusBadRequest {
		decision, errorType = "http.failed", "request_failed"
	}
	requestID := r.Header.Get("X-Request-ID")
	if !validIdempotencyKey(requestID) {
		requestID = ""
	}
	return h.governance.Record(ctx, AuditEvent{TenantID: response.tenant.TenantID, UserID: response.tenant.UserID, Decision: decision, ErrorType: errorType, RequestID: requestID, TraceID: newTraceID(), Latency: time.Since(started), OccurredAt: time.Now().UTC()})
}
