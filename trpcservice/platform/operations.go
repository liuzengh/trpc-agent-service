package platform

import (
	"context"
	"net/http"
	"strings"
	"time"
)

type DrainState string

const (
	DrainIdle     DrainState = "idle"
	DrainDraining DrainState = "draining"
	DrainClosed   DrainState = "closed"
	DrainFailed   DrainState = "failed"
)

type DrainStatus struct {
	State            DrainState `json:"state"`
	StartedAt        time.Time  `json:"started_at,omitempty"`
	CompletedAt      time.Time  `json:"completed_at,omitempty"`
	Error            string     `json:"error,omitempty"`
	ActiveExecutions int64      `json:"active_executions"`
}

type drainLifecycle interface {
	BeginShutdown()
	Wait(context.Context) error
}

func (h *AdminHandler) handleOperationsDrain(w http.ResponseWriter, r *http.Request, tenant TenantContext) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET or POST")
		return
	}
	if !canOperate(tenant.Role) {
		writeError(w, http.StatusForbidden, "forbidden", "operator role is required")
		return
	}
	if r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, h.drainStatus(tenant))
		return
	}

	var request struct {
		Confirm bool `json:"confirm"`
	}
	if err := decodeStrict(r, &request); err != nil || !request.Confirm {
		writeError(w, http.StatusBadRequest, "drain_confirmation_required", "drain confirmation is required")
		return
	}
	h.mu.Lock()
	if h.drainState != DrainIdle {
		state := h.drainState
		h.mu.Unlock()
		code := "drain_in_progress"
		status := http.StatusConflict
		if state == DrainClosed {
			code, status = "drain_completed", http.StatusConflict
		} else if state == DrainFailed {
			code, status = "drain_failed", http.StatusServiceUnavailable
		}
		writeError(w, status, code, "service drain cannot be started")
		return
	}
	life, ok := h.life.(drainLifecycle)
	if !ok {
		h.mu.Unlock()
		writeError(w, http.StatusServiceUnavailable, "drain_unavailable", "graceful drain is not configured")
		return
	}
	requestID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
	if !validIdempotencyKey(requestID) {
		requestID = newRequestID()
	}
	traceID := newTraceID()
	auditCtx, cancelAudit := context.WithTimeout(context.Background(), 2*time.Second)
	auditErr := h.governance.Record(auditCtx, AuditEvent{
		TenantID: tenant.TenantID, UserID: tenant.UserID, Decision: "operations.drain.started",
		RequestID: requestID, TraceID: traceID, OccurredAt: time.Now().UTC(),
	})
	cancelAudit()
	if auditErr != nil {
		h.mu.Unlock()
		writeError(w, http.StatusServiceUnavailable, "audit_unavailable", "audit service is unavailable")
		return
	}
	h.drainState = DrainDraining
	h.drainStartedAt = time.Now().UTC()
	h.mu.Unlock()

	life.BeginShutdown()

	go func() {
		err := life.Wait(context.Background())
		h.mu.Lock()
		h.drainCompletedAt = time.Now().UTC()
		if err != nil {
			h.drainState = DrainFailed
			h.drainError = "drain_timeout"
		} else {
			h.drainState = DrainClosed
		}
		h.mu.Unlock()
		if err != nil {
			return
		}
		_ = h.Close()
	}()
	writeJSON(w, http.StatusAccepted, h.drainStatus(tenant))
}

func (h *AdminHandler) handleOperationsFaults(w http.ResponseWriter, r *http.Request, tenant TenantContext) {
	if r.Method == http.MethodGet {
		h.mu.Lock()
		enabled := h.faultInjectionEnabled
		h.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{"enabled": enabled, "scenarios": []string{"none", "runner_delay", "runner_error", "tool_error"}})
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET or POST")
		return
	}
	h.mu.Lock()
	enabled := h.faultInjectionEnabled
	h.mu.Unlock()
	if !enabled {
		writeError(w, http.StatusForbidden, "fault_injection_disabled", "fault injection is disabled")
		return
	}
	if !canOperate(tenant.Role) {
		writeError(w, http.StatusForbidden, "forbidden", "operator role is required")
		return
	}
	var request struct {
		AgentAppID string `json:"agent_app_id"`
		Scenario   string `json:"scenario"`
		DelayMS    int    `json:"delay_ms"`
	}
	if err := decodeStrict(r, &request); err != nil || !validResourceID(request.AgentAppID) || request.DelayMS < 0 || request.DelayMS > 5000 {
		writeError(w, http.StatusBadRequest, "invalid_fault_injection", "fault injection request is invalid")
		return
	}
	switch request.Scenario {
	case "none", "runner_delay", "runner_error", "tool_error":
	default:
		writeError(w, http.StatusBadRequest, "invalid_fault_injection", "fault injection scenario is invalid")
		return
	}
	h.runtime.SetFault(tenant.TenantID, request.AgentAppID, RuntimeFaultConfiguration{Scenario: request.Scenario, DelayMS: request.DelayMS})
	writeJSON(w, http.StatusOK, RuntimeFaultConfiguration{Scenario: request.Scenario, DelayMS: request.DelayMS})
}

func (h *AdminHandler) drainStatus(tenant TenantContext) DrainStatus {
	h.mu.Lock()
	status := DrainStatus{
		State: h.drainState, StartedAt: h.drainStartedAt,
		CompletedAt: h.drainCompletedAt, Error: h.drainError,
	}
	h.mu.Unlock()
	if components := h.runtime.StatusFor(tenant); len(components) > 0 {
		status.ActiveExecutions = components[0].Active
	}
	return status
}
