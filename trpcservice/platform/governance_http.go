package platform

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func (h *AdminHandler) handleGovernance(w http.ResponseWriter, r *http.Request) {
	tenant, ok := trustedTenant(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "identity_required", "authenticated identity is required")
		return
	}
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/admin/governance/"), "/")
	switch {
	case path == "policy":
		h.handleGovernancePolicy(w, r, tenant)
	case path == "audit":
		h.handleGovernanceAudit(w, r, tenant)
	case path == "metrics":
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET")
			return
		}
		from, fromErr := parseOptionalRFC3339(r.URL.Query().Get("from"))
		to, toErr := parseOptionalRFC3339(r.URL.Query().Get("to"))
		if fromErr != nil || toErr != nil {
			writeError(w, http.StatusBadRequest, "invalid_metrics_query", "metrics query is invalid")
			return
		}
		metrics, err := h.governance.QueryMetrics(MetricsQuery{
			TenantID: tenant.TenantID, AgentAppID: strings.TrimSpace(r.URL.Query().Get("app_id")),
			Provider: strings.TrimSpace(r.URL.Query().Get("provider")), From: from, To: to,
		})
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_metrics_query", "metrics query is invalid")
			return
		}
		writeJSON(w, http.StatusOK, metrics)
	case path == "traces":
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET")
			return
		}
		trace, found := h.governance.Trace(tenant.TenantID, strings.TrimSpace(r.URL.Query().Get("trace_id")), strings.TrimSpace(r.URL.Query().Get("request_id")))
		if !found {
			writeError(w, http.StatusNotFound, "trace_not_found", "trace was not found")
			return
		}
		writeJSON(w, http.StatusOK, trace)
	case path == "confirmations":
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET")
			return
		}
		confirmations := h.governance.Confirmations(tenant.TenantID)
		for _, confirmation := range confirmations {
			if confirmation.Status != ConfirmationExpired || confirmation.SessionID == "" {
				continue
			}
			eventCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			_ = h.appendConfirmationSessionEvent(eventCtx, confirmation)
			cancel()
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": confirmations})
	case strings.HasPrefix(path, "confirmations/") && strings.HasSuffix(path, "/decision"):
		h.handleConfirmationDecision(w, r, tenant, strings.TrimSuffix(strings.TrimPrefix(path, "confirmations/"), "/decision"))
	default:
		http.NotFound(w, r)
	}
}

func parseOptionalRFC3339(value string) (time.Time, error) {
	if strings.TrimSpace(value) == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339, value)
}

func (h *AdminHandler) handleGovernancePolicy(w http.ResponseWriter, r *http.Request, tenant TenantContext) {
	switch r.Method {
	case http.MethodGet:
		appID := strings.TrimSpace(r.URL.Query().Get("app_id"))
		policy, found, err := h.governance.Policy(r.Context(), tenant.TenantID, appID)
		if writeControlPlaneError(w, err) {
			return
		}
		if !found {
			writeError(w, http.StatusNotFound, "policy_not_found", "governance policy was not found")
			return
		}
		writeJSON(w, http.StatusOK, publicPolicy(policy))
	case http.MethodPost, http.MethodPut:
		if !canMutate(tenant.Role) {
			writeError(w, http.StatusForbidden, "forbidden", "tenant administrator role is required")
			return
		}
		var policy TenantPolicy
		if err := decodeStrict(r, &policy); err != nil || policy.AgentAppID == "" {
			writeError(w, http.StatusBadRequest, "invalid_policy", "governance policy is invalid")
			return
		}
		if _, found, err := h.platform.app(r.Context(), tenant.TenantID, policy.AgentAppID); err != nil || !found {
			if writeControlPlaneError(w, err) {
				return
			}
			writeError(w, http.StatusNotFound, "agent_app_not_found", "Agent App was not found")
			return
		}
		policy.TenantID = tenant.TenantID
		if existing, found, err := h.governance.Policy(r.Context(), tenant.TenantID, policy.AgentAppID); err != nil {
			writeControlPlaneError(w, err)
			return
		} else if found {
			replacements := []string{}
			for _, value := range policy.RedactedPatterns {
				if value != "[REDACTED]" {
					replacements = append(replacements, value)
				}
			}
			if len(replacements) == 0 {
				policy.RedactedPatterns = existing.RedactedPatterns
			} else {
				policy.RedactedPatterns = replacements
			}
		}
		updated, err := h.governance.PutPolicy(r.Context(), policy)
		if err != nil {
			if errors.Is(err, errControlPlaneUnavailable) {
				writeControlPlaneError(w, err)
				return
			}
			if err.Error() == "invalid_policy" {
				writeError(w, http.StatusBadRequest, "invalid_policy", "governance policy is invalid")
			} else {
				writeError(w, http.StatusServiceUnavailable, "audit_unavailable", "audit service is unavailable")
			}
			return
		}
		writeJSON(w, http.StatusOK, publicPolicy(updated))
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET, POST, or PUT")
	}
}

func publicPolicy(policy TenantPolicy) TenantPolicy {
	for index := range policy.RedactedPatterns {
		policy.RedactedPatterns[index] = "[REDACTED]"
	}
	return policy
}

func (h *AdminHandler) handleGovernanceAudit(w http.ResponseWriter, r *http.Request, tenant TenantContext) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET")
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	from, _ := time.Parse(time.RFC3339, r.URL.Query().Get("from"))
	to, _ := time.Parse(time.RFC3339, r.URL.Query().Get("to"))
	query := AuditQuery{
		TenantID: tenant.TenantID, Channel: r.URL.Query().Get("channel"), AgentName: r.URL.Query().Get("agent_name"),
		Decision: r.URL.Query().Get("decision"), ErrorType: r.URL.Query().Get("error_type"), UserID: r.URL.Query().Get("user_id"),
		SessionID: r.URL.Query().Get("session_id"), RequestID: r.URL.Query().Get("request_id"), TraceID: r.URL.Query().Get("trace_id"),
		From: from, To: to, Offset: offset, Limit: limit,
	}
	items, err := h.governance.QueryAuditEvents(r.Context(), query)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "audit_unavailable", "audit service is unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h *AdminHandler) handleConfirmationDecision(w http.ResponseWriter, r *http.Request, tenant TenantContext, id string) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be POST")
		return
	}
	if !canOperate(tenant.Role) {
		writeError(w, http.StatusForbidden, "forbidden", "operator role is required")
		return
	}
	var request struct {
		Approve bool `json:"approve"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_confirmation_decision", "confirmation decision is invalid")
		return
	}
	confirmation, err := h.governance.DecideConfirmation(r.Context(), tenant.TenantID, id, tenant.UserID, request.Approve)
	if errors.Is(err, ErrNotFound) {
		writeError(w, http.StatusNotFound, "confirmation_not_found", "confirmation was not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "audit_unavailable", "audit service is unavailable")
		return
	}
	if err := h.appendConfirmationSessionEvent(r.Context(), confirmation); err != nil {
		writeError(w, http.StatusServiceUnavailable, "storage_error", "confirmation event could not be persisted")
		return
	}
	if !h.waitForChatRunExit(r.Context(), confirmation) {
		writeError(w, http.StatusServiceUnavailable, "request_cancelled", "confirmation decision was interrupted")
		return
	}
	if confirmation.Status == ConfirmationRejected || confirmation.Status == ConfirmationExpired {
		if err := h.finalizeRejectedConfirmation(r.Context(), confirmation); err != nil {
			writeError(w, http.StatusServiceUnavailable, "storage_error", "rejected confirmation could not be finalized")
			return
		}
	}
	writeJSON(w, http.StatusOK, confirmation)
}

func (h *AdminHandler) waitForChatRunExit(ctx context.Context, confirmation ToolConfirmation) bool {
	key := chatRunKey(confirmation.TenantID, confirmation.SessionID, confirmation.RequestID)
	h.chatMu.Lock()
	active, running := h.activeRuns[key]
	h.chatMu.Unlock()
	if !running || active.done == nil {
		return true
	}
	select {
	case <-active.done:
		return true
	case <-ctx.Done():
		return false
	}
}

func (h *AdminHandler) finalizeRejectedConfirmation(ctx context.Context, confirmation ToolConfirmation) error {
	if _, err := completeGovernance(ctx, h.governance, GovernanceCompletion{
		TenantID: confirmation.TenantID, AgentAppID: confirmation.AgentAppID, RequestID: confirmation.RequestID,
		UserID: confirmation.UserID, SessionID: confirmation.SessionID, ErrorType: "confirmation_rejected",
	}); err != nil {
		return err
	}
	store, release, err := h.acquireStore(ctx, confirmation.TenantID)
	if err != nil {
		return err
	}
	defer release()
	payload := map[string]string{
		"tenant_id": confirmation.TenantID, "app_id": confirmation.AgentAppID, "session_id": confirmation.SessionID,
		"user_id": confirmation.UserID, "request_id": confirmation.RequestID, "trace_id": confirmation.TraceID,
		"policy_revision": strconv.FormatUint(confirmation.PolicyRevision, 10), "error": "confirmation_rejected",
	}
	return h.appendCriticalChatEvent(ctx, store, confirmation.TenantID, confirmation.SessionID, confirmation.RequestID+":terminal", "run.failed", payload)
}

func (h *AdminHandler) appendConfirmationSessionEvent(ctx context.Context, confirmation ToolConfirmation) error {
	store, release, err := h.acquireStore(ctx, confirmation.TenantID)
	if err != nil {
		return err
	}
	defer release()
	payload := map[string]string{
		"tenant_id": confirmation.TenantID, "app_id": confirmation.AgentAppID, "session_id": confirmation.SessionID,
		"request_id": confirmation.RequestID, "trace_id": confirmation.TraceID, "confirmation_id": confirmation.ID,
		"tool_name": confirmation.ToolName, "argument_summary": confirmation.ArgumentSummary, "status": string(confirmation.Status),
	}
	return h.appendCriticalChatEvent(ctx, store, confirmation.TenantID, confirmation.SessionID, confirmation.RequestID+":confirmation-"+string(confirmation.Status), "tool.confirmation."+string(confirmation.Status), payload)
}
