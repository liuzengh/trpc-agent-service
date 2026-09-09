package platform

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

type toolGovernanceCall struct {
	Request GovernanceRequest `json:"request"`
	TraceID string            `json:"trace_id"`
	Tool    string            `json:"tool"`
	Args    []byte            `json:"arguments,omitempty"`
	Error   string            `json:"error,omitempty"`
}

type RemoteToolGovernance struct {
	endpoint string
	token    string
	client   *http.Client
}

func NewRemoteToolGovernance(endpoint, token string) *RemoteToolGovernance {
	return &RemoteToolGovernance{endpoint: strings.TrimRight(endpoint, "/"), token: token, client: &http.Client{}}
}

func (c *RemoteToolGovernance) AuthorizeTool(ctx context.Context, request GovernanceRequest, traceID, tool string, args []byte) error {
	return c.call(ctx, "authorize", toolGovernanceCall{Request: request, TraceID: traceID, Tool: tool, Args: args})
}

func (c *RemoteToolGovernance) CompleteTool(ctx context.Context, request GovernanceRequest, traceID, tool string, runErr error) error {
	call := toolGovernanceCall{Request: request, TraceID: traceID, Tool: tool}
	if runErr != nil {
		call.Error = runErr.Error()
	}
	return c.call(ctx, "complete", call)
}

func (c *RemoteToolGovernance) call(ctx context.Context, operation string, call toolGovernanceCall) error {
	if c.endpoint == "" {
		return &GovernanceError{Code: "governance_unavailable", TraceID: call.TraceID}
	}
	payload, _ := json.Marshal(call)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/internal/governance/tool/"+operation, bytes.NewReader(payload))
	if err != nil {
		return &GovernanceError{Code: "governance_unavailable", TraceID: call.TraceID}
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	request.Header.Set("Content-Type", "application/json")
	response, err := c.client.Do(request)
	if err != nil {
		return &GovernanceError{Code: "governance_unavailable", TraceID: call.TraceID}
	}
	defer response.Body.Close()
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return nil
	}
	var result struct {
		Error struct {
			Code           string `json:"code"`
			TraceID        string `json:"trace_id"`
			ConfirmationID string `json:"confirmation_id"`
		} `json:"error"`
	}
	_ = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result)
	if result.Error.Code == "" {
		result.Error.Code = "governance_unavailable"
	}
	return &GovernanceError{Code: result.Error.Code, TraceID: result.Error.TraceID, ConfirmationID: result.Error.ConfirmationID}
}

func (h *AdminHandler) ConfigureInternalGovernance(token string) { h.internalGovernanceToken = token }

func (h *AdminHandler) internalRequestAuthorized(r *http.Request) bool {
	provided := []byte(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	expected := []byte(h.internalGovernanceToken)
	return len(expected) > 0 && subtle.ConstantTimeCompare(provided, expected) == 1
}

func (h *AdminHandler) handleInternalGovernance(w http.ResponseWriter, r *http.Request) {
	if !h.internalRequestAuthorized(r) {
		writeError(w, http.StatusUnauthorized, "governance_unauthorized", "governance authorization failed")
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be POST")
		return
	}
	var call toolGovernanceCall
	if err := decodeStrict(r, &call); err != nil || call.Request.TenantID == "" || call.Request.RequestID == "" || call.Tool == "" {
		writeError(w, http.StatusBadRequest, "invalid_governance_request", "governance request is invalid")
		return
	}
	var err error
	switch strings.TrimPrefix(r.URL.Path, "/internal/governance/tool/") {
	case "authorize":
		err = h.governance.AuthorizeTool(r.Context(), call.Request, call.TraceID, call.Tool, call.Args)
	case "complete":
		var runErr error
		if call.Error != "" {
			runErr = errors.New(call.Error)
		}
		err = h.governance.CompleteTool(r.Context(), call.Request, call.TraceID, call.Tool, runErr)
	default:
		writeError(w, http.StatusNotFound, "not_found", "governance endpoint was not found")
		return
	}
	if err != nil {
		var governanceErr *GovernanceError
		if errors.As(err, &governanceErr) {
			writeJSON(w, http.StatusConflict, map[string]any{"error": map[string]string{
				"code": governanceErr.Code, "trace_id": governanceErr.TraceID, "confirmation_id": governanceErr.ConfirmationID,
			}})
			return
		}
		writeError(w, http.StatusServiceUnavailable, "governance_unavailable", "governance service is unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *AdminHandler) handleInternalMetrics(w http.ResponseWriter, r *http.Request) {
	if !h.internalRequestAuthorized(r) {
		writeError(w, http.StatusUnauthorized, "metrics_unauthorized", "metrics authorization failed")
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET")
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, h.governance.PrometheusMetrics())
}
