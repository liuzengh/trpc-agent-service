package platform

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
)

// RunHandler adapts JSON HTTP requests to a RunnerAdapter. Tenant identity is
// obtained from trusted middleware, never from the request body.
type RunHandler struct{ Runner RunnerAdapter }

// EchoRunner is the deterministic runner used by the stage-0 process. Later
// stages replace it with an adapter around trpc-agent-go without changing the
// HTTP contract.
type EchoRunner struct{}

func (EchoRunner) Run(ctx context.Context, req RunnerRequest) (RunnerResponse, error) {
	select {
	case <-ctx.Done():
		return RunnerResponse{}, ctx.Err()
	default:
		return RunnerResponse{Output: "echo:" + req.Input}, nil
	}
}

type runRequest struct {
	AppID     string `json:"app_id"`
	SessionID string `json:"session_id"`
	Input     string `json:"input"`
}
type runResponse struct {
	SessionID string `json:"session_id"`
	Output    string `json:"output,omitempty"`
}
type errorResponse struct {
	Error errorBody `json:"error"`
}
type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (h RunHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be POST")
		return
	}
	if h.Runner == nil {
		writeError(w, http.StatusInternalServerError, "runner_unavailable", "runner is not configured")
		return
	}
	var req runRequest
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil || req.AppID == "" || req.SessionID == "" || req.Input == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "app_id, session_id, and input are required")
		return
	}
	if _, ok := TenantContextFromContext(r.Context()); !ok {
		writeError(w, http.StatusUnauthorized, "tenant_context_missing", "trusted tenant context is required")
		return
	}
	if err := r.Context().Err(); err != nil {
		writeError(w, http.StatusRequestTimeout, "request_cancelled", "request context cancelled")
		return
	}
	result, err := h.Runner.Run(r.Context(), RunnerRequest{AppID: req.AppID, SessionID: req.SessionID, Input: req.Input})
	if err != nil {
		if errors.Is(err, r.Context().Err()) {
			writeError(w, http.StatusRequestTimeout, "request_cancelled", "request context cancelled")
			return
		}
		writeError(w, http.StatusBadGateway, "runner_error", "runner failed")
		return
	}
	writeJSON(w, http.StatusOK, runResponse{SessionID: req.SessionID, Output: result.Output})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorResponse{Error: errorBody{Code: code, Message: message}})
}
