package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/controlplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/governance"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/worker"
)

func TestHTTPExecutionAdmissionPreservesExecutionFailurePolicy(t *testing.T) {
	for _, tc := range []struct {
		name       string
		cause      error
		status     int
		retryAfter string
		retryable  bool
	}{
		{"execution running", controlplane.ErrExecutionInProgress, http.StatusConflict, "2", true},
		{"session running", controlplane.ErrSessionExecutionInProgress, http.StatusConflict, "2", true},
		{"unknown outcome", controlplane.ErrExecutionOutcomeUnknown, http.StatusLocked, "", false},
		{"unsafe retry", controlplane.ErrExecutionRetryUnsafe, http.StatusLocked, "", false},
		{"blocked session", controlplane.ErrSessionReconciliationRequired, http.StatusLocked, "", false},
		{"expired successful result", controlplane.ErrExecutionAlreadySucceeded, http.StatusGone, "", false},
		{"payload conflict", controlplane.ErrPayloadConflict, http.StatusConflict, "", false},
		{"identity conflict", controlplane.ErrRequestIdentityConflict, http.StatusConflict, "", false},
		{"version conflict", controlplane.ErrVersionBindingConflict, http.StatusConflict, "", false},
		{"database unavailable", errors.New("database unavailable"), http.StatusServiceUnavailable, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := preflightRequest()
			response := httptest.NewRecorder()
			starts := 0
			handle, admitted := admitHTTPExecutionWithApprovalGate(response, context.Background(), &req,
				governance.NewMemoryApprovalStore(), func() (controlplane.ExecutionHandle, error) {
					starts++
					return controlplane.ExecutionHandle{}, fmt.Errorf("admit: %w", tc.cause)
				})
			if admitted || handle.ID != 0 || starts != 1 {
				t.Fatalf("admitted=%v handle=%+v starts=%d", admitted, handle, starts)
			}
			if response.Code != tc.status || response.Header().Get("Retry-After") != tc.retryAfter {
				t.Fatalf("status=%d retry-after=%q, want %d %q", response.Code, response.Header().Get("Retry-After"), tc.status, tc.retryAfter)
			}
			statusError := &worker.HTTPStatusError{StatusCode: response.Code}
			if response.Header().Get("Retry-After") != "" {
				statusError.RetryAfter = 2 * time.Second
			}
			if statusError.Retryable() != tc.retryable {
				t.Fatalf("consumer retryable=%v, want %v", statusError.Retryable(), tc.retryable)
			}
		})
	}
}

func TestHTTPExecutionAdmissionKeepsApprovalPolicy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cause  error
		status int
	}{
		{"ambiguous", governance.ErrApprovalAmbiguous, http.StatusLocked},
		{"unavailable", governance.ErrApprovalStoreUnavailable, http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := preflightRequest()
			response := httptest.NewRecorder()
			_, admitted := admitHTTPExecutionWithApprovalGate(response, context.Background(), &req,
				staticPreflightApprovalStore{findErr: tc.cause}, func() (controlplane.ExecutionHandle, error) {
					t.Fatal("execution started after failed approval inspection")
					return controlplane.ExecutionHandle{}, nil
				})
			if admitted || response.Code != tc.status {
				t.Fatalf("admitted=%v status=%d, want false %d", admitted, response.Code, tc.status)
			}
		})
	}
	store := governance.NewMemoryApprovalStore()
	req := preflightRequest()
	challenge, err := store.CreateChallenge(context.Background(), preflightApprovalRequest(req), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	starts := 0
	start := func() (controlplane.ExecutionHandle, error) {
		starts++
		return controlplane.ExecutionHandle{ID: 42}, nil
	}
	response := httptest.NewRecorder()
	_, admitted := admitHTTPExecutionWithApprovalGate(response, context.Background(), &req, store, start)
	if admitted || starts != 0 || response.Code != http.StatusPreconditionRequired {
		t.Fatalf("pending approval admitted=%v starts=%d status=%d", admitted, starts, response.Code)
	}
	if _, err := store.Grant(context.Background(), challenge.ChallengeID, "operator"); err != nil {
		t.Fatal(err)
	}
	response = httptest.NewRecorder()
	handle, admitted := admitHTTPExecutionWithApprovalGate(response, context.Background(), &req, store, start)
	if !admitted || starts != 1 || handle.ID != 42 || req.ApprovalResumeChallengeID != challenge.ChallengeID || response.Body.Len() != 0 {
		t.Fatalf("granted approval admitted=%v starts=%d handle=%+v marker=%q body=%q", admitted, starts, handle, req.ApprovalResumeChallengeID, response.Body.String())
	}
}
