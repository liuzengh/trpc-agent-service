package platform

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeRunner struct{ err error }

func (f fakeRunner) Run(ctx context.Context, req RunnerRequest) (RunnerResponse, error) {
	if f.err != nil {
		return RunnerResponse{}, f.err
	}
	return RunnerResponse{Output: "echo:" + req.Input}, nil
}

func TestRunHandlerSuccessAndTenantInjection(t *testing.T) {
	h := RunHandler{Runner: fakeRunner{}}
	r := httptest.NewRequest(http.MethodPost, "/run", strings.NewReader(`{"app_id":"app","session_id":"s","input":"hi"}`))
	r = r.WithContext(WithTenantContext(r.Context(), TenantContext{TenantID: "t"}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	var got runResponse
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Output != "echo:hi" || got.SessionID != "s" {
		t.Fatalf("response = %#v", got)
	}
}

func TestRunHandlerValidationAndRunnerError(t *testing.T) {
	ctx := WithTenantContext(context.Background(), TenantContext{TenantID: "t"})
	for _, tc := range []struct {
		body   string
		runner error
		status int
		code   string
	}{{"{}", nil, 400, "invalid_request"}, {`{"app_id":"a","session_id":"s","input":"x"}`, errors.New("boom"), 502, "runner_error"}} {
		r := httptest.NewRequest(http.MethodPost, "/run", strings.NewReader(tc.body)).WithContext(ctx)
		w := httptest.NewRecorder()
		(RunHandler{Runner: fakeRunner{err: tc.runner}}).ServeHTTP(w, r)
		var got errorResponse
		_ = json.NewDecoder(w.Body).Decode(&got)
		if w.Code != tc.status || got.Error.Code != tc.code {
			t.Fatalf("got %d/%q, want %d/%q", w.Code, got.Error.Code, tc.status, tc.code)
		}
	}
}

func TestRunHandlerRequiresTrustedTenantContext(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/run", strings.NewReader(`{"app_id":"a","session_id":"s","input":"x"}`))
	w := httptest.NewRecorder()
	(RunHandler{Runner: fakeRunner{}}).ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", w.Code)
	}
}

func TestRunHandlerRejectsCancelledRequestBeforeRunner(t *testing.T) {
	ctx, cancel := context.WithCancel(WithTenantContext(context.Background(), TenantContext{TenantID: "t"}))
	cancel()
	r := httptest.NewRequest(http.MethodPost, "/run", strings.NewReader(`{"app_id":"a","session_id":"s","input":"x"}`)).WithContext(ctx)
	w := httptest.NewRecorder()
	(RunHandler{Runner: fakeRunner{}}).ServeHTTP(w, r)
	var got errorResponse
	_ = json.NewDecoder(w.Body).Decode(&got)
	if w.Code != http.StatusRequestTimeout || got.Error.Code != "request_cancelled" {
		t.Fatalf("got %d/%q, want %d/request_cancelled", w.Code, got.Error.Code, http.StatusRequestTimeout)
	}
}
