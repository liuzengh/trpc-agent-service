package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/controlplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/worker"
)

func TestParseExecutionTimeoutRejectsUnsafeOperatorConfig(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		want      time.Duration
		wantErr   bool
		wantErrIs error
	}{
		{name: "default", want: worker.DefaultExecutionTimeout},
		{name: "valid", value: "2m", want: 2 * time.Minute},
		{name: "malformed", value: "eventually", wantErr: true},
		{name: "too short", value: "500ms", wantErr: true, wantErrIs: worker.ErrInvalidExecutionTimeout},
		{name: "too long", value: "16m", wantErr: true, wantErrIs: worker.ErrInvalidExecutionTimeout},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseExecutionTimeout(test.value)
			if test.wantErr {
				if err == nil {
					t.Fatal("unsafe timeout was accepted")
				}
				if test.wantErrIs != nil && !errors.Is(err, test.wantErrIs) {
					t.Fatalf("error=%v, want %v", err, test.wantErrIs)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("timeout=%s error=%v, want %s", got, err, test.want)
			}
		})
	}
}

func TestClassifyWorkerProcessFailurePreservesSideEffectBoundary(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		code      string
		retrySafe bool
		status    int
	}{
		{
			name: "preflight deadline is retryable", err: worker.ErrExecutionPreflightTimedOut,
			code: "execution_preflight_timeout", retrySafe: true, status: http.StatusServiceUnavailable,
		},
		{
			name: "transient preflight failure is retryable", err: worker.ErrExecutionPreflight,
			code: "execution_preflight_failed", retrySafe: true, status: http.StatusServiceUnavailable,
		},
		{
			name: "permanent preflight rejection is terminal", err: worker.ErrExecutionPreflightPermanent,
			code: "execution_preflight_rejected", retrySafe: false, status: http.StatusBadRequest,
		},
		{
			name: "runner deadline requires reconciliation", err: worker.ErrExecutionTimedOut,
			code: "execution_timeout", retrySafe: false, status: http.StatusLocked,
		},
		{
			name: "ordinary worker error remains conservative", err: errors.New("worker failed"),
			code: "execution_outcome_unknown", retrySafe: false, status: http.StatusLocked,
		},
		{
			name: "dispatched execution overrides nested preflight marker", err: errors.Join(worker.ErrWorkerExecutionOutcomeUnknown, worker.ErrExecutionPreflight),
			code: "execution_outcome_unknown", retrySafe: false, status: http.StatusLocked,
		},
		{
			name: "dispatched deadline preserves timeout diagnosis", err: errors.Join(worker.ErrWorkerExecutionOutcomeUnknown, worker.ErrExecutionTimedOut),
			code: "execution_timeout", retrySafe: false, status: http.StatusLocked,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := classifyWorkerProcessFailure(test.err)
			if got.code != test.code || got.safeToRetry != test.retrySafe || got.statusCode != test.status {
				t.Fatalf("classification=%+v", got)
			}
		})
	}
}

func TestClassifyWorkerPostRunnerFailureAlwaysRequiresReconciliation(t *testing.T) {
	for _, code := range []string{"response_encoding_failed", "empty_worker_response", "result_persistence_failed", ""} {
		t.Run(code, func(t *testing.T) {
			failure := classifyWorkerPostRunnerFailure(code)
			if failure.safeToRetry {
				t.Fatal("post-Runner failure was classified as retry-safe")
			}
			if failure.statusCode != http.StatusLocked {
				t.Fatalf("status=%d, want %d", failure.statusCode, http.StatusLocked)
			}
			if failure.message == "" || failure.code == "" {
				t.Fatalf("incomplete post-Runner classification: %+v", failure)
			}
		})
	}
}

func TestClassifyWorkerInitializationFailurePreservesPreflightTimeout(t *testing.T) {
	deadlineCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	for _, test := range []struct {
		name      string
		ctx       context.Context
		err       error
		code      string
		status    int
		retrySafe bool
	}{
		{
			name: "returned deadline", ctx: context.Background(), err: context.DeadlineExceeded,
			code: "execution_preflight_timeout", status: http.StatusServiceUnavailable, retrySafe: true,
		},
		{
			name: "parent deadline", ctx: deadlineCtx, err: context.Canceled,
			code: "execution_preflight_timeout", status: http.StatusServiceUnavailable, retrySafe: true,
		},
		{
			name: "cache saturated", ctx: context.Background(), err: worker.ErrCacheSaturated,
			code: "worker_capacity_exhausted", status: http.StatusServiceUnavailable, retrySafe: true,
		},
		{
			name: "ordinary construction", ctx: context.Background(), err: errors.New("build failed"),
			code: "worker_initialization_failed", status: http.StatusInternalServerError, retrySafe: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := classifyWorkerInitializationFailure(test.ctx, test.err)
			if got.code != test.code || got.statusCode != test.status || got.safeToRetry != test.retrySafe {
				t.Fatalf("classification=%+v", got)
			}
		})
	}
}

type preflightTenantReader struct {
	value *tenant.Tenant
	err   error
}

func (r *preflightTenantReader) GetTenant(context.Context, string) (*tenant.Tenant, error) {
	return r.value, r.err
}

const validWorkerRequest = `{
	"tenantId":"tenant-a",
	"channelType":"telegram",
	"conversationId":"chat-a",
	"messageId":"message-a",
	"agentApp":"assistant",
	"idempotencyKey":"inbox:1",
	"payloadHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	"userId":"user-a",
	"sessionId":"session-a",
	"content":"hello"
}`

func TestDecodeWorkerRequestStrictJSONBoundary(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		body        string
		want        int
	}{
		{"valid", "application/json", validWorkerRequest, http.StatusOK},
		{"media type parameters", "application/json; charset=utf-8", validWorkerRequest, http.StatusOK},
		{"missing content type", "", validWorkerRequest, http.StatusUnsupportedMediaType},
		{"unknown field", "application/json", strings.TrimSuffix(validWorkerRequest, "\n}") + `,"unexpected":true}`, http.StatusBadRequest},
		{"multiple values", "application/json", validWorkerRequest + `{}`, http.StatusBadRequest},
		{"oversized", "application/json", `{"content":"` + strings.Repeat("x", maxWorkerRequestBytes) + `"}`, http.StatusRequestEntityTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/process", strings.NewReader(test.body))
			if test.contentType != "" {
				request.Header.Set("Content-Type", test.contentType)
			}
			response := httptest.NewRecorder()
			var decoded worker.Request
			ok := decodeWorkerRequest(response, request, &decoded)
			if test.want == http.StatusOK {
				if !ok {
					t.Fatalf("valid request rejected: status=%d body=%s", response.Code, response.Body.String())
				}
				if err := validateWorkerRequest(decoded); err != nil {
					t.Fatalf("valid request failed validation: %v", err)
				}
				return
			}
			if ok || response.Code != test.want {
				t.Fatalf("ok=%v status=%d want=%d", ok, response.Code, test.want)
			}
		})
	}
}

func TestValidateWorkerExecutionContractFailsClosedBeforeProcessing(t *testing.T) {
	configured := 90 * time.Second
	valid, err := worker.NewExecutionContract(configured)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		contract *worker.ExecutionContract
		want     int
	}{
		{name: "valid", contract: &valid, want: http.StatusOK},
		{name: "missing", want: http.StatusServiceUnavailable},
		{
			name: "drifted",
			contract: &worker.ExecutionContract{
				Version: worker.ExecutionContractVersion,
				Timeout: "2m",
			},
			want: http.StatusServiceUnavailable,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := &worker.Request{ExecutionContract: test.contract}
			ok := validateWorkerExecutionContract(recorder, request, configured)
			if test.want == http.StatusOK {
				if !ok {
					t.Fatalf("valid contract rejected: status=%d", recorder.Code)
				}
				return
			}
			if ok || recorder.Code != test.want || recorder.Header().Get("Retry-After") == "" {
				t.Fatalf("ok=%v status=%d retry-after=%q", ok, recorder.Code, recorder.Header().Get("Retry-After"))
			}
		})
	}
}

func TestVersionedProcessRouteValidatesContractBeforeTenantWork(t *testing.T) {
	configured := 90 * time.Second
	valid, err := worker.NewExecutionContract(configured)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		contract  *worker.ExecutionContract
		want      int
		wantCalls int
	}{
		{name: "missing", want: http.StatusServiceUnavailable},
		{
			name: "drifted",
			contract: &worker.ExecutionContract{
				Version: worker.ExecutionContractVersion,
				Timeout: "2m",
			},
			want: http.StatusServiceUnavailable,
		},
		{name: "valid", contract: &valid, want: http.StatusNoContent, wantCalls: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			tenantCalls := 0
			inner := http.NewServeMux()
			inner.HandleFunc(worker.ExecutionContractProcessPath, func(writer http.ResponseWriter, request *http.Request) {
				var decoded worker.Request
				if !decodeWorkerRequest(writer, request, &decoded) {
					return
				}
				if !validateWorkerExecutionContract(writer, &decoded, configured) {
					return
				}
				tenantCalls++
				writer.WriteHeader(http.StatusNoContent)
			})
			outer := http.NewServeMux()
			registerWorkerProcessRoutes(outer, inner)

			body, err := json.Marshal(worker.Request{ExecutionContract: test.contract})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, worker.ExecutionContractProcessPath, strings.NewReader(string(body)))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			outer.ServeHTTP(response, request)
			if response.Code != test.want || tenantCalls != test.wantCalls {
				t.Fatalf("status=%d tenantCalls=%d, want status=%d calls=%d", response.Code, tenantCalls, test.want, test.wantCalls)
			}
		})
	}
}

func TestLegacyProcessRouteCannotBypassExecutionContract(t *testing.T) {
	inner := http.NewServeMux()
	inner.HandleFunc("/process", writeLegacyProcessUnavailable)
	outer := http.NewServeMux()
	registerWorkerProcessRoutes(outer, inner)

	request := httptest.NewRequest(http.MethodPost, "/process", strings.NewReader(validWorkerRequest))
	response := httptest.NewRecorder()
	outer.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || response.Header().Get("Retry-After") == "" {
		t.Fatalf("status=%d retry-after=%q", response.Code, response.Header().Get("Retry-After"))
	}
}

func TestValidateWorkerRequestRejectsNonCanonicalHashAndControlCharacters(t *testing.T) {
	request := worker.Request{
		TenantID: "tenant-a", ChannelType: "telegram", ConversationID: "chat-a",
		MessageID: "message-a", AgentApp: "assistant", IdempotencyKey: "inbox:1",
		PayloadHash: strings.Repeat("g", 64), UserID: "user-a", SessionID: "session-a",
	}
	if err := validateWorkerRequest(request); err == nil {
		t.Fatal("non-hex hash accepted")
	}
	request.PayloadHash = strings.Repeat("a", 64)
	request.SessionID = "session\nforged"
	if err := validateWorkerRequest(request); err == nil {
		t.Fatal("control character accepted")
	}
}

func TestValidateWorkerRequestUsesAgentAppByteLimit(t *testing.T) {
	request := worker.Request{
		TenantID: "tenant-a", ChannelType: "telegram", ConversationID: "chat-a",
		MessageID: "message-a", IdempotencyKey: "inbox:1",
		PayloadHash: strings.Repeat("a", 64), UserID: "user-a", SessionID: "session-a",
	}
	request.AgentApp = strings.Repeat("a", 128)
	if err := validateWorkerRequest(request); err != nil {
		t.Fatalf("128-byte agent app rejected: %v", err)
	}
	request.AgentApp += "a"
	if err := validateWorkerRequest(request); err == nil {
		t.Fatal("129-byte agent app accepted")
	}
	request.AgentApp = "客服"
	if err := validateWorkerRequest(request); err == nil {
		t.Fatal("non-slug agent app accepted")
	}
}

func TestValidateWorkerRequestMatchesSessionBackendKeyLimits(t *testing.T) {
	request := worker.Request{
		TenantID: "tenant-a", ChannelType: "telegram", ConversationID: "chat-a",
		MessageID: "message-a", AgentApp: "assistant", IdempotencyKey: "inbox:1",
		PayloadHash: strings.Repeat("a", 64), UserID: strings.Repeat("u", 255),
		SessionID: strings.Repeat("s", 255),
	}
	if err := validateWorkerRequest(request); err != nil {
		t.Fatalf("backend maximum identifiers rejected: %v", err)
	}
	request.UserID += "u"
	if err := validateWorkerRequest(request); err == nil {
		t.Fatal("256-byte user ID accepted by VARCHAR(255) session backend")
	}
	request.UserID = "user"
	request.SessionID += "s"
	if err := validateWorkerRequest(request); err == nil {
		t.Fatal("256-byte session ID accepted by VARCHAR(255) session backend")
	}
}

func TestAuthorizeWorkerRequestRejectsSuspendedTenant(t *testing.T) {
	request := worker.Request{TenantID: "tenant-a", IdempotencyKey: "inbox:1", PayloadHash: strings.Repeat("a", 64)}

	_, err := authorizeWorkerRequest(
		context.Background(),
		&preflightTenantReader{value: &tenant.Tenant{ID: "tenant-a", Status: tenant.TenantStatusSuspended}},
		request,
	)
	if !errors.Is(err, errTenantSuspended) {
		t.Fatalf("preflight error = %v, want errTenantSuspended", err)
	}
}

func TestAuthorizeWorkerRequestDistinguishesMissingAndUnavailableTenant(t *testing.T) {
	request := worker.Request{TenantID: "tenant-a", IdempotencyKey: "inbox:1", PayloadHash: strings.Repeat("a", 64)}

	_, missingErr := authorizeWorkerRequest(
		context.Background(),
		&preflightTenantReader{err: tenant.ErrTenantNotFound},
		request,
	)
	if !errors.Is(missingErr, errTenantNotFound) || errors.Is(missingErr, errTenantLookup) {
		t.Fatalf("missing tenant error=%v, want only errTenantNotFound", missingErr)
	}

	databaseErr := errors.New("database temporarily unavailable")
	_, unavailableErr := authorizeWorkerRequest(
		context.Background(),
		&preflightTenantReader{err: databaseErr},
		request,
	)
	if !errors.Is(unavailableErr, errTenantLookup) || errors.Is(unavailableErr, errTenantNotFound) ||
		!errors.Is(unavailableErr, databaseErr) {
		t.Fatalf("unavailable tenant error=%v, want retryable lookup classification", unavailableErr)
	}
}

func TestWriteWorkerAuthorizationErrorPreservesRetryAndReconciliationSemantics(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		status int
	}{
		{name: "missing is permanent", err: errTenantNotFound, status: http.StatusNotFound},
		{name: "lookup failure is retryable", err: errTenantLookup, status: http.StatusServiceUnavailable},
		{name: "suspended waits for reconciliation", err: errTenantSuspended, status: http.StatusLocked},
		{name: "unsupported status is permanent", err: errTenantNotActive, status: http.StatusForbidden},
		{name: "unsafe storage is retryable", err: errTenantStorageUnsafe, status: http.StatusServiceUnavailable},
		{name: "unknown failure is retryable", err: errors.New("unknown"), status: http.StatusServiceUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			writeWorkerAuthorizationError(recorder, "tenant-a", test.err)
			if recorder.Code != test.status {
				t.Fatalf("status=%d, want %d", recorder.Code, test.status)
			}
		})
	}
}

func TestAuthorizeWorkerRequestAcceptsActiveTenantWithDistributedStorage(t *testing.T) {
	request := worker.Request{TenantID: "tenant-a", IdempotencyKey: "inbox:1", PayloadHash: strings.Repeat("a", 64)}

	gotTenant, err := authorizeWorkerRequest(
		context.Background(),
		&preflightTenantReader{value: &tenant.Tenant{
			ID:     "tenant-a",
			Status: tenant.TenantStatusActive,
			Storage: tenant.StorageConfig{
				SessionBackend: "redis",
				MemoryBackend:  "postgres",
			},
		}},
		request,
	)
	if err != nil {
		t.Fatal(err)
	}
	if gotTenant == nil || gotTenant.ID != "tenant-a" {
		t.Fatalf("active tenant authorization = %#v", gotTenant)
	}
}

func TestWriteExecutionStartErrorPreservesSessionAdmissionSemantics(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		status     int
		retryAfter string
	}{
		{
			name:       "same session is retryable",
			err:        controlplane.ErrSessionExecutionInProgress,
			status:     http.StatusConflict,
			retryAfter: "2",
		},
		{
			name:   "session requires operator reconciliation",
			err:    controlplane.ErrSessionReconciliationRequired,
			status: http.StatusLocked,
		},
		{
			name:   "unknown attempt outcome requires operator reconciliation",
			err:    controlplane.ErrExecutionOutcomeUnknown,
			status: http.StatusLocked,
		},
		{
			name:   "unsafe retry requires operator reconciliation",
			err:    controlplane.ErrExecutionRetryUnsafe,
			status: http.StatusLocked,
		},
		{
			name:   "idempotency conflict remains permanent",
			err:    controlplane.ErrPayloadConflict,
			status: http.StatusConflict,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			writeExecutionStartError(response, test.err)
			if response.Code != test.status {
				t.Fatalf("status=%d, want %d; body=%s", response.Code, test.status, response.Body.String())
			}
			if got := response.Header().Get("Retry-After"); got != test.retryAfter {
				t.Fatalf("Retry-After=%q, want %q", got, test.retryAfter)
			}
		})
	}
}

func TestEncodeWorkerResponseRejectsNilSuccess(t *testing.T) {
	encoded, err := encodeWorkerResponse(nil)
	if !errors.Is(err, errEmptyWorkerResponse) || encoded != nil {
		t.Fatalf("nil response encoded as %q with error %v", encoded, err)
	}

	encoded, err = encodeWorkerResponse(&worker.Response{ContentType: "text", SessionID: "session-a", Content: "ok"})
	if err != nil || string(encoded) == "null" {
		t.Fatalf("valid response encoded as %q with error %v", encoded, err)
	}
}
