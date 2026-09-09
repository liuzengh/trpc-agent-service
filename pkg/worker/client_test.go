package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"strings"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/auth"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/governance"
)

type acceptingNonceStore struct{}

func (acceptingNonceStore) UseOnce(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestHTTPClientRequiresBoundedJSONResponse(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		body        string
		wantError   bool
	}{
		{"valid", "application/json", `{"contentType":"text","sessionId":"s","content":"ok"}`, false},
		{"missing content type", "", `{"content":"ok"}`, true},
		{"unknown field", "application/json", `{"content":"ok","unexpected":true}`, true},
		{"multiple values", "application/json", `{"content":"ok"}{}`, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if test.contentType != "" {
					writer.Header().Set("Content-Type", test.contentType)
				}
				_, _ = writer.Write([]byte(test.body))
			}))
			defer server.Close()
			client := NewHTTPClientWithTimeout(server.URL, time.Second)
			_, err := client.ProcessMessage(context.Background(), &Request{})
			if (err != nil) != test.wantError {
				t.Fatalf("error=%v wantError=%v", err, test.wantError)
			}
		})
	}
}

func TestHTTPClientParsesApprovalExpiryWithoutExposingBody(t *testing.T) {
	expiresAt := time.Now().UTC().Add(5 * time.Minute).Truncate(time.Second)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Retry-After", "5")
		writer.WriteHeader(http.StatusPreconditionRequired)
		_, _ = writer.Write([]byte(`{"error":"tool_approval_required","challenge_id":"challenge-a","expires_at":"` + expiresAt.Format(time.RFC3339) + `"}`))
	}))
	defer server.Close()
	client := NewHTTPClientWithTimeout(server.URL, time.Second)
	_, err := client.ProcessMessage(context.Background(), &Request{})
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr == nil {
		t.Fatalf("error=%v, want HTTPStatusError", err)
	}
	if statusErr.StatusCode != http.StatusPreconditionRequired ||
		statusErr.RetryAfter != 5*time.Second ||
		!statusErr.ApprovalExpiresAt.Equal(expiresAt) {
		t.Fatalf("approval status=%#v", statusErr)
	}
	if strings.Contains(statusErr.Error(), "challenge-a") {
		t.Fatalf("approval response leaked through Error(): %q", statusErr.Error())
	}
}

func TestAsApprovalPauseNormalizesBuiltInClientErrors(t *testing.T) {
	expiresAt := time.Now().UTC().Add(5 * time.Minute).Truncate(time.Second)
	tests := []struct {
		name      string
		err       error
		wantPause bool
		wantDelay time.Duration
	}{
		{
			name: "http",
			err: &HTTPStatusError{
				StatusCode: http.StatusPreconditionRequired, ApprovalExpiresAt: expiresAt, RetryAfter: time.Second,
			},
			wantPause: true,
			wantDelay: time.Second,
		},
		{
			name:      "local",
			err:       &governance.ApprovalRequiredError{Challenge: governance.ApprovalChallenge{ExpiresAt: expiresAt}},
			wantPause: true,
		},
		{name: "ordinary", err: errors.New("temporary"), wantPause: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pause, ok := AsApprovalPause(test.err)
			if ok != test.wantPause {
				t.Fatalf("AsApprovalPause(%v) ok=%t, want %t", test.err, ok, test.wantPause)
			}
			if !ok {
				return
			}
			if !pause.ExpiresAt.Equal(expiresAt) || pause.RetryAfter != test.wantDelay {
				t.Fatalf("pause=%#v, want expiry=%v retryAfter=%v", pause, expiresAt, test.wantDelay)
			}
		})
	}
}

func TestHTTPClientBoundsErrorBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
		_, _ = writer.Write([]byte(strings.Repeat("x", maxWorkerErrorBytes*4)))
	}))
	defer server.Close()
	client := NewHTTPClientWithTimeout(server.URL, time.Second)
	_, err := client.ProcessMessage(context.Background(), &Request{})
	if err == nil {
		t.Fatal("error response accepted")
	}
	if len(err.Error()) > maxWorkerErrorBytes+128 {
		t.Fatalf("error body was not bounded: %d bytes", len(err.Error()))
	}
}

func TestHTTPClientRejectsOversizedTrailingWhitespace(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write(append([]byte(`{"content":"ok"}`), []byte(strings.Repeat(" ", maxWorkerResponseBytes))...))
	}))
	defer server.Close()
	client := NewHTTPClientWithTimeout(server.URL, time.Second)
	if _, err := client.ProcessMessage(context.Background(), &Request{}); err == nil {
		t.Fatal("response with oversized trailing whitespace was accepted")
	}
}

func TestHTTPStatusErrorClassifiesRetryAfterAndPermanentStatuses(t *testing.T) {
	tests := []struct {
		status     int
		retryable  bool
		retryAfter time.Duration
	}{
		{status: http.StatusRequestTimeout, retryable: true},
		{status: http.StatusPreconditionRequired, retryable: true},
		{status: http.StatusConflict, retryable: false},
		{status: http.StatusConflict, retryable: true, retryAfter: time.Second},
		{status: http.StatusLocked, retryable: false},
	}
	for _, test := range tests {
		err := (&HTTPStatusError{StatusCode: test.status, RetryAfter: test.retryAfter})
		if got := err.Retryable(); got != test.retryable {
			t.Errorf("status %d retryable=%v, want %v", test.status, got, test.retryable)
		}
	}
}

func TestHTTPStatusErrorDoesNotExposeProviderBodyInErrorChain(t *testing.T) {
	err := &HTTPStatusError{StatusCode: http.StatusBadRequest, Message: "api-key=must-not-leak"}
	if strings.Contains(err.Error(), "must-not-leak") {
		t.Fatalf("provider diagnostic leaked through Error(): %q", err.Error())
	}
}

func TestHTTPClientRejectsAmbiguousBaseURLs(t *testing.T) {
	for _, baseURL := range []string{
		"",
		"not a URL",
		"file:///tmp/worker",
		"http://user:secret@example.test",
		"http://example.test/process?token=secret",
		"http://example.test/process#fragment",
	} {
		t.Run(baseURL, func(t *testing.T) {
			if err := ValidateHTTPBaseURL(baseURL); err == nil {
				t.Fatalf("ValidateHTTPBaseURL(%q) accepted ambiguous endpoint", baseURL)
			}
			client := NewHTTPClientWithTimeout(baseURL, time.Second)
			_, err := client.ProcessMessage(context.Background(), &Request{})
			if err == nil || !strings.Contains(err.Error(), "not configured") {
				t.Fatalf("base URL %q error=%v, want rejected configuration", baseURL, err)
			}
		})
	}
}

func TestValidateHTTPBaseURLUsesClientCanonicalization(t *testing.T) {
	const endpoint = "  https://worker.internal.example/base/  "
	if err := ValidateHTTPBaseURL(endpoint); err != nil {
		t.Fatalf("ValidateHTTPBaseURL(%q): %v", endpoint, err)
	}
	client := NewHTTPClientWithTimeout(endpoint, time.Second)
	if got, want := client.processURL, "https://worker.internal.example/base/process"; got != want {
		t.Fatalf("process URL = %q, want %q", got, want)
	}
	if got, want := client.healthURL, "https://worker.internal.example/base/health"; got != want {
		t.Fatalf("health URL = %q, want %q", got, want)
	}
}

func TestValidateProductionHTTPBaseURLRequiresHTTPS(t *testing.T) {
	if err := ValidateProductionHTTPBaseURL("https://worker.internal/base"); err != nil {
		t.Fatalf("verified HTTPS endpoint rejected: %v", err)
	}
	if err := ValidateProductionHTTPBaseURL("http://worker.internal/base"); !errors.Is(err, ErrInsecureWorkerTransport) {
		t.Fatalf("HTTP endpoint error=%v, want ErrInsecureWorkerTransport", err)
	}
}

func TestValidateHTTPBaseURLForMode(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		mode     WorkerTransportMode
		wantErr  error
	}{
		{name: "production https", endpoint: "https://worker.internal", mode: WorkerTransportProduction},
		{name: "production http", endpoint: "http://worker.internal", mode: WorkerTransportProduction, wantErr: ErrInsecureWorkerTransport},
		{name: "development loopback", endpoint: "http://127.0.0.1:9090", mode: WorkerTransportDevelopment},
		{name: "development IPv6 loopback", endpoint: "http://[::1]:9090", mode: WorkerTransportDevelopment},
		{name: "development compose service", endpoint: "http://worker:9090", mode: WorkerTransportDevelopment},
		{name: "development remote host", endpoint: "http://worker.internal:9090", mode: WorkerTransportDevelopment, wantErr: ErrWorkerDevelopmentEndpointInvalid},
		{name: "development IPv6 link local", endpoint: "http://[fe80::1]:9090", mode: WorkerTransportDevelopment, wantErr: ErrWorkerDevelopmentEndpointInvalid},
		{name: "development IPv6 link local zone", endpoint: "http://[fe80::1%25eth0]:9090", mode: WorkerTransportDevelopment, wantErr: ErrWorkerDevelopmentEndpointInvalid},
		{name: "development https", endpoint: "https://worker.internal:9090", mode: WorkerTransportDevelopment},
		{name: "mesh http", endpoint: "http://agent-worker:9090", mode: WorkerTransportMesh},
		{name: "unknown mode", endpoint: "https://worker.internal", mode: "unsupported", wantErr: ErrWorkerTransportModeInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateHTTPBaseURLForMode(test.endpoint, test.mode)
			if test.wantErr == nil {
				if err != nil {
					t.Fatalf("validation error=%v", err)
				}
				return
			}
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("validation error=%v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestValidateHTTPBaseURLForModeDefaultsEmptyModeToProduction(t *testing.T) {
	if err := ValidateHTTPBaseURLForMode("http://worker.internal", ""); !errors.Is(err, ErrInsecureWorkerTransport) {
		t.Fatalf("empty mode error=%v, want production HTTPS gate", err)
	}
}

func TestNewAuthenticatedProductionHTTPClientRejectsPlaintext(t *testing.T) {
	if client, err := NewAuthenticatedProductionHTTPClientWithTimeout("http://worker.internal", time.Second, nil); client != nil || !errors.Is(err, ErrInsecureWorkerTransport) {
		t.Fatalf("client=%v error=%v, want plaintext transport rejection", client, err)
	}
}

func TestHTTPClientHealthCheckUsesFixedHealthPathAndRejectsNonOK(t *testing.T) {
	t.Run("healthy", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodGet || request.URL.Path != "/worker/health" {
				t.Fatalf("request=%s %s, want GET /worker/health", request.Method, request.URL.Path)
			}
			writer.WriteHeader(http.StatusOK)
		}))
		defer server.Close()
		parsed, err := url.Parse(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		parsed.Path = "/worker/"
		if err := NewHTTPClientWithTimeout(parsed.String(), time.Second).HealthCheck(context.Background()); err != nil {
			t.Fatalf("HealthCheck: %v", err)
		}
	})

	t.Run("unhealthy", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			writer.WriteHeader(http.StatusServiceUnavailable)
			_, _ = writer.Write([]byte("provider diagnostic must stay bounded"))
		}))
		defer server.Close()
		if err := NewHTTPClientWithTimeout(server.URL, time.Second).HealthCheck(context.Background()); err == nil {
			t.Fatal("HealthCheck accepted a non-OK response")
		}
	})
}

func TestHTTPClientValidatesLiveWorkerProcessBudget(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/health" {
			t.Fatalf("health path=%q, want /health", request.URL.Path)
		}
		writer.Header().Set(ExecutionTimeoutHeader, "90s")
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client, err := NewAuthenticatedHTTPClientWithProcessBudget(
		server.URL,
		125*time.Second,
		90*time.Second,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ValidateProcessBudget(context.Background(), 125*time.Second, 90*time.Second); err != nil {
		t.Fatalf("ValidateProcessBudget: %v", err)
	}
	if err := client.ValidateProcessBudget(context.Background(), 124*time.Second, 90*time.Second); !errors.Is(err, ErrWorkerExecutionBudgetInvalid) {
		t.Fatalf("short process budget error=%v, want ErrWorkerExecutionBudgetInvalid", err)
	}
	if err := client.ValidateProcessBudget(context.Background(), 125*time.Second, 60*time.Second); !errors.Is(err, ErrWorkerExecutionBudgetInvalid) {
		t.Fatalf("drifted execution budget error=%v, want ErrWorkerExecutionBudgetInvalid", err)
	}
}

func TestHTTPClientRejectsMissingOrAmbiguousWorkerBudget(t *testing.T) {
	for _, test := range []struct {
		name   string
		values []string
	}{
		{name: "missing"},
		{name: "malformed", values: []string{"ninety seconds"}},
		{name: "duplicate", values: []string{"90s", "90s"}},
		{name: "unsafe", values: []string{"30m"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				for _, value := range test.values {
					writer.Header().Add(ExecutionTimeoutHeader, value)
				}
				writer.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			client, err := NewAuthenticatedHTTPClientWithProcessBudget(
				server.URL,
				125*time.Second,
				90*time.Second,
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			if err := client.ValidateProcessBudget(context.Background(), 125*time.Second, 90*time.Second); !errors.Is(err, ErrWorkerExecutionBudgetInvalid) {
				t.Fatalf("ValidateProcessBudget error=%v, want ErrWorkerExecutionBudgetInvalid", err)
			}
		})
	}
}

func TestBudgetClientUsesVersionedRouteSoOldWorkerCannotExecute(t *testing.T) {
	legacyExecutions := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/process", func(writer http.ResponseWriter, _ *http.Request) {
		legacyExecutions++
		writer.WriteHeader(http.StatusInternalServerError)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client, err := NewAuthenticatedHTTPClientWithProcessBudget(
		server.URL,
		125*time.Second,
		90*time.Second,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.ProcessMessage(context.Background(), &Request{})
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusNotFound || !statusErr.Retryable() {
		t.Fatalf("error=%v, want retryable old-Worker protocol miss", err)
	}
	if legacyExecutions != 0 {
		t.Fatalf("old Worker executed %d versioned requests", legacyExecutions)
	}
}

func TestBudgetClientDoesNotRetryNewWorkerBusinessNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != ExecutionContractProcessPath {
			t.Fatalf("path=%q", request.URL.Path)
		}
		writer.Header().Set(ExecutionContractVersionHeader, "1")
		writer.Header().Set(ExecutionTimeoutHeader, "90s")
		http.Error(writer, "Tenant not found", http.StatusNotFound)
	}))
	defer server.Close()
	client, err := NewAuthenticatedHTTPClientWithProcessBudget(
		server.URL,
		125*time.Second,
		90*time.Second,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.ProcessMessage(context.Background(), &Request{})
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusNotFound || statusErr.Retryable() {
		t.Fatalf("error=%v, want permanent business 404", err)
	}
}

func TestBudgetClientRejectsSuccessWithoutProtocolProof(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"contentType":"text","sessionId":"s","content":"ok"}`))
	}))
	defer server.Close()
	client, err := NewAuthenticatedHTTPClientWithProcessBudget(
		server.URL,
		125*time.Second,
		90*time.Second,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.ProcessMessage(context.Background(), &Request{})
	if !errors.Is(err, ErrWorkerExecutionBudgetInvalid) || !errors.Is(err, ErrWorkerExecutionOutcomeUnknown) {
		t.Fatalf("error=%v, want missing protocol proof and unknown outcome", err)
	}
}

func TestBudgetClientTreatsMalformedSuccessfulResponseAsUnknownOutcome(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set(ExecutionContractVersionHeader, "1")
		writer.Header().Set(ExecutionTimeoutHeader, "90s")
		writer.Header().Set("Content-Type", "text/plain")
		_, _ = writer.Write([]byte("provider response"))
	}))
	defer server.Close()
	client, err := NewAuthenticatedHTTPClientWithProcessBudget(
		server.URL,
		125*time.Second,
		90*time.Second,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.ProcessMessage(context.Background(), &Request{})
	if !errors.Is(err, ErrWorkerExecutionOutcomeUnknown) || !errors.Is(err, ErrWorkerExecutionBudgetInvalid) {
		t.Fatalf("error=%v, want unknown outcome for malformed successful response", err)
	}
}

func TestProductionBudgetClientRequiresSigner(t *testing.T) {
	client, err := NewAuthenticatedProductionHTTPClientWithProcessBudget(
		"https://worker.internal",
		125*time.Second,
		90*time.Second,
		nil,
	)
	if client != nil || !errors.Is(err, ErrWorkerExecutionBudgetInvalid) {
		t.Fatalf("client=%v error=%v, want fail-closed missing signer", client, err)
	}
}

func TestExecutionContractIsBoundByServiceBodySignature(t *testing.T) {
	const secret = "0123456789abcdef0123456789abcdef"
	verifier, err := auth.NewVerifier(secret, acceptingNonceStore{}, "consumer")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(verifier.Middleware(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != ExecutionContractProcessPath {
			t.Errorf("path=%q, want %q", request.URL.Path, ExecutionContractProcessPath)
		}
		var received Request
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
			t.Errorf("decode signed request: %v", err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := ValidateExecutionContract(received.ExecutionContract, 90*time.Second); err != nil {
			t.Errorf("validate signed contract: %v", err)
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writer.Header().Set(ExecutionContractVersionHeader, "1")
		writer.Header().Set(ExecutionTimeoutHeader, "90s")
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"contentType":"text","sessionId":"s","content":"ok"}`))
	})))
	defer server.Close()
	signer, err := auth.NewSigner("consumer", secret)
	if err != nil {
		t.Fatal(err)
	}
	newClient := func() *HTTPClient {
		client, err := NewAuthenticatedHTTPClientWithProcessBudget(
			server.URL,
			125*time.Second,
			90*time.Second,
			signer,
		)
		if err != nil {
			t.Fatal(err)
		}
		return client
	}

	if _, err := newClient().ProcessMessage(context.Background(), &Request{}); err != nil {
		t.Fatalf("signed contract rejected: %v", err)
	}

	tampered := newClient()
	baseTransport := http.DefaultTransport
	tampered.client.Transport = roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		body = bytes.Replace(body, []byte(`"timeout":"1m30s"`), []byte(`"timeout":"1m31s"`), 1)
		request.Body = io.NopCloser(bytes.NewReader(body))
		request.ContentLength = int64(len(body))
		return baseTransport.RoundTrip(request)
	})
	_, err = tampered.ProcessMessage(context.Background(), &Request{})
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusUnauthorized {
		t.Fatalf("tampered signed contract error=%v, want 401", err)
	}
}

func TestBudgetClientRejectsSuccessfulWorkerBudgetDrift(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set(ExecutionContractVersionHeader, "1")
		writer.Header().Set(ExecutionTimeoutHeader, "120s")
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"contentType":"text","sessionId":"s","content":"ok"}`))
	}))
	defer server.Close()
	client, err := NewAuthenticatedHTTPClientWithProcessBudget(
		server.URL, 125*time.Second, 90*time.Second, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.ProcessMessage(context.Background(), &Request{})
	if !errors.Is(err, ErrWorkerExecutionBudgetInvalid) {
		t.Fatalf("error=%v, want budget drift rejection", err)
	}
}

func TestValidateConsumerProcessTimeoutRejectsOverflowAndInvalidValues(t *testing.T) {
	if err := ValidateConsumerProcessTimeout(0, DefaultExecutionTimeout); !errors.Is(err, ErrWorkerExecutionBudgetInvalid) {
		t.Fatalf("zero process timeout error=%v", err)
	}
	if err := ValidateConsumerProcessTimeout(time.Minute, 500*time.Millisecond); !errors.Is(err, ErrWorkerExecutionBudgetInvalid) {
		t.Fatalf("unsafe execution timeout error=%v", err)
	}
}

func TestHTTPClientUsesFixedProcessPathAndRejectsRedirects(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/worker/process" {
			t.Fatalf("path=%q, want /worker/process", request.URL.Path)
		}
		writer.Header().Set("Location", "https://example.test/process")
		writer.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	parsed, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/worker/"
	client := NewHTTPClientWithTimeout(parsed.String(), time.Second)
	_, err = client.ProcessMessage(context.Background(), &Request{})
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("redirect error=%v, want non-followed 307 status", err)
	}
}

func TestHTTPClientTransportErrorDoesNotExposeEndpoint(t *testing.T) {
	client := NewHTTPClientWithTimeout("http://127.0.0.1:1", time.Second)
	_, err := client.ProcessMessage(context.Background(), &Request{})
	if err == nil || strings.Contains(err.Error(), "127.0.0.1:1") {
		t.Fatalf("transport error=%v, endpoint leaked or no error", err)
	}
}

func TestBudgetClientClassifiesTransportErrorAfterWriteAsUnknown(t *testing.T) {
	const endpoint = "https://worker.internal"
	tests := []struct {
		name            string
		invokeWriteHook bool
		invokeConnHook  bool
		wantUnknown     bool
	}{
		{name: "before write", wantUnknown: false},
		{name: "connected before write callback", invokeConnHook: true, wantUnknown: true},
		{name: "after write", invokeWriteHook: true, wantUnknown: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, err := NewAuthenticatedHTTPClientWithProcessBudget(
				endpoint,
				125*time.Second,
				90*time.Second,
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			client.client.Transport = roundTripperFunc(func(request *http.Request) (*http.Response, error) {
				if test.invokeConnHook {
					if trace := httptrace.ContextClientTrace(request.Context()); trace != nil && trace.GotConn != nil {
						trace.GotConn(httptrace.GotConnInfo{})
					}
				}
				if test.invokeWriteHook {
					if trace := httptrace.ContextClientTrace(request.Context()); trace != nil && trace.WroteRequest != nil {
						trace.WroteRequest(httptrace.WroteRequestInfo{Err: errors.New("partial write")})
					}
				}
				return nil, errors.New("connection reset by peer")
			})
			_, err = client.ProcessMessage(context.Background(), &Request{})
			if errors.Is(err, ErrWorkerExecutionOutcomeUnknown) != test.wantUnknown {
				t.Fatalf("error=%v, unknown=%v, want %v", err, errors.Is(err, ErrWorkerExecutionOutcomeUnknown), test.wantUnknown)
			}
		})
	}
}
