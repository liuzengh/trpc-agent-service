//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httptrace"
	"net/netip"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/auth"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/governance"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
)

// WorkerTransportMode controls the trust boundary used for Consumer to
// Worker requests. Production requires application-visible HTTPS. Development
// is an explicit local-only exception, while mesh delegates confidentiality to
// an operator-managed service mesh and must never be described as Worker TLS.
type WorkerTransportMode string

const (
	// WorkerTransportProduction is the default and requires HTTPS.
	WorkerTransportProduction WorkerTransportMode = "production"
	// WorkerTransportDevelopment permits HTTP only for an explicitly isolated
	// local development endpoint.
	WorkerTransportDevelopment WorkerTransportMode = "development"
	// WorkerTransportMesh permits an HTTP app hop only when an external service
	// mesh provides the authenticated encrypted boundary.
	WorkerTransportMesh WorkerTransportMode = "mesh"
)

var (
	// ErrInsecureWorkerTransport identifies a production Worker endpoint that
	// does not provide application-visible transport encryption.
	ErrInsecureWorkerTransport = errors.New("worker endpoint requires encrypted transport")
	// ErrWorkerTransportModeInvalid identifies an unsupported transport mode.
	ErrWorkerTransportModeInvalid = errors.New("worker transport mode is invalid")
	// ErrWorkerMeshMTLSAssertionRequired identifies a mesh-mode configuration
	// that has not explicitly declared an operator-enforced mTLS boundary.
	ErrWorkerMeshMTLSAssertionRequired = errors.New("mesh worker transport requires an explicit mTLS assertion")
	// ErrWorkerDevelopmentEndpointInvalid identifies an HTTP development
	// endpoint outside the local development boundary.
	ErrWorkerDevelopmentEndpointInvalid = errors.New("development worker endpoint must be local")
	// ErrWorkerExecutionBudgetInvalid identifies a missing or malformed Worker
	// capability response, an unsafe Consumer timeout, or configuration drift
	// between the Consumer and the Worker it reached.
	ErrWorkerExecutionBudgetInvalid = errors.New("worker execution budget is invalid")
	// ErrWorkerExecutionOutcomeUnknown identifies an error after Runner entry
	// or a response whose execution proof could not be validated. A model or
	// Tool may already have run, so every transport must require reconciliation
	// instead of automatically retrying.
	ErrWorkerExecutionOutcomeUnknown = errors.New("worker execution outcome is unknown")
)

// WorkerProtocolError is returned when a budget-aware Worker response cannot
// prove which execution contract actually bounded the request. Its Error
// method is deliberately opaque: response headers and bodies are
// provider-controlled and must not enter logs or queue diagnostics. The
// unwrap chain retains only stable classification sentinels.
type WorkerProtocolError struct{}

func (e *WorkerProtocolError) Error() string {
	return "worker response execution contract could not be proven"
}

func (*WorkerProtocolError) Unwrap() error {
	return errors.Join(ErrWorkerExecutionOutcomeUnknown, ErrWorkerExecutionBudgetInvalid)
}

// ExecutionTimeoutHeader advertises the Worker-owned execution timeout on
// its health response. A production Consumer checks it before claiming Inbox
// work so independently configured process deadlines cannot cancel an active
// model or Tool call first.
const ExecutionTimeoutHeader = "X-TRPC-Agent-Execution-Timeout"

// Client defines the interface for worker clients.
type Client interface {
	// ProcessMessage processes a message request and returns a response.
	ProcessMessage(ctx context.Context, req *Request) (*Response, error)
}

// LocalClient implements Client using a local worker.
type LocalClient struct {
	worker *Worker
}

// NewLocalClient creates a local worker client.
func NewLocalClient(worker *Worker) *LocalClient {
	return &LocalClient{
		worker: worker,
	}
}

// ProcessMessage processes a message using the local worker.
func (c *LocalClient) ProcessMessage(ctx context.Context, req *Request) (*Response, error) {
	if c == nil || c.worker == nil {
		return nil, fmt.Errorf("local worker client is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return c.worker.Process(ctx, req)
}

// HTTPClient implements Client using HTTP transport.
type HTTPClient struct {
	baseURL                  string
	processURL               string
	healthURL                string
	client                   *http.Client
	signer                   *auth.Signer
	executionContract        ExecutionContract
	requireExecutionContract bool
}

// HTTPStatusError preserves the worker protocol status so the Consumer can
// distinguish transient admission contention from an operator-blocked
// execution. Body text is bounded and should be treated as diagnostic only.
type HTTPStatusError struct {
	StatusCode        int
	RetryAfter        time.Duration
	ApprovalExpiresAt time.Time
	Message           string
	protocolRetryable bool
}

// ApprovalPause is the transport-neutral representation of a pre-execution
// tool-approval pause. The expiry originates from the approval store; the
// Consumer must delegate its validity to the durable Inbox store's clock.
type ApprovalPause struct {
	ExpiresAt  time.Time
	RetryAfter time.Duration
}

// AsApprovalPause extracts a pending approval from either built-in Worker
// transport. LocalClient returns the Worker error directly, while HTTPClient
// represents the same result as HTTP 428. Keeping this translation in worker
// prevents queue orchestration from depending on a specific transport.
func AsApprovalPause(err error) (ApprovalPause, bool) {
	if errors.Is(err, ErrWorkerExecutionOutcomeUnknown) {
		return ApprovalPause{}, false
	}
	var statusErr *HTTPStatusError
	if errors.As(err, &statusErr) && statusErr != nil && statusErr.StatusCode == http.StatusPreconditionRequired {
		return ApprovalPause{
			ExpiresAt:  statusErr.ApprovalExpiresAt,
			RetryAfter: statusErr.RetryAfter,
		}, true
	}
	var approvalErr *governance.ApprovalRequiredError
	if errors.As(err, &approvalErr) && approvalErr != nil {
		return ApprovalPause{ExpiresAt: approvalErr.Challenge.ExpiresAt}, true
	}
	return ApprovalPause{}, false
}

// Retryable reports whether the status represents a transport/admission
// condition that may succeed without changing the request. A bare 409 is
// intentionally not retryable: payload/idempotency conflicts are permanent;
// the Worker sets Retry-After only for the in-progress case.
func (e *HTTPStatusError) Retryable() bool {
	if e == nil {
		return false
	}
	return e.StatusCode == http.StatusTooManyRequests ||
		e.StatusCode == http.StatusPreconditionRequired ||
		e.StatusCode == http.StatusRequestTimeout ||
		e.StatusCode >= 500 ||
		e.protocolRetryable ||
		(e.StatusCode == http.StatusConflict && e.RetryAfter > 0)
}

func (e *HTTPStatusError) Error() string {
	if e == nil {
		return "worker returned an unknown HTTP status"
	}
	// The response body is provider-controlled diagnostic material. It may
	// contain prompts, URLs or credentials, so it must not enter ordinary
	// error chains, logs, dead-letter rows or HTTP responses. Callers that need
	// a protected diagnostic sink can inspect Message explicitly.
	return fmt.Sprintf("worker returned status %d", e.StatusCode)
}

const maxWorkerResponseBytes = 1 << 20
const maxWorkerErrorBytes = 4096

// ValidateHTTPBaseURL verifies an operator-configured Worker endpoint using
// the same canonical process-path rules as HTTPClient. Callers that construct
// a long-lived Consumer should use it during startup so configuration errors
// do not first surface as retryable queue failures.
func ValidateHTTPBaseURL(baseURL string) error {
	_, err := workerProcessURL(strings.TrimSpace(baseURL))
	return err
}

// ValidateProductionHTTPBaseURL verifies the endpoint used by a production
// Consumer. It applies the ordinary URL/path checks and then requires HTTPS;
// callers can use errors.Is with ErrInsecureWorkerTransport to classify a
// plaintext configuration error without exposing the configured URL.
func ValidateProductionHTTPBaseURL(baseURL string) error {
	parsed, err := parseWorkerBaseURL(strings.TrimSpace(baseURL))
	if err != nil {
		return err
	}
	if parsed.Scheme != "https" {
		return ErrInsecureWorkerTransport
	}
	return nil
}

// ValidateHTTPBaseURLForMode applies the transport policy for a Consumer
// deployment. An empty mode is treated as production. Development accepts
// HTTP only for loopback or a single-label local service name (for example the
// Docker Compose service "worker"). Mesh accepts either scheme because TLS is
// expected to be enforced outside this process; the caller must separately
// assert that the mesh policy is active before using that mode.
func ValidateHTTPBaseURLForMode(baseURL string, mode WorkerTransportMode) error {
	mode = WorkerTransportMode(strings.ToLower(strings.TrimSpace(string(mode))))
	if mode == "" {
		mode = WorkerTransportProduction
	}
	switch mode {
	case WorkerTransportProduction:
		return ValidateProductionHTTPBaseURL(baseURL)
	case WorkerTransportDevelopment:
		parsed, err := parseWorkerBaseURL(strings.TrimSpace(baseURL))
		if err != nil {
			return err
		}
		if parsed.Scheme == "https" {
			return nil
		}
		if parsed.Scheme != "http" || !isLocalDevelopmentHost(parsed.Hostname()) {
			return ErrWorkerDevelopmentEndpointInvalid
		}
		return nil
	case WorkerTransportMesh:
		return ValidateHTTPBaseURL(baseURL)
	default:
		return ErrWorkerTransportModeInvalid
	}
}

// NewAuthenticatedProductionHTTPClientWithTimeout creates an authenticated
// client only after enforcing the production HTTPS contract. The older
// NewAuthenticatedHTTPClientWithTimeout constructor remains available for
// local/in-process callers and intentionally performs only syntax validation.
func NewAuthenticatedProductionHTTPClientWithTimeout(baseURL string, timeout time.Duration, signer *auth.Signer) (*HTTPClient, error) {
	if err := ValidateProductionHTTPBaseURL(baseURL); err != nil {
		return nil, err
	}
	if signer == nil {
		return nil, fmt.Errorf("%w: service signer is required", ErrWorkerExecutionBudgetInvalid)
	}
	return NewAuthenticatedHTTPClientWithTimeout(baseURL, timeout, signer), nil
}

// NewAuthenticatedProductionHTTPClientWithProcessBudget creates the
// budget-aware production client. It sends requests only to the versioned
// endpoint and embeds the expected Worker timeout in the HMAC-signed body.
func NewAuthenticatedProductionHTTPClientWithProcessBudget(
	baseURL string,
	processTimeout time.Duration,
	executionTimeout time.Duration,
	signer *auth.Signer,
) (*HTTPClient, error) {
	if err := ValidateProductionHTTPBaseURL(baseURL); err != nil {
		return nil, err
	}
	if signer == nil {
		return nil, fmt.Errorf("%w: service signer is required", ErrWorkerExecutionBudgetInvalid)
	}
	return NewAuthenticatedHTTPClientWithProcessBudget(
		baseURL,
		processTimeout,
		executionTimeout,
		signer,
	)
}

// NewAuthenticatedHTTPClientWithTimeout creates a legacy authenticated HTTP
// client for the unversioned /process protocol. It remains available for
// compatibility with older Workers and tests, but production composition must
// use NewAuthenticatedHTTPClientWithProcessBudget: hardened Workers reject this
// route before execution. It does not select or enforce a deployment transport
// policy; composition roots should validate the endpoint with
// ValidateHTTPBaseURLForMode first. Worker rejects unsigned requests and
// replayed nonces.
func NewAuthenticatedHTTPClientWithTimeout(baseURL string, timeout time.Duration, signer *auth.Signer) *HTTPClient {
	client := NewHTTPClientWithTimeout(baseURL, timeout)
	client.signer = signer
	return client
}

// NewAuthenticatedHTTPClientWithProcessBudget creates a budget-aware client
// after proving that its outer process window safely contains Worker execution
// and detached persistence. The caller remains responsible for selecting the
// deployment transport policy.
func NewAuthenticatedHTTPClientWithProcessBudget(
	baseURL string,
	processTimeout time.Duration,
	executionTimeout time.Duration,
	signer *auth.Signer,
) (*HTTPClient, error) {
	if err := ValidateConsumerProcessTimeout(processTimeout, executionTimeout); err != nil {
		return nil, err
	}
	contract, err := NewExecutionContract(executionTimeout)
	if err != nil {
		return nil, err
	}
	processURL, err := workerExecutionContractProcessURL(strings.TrimSpace(baseURL))
	if err != nil {
		return nil, err
	}
	client := NewAuthenticatedHTTPClientWithTimeout(baseURL, processTimeout, signer)
	client.processURL = processURL
	client.executionContract = contract
	client.requireExecutionContract = true
	return client, nil
}

// NewHTTPClient creates an HTTP worker client.
func NewHTTPClient(baseURL string) *HTTPClient {
	return NewHTTPClientWithTimeout(baseURL, 30*time.Second)
}

// NewHTTPClientWithTimeout creates a client with a caller-selected end-to-end
// timeout. Consumer uses this to keep the HTTP timeout below its Inbox lease.
func NewHTTPClientWithTimeout(baseURL string, timeout time.Duration) *HTTPClient {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	baseURL = strings.TrimSpace(baseURL)
	processURL, _ := workerProcessURL(baseURL)
	healthURL, _ := workerHealthURL(baseURL)
	return &HTTPClient{
		baseURL:    baseURL,
		processURL: processURL,
		healthURL:  healthURL,
		client: &http.Client{
			Timeout: timeout,
			// A worker request can carry a service signature. Never follow a
			// redirect that could forward those headers to another origin.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

// HealthCheck verifies that the configured Worker is accepting requests before
// a Consumer begins claiming durable Inbox work. It deliberately uses the
// Worker health endpoint rather than /process: no user payload or service
// signature is needed to establish this startup dependency.
func (c *HTTPClient) HealthCheck(ctx context.Context) error {
	_, err := c.healthCapabilities(ctx, false)
	return err
}

// ValidateProcessBudget verifies both the reachable Worker's advertised
// execution timeout and the Consumer's end-to-end processing window. The
// expected timeout comes from the shared deployment configuration; comparing
// it with the live health response catches rollout/configuration drift before
// the Consumer claims durable work.
func (c *HTTPClient) ValidateProcessBudget(
	ctx context.Context,
	processTimeout time.Duration,
	expectedExecutionTimeout time.Duration,
) error {
	if err := ValidateExecutionTimeout(expectedExecutionTimeout); err != nil {
		return fmt.Errorf("%w: expected timeout: %v", ErrWorkerExecutionBudgetInvalid, err)
	}
	if c == nil {
		return fmt.Errorf("%w: HTTP worker client is not configured", ErrWorkerExecutionBudgetInvalid)
	}
	if c.requireExecutionContract {
		if err := ValidateExecutionContract(&c.executionContract, expectedExecutionTimeout); err != nil {
			return err
		}
	}
	actual, err := c.healthCapabilities(ctx, true)
	if err != nil {
		return err
	}
	if actual != expectedExecutionTimeout {
		return fmt.Errorf(
			"%w: advertised timeout %s does not match configured timeout %s",
			ErrWorkerExecutionBudgetInvalid,
			actual,
			expectedExecutionTimeout,
		)
	}
	return ValidateConsumerProcessTimeout(processTimeout, actual)
}

// ValidateConsumerProcessTimeout requires enough time for Worker execution,
// Worker-side completion and a final detached Consumer persistence operation.
func ValidateConsumerProcessTimeout(processTimeout, executionTimeout time.Duration) error {
	if processTimeout <= 0 {
		return fmt.Errorf("%w: consumer process timeout must be positive", ErrWorkerExecutionBudgetInvalid)
	}
	if err := ValidateExecutionTimeout(executionTimeout); err != nil {
		return fmt.Errorf("%w: %v", ErrWorkerExecutionBudgetInvalid, err)
	}
	const maxDuration = time.Duration(1<<63 - 1)
	if executionTimeout > maxDuration-ResponseCompletionGrace-ConsumerPersistenceGrace {
		return fmt.Errorf("%w: timeout calculation overflows", ErrWorkerExecutionBudgetInvalid)
	}
	minimum := executionTimeout + ResponseCompletionGrace + ConsumerPersistenceGrace
	if processTimeout < minimum {
		return fmt.Errorf(
			"%w: consumer process timeout %s must be at least %s for execution timeout %s",
			ErrWorkerExecutionBudgetInvalid,
			processTimeout,
			minimum,
			executionTimeout,
		)
	}
	return nil
}

func (c *HTTPClient) healthCapabilities(ctx context.Context, requireExecutionTimeout bool) (time.Duration, error) {
	if c == nil || c.client == nil || c.healthURL == "" {
		return 0, fmt.Errorf("HTTP worker client is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.healthURL, nil)
	if err != nil {
		return 0, fmt.Errorf("create worker health request: %w", err)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, &workerTransportError{cause: err}
	}
	defer resp.Body.Close()
	// Drain only a tiny bounded body so a keep-alive connection can be reused;
	// health diagnostics must not flow into normal queue logs or error chains.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("worker health endpoint returned status %d", resp.StatusCode)
	}
	values := resp.Header.Values(ExecutionTimeoutHeader)
	if !requireExecutionTimeout && len(values) == 0 {
		return 0, nil
	}
	if len(values) != 1 || strings.TrimSpace(values[0]) != values[0] || values[0] == "" ||
		strings.ContainsAny(values[0], "\r\n\x00") {
		return 0, fmt.Errorf("%w: health response has no unambiguous timeout", ErrWorkerExecutionBudgetInvalid)
	}
	timeout, err := time.ParseDuration(values[0])
	if err != nil {
		return 0, fmt.Errorf("%w: health response timeout is malformed", ErrWorkerExecutionBudgetInvalid)
	}
	if err := ValidateExecutionTimeout(timeout); err != nil {
		return 0, fmt.Errorf("%w: health response timeout is unsafe", ErrWorkerExecutionBudgetInvalid)
	}
	return timeout, nil
}

// ProcessMessage processes a message via HTTP.
func (c *HTTPClient) ProcessMessage(ctx context.Context, req *Request) (*Response, error) {
	if c == nil || c.client == nil || c.processURL == "" {
		return nil, fmt.Errorf("HTTP worker client is not configured")
	}
	if req == nil {
		return nil, fmt.Errorf("worker request is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	requestCopy := *req
	if c.requireExecutionContract {
		contract := c.executionContract
		requestCopy.ExecutionContract = &contract
	}
	// Marshal the copy so a shared/retried caller request is never mutated by
	// transport-owned protocol fields. The exact bytes are signed below.
	jsonData, err := json.Marshal(&requestCopy)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Create HTTP request with context
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.processURL, bytes.NewReader(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if req.Metadata != nil {
		if traceParent, ok := req.Metadata["traceparent"].(string); ok && traceParent != "" {
			httpReq.Header.Set("traceparent", traceParent)
		}
	}
	telemetry.InjectHTTP(ctx, httpReq)
	if c.signer != nil {
		if err := c.signer.Sign(httpReq, jsonData); err != nil {
			return nil, err
		}
	}

	// GotConn precedes dispatch; WroteRequest may arrive after Do returns.
	// Once connected, a transport failure cannot prove that the Worker did
	// not execute. Failures during DNS or connection setup remain retryable.
	var mayHaveDispatched atomic.Bool
	trace := &httptrace.ClientTrace{
		GotConn: func(httptrace.GotConnInfo) {
			mayHaveDispatched.Store(true)
		},
		WroteRequest: func(httptrace.WroteRequestInfo) {
			// The callback itself means the transport reached its write hook. An
			// error may indicate a partial write, which is still an unknown
			// outcome for a side-effecting Worker request.
			mayHaveDispatched.Store(true)
		},
	}
	httpReq = httpReq.WithContext(httptrace.WithClientTrace(ctx, trace))
	resp, err := c.client.Do(httpReq)
	if err != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		return nil, &workerTransportError{
			cause:   err,
			unknown: c.requireExecutionContract && mayHaveDispatched.Load(),
		}
	}
	defer resp.Body.Close()
	contractUnderstood, contractHeaderErr := executionContractResponseProof(resp.Header, c.executionContract)
	if c.requireExecutionContract && resp.StatusCode == http.StatusOK &&
		(contractHeaderErr != nil || !contractUnderstood) {
		return nil, &WorkerProtocolError{}
	}

	// Check status
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxWorkerErrorBytes+1))
		if len(body) > maxWorkerErrorBytes {
			body = body[:maxWorkerErrorBytes]
		}
		message := strings.TrimSpace(string(body))
		if message == "" {
			message = http.StatusText(resp.StatusCode)
		}
		var retryAfter time.Duration
		if value := strings.TrimSpace(resp.Header.Get("Retry-After")); value != "" {
			if seconds, parseErr := strconv.ParseInt(value, 10, 64); parseErr == nil && seconds >= 0 {
				const maxRetryAfterSeconds = int64((1<<63 - 1) / int64(time.Second))
				if seconds > maxRetryAfterSeconds {
					seconds = maxRetryAfterSeconds
				}
				retryAfter = time.Duration(seconds) * time.Second
			} else if when, parseErr := http.ParseTime(value); parseErr == nil {
				if delay := time.Until(when); delay > 0 {
					retryAfter = delay
				}
			}
		}
		statusErr := &HTTPStatusError{
			StatusCode: resp.StatusCode,
			RetryAfter: retryAfter,
			Message:    message,
			// An older Worker has no versioned route and returns 404/405 before
			// execution. Keep that rollout response retryable without making
			// ordinary not-found application responses retryable.
			protocolRetryable: c.requireExecutionContract && !contractUnderstood &&
				(resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed),
		}
		if resp.StatusCode == http.StatusPreconditionRequired {
			// The approval response is intentionally tiny and contains only a
			// challenge identifier plus an expiry timestamp. Parse the expiry for
			// the durable queue deadline; retain the bounded body only as opaque
			// diagnostics and never expose it through Error().
			var approval struct {
				Error     string    `json:"error"`
				ExpiresAt time.Time `json:"expires_at"`
			}
			if json.Unmarshal(body, &approval) == nil &&
				approval.Error == "tool_approval_required" && !approval.ExpiresAt.IsZero() {
				statusErr.ApprovalExpiresAt = approval.ExpiresAt.UTC()
			}
		}
		return nil, statusErr
	}

	// Parse response
	mediaType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return nil, c.unknownSuccessfulResponseError(fmt.Errorf("worker returned an invalid content type"))
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxWorkerResponseBytes+1))
	if err != nil {
		return nil, c.unknownSuccessfulResponseError(fmt.Errorf("failed to read worker response: %w", err))
	}
	if len(body) > maxWorkerResponseBytes {
		return nil, c.unknownSuccessfulResponseError(fmt.Errorf("worker response exceeds %d bytes", maxWorkerResponseBytes))
	}
	var response Response
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&response); err != nil {
		return nil, c.unknownSuccessfulResponseError(fmt.Errorf("failed to decode response: %w", err))
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, c.unknownSuccessfulResponseError(fmt.Errorf("worker returned multiple JSON values"))
	}

	return &response, nil
}

func (c *HTTPClient) unknownSuccessfulResponseError(cause error) error {
	if c != nil && c.requireExecutionContract {
		// A 200 response proves that the Worker reached its response boundary,
		// but malformed or truncated data does not prove whether an external
		// model/Tool side effect was committed. Budget-aware callers must pause
		// for reconciliation rather than retrying the invocation.
		return &WorkerProtocolError{}
	}
	return cause
}

type workerTransportError struct {
	cause   error
	unknown bool
}

func (e *workerTransportError) Error() string {
	return "worker request failed"
}

func (e *workerTransportError) Unwrap() error {
	if e == nil {
		return nil
	}
	if e.unknown {
		return errors.Join(e.cause, ErrWorkerExecutionOutcomeUnknown)
	}
	return e.cause
}

func workerProcessURL(baseURL string) (string, error) {
	parsed, err := parseWorkerBaseURL(baseURL)
	if err != nil {
		return "", err
	}
	parsed.Path = path.Join("/", parsed.Path, "process")
	parsed.RawPath = ""
	return parsed.String(), nil
}

func workerExecutionContractProcessURL(baseURL string) (string, error) {
	parsed, err := parseWorkerBaseURL(baseURL)
	if err != nil {
		return "", err
	}
	parsed.Path = path.Join("/", parsed.Path, ExecutionContractProcessPath)
	parsed.RawPath = ""
	return parsed.String(), nil
}

func executionContractResponseUnderstood(header http.Header) (bool, error) {
	values := header.Values(ExecutionContractVersionHeader)
	if len(values) == 0 {
		return false, nil
	}
	if len(values) != 1 || strings.TrimSpace(values[0]) != values[0] ||
		values[0] == "" || strings.ContainsAny(values[0], "\r\n\x00") {
		return false, fmt.Errorf("execution contract response version is ambiguous")
	}
	version, err := strconv.Atoi(values[0])
	if err != nil {
		return false, fmt.Errorf("execution contract response version is malformed")
	}
	if version != ExecutionContractVersion {
		return false, fmt.Errorf("execution contract response version is unsupported")
	}
	return true, nil
}

// executionContractResponseProof validates the two response capabilities that
// make a successful versioned invocation safe to accept. The version identifies
// the protocol implementation; the timeout identifies the Worker budget that
// actually bounded this request. Checking both on every successful response
// catches a mixed rollout behind a load balancer even when startup health was
// sampled from a different replica.
func executionContractResponseProof(header http.Header, expected ExecutionContract) (bool, error) {
	understood, err := executionContractResponseUnderstood(header)
	if err != nil || !understood {
		return understood, err
	}
	values := header.Values(ExecutionTimeoutHeader)
	if len(values) != 1 || strings.TrimSpace(values[0]) != values[0] ||
		values[0] == "" || strings.ContainsAny(values[0], "\r\n\x00") {
		return false, fmt.Errorf("execution contract response timeout is ambiguous")
	}
	actual, err := time.ParseDuration(values[0])
	if err != nil {
		return false, fmt.Errorf("execution contract response timeout is malformed")
	}
	if err := ValidateExecutionTimeout(actual); err != nil {
		return false, fmt.Errorf("execution contract response timeout is unsafe")
	}
	configured, err := time.ParseDuration(expected.Timeout)
	if err != nil || expected.Version != ExecutionContractVersion {
		return false, fmt.Errorf("local execution contract is invalid")
	}
	if actual != configured {
		return false, fmt.Errorf("execution contract response timeout does not match request")
	}
	return true, nil
}

func parseWorkerBaseURL(baseURL string) (*url.URL, error) {
	if baseURL == "" {
		return nil, fmt.Errorf("worker base URL is required")
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed == nil {
		return nil, fmt.Errorf("worker base URL is invalid")
	}
	if !strings.EqualFold(parsed.Scheme, "http") && !strings.EqualFold(parsed.Scheme, "https") {
		return nil, fmt.Errorf("worker base URL must use HTTP or HTTPS")
	}
	if parsed.Host == "" || parsed.Hostname() == "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Opaque != "" {
		return nil, fmt.Errorf("worker base URL must have a host without credentials, query or fragment")
	}
	if parsed.RawPath != "" {
		return nil, fmt.Errorf("worker base URL must not contain an escaped path")
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	return parsed, nil
}

func isLocalDevelopmentHost(host string) bool {
	host = strings.TrimSpace(strings.ToLower(host))
	if host == "localhost" {
		return true
	}
	if strings.Contains(host, ":") {
		// URL.Hostname may retain an IPv6 zone identifier. Zones are valid for
		// link-local addresses but never make a non-loopback address local to
		// this process, so strip the zone before requiring loopback explicitly.
		if zone := strings.IndexByte(host, '%'); zone >= 0 {
			host = host[:zone]
		}
		addr, err := netip.ParseAddr(host)
		return err == nil && addr.IsLoopback()
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		return addr.IsLoopback()
	}
	// A single-label name is normally resolved by the local/container network
	// and cannot be a public DNS name. Development mode is still explicit and
	// must never be used as a production deployment contract.
	return host != "" && !strings.Contains(host, ".")
}

func workerHealthURL(baseURL string) (string, error) {
	processURL, err := workerProcessURL(baseURL)
	if err != nil {
		return "", err
	}
	parsed, err := url.Parse(processURL)
	if err != nil || parsed == nil {
		return "", fmt.Errorf("worker process URL is invalid")
	}
	parsed.Path = path.Join(path.Dir(parsed.Path), "health")
	parsed.RawPath = ""
	return parsed.String(), nil
}
