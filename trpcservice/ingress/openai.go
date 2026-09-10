// Package ingress adapts authenticated HTTP protocols to the platform gateway.
package ingress

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformtelemetry "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	openaiserver "trpc.group/trpc-go/trpc-agent-go/server/openai"
)

const (
	maxOpenAIRequestBytes  = 1 << 20
	maxOpenAIIdentityBytes = 256

	headerRequestID                = "X-Request-ID"
	headerIdempotencyKey           = "Idempotency-Key"
	headerSessionID                = "X-Session-ID"
	headerUserID                   = "X-User-ID"
	headerSessionPrincipal         = "X-Session-Principal-ID"
	headerTraceID                  = "X-Trace-ID"
	retryAfterSeconds              = "60"
	openAIChatPath                 = "/v1/chat/completions"
	defaultDurableEventWaitTimeout = 2 * time.Minute
	defaultActiveStreamingLimit    = 64
)

var errInvalidRequestIdentity = errors.New("invalid request identity")
var errOpenAIExecutionFailed = errors.New("execution failed")
var errActiveStreamingLimit = errors.New("active streaming limit exceeded")

type openAIStreamStateKey struct{}
type openAIStreamRequestKey struct{}

type openAIStreamState struct {
	terminal atomic.Bool
}

// OpenAIHandlerOption configures the HTTP wait boundary after durable
// admission. It must not be used to cancel the already admitted execution.
type OpenAIHandlerOption func(*openAIHandler) error

// WithDurableEventWaitTimeout bounds how long HTTP waits for persisted
// execution events after admission. A disconnected/expired HTTP request does
// not remove the durable execution from the dispatch queue.
func WithDurableEventWaitTimeout(timeout time.Duration) OpenAIHandlerOption {
	return func(handler *openAIHandler) error {
		if timeout <= 0 {
			return errors.New("durable event wait timeout must be positive")
		}
		handler.durableEventWaitTimeout = timeout
		return nil
	}
}

// WithActiveStreamingLimit bounds active streaming HTTP lifecycles in one
// Gateway process. It is intentionally separate from durable admission: a
// rejected stream must not consume an admission slot.
func WithActiveStreamingLimit(limit int) OpenAIHandlerOption {
	return func(handler *openAIHandler) error {
		if limit <= 0 {
			return errors.New("active streaming limit must be positive")
		}
		handler.activeStreamingSlots = make(chan struct{}, limit)
		return nil
	}
}

// NewOpenAIHandler creates the OpenAI-compatible endpoint at
// /v1/chat/completions. It requires Authorization: Bearer, X-Request-ID,
// Idempotency-Key, and X-Session-ID headers. X-Trace-ID defaults to
// X-Request-ID. The authenticated credential determines tenant, application,
// user, and session principal; callers can select only a session within that
// service principal. The OpenAI request payload cannot select tenant,
// application, session, user, or config scope. This entry accepts one user
// text message and no client-declared tools or conversation history because
// the durable backend owns those concerns.
func NewOpenAIHandler(authenticator auth.HTTPAPIKeyResolver, queued *gateway.QueuedRunner) (http.Handler, error) {
	return NewOpenAIHandlerWithOptions(authenticator, queued)
}

// NewOpenAIHandlerWithOptions creates the OpenAI-compatible handler with
// explicit HTTP wait-boundary options.
func NewOpenAIHandlerWithOptions(
	authenticator auth.HTTPAPIKeyResolver,
	queued *gateway.QueuedRunner,
	opts ...OpenAIHandlerOption,
) (http.Handler, error) {
	if authenticator.Credentials == nil {
		return nil, errors.New("credential store is required")
	}
	if authenticator.Directory == nil {
		return nil, errors.New("tenant directory is required")
	}
	if queued == nil {
		return nil, errors.New("queued runner is required")
	}
	server, err := openaiserver.New(openaiserver.WithRunner(openAIQueuedRunner{queued: queued}))
	if err != nil {
		return nil, err
	}
	handler := openAIHandler{
		authenticator:           authenticator,
		queued:                  queued,
		next:                    server.Handler(),
		durableEventWaitTimeout: defaultDurableEventWaitTimeout,
		activeStreamingSlots:    make(chan struct{}, defaultActiveStreamingLimit),
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&handler); err != nil {
			return nil, err
		}
	}
	return handler, nil
}

type openAIHandler struct {
	authenticator           auth.HTTPAPIKeyResolver
	queued                  *gateway.QueuedRunner
	next                    http.Handler
	durableEventWaitTimeout time.Duration
	activeStreamingSlots    chan struct{}
}

func (h openAIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != openAIChatPath && r.URL.Path != openAIChatPath+"/" {
		h.next.ServeHTTP(w, r)
		return
	}
	if r.Method == http.MethodOptions {
		h.next.ServeHTTP(w, r)
		return
	}
	if r.Method != http.MethodPost {
		h.next.ServeHTTP(w, r)
		return
	}
	r = r.WithContext(platformtelemetry.ExtractHTTP(r.Context(), map[string]string{
		"traceparent": r.Header.Get("traceparent"),
		"tracestate":  r.Header.Get("tracestate"),
	}))
	request, err := h.authenticatedRequest(r)
	if err != nil {
		writeAuthenticationError(w, err)
		return
	}
	message, stream, err := validateQueuedOpenAIRequest(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx, err := gateway.ContextWithAuthenticatedRequest(r.Context(), request)
	if err != nil {
		writeAuthenticationError(w, err)
		return
	}
	if stream {
		release, err := h.acquireStreaming(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			writeAdmissionError(w, err)
			return
		}
		defer release()
	}
	ctx, err = h.queued.Admit(ctx, gateway.Message{Text: message})
	if err != nil {
		writeAdmissionError(w, err)
		return
	}
	waitCtx, cancelWait := context.WithTimeout(ctx, h.durableEventWaitTimeout)
	defer cancelWait()
	ctx = waitCtx
	state := &openAIStreamState{}
	ctx = context.WithValue(ctx, openAIStreamStateKey{}, state)
	ctx = context.WithValue(ctx, openAIStreamRequestKey{}, stream)
	h.next.ServeHTTP(&openAIResponseWriter{ResponseWriter: w, state: state}, r.WithContext(ctx))
}

func (h openAIHandler) acquireStreaming(ctx context.Context) (func(), error) {
	if h.activeStreamingSlots == nil {
		return func() {}, nil
	}
	select {
	case h.activeStreamingSlots <- struct{}{}:
		return func() { <-h.activeStreamingSlots }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return nil, errActiveStreamingLimit
	}
}

type queuedOpenAIRequest struct {
	Model            string                       `json:"model"`
	Messages         []queuedOpenAIRequestMessage `json:"messages"`
	Temperature      *float64                     `json:"temperature,omitempty"`
	MaxTokens        *int                         `json:"max_tokens,omitempty"`
	Stream           bool                         `json:"stream,omitempty"`
	Tools            json.RawMessage              `json:"tools"`
	ToolChoice       json.RawMessage              `json:"tool_choice"`
	TopP             *float64                     `json:"top_p,omitempty"`
	Stop             []string                     `json:"stop,omitempty"`
	PresencePenalty  *float64                     `json:"presence_penalty,omitempty"`
	FrequencyPenalty *float64                     `json:"frequency_penalty,omitempty"`
	User             string                       `json:"user,omitempty"`
}

type queuedOpenAIRequestMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func validateQueuedOpenAIRequest(w http.ResponseWriter, r *http.Request) (string, bool, error) {
	if r == nil || r.Body == nil {
		return "", false, errors.New("request body is required")
	}
	body := http.MaxBytesReader(w, r.Body, maxOpenAIRequestBytes)
	encoded, err := io.ReadAll(body)
	if err != nil {
		return "", false, err
	}
	if err := body.Close(); err != nil {
		return "", false, err
	}
	r.Body = io.NopCloser(bytes.NewReader(encoded))

	var request queuedOpenAIRequest
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return "", false, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return "", false, errors.New("request body contains multiple values")
	}
	if len(request.Messages) != 1 || request.Messages[0].Role != "user" {
		return "", false, errors.New("exactly one user message is required")
	}
	if request.Messages[0].Content == "" {
		return "", false, errors.New("user message content is required")
	}
	if hasJSONValue(request.Tools) || hasJSONValue(request.ToolChoice) {
		return "", false, errors.New("client tools are not supported")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return "", false, err
	}
	normalizedFields := make(map[string]json.RawMessage, len(fields))
	for field, value := range fields {
		normalizedFields[strings.ToLower(field)] = value
	}
	for _, field := range []string{
		"temperature",
		"max_tokens",
		"top_p",
		"stop",
		"presence_penalty",
		"frequency_penalty",
	} {
		if _, provided := normalizedFields[field]; provided {
			return "", false, fmt.Errorf("unsupported OpenAI field %q", field)
		}
	}
	return request.Messages[0].Content, request.Stream, nil
}

type openAIQueuedRunner struct {
	queued *gateway.QueuedRunner
}

func (r openAIQueuedRunner) Run(
	ctx context.Context,
	userID string,
	sessionID string,
	message model.Message,
	runOpts ...agent.RunOption,
) (<-chan *event.Event, error) {
	if r.queued == nil {
		return nil, errors.New("queued runner is required")
	}
	input, err := r.queued.RunWithTerminalErrors(ctx, userID, sessionID, message, runOpts...)
	if err != nil {
		return nil, err
	}
	state, _ := ctx.Value(openAIStreamStateKey{}).(*openAIStreamState)
	stream, _ := ctx.Value(openAIStreamRequestKey{}).(bool)
	if stream {
		return openAIStreamEvents(ctx, input, state)
	}
	return openAINonStreamingEvents(ctx, input, state)
}

func (r openAIQueuedRunner) Close() error { return nil }

func openAINonStreamingEvents(
	ctx context.Context,
	input <-chan *event.Event,
	state *openAIStreamState,
) (<-chan *event.Event, error) {
	events := make([]*event.Event, 0)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case evt, ok := <-input:
			if !ok {
				if len(events) == 0 {
					return nil, errOpenAIExecutionFailed
				}
				return eventChannel(ctx, events), nil
			}
			if evt != nil && evt.IsTerminalError() {
				markOpenAIExecutionFailed(state)
				return nil, errOpenAIExecutionFailed
			}
			if evt != nil {
				events = append(events, evt)
			}
		}
	}
}

func openAIStreamEvents(
	ctx context.Context,
	input <-chan *event.Event,
	state *openAIStreamState,
) (<-chan *event.Event, error) {
	first, ok := <-input
	if !ok {
		return nil, errOpenAIExecutionFailed
	}
	if first != nil && first.IsTerminalError() {
		markOpenAIExecutionFailed(state)
		return nil, errOpenAIExecutionFailed
	}
	output := make(chan *event.Event)
	go func() {
		defer close(output)
		select {
		case output <- first:
		case <-ctx.Done():
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			case evt, ok := <-input:
				if !ok {
					return
				}
				if evt != nil && evt.IsTerminalError() {
					markOpenAIExecutionFailed(state)
					return
				}
				select {
				case output <- evt:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return output, nil
}

func eventChannel(ctx context.Context, events []*event.Event) <-chan *event.Event {
	output := make(chan *event.Event, len(events))
	for _, evt := range events {
		select {
		case output <- evt:
		case <-ctx.Done():
			close(output)
			return output
		}
	}
	close(output)
	return output
}

func markOpenAIExecutionFailed(state *openAIStreamState) {
	if state != nil {
		state.terminal.Store(true)
	}
}

type openAIResponseWriter struct {
	http.ResponseWriter
	state *openAIStreamState
}

func (w *openAIResponseWriter) Write(payload []byte) (int, error) {
	if w.state != nil && w.state.terminal.Load() &&
		bytes.Equal(payload, []byte("data: [DONE]\n\n")) {
		payload = []byte("data: {\"error\":{\"message\":\"execution failed\",\"type\":\"internal_error\"}}\n\n")
	}
	return w.ResponseWriter.Write(payload)
}

func (w *openAIResponseWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

var _ runner.Runner = openAIQueuedRunner{}

func hasJSONValue(value json.RawMessage) bool {
	return len(value) != 0 && !bytes.Equal(bytes.TrimSpace(value), []byte("null"))
}

func (h openAIHandler) authenticatedRequest(r *http.Request) (gateway.AuthenticatedRequest, error) {
	if r == nil {
		return gateway.AuthenticatedRequest{}, errInvalidRequestIdentity
	}
	requestID, err := requiredHeader(r, headerRequestID)
	if err != nil {
		return gateway.AuthenticatedRequest{}, err
	}
	idempotencyKey, err := requiredHeader(r, headerIdempotencyKey)
	if err != nil {
		return gateway.AuthenticatedRequest{}, err
	}
	sessionID, err := requiredHeader(r, headerSessionID)
	if err != nil {
		return gateway.AuthenticatedRequest{}, err
	}
	if err := rejectHeader(r, headerUserID); err != nil {
		return gateway.AuthenticatedRequest{}, err
	}
	if err := rejectHeader(r, headerSessionPrincipal); err != nil {
		return gateway.AuthenticatedRequest{}, err
	}
	traceID, err := optionalHeader(r, headerTraceID)
	if err != nil {
		return gateway.AuthenticatedRequest{}, err
	}
	if traceID == "" {
		traceID = requestID
	}
	tenantResolver, err := h.authenticator.Resolve(r.Context(), r, auth.RequestIdentity{
		SessionID: sessionID,
		TraceID:   traceID,
	})
	if err != nil {
		return gateway.AuthenticatedRequest{}, err
	}
	return gateway.AuthenticatedRequest{
		RequestID:      requestID,
		IdempotencyKey: idempotencyKey,
		Tenant:         tenantResolver,
	}, nil
}

func requiredHeader(r *http.Request, name string) (string, error) {
	value, err := optionalHeader(r, name)
	if err != nil || value == "" {
		return "", errInvalidRequestIdentity
	}
	return value, nil
}

func optionalHeader(r *http.Request, name string) (string, error) {
	values := r.Header.Values(name)
	if len(values) > 1 {
		return "", errInvalidRequestIdentity
	}
	if len(values) == 0 {
		return "", nil
	}
	value := strings.TrimSpace(values[0])
	if len(value) > maxOpenAIIdentityBytes {
		return "", errInvalidRequestIdentity
	}
	return value, nil
}

func rejectHeader(r *http.Request, name string) error {
	if len(r.Header.Values(name)) != 0 {
		return errInvalidRequestIdentity
	}
	return nil
}

func writeAuthenticationError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errInvalidRequestIdentity):
		http.Error(w, "invalid request identity", http.StatusBadRequest)
	case errors.Is(err, auth.ErrUnauthenticated):
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	case errors.Is(err, auth.ErrCredentialInactive),
		errors.Is(err, auth.ErrTenantInactive),
		errors.Is(err, auth.ErrAppInactive):
		http.Error(w, "forbidden", http.StatusForbidden)
	default:
		http.Error(w, "authentication unavailable", http.StatusServiceUnavailable)
	}
}

func writeAdmissionError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, gateway.ErrInvalidArtifactRef):
		http.Error(w, "invalid artifact reference", http.StatusBadRequest)
	case errors.Is(err, gateway.ErrAdmissionRateLimited):
		w.Header().Set("Retry-After", admissionRetryAfter(err))
		http.Error(w, "request admission rate limited", http.StatusTooManyRequests)
	case errors.Is(err, gateway.ErrAdmissionConcurrencyLimit):
		w.Header().Set("Retry-After", "1")
		http.Error(w, "request admission is busy", http.StatusTooManyRequests)
	case errors.Is(err, gateway.ErrAdmissionQuotaExceeded), errors.Is(err, tenant.ErrQuotaExceeded):
		http.Error(w, "request period quota exceeded", http.StatusTooManyRequests)
	case errors.Is(err, errActiveStreamingLimit):
		w.Header().Set("Retry-After", "1")
		http.Error(w, "active streaming limit reached", http.StatusTooManyRequests)
	case errors.Is(err, gateway.ErrAdmissionDraining):
		w.Header().Set("Retry-After", retryAfterSeconds)
		http.Error(w, "request admission is draining", http.StatusServiceUnavailable)
	case errors.Is(err, gateway.ErrIdempotencyConflict):
		http.Error(w, "request idempotency conflict", http.StatusConflict)
	case errors.Is(err, auth.ErrUnauthenticated),
		errors.Is(err, auth.ErrCredentialInactive),
		errors.Is(err, auth.ErrTenantInactive),
		errors.Is(err, auth.ErrAppInactive):
		writeAuthenticationError(w, err)
	default:
		http.Error(w, "request admission unavailable", http.StatusServiceUnavailable)
	}
}

func admissionRetryAfter(err error) string {
	var rateErr *gateway.AdmissionRateLimitError
	if !errors.As(err, &rateErr) || rateErr == nil || rateErr.RetryAfter <= 0 {
		return "1"
	}
	seconds := int64((rateErr.RetryAfter + time.Second - 1) / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	return strconv.FormatInt(seconds, 10)
}
