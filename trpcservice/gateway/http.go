package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	runtimebudget "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget"
	runtimerunner "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/runner"
)

const (
	defaultHTTPMaxBodyBytes   = 1 << 20
	defaultHTTPRequestTimeout = 30 * time.Second
	requestIDHeader           = "X-Request-ID"
	traceIDHeader             = "X-Trace-ID"
)

// HTTPConfig wires the protocol adapter to the protocol-neutral Dispatcher.
// A nil Dispatcher or Authenticator intentionally leaves health available but
// keeps readiness false; the command can therefore start safely before its
// control-plane dependencies are loaded.
type HTTPConfig struct {
	Dispatcher     DispatchService
	Authenticator  APIAuthenticator
	Admin          http.Handler
	AdminAuth      http.Handler
	WeCom          http.Handler
	Web            http.Handler
	Ready          func() bool
	Limiter        *TenantLimiter
	Idempotency    *IdempotencyStore
	MaxBodyBytes   int64
	RequestTimeout time.Duration
	Observability  observability.Provider
}

// HTTPHandler serves the first strict JSON/SSE Gateway surface.
type HTTPHandler struct {
	dispatcher     DispatchService
	authenticator  APIAuthenticator
	admin          http.Handler
	adminAuth      http.Handler
	wecom          http.Handler
	web            http.Handler
	ready          func() bool
	limiter        *TenantLimiter
	idempotency    *IdempotencyStore
	maxBodyBytes   int64
	requestTimeout time.Duration
	telemetry      observability.Provider
	metrics        metrics.Catalog
	ownLimiter     bool
	ownIdempotency bool
	draining       atomic.Bool
}

type chatRequest struct {
	Content           string                    `json:"content"`
	ContentType       string                    `json:"content_type,omitempty"`
	ExternalMessageID string                    `json:"external_message_id,omitempty"`
	ExternalUserID    string                    `json:"external_user_id"`
	ConversationKind  channels.ConversationKind `json:"conversation_kind"`
	ExternalPeerID    string                    `json:"external_peer_id,omitempty"`
	ExternalChatID    string                    `json:"external_chat_id,omitempty"`
	ExternalThreadID  string                    `json:"external_thread_id,omitempty"`
}

type chatResponse struct {
	RequestID string `json:"request_id"`
	TraceID   string `json:"trace_id,omitempty"`
	Text      string `json:"text,omitempty"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
	Done      bool   `json:"done"`
}

type httpErrorResponse struct {
	RequestID string `json:"request_id"`
	TraceID   string `json:"trace_id,omitempty"`
	Error     string `json:"error"`
}

// NewHTTPHandler creates the strict HTTP adapter and its default process-local
// protection components.
func NewHTTPHandler(config HTTPConfig) (*HTTPHandler, error) {
	if config.MaxBodyBytes == 0 {
		config.MaxBodyBytes = defaultHTTPMaxBodyBytes
	}
	if config.MaxBodyBytes < 1 {
		return nil, fmt.Errorf("%w: HTTP body limit must be positive", ErrInvalid)
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = defaultHTTPRequestTimeout
	}
	if config.RequestTimeout < 0 {
		return nil, fmt.Errorf("%w: HTTP request timeout cannot be negative", ErrInvalid)
	}
	if config.Ready == nil {
		config.Ready = func() bool { return config.Dispatcher != nil && config.Authenticator != nil }
	}
	if config.Observability == nil {
		config.Observability = observability.NewNoopProvider()
	}
	handler := &HTTPHandler{
		dispatcher: config.Dispatcher, authenticator: config.Authenticator, ready: config.Ready,
		admin:        config.Admin,
		adminAuth:    config.AdminAuth,
		wecom:        config.WeCom,
		web:          config.Web,
		maxBodyBytes: config.MaxBodyBytes, requestTimeout: config.RequestTimeout,
		telemetry: config.Observability, metrics: metrics.New(config.Observability),
		limiter: config.Limiter, idempotency: config.Idempotency,
	}
	if handler.limiter == nil {
		var err error
		handler.limiter, err = NewTenantLimiter(TenantLimiterConfig{})
		if err != nil {
			return nil, err
		}
		handler.ownLimiter = true
	}
	if handler.idempotency == nil {
		var err error
		handler.idempotency, err = NewIdempotencyStore(IdempotencyConfig{})
		if err != nil {
			return nil, err
		}
		handler.ownIdempotency = true
	}
	return handler, nil
}

// Handler returns the net/http handler for this Gateway.
func (handler *HTTPHandler) Handler() http.Handler { return handler }

// Ready reports whether the adapter may accept execution requests.
func (handler *HTTPHandler) Ready() bool {
	if handler == nil || handler.draining.Load() || handler.dispatcher == nil || handler.authenticator == nil {
		return false
	}
	if handler.limiter == nil || !handler.limiter.Ready() || handler.idempotency == nil || !handler.idempotency.Ready() {
		return false
	}
	return handler.ready == nil || handler.ready()
}

// BeginShutdown makes readiness fail and stops new execution requests. The
// caller must wait for net/http.Server.Shutdown before calling Close.
func (handler *HTTPHandler) BeginShutdown() {
	if handler == nil {
		return
	}
	handler.draining.Store(true)
}

// Close releases process-local admission state owned by the handler. It is
// intentionally separate from BeginShutdown so in-flight requests can finish
// while the HTTP server drains.
func (handler *HTTPHandler) Close() error {
	if handler == nil {
		return nil
	}
	handler.BeginShutdown()
	var closeErr error
	if handler.ownLimiter && handler.limiter != nil {
		closeErr = errors.Join(closeErr, handler.limiter.Close())
	}
	if handler.ownIdempotency && handler.idempotency != nil {
		closeErr = errors.Join(closeErr, handler.idempotency.Close())
	}
	return closeErr
}

func (handler *HTTPHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request == nil {
		return
	}
	started := time.Now()
	statusWriter := &httpStatusWriter{ResponseWriter: writer}
	ctx, _, finish := observability.StartOperation(request.Context(), handler.telemetry, observability.OperationHTTPRequest, "http")
	request = request.WithContext(ctx)
	_ = handler.metrics.Request(ctx, map[string]string{"component": "http", "operation": observability.OperationHTTPRequest, "status": "started"})
	defer func() {
		var outcome error
		if statusWriter.status >= http.StatusBadRequest {
			outcome = errors.New("http request failed")
		}
		finish(outcome)
		_ = handler.metrics.Operation(ctx, started, map[string]string{"component": "http", "operation": observability.OperationHTTPRequest}, outcome)
	}()
	writer = statusWriter
	if strings.HasPrefix(request.URL.Path, "/wecom/callback/") {
		if handler.wecom == nil {
			handler.writeError(writer, request, http.StatusNotFound, "not found", "", "")
			return
		}
		handler.wecom.ServeHTTP(writer, request)
		return
	}
	if request.URL.Path == "/admin/auth" || strings.HasPrefix(request.URL.Path, "/admin/auth/") {
		if handler.adminAuth == nil {
			handler.writeError(writer, request, http.StatusNotFound, "not found", "", "")
			return
		}
		handler.adminAuth.ServeHTTP(writer, request)
		return
	}
	if request.URL.Path == "/admin/v1" || strings.HasPrefix(request.URL.Path, "/admin/v1/") {
		if handler.admin == nil {
			handler.writeError(writer, request, http.StatusNotFound, "not found", "", "")
			return
		}
		handler.admin.ServeHTTP(writer, request)
		return
	}
	switch request.URL.Path {
	case "/healthz":
		handler.health(writer, request)
	case "/readyz":
		handler.readyz(writer, request)
	case "/v1/chat":
		handler.chat(writer, request, false)
	case "/v1/chat/stream":
		handler.chat(writer, request, true)
	default:
		if handler.web != nil && request.URL.Path != "/v1" && !strings.HasPrefix(request.URL.Path, "/v1/") && request.URL.Path != "/admin" && !strings.HasPrefix(request.URL.Path, "/admin/") {
			handler.web.ServeHTTP(writer, request)
			return
		}
		handler.writeError(writer, request, http.StatusNotFound, "not found", "", "")
	}
}

type httpStatusWriter struct {
	http.ResponseWriter
	status int
}

func (w *httpStatusWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}
func (w *httpStatusWriter) Write(value []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(value)
}

func (handler *HTTPHandler) health(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		handler.writeError(writer, request, http.StatusMethodNotAllowed, "method not allowed", "", "")
		return
	}
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(writer, "ok\n")
}

func (handler *HTTPHandler) readyz(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		handler.writeError(writer, request, http.StatusMethodNotAllowed, "method not allowed", "", "")
		return
	}
	if !handler.Ready() {
		handler.writeError(writer, request, http.StatusServiceUnavailable, "not ready", "", "")
		return
	}
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(writer, "ready\n")
}

func (handler *HTTPHandler) chat(writer http.ResponseWriter, request *http.Request, stream bool) {
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodPost)
		handler.writeError(writer, request, http.StatusMethodNotAllowed, "method not allowed", "", "")
		return
	}
	requestID, traceID, err := requestCorrelation(request)
	if err != nil {
		handler.writeError(writer, request, http.StatusBadRequest, "invalid request correlation", requestID, traceID)
		return
	}
	if !handler.Ready() {
		handler.writeError(writer, request, http.StatusServiceUnavailable, "not ready", requestID, traceID)
		return
	}
	ctx := request.Context()
	if handler.requestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, handler.requestTimeout)
		defer cancel()
	}
	authenticated, err := handler.authenticator.Authenticate(ctx, request)
	if err != nil {
		handler.writeMappedError(writer, request, requestID, traceID, err)
		return
	}
	principal, err := newAPIPrincipal(authenticated)
	if err != nil {
		handler.writeMappedError(writer, request, requestID, traceID, err)
		return
	}
	message, err := handler.decodeMessage(writer, request)
	if err != nil {
		handler.writeMappedError(writer, request, requestID, traceID, err)
		return
	}
	if message.ExternalMessageID == "" {
		message.ExternalMessageID = requestID
	}
	limitLease, err := handler.limiter.Acquire(ctx, principal.TenantID())
	if err != nil {
		handler.writeMappedError(writer, request, requestID, traceID, err)
		return
	}
	defer func() { _ = limitLease.Release() }()
	claim, replay, err := handler.idempotency.Begin(ctx, principal, message)
	if err != nil {
		handler.writeMappedError(writer, request, requestID, traceID, err)
		return
	}
	if claim == nil {
		replay = rebindDispatchEvents(replay, requestID, traceID)
		if stream {
			handler.writeReplayStream(writer, replay)
			return
		}
		handler.writeFinalResponse(writer, requestID, traceID, replay)
		return
	}
	completed := false
	defer func() {
		if !completed {
			_ = claim.Fail()
		}
	}()
	dispatchEvents, err := handler.dispatcher.Dispatch(ctx, DispatchRequest{Principal: principal, Message: message, RequestID: requestID, TraceID: traceID})
	if err != nil {
		handler.writeMappedError(writer, request, requestID, traceID, err)
		return
	}
	if dispatchEvents == nil {
		handler.writeError(writer, request, http.StatusBadGateway, "execution failed", requestID, traceID)
		return
	}
	if stream {
		if !supportsFlush(writer) {
			handler.writeError(writer, request, http.StatusInternalServerError, "streaming unavailable", requestID, traceID)
			return
		}
		completed = handler.writeStream(writer, ctx, claim, dispatchEvents)
		return
	}
	events, collectErr := collectHTTPEvents(ctx, dispatchEvents)
	if collectErr != nil {
		handler.writeMappedError(writer, request, requestID, traceID, collectErr)
		return
	}
	if err := claim.Complete(events); err != nil {
		handler.writeError(writer, request, http.StatusInternalServerError, "gateway error", requestID, traceID)
		return
	}
	completed = true
	handler.writeFinalResponse(writer, requestID, traceID, events)
}

func supportsFlush(writer http.ResponseWriter) bool {
	if wrapped, ok := writer.(*httpStatusWriter); ok {
		_, ok = wrapped.ResponseWriter.(http.Flusher)
		return ok
	}
	_, ok := writer.(http.Flusher)
	return ok
}

func (handler *HTTPHandler) decodeMessage(writer http.ResponseWriter, request *http.Request) (InboundMessage, error) {
	if !isJSONContentType(request.Header.Get("Content-Type")) {
		return InboundMessage{}, fmt.Errorf("%w: content type must be application/json", ErrInvalid)
	}
	body := http.MaxBytesReader(writer, request.Body, handler.maxBodyBytes)
	defer func() { _ = body.Close() }()
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	var input chatRequest
	if err := decoder.Decode(&input); err != nil {
		return InboundMessage{}, fmt.Errorf("%w: request JSON is invalid", ErrInvalid)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return InboundMessage{}, fmt.Errorf("%w: request JSON has trailing data", ErrInvalid)
	}
	message := InboundMessage{
		Content: input.Content, ContentType: input.ContentType,
		ExternalMessageID: input.ExternalMessageID, ExternalUserID: input.ExternalUserID,
		ConversationKind: input.ConversationKind, ExternalPeerID: input.ExternalPeerID,
		ExternalChatID: input.ExternalChatID, ExternalThreadID: input.ExternalThreadID,
	}
	return message.Normalize()
}

func requestCorrelation(request *http.Request) (string, string, error) {
	requestID, err := normalizeCorrelationID(request.Header.Get(requestIDHeader), true)
	if err != nil {
		return "", "", err
	}
	traceID, err := normalizeCorrelationID(request.Header.Get(traceIDHeader), false)
	if err != nil {
		return requestID, "", err
	}
	return requestID, traceID, nil
}

func isJSONContentType(value string) bool {
	mediaType, _, err := mime.ParseMediaType(value)
	return err == nil && mediaType == "application/json"
}

func collectHTTPEvents(ctx context.Context, events <-chan DispatchEvent) ([]DispatchEvent, error) {
	collected := make([]DispatchEvent, 0, 4)
	for {
		select {
		case event, ok := <-events:
			if !ok {
				return collected, nil
			}
			collected = append(collected, event)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (handler *HTTPHandler) writeStream(writer http.ResponseWriter, ctx context.Context, claim *IdempotencyClaim, events <-chan DispatchEvent) bool {
	var flusher http.Flusher
	var ok bool
	if wrapped, wrappedOK := writer.(*httpStatusWriter); wrappedOK {
		flusher, ok = wrapped.ResponseWriter.(http.Flusher)
	} else {
		flusher, ok = writer.(http.Flusher)
	}
	if !ok {
		return false
	}
	writer.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("Connection", "keep-alive")
	writer.WriteHeader(http.StatusOK)
	flusher.Flush()
	collected := make([]DispatchEvent, 0, 4)
	for {
		select {
		case event, open := <-events:
			if !open {
				if len(collected) > 0 {
					_ = claim.Complete(collected)
					return true
				}
				return false
			}
			collected = append(collected, event)
			if err := writeSSEEvent(writer, event); err != nil {
				return false
			}
			flusher.Flush()
			if event.Done {
				_ = claim.Complete(collected)
				return true
			}
		case <-ctx.Done():
			return false
		}
	}
}

func (handler *HTTPHandler) writeReplayStream(writer http.ResponseWriter, events []DispatchEvent) {
	var flusher http.Flusher
	var ok bool
	if wrapped, wrappedOK := writer.(*httpStatusWriter); wrappedOK {
		flusher, ok = wrapped.ResponseWriter.(http.Flusher)
	} else {
		flusher, ok = writer.(http.Flusher)
	}
	if !ok {
		handler.writeError(writer, nil, http.StatusInternalServerError, "gateway error", "", "")
		return
	}
	writer.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.WriteHeader(http.StatusOK)
	for _, event := range events {
		if writeSSEEvent(writer, event) != nil {
			return
		}
		flusher.Flush()
	}
}

func writeSSEEvent(writer io.Writer, event DispatchEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(writer, "event: %s\ndata: %s\n\n", event.Type, data); err != nil {
		return err
	}
	return nil
}

func (handler *HTTPHandler) writeFinalResponse(writer http.ResponseWriter, requestID, traceID string, events []DispatchEvent) {
	response := finalChatResponse(requestID, traceID, events)
	handler.writeJSON(writer, http.StatusOK, response)
}

func finalChatResponse(requestID, traceID string, events []DispatchEvent) chatResponse {
	response := chatResponse{RequestID: requestID, TraceID: traceID, Status: "complete", Done: true}
	for _, event := range events {
		switch event.Type {
		case DispatchEventMessage:
			response.Text += event.Text
		case DispatchEventStatus:
			if event.Status != "" {
				response.Status = event.Status
			}
		case DispatchEventError:
			response.Error = event.Error
			response.Status = "error"
		case DispatchEventDone:
			response.Done = event.Done
			if event.Status != "" {
				response.Status = event.Status
			}
		}
	}
	return response
}

func rebindDispatchEvents(events []DispatchEvent, requestID, traceID string) []DispatchEvent {
	clone := cloneDispatchEvents(events)
	for index := range clone {
		clone[index].RequestID = requestID
		clone[index].TraceID = traceID
	}
	return clone
}

func (handler *HTTPHandler) writeMappedError(writer http.ResponseWriter, request *http.Request, requestID, traceID string, err error) {
	status, message := mapHTTPError(err)
	handler.writeError(writer, request, status, message, requestID, traceID)
}

func mapHTTPError(err error) (int, string) {
	switch {
	case err == nil:
		return http.StatusInternalServerError, "gateway error"
	case errors.Is(err, context.Canceled):
		return http.StatusRequestTimeout, "request canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout, "request deadline exceeded"
	case errors.Is(err, ErrUnauthenticated):
		return http.StatusUnauthorized, "unauthenticated"
	case errors.Is(err, ErrInvalid):
		return http.StatusBadRequest, "invalid request"
	case errors.Is(err, ErrRateLimited):
		return http.StatusTooManyRequests, "rate limited"
	case errors.Is(err, runtimebudget.ErrExceeded):
		return http.StatusTooManyRequests, "budget exceeded"
	case errors.Is(err, runtimebudget.ErrCostUnavailable):
		return http.StatusServiceUnavailable, "cost configuration unavailable"
	case errors.Is(err, runtimebudget.ErrUnavailable):
		return http.StatusServiceUnavailable, "budget unavailable"
	case errors.Is(err, ErrDuplicateMessage):
		return http.StatusConflict, "duplicate message"
	case errors.Is(err, ErrNotReady), errors.Is(err, ErrClosed), errors.Is(err, runtimerunner.ErrNotReady), errors.Is(err, runtimerunner.ErrClosed):
		return http.StatusServiceUnavailable, "not ready"
	case errors.Is(err, ErrIdempotencyCapacity):
		return http.StatusServiceUnavailable, "gateway capacity unavailable"
	case errors.Is(err, ErrAuditWriteFailed):
		return http.StatusBadGateway, ErrAuditWriteFailed.Error()
	case errors.Is(err, ErrExecution), errors.Is(err, ErrPlanUnavailable), errors.Is(err, runtimerunner.ErrRunnerUnavailable):
		return http.StatusBadGateway, "execution failed"
	default:
		return http.StatusInternalServerError, "gateway error"
	}
}

func (handler *HTTPHandler) writeError(writer http.ResponseWriter, _ *http.Request, status int, message, requestID, traceID string) {
	handler.writeJSON(writer, status, httpErrorResponse{RequestID: requestID, TraceID: traceID, Error: message})
}

func (handler *HTTPHandler) writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
