package openclaw

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	servicemetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Submitter asynchronously schedules a durable RunRequest.
type Submitter interface {
	Submit(gateway.RunRequest) error
}

// Handler authenticates bindings, canonicalizes identities, claims Inbox, and acknowledges quickly.
type Handler struct {
	Routes     Routes
	Inbox      idempotency.Store
	Submitter  Submitter
	Hub        EventBus
	Status     StatusStore
	Canceler   Canceler
	Approver   Approver
	ClaimOwner string
	ClaimTTL   time.Duration
	Telemetry  *servicemetrics.Telemetry
	Readiness  func(context.Context) error
	Admission  AdmissionLimiter
}

var _ gateway.InboundAcceptor = (*Handler)(nil)

// AcceptInbound persists and schedules an already provider-authenticated
// message. Channel adapters own signature verification and tenant binding;
// this method owns the shared Inbox/queue durability boundary.
func (handler *Handler) AcceptInbound(ctx context.Context, inbound gateway.InboundMessage) (accepted gateway.AcceptedMessage, resultErr error) {
	if handler == nil || handler.Inbox == nil || handler.Submitter == nil || handler.ClaimOwner == "" {
		return gateway.AcceptedMessage{}, errors.New("gateway is not configured")
	}
	if inbound.Media != nil {
		return gateway.AcceptedMessage{}, errors.New("opaque media reference must be resolved before Inbox persistence")
	}
	if inbound.TenantID == "" || inbound.AppID == "" || inbound.BindingID == "" || inbound.ExternalMessageID == "" || inbound.ExternalUserID == "" || inbound.UserID == "" || inbound.SessionID == "" || strings.TrimSpace(inbound.Text) == "" || inbound.ConfigVersion == 0 {
		return gateway.AcceptedMessage{}, errors.New("complete verified inbound message is required")
	}
	if !validCorrelationID(inbound.TraceID) {
		inbound.TraceID = newID()
	}
	if inbound.ReceivedAt.IsZero() {
		inbound.ReceivedAt = time.Now().UTC()
	}
	if handler.Admission != nil {
		if err := handler.Admission.Allow(ctx, inbound); err != nil {
			return gateway.AcceptedMessage{}, err
		}
	}
	callbackCtx, callbackSpan := handler.Telemetry.Start(ctx, "channel.callback", servicemetrics.SpanFields{TenantID: inbound.TenantID, AppID: inbound.AppID, Channel: inbound.BindingID, RequestID: inbound.ExternalMessageID, TraceID: inbound.TraceID})
	defer callbackSpan.End()
	started := time.Now()
	defer func() {
		status := "accepted"
		if resultErr != nil {
			status = "failed"
		}
		handler.Telemetry.Request(callbackCtx, servicemetrics.Labels{TenantID: inbound.TenantID, AppID: inbound.AppID, Channel: inbound.BindingID, Operation: "callback", Status: status}, time.Since(started), 0, 0)
	}()
	if inbound.TraceContext == nil {
		inbound.TraceContext = handler.Telemetry.Inject(callbackCtx)
	}
	claimCtx, claimSpan := handler.Telemetry.Start(callbackCtx, "inbox.claim", servicemetrics.SpanFields{TenantID: inbound.TenantID, AppID: inbound.AppID, Channel: inbound.BindingID, RequestID: inbound.ExternalMessageID, TraceID: inbound.TraceID})
	claim, won, err := handler.Inbox.Claim(claimCtx, inbound, handler.ClaimOwner, handler.claimTTL())
	claimSpan.End()
	if err != nil {
		return gateway.AcceptedMessage{}, err
	}
	accepted = gateway.AcceptedMessage{RequestID: claim.InboxID, SessionID: inbound.SessionID, TraceID: inbound.TraceID, Duplicate: !won}
	if !won {
		accepted.TraceID = claim.Message.TraceID
		return accepted, nil
	}
	if handler.Status != nil {
		handler.Status.Publish(gateway.RunEvent{Type: "run.accepted", TenantID: inbound.TenantID, BindingID: inbound.BindingID, RequestID: claim.InboxID, SessionID: inbound.SessionID, TraceID: inbound.TraceID})
	}
	run := claim.RunRequest()
	if err := handler.Submitter.Submit(run); err != nil {
		_ = handler.Inbox.Fail(context.Background(), claim, err, time.Now().UTC().Add(time.Second))
		return gateway.AcceptedMessage{}, err
	}
	return accepted, nil
}

// Routes returns the OpenClaw-compatible HTTP surface.
func (handler *Handler) RoutesHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/healthz", method(http.MethodGet, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})))
	mux.Handle("/readyz", method(http.MethodGet, http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if handler.Readiness != nil {
			ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
			defer cancel()
			if err := handler.Readiness(ctx); err != nil {
				writeError(w, http.StatusServiceUnavailable, errors.New("dependencies unavailable"))
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})))
	mux.Handle("/v1/gateway/status", method(http.MethodGet, http.HandlerFunc(handler.status)))
	mux.Handle("/v1/gateway/cancel", method(http.MethodPost, http.HandlerFunc(handler.cancel)))
	mux.Handle("/v1/gateway/approve", method(http.MethodPost, http.HandlerFunc(handler.approve)))
	mux.Handle("/v1/gateway/messages", method(http.MethodPost, http.HandlerFunc(handler.message)))
	mux.Handle("/v1/gateway/messages:stream", method(http.MethodPost, http.HandlerFunc(handler.stream)))
	return mux
}

func (handler *Handler) status(w http.ResponseWriter, request *http.Request) {
	route, err := handler.authenticate(request)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err)
		return
	}
	requestID := request.URL.Query().Get("request_id")
	if requestID == "" || !strings.HasPrefix(requestID, route.TenantID+"/"+route.BindingID+"/") {
		writeError(w, http.StatusNotFound, errors.New("request not found"))
		return
	}
	status, ok := handler.Status.Get(request.Context(), route.TenantID, requestID)
	if !ok {
		writeError(w, http.StatusNotFound, errors.New("request not found"))
		return
	}
	writeJSON(w, http.StatusOK, status)
}
func (handler *Handler) cancel(w http.ResponseWriter, request *http.Request) {
	route, err := handler.authenticate(request)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err)
		return
	}
	var input struct {
		RequestID string `json:"request_id"`
	}
	if json.NewDecoder(io.LimitReader(request.Body, 4096)).Decode(&input) != nil || !strings.HasPrefix(input.RequestID, route.TenantID+"/"+route.BindingID+"/") {
		writeError(w, http.StatusBadRequest, errors.New("valid request_id is required"))
		return
	}
	if handler.Canceler == nil || !handler.Canceler.Cancel(route.TenantID, input.RequestID) {
		writeError(w, http.StatusNotFound, errors.New("active request not found"))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"request_id": input.RequestID, "canceled": true})
}
func (handler *Handler) approve(w http.ResponseWriter, request *http.Request) {
	route, err := handler.authenticate(request)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err)
		return
	}
	var input struct {
		RequestID string `json:"request_id"`
		ToolName  string `json:"tool_name"`
	}
	if json.NewDecoder(io.LimitReader(request.Body, 4096)).Decode(&input) != nil || !strings.HasPrefix(input.RequestID, route.TenantID+"/"+route.BindingID+"/") || strings.TrimSpace(input.ToolName) == "" {
		writeError(w, http.StatusBadRequest, errors.New("valid request_id and tool_name are required"))
		return
	}
	if handler.Approver == nil || !handler.Approver.Grant(route.TenantID, input.RequestID, input.ToolName) {
		writeError(w, http.StatusServiceUnavailable, errors.New("approval store is unavailable"))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"request_id": input.RequestID, "tool_name": input.ToolName, "approved": true})
}
func (handler *Handler) authenticate(request *http.Request) (Route, error) {
	if handler == nil || handler.Routes == nil {
		return Route{}, errors.New("gateway is not configured")
	}
	bindingID := strings.TrimSpace(request.Header.Get("X-Channel-Binding"))
	credential := strings.TrimSpace(strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer "))
	route, err := handler.Routes.Resolve(bindingID, credential)
	if err != nil {
		return Route{}, errors.New("invalid gateway credential")
	}
	return route, nil
}

func (handler *Handler) message(w http.ResponseWriter, request *http.Request) {
	accepted, _, status, err := handler.accept(w, request)
	if err != nil {
		writeError(w, status, err)
		return
	}
	writeJSON(w, http.StatusAccepted, accepted)
}

func (handler *Handler) stream(w http.ResponseWriter, request *http.Request) {
	accepted, events, status, err := handler.acceptStream(w, request)
	if err != nil {
		writeError(w, status, err)
		return
	}
	defer handler.Hub.Unsubscribe(accepted.RequestID, events)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher, ok := w.(http.Flusher)
	if !ok {
		return
	}
	if accepted.Duplicate {
		writeSSE(w, StreamEvent{Type: "run.ignored", RequestID: accepted.RequestID, SessionID: accepted.SessionID, TraceID: accepted.TraceID, Ignored: true, Terminal: true})
		flusher.Flush()
		return
	}
	for {
		select {
		case event, ok := <-events:
			if !ok {
				writeSSE(w, StreamEvent{Type: "run.error", RequestID: accepted.RequestID, SessionID: accepted.SessionID, TraceID: accepted.TraceID, Error: "event stream closed", Terminal: true})
				flusher.Flush()
				return
			}
			writeSSE(w, event)
			flusher.Flush()
			if event.Terminal {
				return
			}
		case <-request.Context().Done():
			return
		}
	}
}

func (handler *Handler) accept(writer http.ResponseWriter, request *http.Request) (MessageResponse, <-chan StreamEvent, int, error) {
	return handler.acceptWithSubscription(writer, request, false)
}
func (handler *Handler) acceptStream(writer http.ResponseWriter, request *http.Request) (MessageResponse, <-chan StreamEvent, int, error) {
	return handler.acceptWithSubscription(writer, request, true)
}

func (handler *Handler) acceptWithSubscription(writer http.ResponseWriter, request *http.Request, subscribe bool) (MessageResponse, <-chan StreamEvent, int, error) {
	if handler == nil || handler.Routes == nil || handler.Inbox == nil || handler.Submitter == nil || handler.ClaimOwner == "" {
		return MessageResponse{}, nil, http.StatusServiceUnavailable, errors.New("gateway is not configured")
	}
	if subscribe && handler.Hub == nil {
		return MessageResponse{}, nil, http.StatusServiceUnavailable, errors.New("stream event hub is not configured")
	}
	route, err := handler.authenticate(request)
	if err != nil {
		return MessageResponse{}, nil, http.StatusUnauthorized, errors.New("invalid gateway credential")
	}
	callbackStarted := time.Now()
	parentCtx := handler.Telemetry.ExtractHTTP(request.Context(), request.Header)
	callbackCtx, callbackSpan := handler.Telemetry.Start(parentCtx, "gateway.callback", servicemetrics.SpanFields{TenantID: route.TenantID, AppID: route.AppID, Channel: string(route.ChannelType)})
	defer callbackSpan.End()
	request = request.WithContext(callbackCtx)
	var input MessageRequest
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return MessageResponse{}, nil, http.StatusBadRequest, fmt.Errorf("decode message: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return MessageResponse{}, nil, http.StatusBadRequest, errors.New("decode message: trailing data")
	}
	if input.MessageID == "" || input.SessionID != "" {
		return MessageResponse{}, nil, http.StatusUnprocessableEntity, errors.New("message_id is required and session_id is server-generated")
	}
	if input.Channel != "" && input.Channel != string(route.ChannelType) {
		return MessageResponse{}, nil, http.StatusUnprocessableEntity, errors.New("channel does not match authenticated binding")
	}
	externalUserID := strings.TrimSpace(input.From)
	if externalUserID == "" {
		externalUserID = strings.TrimSpace(input.UserID)
	}
	if externalUserID == "" {
		return MessageResponse{}, nil, http.StatusUnprocessableEntity, errors.New("from or user_id is required")
	}
	text := strings.TrimSpace(input.Text)
	for _, part := range input.ContentParts {
		if part.Type == "text" && strings.TrimSpace(part.Text) != "" {
			if text != "" {
				text += "\n"
			}
			text += strings.TrimSpace(part.Text)
		}
	}
	if text == "" {
		return MessageResponse{}, nil, http.StatusUnprocessableEntity, errors.New("text content is required")
	}
	userID, err := tenant.CanonicalUserID(route.ChannelType, route.BindingID, externalUserID)
	if err != nil {
		return MessageResponse{}, nil, http.StatusUnprocessableEntity, err
	}
	threadID := input.ThreadID
	if threadID == "" {
		threadID = input.Thread
	}
	sessionID, err := canonicalSession(route.BindingID, externalUserID, input.ConversationID, threadID)
	if err != nil {
		return MessageResponse{}, nil, http.StatusUnprocessableEntity, err
	}
	traceID := strings.TrimSpace(request.Header.Get("X-Trace-ID"))
	if !validCorrelationID(traceID) {
		traceID = newID()
	}
	inbound := gateway.InboundMessage{TenantID: route.TenantID, AppID: route.AppID, BindingID: route.BindingID, ExternalMessageID: input.MessageID, ExternalUserID: externalUserID, ConversationID: input.ConversationID, UserID: userID, SessionID: sessionID, Text: text, TraceID: traceID, ConfigVersion: route.ConfigVersion, ReceivedAt: time.Now().UTC(), TraceContext: handler.Telemetry.Inject(request.Context())}
	if handler.Admission != nil {
		if err := handler.Admission.Allow(request.Context(), inbound); err != nil {
			if errors.Is(err, ErrAdmissionLimited) {
				return MessageResponse{}, nil, http.StatusTooManyRequests, err
			}
			return MessageResponse{}, nil, http.StatusServiceUnavailable, err
		}
	}
	claimCtx, claimSpan := handler.Telemetry.Start(request.Context(), "inbox.claim", servicemetrics.SpanFields{TenantID: route.TenantID, AppID: route.AppID, Channel: string(route.ChannelType), RequestID: input.MessageID, TraceID: traceID})
	claim, won, err := handler.Inbox.Claim(claimCtx, inbound, handler.ClaimOwner, handler.claimTTL())
	claimSpan.End()
	if err != nil {
		return MessageResponse{}, nil, http.StatusServiceUnavailable, err
	}
	response := MessageResponse{RequestID: claim.InboxID, SessionID: sessionID, TraceID: traceID, Accepted: true, Duplicate: !won}
	if !won {
		response.TraceID = claim.Message.TraceID
		return response, nil, http.StatusAccepted, nil
	}
	if handler.Status != nil {
		handler.Status.Publish(gateway.RunEvent{Type: "run.accepted", TenantID: route.TenantID, BindingID: route.BindingID, RequestID: claim.InboxID, SessionID: sessionID, TraceID: traceID})
	}
	var events <-chan StreamEvent
	var unsubscribe func()
	if subscribe && handler.Hub != nil {
		events, unsubscribe = handler.Hub.Subscribe(claim.InboxID)
	}
	run := gateway.RunRequest{InboxID: claim.InboxID, InboxSeq: claim.InboxSeq, TenantID: route.TenantID, AppID: route.AppID, BindingID: route.BindingID, ExternalMessageID: input.MessageID, ExternalUserID: externalUserID, ConversationID: input.ConversationID, UserID: userID, SessionID: sessionID, Text: text, TraceID: traceID, TraceContext: inbound.TraceContext, ConfigVersion: route.ConfigVersion, ClaimOwner: claim.Owner, ClaimToken: claim.ClaimToken, ClaimAttempt: claim.Attempt, ClaimLeaseUntil: claim.LeaseUntil}
	if err := handler.Submitter.Submit(run); err != nil {
		if unsubscribe != nil {
			unsubscribe()
		}
		_ = handler.Inbox.Fail(context.Background(), claim, err, time.Now().UTC().Add(time.Second))
		return MessageResponse{}, nil, http.StatusServiceUnavailable, err
	}
	handler.Telemetry.Request(request.Context(), servicemetrics.Labels{TenantID: route.TenantID, AppID: route.AppID, Channel: string(route.ChannelType), Operation: "callback", Status: "accepted"}, time.Since(callbackStarted), 0, 0)
	return response, events, http.StatusAccepted, nil
}

func validCorrelationID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, current := range value {
		if (current >= 'a' && current <= 'z') || (current >= 'A' && current <= 'Z') || (current >= '0' && current <= '9') || current == '-' || current == '_' || current == '.' {
			continue
		}
		return false
	}
	return true
}

func canonicalSession(bindingID, userID, conversationID, threadID string) (string, error) {
	var base string
	var err error
	if strings.TrimSpace(conversationID) == "" {
		base, err = tenant.DirectSessionID(bindingID, userID)
	} else {
		base, err = tenant.GroupSessionID(bindingID, conversationID)
	}
	if err != nil || strings.TrimSpace(threadID) == "" {
		return base, err
	}
	return tenant.ThreadSessionID(base, threadID)
}
func (handler *Handler) claimTTL() time.Duration {
	if handler.ClaimTTL > 0 {
		return handler.ClaimTTL
	}
	return 30 * time.Second
}
func newID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("trace-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(value[:])
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func writeError(w http.ResponseWriter, status int, err error) {
	errorType := "invalid_request"
	if status >= 500 {
		errorType = "server_error"
	} else if status == http.StatusUnauthorized {
		errorType = "authentication_error"
	}
	writeJSON(w, status, ErrorResponse{Error: APIError{Type: errorType, Message: err.Error()}})
}
func writeSSE(w io.Writer, event StreamEvent) {
	payload, _ := json.Marshal(event)
	_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.Type, payload)
}

func method(want string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != want {
			w.Header().Set("Allow", want)
			writeError(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
			return
		}
		next.ServeHTTP(w, request)
	})
}
