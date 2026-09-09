// Package httpapi exposes the phase 1 health and demo message endpoints.
package httpapi

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/executor"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/redaction"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

const (
	maxBodyBytes = 64 << 10
	maxIDBytes   = 256
	maxTextBytes = 16 << 10
)

type Backend interface {
	Ready(context.Context) error
	Handle(context.Context, message.InboundMessage) (message.OutboundMessage, error)
}

func NewHandler(backend Backend) http.Handler {
	mux := http.NewServeMux()
	registerHealth(mux, backend)
	digestV2 := traceDigestV2Enabled(backend)
	mux.HandleFunc("/api/v1/demo/messages", messageHandler(backend, digestV2))
	if async, ok := backend.(asyncBackend); ok {
		mux.HandleFunc("/api/v1/web/messages", webMessageHandler(async, digestV2))
		mux.HandleFunc("/api/v1/web/messages/", webSnapshotHandler(async))
		mux.Handle("/", webUIHandler())
	}
	return mux
}

type asyncBackend interface {
	Ready(context.Context) error
	Accept(context.Context, message.InboundMessage) (channels.AcceptResult, error)
	Snapshot(context.Context, string, string, string) (messaging.Snapshot, error)
}

type webMessageRequest struct {
	BindingID      string `json:"binding_id"`
	MessageID      string `json:"message_id"`
	ExternalUserID string `json:"external_user_id"`
	ConversationID string `json:"conversation_id"`
	Text           string `json:"text"`
}

func webMessageHandler(backend asyncBackend, digestV2 bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		traceCtx, span := telemetry.Start(telemetry.ExtractTraceParent(r.Context(), r.Header.Get("traceparent")), "channel.ingress")
		defer span.End()
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "", "")
			return
		}
		requestID, err := identity.RequestID()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "request_id_failed", "could not create request ID", "", "")
			return
		}
		traceID, err := identity.TraceID()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "trace_id_failed", "could not create trace ID", requestID, "")
			return
		}
		body := http.MaxBytesReader(w, r.Body, maxBodyBytes)
		defer body.Close()
		var input webMessageRequest
		decoder := json.NewDecoder(body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "invalid web message", requestID, traceID)
			return
		}
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF || validate(messageRequest(input)) != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "invalid web message", requestID, traceID)
			return
		}
		if err := backend.Ready(traceCtx); err != nil {
			writeError(w, http.StatusServiceUnavailable, "not_ready", "service dependency unavailable", requestID, traceID)
			return
		}
		result, err := backend.Accept(traceCtx, message.InboundMessage{
			Channel: "demo", BindingID: input.BindingID, PlatformMessageID: input.MessageID, ActorUserID: input.ExternalUserID,
			ConversationID: input.ConversationID, ConversationType: message.ConversationDirect, Text: input.Text,
			RequestID: requestID, TraceID: traceID, TraceParent: gatedTraceParent(r, digestV2), ReceivedAt: time.Now().UTC(),
		})
		if err != nil {
			writeMappedError(w, err, requestID, traceID)
			return
		}
		status := http.StatusAccepted
		if result.Terminal {
			status = http.StatusOK
		}
		writeJSON(w, status, map[string]any{"request_id": result.RequestID, "trace_id": result.TraceID, "message_id": input.MessageID, "submitted": true})
	}
}

func webSnapshotHandler(backend asyncBackend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		traceCtx, span := telemetry.Start(telemetry.ExtractTraceParent(r.Context(), r.Header.Get("traceparent")), "channel.snapshot")
		defer span.End()
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "", "")
			return
		}
		messageID := strings.TrimPrefix(r.URL.Path, "/api/v1/web/messages/")
		bindingID := r.URL.Query().Get("binding_id")
		if messageID == "" || strings.Contains(messageID, "/") {
			writeError(w, http.StatusBadRequest, "invalid_request", "message_id and binding_id are required", "", "")
			return
		}
		if bindingID == "" {
			writeError(w, http.StatusBadRequest, "invalid_request", "binding_id is required", "", "")
			return
		}
		snapshot, err := backend.Snapshot(traceCtx, "demo", bindingID, messageID)
		if err != nil {
			if errors.Is(err, messaging.ErrInboxMissing) {
				writeError(w, http.StatusNotFound, "message_not_found", "message not found", "", "")
				return
			}
			if errors.Is(err, executor.ErrUnknownBinding) {
				writeError(w, http.StatusNotFound, "binding_not_found", "binding not found", "", "")
				return
			}
			if errors.Is(err, gateway.ErrMessageConflict) {
				writeError(w, http.StatusConflict, "message_conflict", "message conflict", "", "")
				return
			}
			writeError(w, http.StatusServiceUnavailable, "not_ready", "service dependency unavailable", "", "")
			return
		}
		body := map[string]any{"message_id": messageID, "status": webStatus(snapshot.State), "request_id": snapshot.RequestID, "trace_id": snapshot.TraceID}
		if snapshot.Result != nil {
			if snapshot.Result.Succeeded {
				body["text"] = snapshot.Result.Reply.Text
			} else {
				body["error_code"] = snapshot.Result.ErrorCode
			}
		}
		writeJSON(w, http.StatusOK, body)
	}
}

func webStatus(state string) string {
	switch state {
	case messaging.StateSucceeded:
		return "succeeded"
	case messaging.StateFailedTerminal:
		return "failed"
	case messaging.StateProcessing, messaging.StateRetryWait:
		return "processing"
	case messaging.StateQueued:
		return "submitted"
	default:
		return "processing"
	}
}

func webUIHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(webUIHTML))
	})
}

//go:embed static/index.html
var webUIHTML string

type ReadyBackend interface {
	Ready(context.Context) error
}

func NewHealthHandler(backend ReadyBackend) http.Handler {
	mux := http.NewServeMux()
	registerHealth(mux, backend)
	return mux
}

func registerHealth(mux *http.ServeMux, backend ReadyBackend) {
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "", "")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", "", "")
			return
		}
		if err := backend.Ready(r.Context()); err != nil {
			writeError(w, http.StatusServiceUnavailable, "not_ready", "service is not ready", "", "")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
}

type messageRequest struct {
	BindingID      string `json:"binding_id"`
	MessageID      string `json:"message_id"`
	ExternalUserID string `json:"external_user_id"`
	ConversationID string `json:"conversation_id"`
	Text           string `json:"text"`
}

func messageHandler(backend Backend, digestV2 bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestID, err := identity.RequestID()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "request_id_failed", "could not create request ID", "", "")
			return
		}
		traceID, err := identity.TraceID()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "trace_id_failed", "could not create trace ID", requestID, "")
			return
		}
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", requestID, traceID)
			return
		}

		body := http.MaxBytesReader(w, r.Body, maxBodyBytes)
		defer body.Close()
		decoder := json.NewDecoder(body)
		decoder.DisallowUnknownFields()
		var input messageRequest
		if err := decoder.Decode(&input); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "invalid JSON request", requestID, traceID)
			return
		}
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			writeError(w, http.StatusBadRequest, "invalid_request", "request must contain one JSON object", requestID, traceID)
			return
		}
		if err := validate(input); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error(), requestID, traceID)
			return
		}

		reply, err := backend.Handle(r.Context(), message.InboundMessage{
			Channel:        "demo",
			BindingID:      input.BindingID,
			MessageID:      input.MessageID,
			ExternalUserID: input.ExternalUserID,
			ConversationID: input.ConversationID,
			Text:           input.Text,
			RequestID:      requestID,
			TraceID:        traceID,
			TraceParent:    gatedTraceParent(r, digestV2),
			ReceivedAt:     time.Now().UTC(),
		})
		if err != nil {
			if reply.TraceID != "" {
				traceID = reply.TraceID
			}
			writeMappedError(w, err, requestID, traceID)
			return
		}
		writeJSON(w, http.StatusOK, reply)
	}
}

func traceDigestV2Enabled(backend any) bool {
	capability, ok := backend.(interface{ TraceDigestV2Enabled() bool })
	return ok && capability.TraceDigestV2Enabled()
}

func gatedTraceParent(r *http.Request, digestV2 bool) string {
	if !digestV2 {
		return ""
	}
	return validTraceParent(r.Header.Get("traceparent"))
}

func validTraceParent(value string) string {
	value = strings.TrimSpace(value)
	parts := strings.Split(value, "-")
	if len(parts) != 4 || len(parts[0]) != 2 || len(parts[1]) != 32 || len(parts[2]) != 16 || len(parts[3]) != 2 {
		return ""
	}
	for _, part := range parts {
		for _, current := range part {
			if !((current >= '0' && current <= '9') || (current >= 'a' && current <= 'f') || (current >= 'A' && current <= 'F')) {
				return ""
			}
		}
	}
	if strings.Trim(parts[1], "0") == "" || strings.Trim(parts[2], "0") == "" || strings.EqualFold(parts[3], "00") {
		return ""
	}
	return strings.ToLower(value)
}

func validate(input messageRequest) error {
	if input.BindingID == "" || input.MessageID == "" || input.ExternalUserID == "" || input.ConversationID == "" {
		return errors.New("binding_id, message_id, external_user_id and conversation_id are required")
	}
	for name, value := range map[string]string{
		"binding_id": input.BindingID, "message_id": input.MessageID,
		"external_user_id": input.ExternalUserID, "conversation_id": input.ConversationID,
	} {
		if len([]byte(value)) > maxIDBytes {
			return errors.New(name + " is too long")
		}
	}
	if strings.TrimSpace(input.Text) == "" {
		return errors.New("text is required")
	}
	if len([]byte(input.Text)) > maxTextBytes {
		return errors.New("text is too long")
	}
	return nil
}

func writeMappedError(w http.ResponseWriter, err error, requestID, traceID string) {
	status := http.StatusBadGateway
	code := "agent_failed"
	message := "agent request failed"
	switch {
	case errors.Is(err, gateway.ErrMessageConflict):
		status, code, message = http.StatusConflict, "message_conflict", "message ID conflicts with an existing payload"
	case errors.Is(err, gateway.ErrTaskPending):
		status, code, message = http.StatusGatewayTimeout, "task_pending", "task is still processing; retry with the same message_id"
	case errors.Is(err, gateway.ErrMessagingUnavailable):
		status, code, message = http.StatusServiceUnavailable, "not_ready", "service dependency unavailable"
	case errors.Is(err, executor.ErrUnknownBinding):
		status, code, message = http.StatusNotFound, "binding_not_found", "binding not found"
	case errors.Is(err, governance.ErrActorForbidden):
		status, code, message = http.StatusForbidden, "actor_forbidden", "actor is not authorized"
	case errors.Is(err, governance.ErrPolicyMissing):
		status, code, message = http.StatusServiceUnavailable, "tenant_policy_missing", "tenant policy is unavailable"
	case errors.Is(err, governance.ErrPolicyUnavailable):
		status, code, message = http.StatusServiceUnavailable, "tenant_policy_unavailable", "tenant policy is unavailable"
	case errors.Is(err, executor.ErrRunnerDraining), errors.Is(err, executor.ErrConfigurationUnavailable), errors.Is(err, executor.ErrDependencyUnavailable):
		status, code, message = http.StatusServiceUnavailable, "not_ready", "service dependency unavailable"
	case errors.Is(err, executor.ErrAgentTimeout):
		status, code, message = http.StatusGatewayTimeout, "model_timeout", "model request timed out"
	case errors.Is(err, executor.ErrWorkerLost):
		status, code, message = http.StatusBadGateway, "worker_lost", "worker lost while processing task"
	case errors.Is(err, executor.ErrEmptyAgentResponse):
		status, code, message = http.StatusBadGateway, "empty_agent_response", "agent returned no text"
	}
	writeError(w, status, code, message, requestID, traceID)
}

func writeError(w http.ResponseWriter, status int, code, message, requestID, traceID string) {
	writeJSON(w, status, map[string]string{
		"code":       code,
		"message":    redaction.Default.Redact(message),
		"request_id": requestID,
		"trace_id":   traceID,
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
