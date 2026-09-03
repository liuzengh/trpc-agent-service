package web

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	agentservice "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	"github.com/liuzengh/trpc-agent-service/trpcservice/routing"
)

const (
	defaultMaxBodyBytes = 32 << 10
	maxMessageIDLength  = 256
	maxBindingKeyLength = 128
	maxUserIDLength     = 128
	maxSessionIDLength  = 256
	maxMessageLength    = 8 << 10
)

// ChatService is the small boundary between the HTTP layer and Agent runtime.
type ChatService interface {
	ChatWithScope(ctx context.Context, input agentservice.ChatInput) (agentservice.ChatResult, error)

	Ready(ctx context.Context) error
}

// Handler serves the tutorial HTTP API.
type Handler struct {
	chatService   ChatService
	maxBodySize   int64
	readiness     []readinessCheck
	routeResolver routing.Resolver
}

type readinessCheck struct {
	name  string
	check func(context.Context) error
}

// Option customizes the HTTP handler.
type Option func(*Handler)

// WithReadinessCheck adds a platform dependency to /readyz.
func WithReadinessCheck(name string, check func(context.Context) error) Option {
	return func(handler *Handler) {
		if check == nil {
			return
		}
		handler.readiness = append(handler.readiness, readinessCheck{
			name:  strings.TrimSpace(name),
			check: check,
		})
	}
}

// WithRouteResolver resolves an untrusted binding key into trusted tenant and
// application identity.
func WithRouteResolver(resolver routing.Resolver) Option {
	return func(handler *Handler) {
		handler.routeResolver = resolver
	}
}

// NewHandler creates a handler with health and chat endpoints.
func NewHandler(chatService ChatService, opts ...Option) http.Handler {
	h := &Handler{
		chatService: chatService,
		maxBodySize: defaultMaxBodyBytes,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(h)
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", h.handleHealth)
	mux.HandleFunc("/readyz", h.handleReady)
	mux.HandleFunc("/chat", h.handleChat)
	return mux
}

func (h *Handler) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "method not allowed"})
		return
	}
	if h.chatService == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "chat service is unavailable"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := h.chatService.Ready(ctx); err != nil {
		log.Printf("readiness check failed: %v", err)
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "service is not ready"})
		return
	}
	for _, dependency := range h.readiness {
		if err := dependency.check(ctx); err != nil {
			log.Printf("readiness check %q failed: %v", dependency.name, err)
			writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "service is not ready"})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type chatRequest struct {
	BindingKey string `json:"binding_key"`
	MessageID  string `json:"message_id"`
	UserID     string `json:"user_id"`
	SessionID  string `json:"session_id"`
	Message    string `json:"message"`
}

type chatResponse struct {
	Reply      string `json:"reply"`
	RequestID  string `json:"request_id,omitempty"`
	MessageID  string `json:"message_id"`
	UserID     string `json:"user_id"`
	SessionID  string `json:"session_id"`
	EventCount int    `json:"event_count"`
	Replayed   bool   `json:"replayed"`
	TenantID   string `json:"tenant_id"`
	AppID      string `json:"app_id"`
	RevisionID string `json:"revision_id"`
	AgentName  string `json:"agent_name"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "method not allowed"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleChat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "method not allowed"})
		return
	}
	if h.chatService == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "chat service is unavailable"})
		return
	}
	if h.routeResolver == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "route resolver is unavailable"})
		return
	}

	var request chatRequest
	if err := decodeJSON(w, r, h.maxBodySize, &request); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	request.BindingKey = strings.TrimSpace(request.BindingKey)
	request.MessageID = strings.TrimSpace(request.MessageID)
	request.UserID = strings.TrimSpace(request.UserID)
	request.SessionID = strings.TrimSpace(request.SessionID)
	request.Message = strings.TrimSpace(request.Message)
	if err := validateChatRequest(request); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}

	scope, err := h.routeResolver.Resolve(r.Context(), request.BindingKey)
	if err != nil {
		if errors.Is(err, routing.ErrBindingNotFound) {
			writeJSON(w, http.StatusNotFound, errorResponse{Error: "channel binding not found"})
			return
		}
		if errors.Is(err, routing.ErrRouteDisabled) {
			writeJSON(w, http.StatusForbidden, errorResponse{Error: "channel route is disabled"})
			return
		}
		log.Printf("resolve chat route failed: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "route resolution failed"})
		return
	}
	result, err := h.chatService.ChatWithScope(r.Context(), agentservice.ChatInput{
		Scope:     scope,
		MessageID: request.MessageID,
		UserID:    request.UserID,
		SessionID: request.SessionID,
		Text:      request.Message,
	})
	if err != nil {
		log.Printf("chat failed: %v", err)
		if errors.Is(err, idempotency.ErrKeyConflict) {
			writeJSON(w, http.StatusConflict, errorResponse{
				Error: "message_id was already used for different content",
			})
			return
		}
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "agent execution failed"})
		return
	}

	writeJSON(w, http.StatusOK, chatResponse{
		Reply:      result.Reply,
		RequestID:  result.RequestID,
		MessageID:  result.MessageID,
		UserID:     request.UserID,
		SessionID:  request.SessionID,
		EventCount: result.EventCount,
		Replayed:   result.Replayed,
		TenantID:   result.TenantID,
		AppID:      result.AppID,
		RevisionID: result.RevisionID,
		AgentName:  result.AgentName,
	})
}

func validateChatRequest(request chatRequest) error {
	switch {
	case request.BindingKey == "":
		return errors.New("binding_key is required")
	case len(request.BindingKey) > maxBindingKeyLength:
		return errors.New("binding_key is too long")
	case request.MessageID == "":
		return errors.New("message_id is required")
	case len(request.MessageID) > maxMessageIDLength:
		return errors.New("message_id is too long")
	case request.UserID == "":
		return errors.New("user_id is required")
	case len(request.UserID) > maxUserIDLength:
		return errors.New("user_id is too long")
	case request.SessionID == "":
		return errors.New("session_id is required")
	case len(request.SessionID) > maxSessionIDLength:
		return errors.New("session_id is too long")
	case request.Message == "":
		return errors.New("message is required")
	case len(request.Message) > maxMessageLength:
		return errors.New("message is too long")
	default:
		return nil
	}
}

func decodeJSON(
	w http.ResponseWriter,
	r *http.Request,
	maxBytes int64,
	target any,
) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return errors.New("request body must be a valid JSON object")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain one JSON object")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("encode HTTP response: %v", err)
	}
}
