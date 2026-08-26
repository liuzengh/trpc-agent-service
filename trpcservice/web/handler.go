package web

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"

	agentservice "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
)

const (
	defaultMaxBodyBytes = 32 << 10
	maxUserIDLength     = 128
	maxSessionIDLength  = 256
	maxMessageLength    = 8 << 10
)

// ChatService is the small boundary between the HTTP layer and Agent runtime.
type ChatService interface {
	Chat(
		ctx context.Context,
		userID string,
		sessionID string,
		text string,
	) (agentservice.ChatResult, error)
}

// Handler serves the tutorial HTTP API.
type Handler struct {
	chatService ChatService
	maxBodySize int64
}

// NewHandler creates a handler with health and chat endpoints.
func NewHandler(chatService ChatService) http.Handler {
	h := &Handler{
		chatService: chatService,
		maxBodySize: defaultMaxBodyBytes,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", h.handleHealth)
	mux.HandleFunc("/chat", h.handleChat)
	return mux
}

type chatRequest struct {
	UserID    string `json:"user_id"`
	SessionID string `json:"session_id"`
	Message   string `json:"message"`
}

type chatResponse struct {
	Reply      string `json:"reply"`
	RequestID  string `json:"request_id,omitempty"`
	UserID     string `json:"user_id"`
	SessionID  string `json:"session_id"`
	EventCount int    `json:"event_count"`
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

	var request chatRequest
	if err := decodeJSON(w, r, h.maxBodySize, &request); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	request.UserID = strings.TrimSpace(request.UserID)
	request.SessionID = strings.TrimSpace(request.SessionID)
	request.Message = strings.TrimSpace(request.Message)
	if err := validateChatRequest(request); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}

	result, err := h.chatService.Chat(
		r.Context(),
		request.UserID,
		request.SessionID,
		request.Message,
	)
	if err != nil {
		log.Printf("chat failed: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "agent execution failed"})
		return
	}

	writeJSON(w, http.StatusOK, chatResponse{
		Reply:      result.Reply,
		RequestID:  result.RequestID,
		UserID:     request.UserID,
		SessionID:  request.SessionID,
		EventCount: result.EventCount,
	})
}

func validateChatRequest(request chatRequest) error {
	switch {
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
