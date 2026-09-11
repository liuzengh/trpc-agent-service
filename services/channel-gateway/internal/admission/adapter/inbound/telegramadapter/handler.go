// Package telegramadapter authenticates Telegram webhooks before calling the
// durable admission boundary. It deliberately does not use the SDK webhook
// handler: an HTTP acknowledgement must follow the admission transaction.
package telegramadapter

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
)

const (
	maxBodyBytes = 1 << 20
	secretHeader = "X-Telegram-Bot-Api-Secret-Token"
)

// Acceptor returns success only after the immutable admission receipt commits.
// It is invoked for ignored events and interactions as well as text inputs.
type Acceptor interface {
	AcceptInbound(context.Context, domain.Inbound) (domain.Receipt, error)
}

type handler struct {
	accountID  string
	secretHash [sha256.Size]byte
	acceptor   Acceptor
}

// ReplyContext is the minimum provider-specific address retained for delivery.
// Callback data is deliberately excluded: interaction handling is not prompting.
type ReplyContext struct {
	ChatID          string `json:"chat_id,omitempty"`
	MessageThreadID string `json:"message_thread_id,omitempty"`
	SourceMessageID string `json:"source_message_id,omitempty"`
	CallbackQueryID string `json:"callback_query_id,omitempty"`
	InlineMessageID string `json:"inline_message_id,omitempty"`
}

// NewHandler binds a trusted, stable account ID to one registered webhook route.
// The secret must use Telegram's 1-256 character [A-Za-z0-9_-] alphabet. Account
// IDs are opaque ASCII identifiers (1-128 letters/digits/dot/colon/dash/underscore).
// The caller owns route registration, HTTP deadlines, and secret rotation.
func NewHandler(accountID string, webhookSecret string, acceptor Acceptor) (http.Handler, error) {
	if !identifier(accountID, 128, true) || !identifier(webhookSecret, 256, false) || acceptor == nil {
		return nil, errors.New("invalid Telegram webhook configuration")
	}
	return &handler{accountID: accountID, secretHash: sha256.Sum256([]byte(webhookSecret)), acceptor: acceptor}, nil
}

func identifier(s string, limit int, allowDot bool) bool {
	if len(s) == 0 || len(s) > limit {
		return false
	}
	for _, c := range s {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || allowDot && (c == '.' || c == ':') {
			continue
		}
		return false
	}
	return true
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		respond(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	values := r.Header.Values(secretHeader)
	provided := sha256.Sum256([]byte(r.Header.Get(secretHeader)))
	if len(values) != 1 || subtle.ConstantTimeCompare(provided[:], h.secretHash[:]) != 1 {
		respond(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if r.ContentLength > maxBodyBytes {
		respond(w, http.StatusRequestEntityTooLarge, "request_too_large")
		return
	}
	body := http.MaxBytesReader(w, r.Body, maxBodyBytes)
	defer body.Close()
	data, err := io.ReadAll(body)
	if err != nil {
		var limit *http.MaxBytesError
		if errors.As(err, &limit) {
			respond(w, http.StatusRequestEntityTooLarge, "request_too_large")
		} else {
			respond(w, http.StatusBadRequest, "invalid_request")
		}
		return
	}
	inbound, err := Normalize(data, h.accountID, time.Now().UTC())
	if err != nil {
		respond(w, http.StatusBadRequest, "invalid_request")
		return
	}
	_, err = h.acceptor.AcceptInbound(r.Context(), inbound)
	switch {
	case err == nil:
		respond(w, http.StatusOK, "")
	case errors.Is(err, domain.ErrInvalidInput):
		respond(w, http.StatusBadRequest, "invalid_request")
	case errors.Is(err, domain.ErrConflict):
		respond(w, http.StatusConflict, "event_conflict")
	default:
		// Includes transaction/commit failures and unknown infrastructure errors.
		// No provider success is emitted when durable acceptance is uncertain.
		w.Header().Set("Retry-After", "1")
		respond(w, http.StatusServiceUnavailable, "temporarily_unavailable")
	}
}

func respond(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if code == "" {
		_, _ = io.WriteString(w, `{"ok":true}`+"\n")
		return
	}
	// code is a local constant, never a provider payload or a database error.
	_, _ = io.WriteString(w, `{"ok":false,"error":"`+code+`"}`+"\n")
}
