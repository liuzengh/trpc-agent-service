// HTTP ingress for IM platform callbacks.
//
// This is the transport half of channels.Manager.HandleWebhook: it extracts the
// signature headers, bounds the body, maps the ingress's errors onto statuses
// and answers the platform's URL-verification handshake.
//
// The route is unauthenticated by design — the platform cannot present a Bearer
// token — so the platform's own signature is the only credential. It is mounted
// only when the ingress is enabled in config, and it is fail-closed: any
// verification failure is a 401 and the event is dropped.
package web

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/channels"
)

// WebhookPathPrefix is the route prefix of the callback ingress. The binding id
// is the last path segment, so each bound account has its own callback URL.
const WebhookPathPrefix = "/webhooks/im/"

// maxWebhookBody bounds a callback body. Platform events are small (well under
// 100 KiB); the bound keeps an unauthenticated endpoint from being used to make
// the process allocate.
const maxWebhookBody = 1 << 20

// WebhookSource is the slice of the IM manager the ingress needs. An interface
// keeps the HTTP layer testable without an IM manager.
type WebhookSource interface {
	HandleWebhook(ctx context.Context, bindingID string, cb channels.WebhookCallback) (channels.WebhookEvent, error)
}

// WebhookAPI serves IM platform callbacks.
type WebhookAPI struct {
	src WebhookSource
}

// NewWebhookAPI returns the callback ingress handler.
func NewWebhookAPI(src WebhookSource) *WebhookAPI {
	return &WebhookAPI{src: src}
}

// Register mounts the callback route. The handler is registered for the exact
// prefix so Go's mux gives it precedence over the management surface.
func (a *WebhookAPI) Register(mux *http.ServeMux) {
	mux.HandleFunc(WebhookPathPrefix, a.handle)
}

// Paths returns the routes that must bypass the platform's auth middleware: a
// platform callback carries a signature, not a session.
func (a *WebhookAPI) Paths() []string { return []string{WebhookPathPrefix} }

func (a *WebhookAPI) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		// Feishu probes the URL with a POST as well; a GET here is a health
		// check or a mistake, not an event.
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	bindingID := strings.TrimPrefix(r.URL.Path, WebhookPathPrefix)
	if bindingID == "" || strings.Contains(bindingID, "/") {
		http.Error(w, "binding id required", http.StatusNotFound)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxWebhookBody+1))
	if err != nil {
		http.Error(w, "unreadable body", http.StatusBadRequest)
		return
	}
	if len(body) > maxWebhookBody {
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}
	event, err := a.src.HandleWebhook(r.Context(), bindingID, channels.WebhookCallback{
		Body:      body,
		Timestamp: r.Header.Get("X-Lark-Request-Timestamp"),
		Nonce:     r.Header.Get("X-Lark-Request-Nonce"),
		Signature: r.Header.Get("X-Lark-Signature"),
	})
	if err != nil {
		writeWebhookError(w, bindingID, err)
		return
	}
	if event.Challenge != "" {
		// The handshake answer is a bare {"challenge": "..."} body.
		writeWebhookJSON(w, http.StatusOK, map[string]string{"challenge": event.Challenge})
		return
	}
	writeWebhookJSON(w, http.StatusOK, map[string]any{"code": 0})
}

// writeWebhookError maps an ingress refusal onto the status the platform reads.
func writeWebhookError(w http.ResponseWriter, bindingID string, err error) {
	switch {
	case errors.Is(err, channels.ErrBindingNotFound):
		// No such binding: nothing to serve. 404 also keeps a probe from
		// learning which binding ids exist.
		http.Error(w, "unknown binding", http.StatusNotFound)
	case errors.Is(err, channels.ErrWebhookUnsupported):
		http.Error(w, "channel is not served over HTTP callbacks", http.StatusNotImplemented)
	case errors.Is(err, channels.ErrWebhookUnauthorized):
		// Fail closed and say nothing about why: a signature oracle helps an
		// attacker.
		slog.Warn("web: IM callback rejected", "binding", bindingID)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	case errors.Is(err, channels.ErrWebhookBusy):
		// Ask the platform to retry: the event is valid, the pipeline is full.
		w.Header().Set("Retry-After", "1")
		http.Error(w, "busy", http.StatusServiceUnavailable)
	default:
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}

// writeWebhookJSON writes a JSON response body.
func writeWebhookJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		slog.Warn("web: writing the callback response failed", "err", err)
	}
}
