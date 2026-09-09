// Package webui implements the browser chat channel and SSE replies.
package webui

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Violet2314/trpc-agent-service/trpcservice/channels"
	"github.com/Violet2314/trpc-agent-service/trpcservice/gateway"
	"github.com/Violet2314/trpc-agent-service/trpcservice/reply"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

const (
	channelType = "webui"
	cookieName  = "trpc_webui_session"
)

// Adapter implements WebUI POST ingestion and SSE streaming.
type Adapter struct {
	hub     StreamHub
	static  http.Handler
	catalog Catalog
}

// New constructs a WebUI adapter. catalog may be nil; the desk dropdown
// then renders empty until control-plane bindings exist.
func New(hub StreamHub, static http.Handler, catalog Catalog) (*Adapter, error) {
	if hub == nil {
		return nil, errors.New("WebUI hub is required")
	}
	return &Adapter{hub: hub, static: static, catalog: catalog}, nil
}

// Type implements channels.Adapter.
func (*Adapter) Type() string {
	return channelType
}

// Run registers WebUI HTTP routes.
func (a *Adapter) Run(_ context.Context, mux *http.ServeMux, sink channels.Sink) error {
	if mux == nil || sink == nil {
		return errors.New("WebUI mux and Gateway sink are required")
	}
	mux.HandleFunc("GET /channels/webui/desks", a.handleDesks)
	mux.HandleFunc("POST /channels/webui/{bindingID}/messages", func(w http.ResponseWriter, r *http.Request) {
		a.handleMessage(w, r, sink)
	})
	mux.HandleFunc("GET /channels/webui/{bindingID}/stream", a.handleStream)
	if a.static != nil {
		mux.Handle("GET /", a.static)
	}
	return nil
}

// NewReplier implements channels.Adapter.
func (a *Adapter) NewReplier(tenant.Snapshot) channels.Replier {
	return &replier{hub: a.hub}
}

func (a *Adapter) handleDesks(w http.ResponseWriter, r *http.Request) {
	if a.catalog == nil {
		writeJSON(w, http.StatusOK, []Desk{})
		return
	}
	desks, err := a.catalog.ListDesks(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "list WebUI desks")
		return
	}
	if desks == nil {
		desks = []Desk{}
	}
	writeJSON(w, http.StatusOK, desks)
}

func (a *Adapter) handleMessage(w http.ResponseWriter, r *http.Request, sink channels.Sink) {
	var body struct {
		ID   string `json:"id"`
		Text string `json:"text"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid message body")
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeJSONError(w, http.StatusBadRequest, "message body must contain one JSON value")
		return
	}
	if body.ID == "" || body.Text == "" {
		writeJSONError(w, http.StatusBadRequest, "id and text are required")
		return
	}
	senderID, err := ensureSenderCookie(w, r)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "create browser session")
		return
	}
	result, err := sink(r.Context(), gateway.InboundMessage{
		Channel:        channelType,
		RouteKey:       r.PathValue("bindingID"),
		MsgID:          body.ID,
		ChatType:       "p2p",
		SenderID:       senderID,
		AddressedToBot: true,
		Text:           body.Text,
		TraceID:        body.ID,
	})
	if err != nil {
		writeJSONError(w, http.StatusServiceUnavailable, "Gateway unavailable")
		return
	}
	if result.SessionID != "" {
		if err := a.hub.Ensure(result.SessionID, senderID); err != nil {
			writeJSONError(w, http.StatusConflict, "browser session ownership conflict")
			return
		}
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *Adapter) handleStream(w http.ResponseWriter, r *http.Request) {
	sessionID := r.URL.Query().Get("session")
	if sessionID == "" {
		writeJSONError(w, http.StatusBadRequest, "session query parameter is required")
		return
	}
	cookie, err := r.Cookie(cookieName)
	if err != nil || cookie.Value == "" {
		writeJSONError(w, http.StatusUnauthorized, "browser session cookie is required")
		return
	}
	events, unsubscribe, err := a.hub.Subscribe(sessionID, cookie.Value)
	if err != nil {
		if errors.Is(err, errStreamForbidden) {
			writeJSONError(w, http.StatusForbidden, "stream belongs to another browser")
			return
		}
		writeJSONError(w, http.StatusNotFound, "stream not found")
		return
	}
	defer unsubscribe()
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSONError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	for {
		select {
		case event, ok := <-events:
			if !ok {
				return
			}
			data, err := json.Marshal(event)
			if err != nil {
				return
			}
			if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.Type, data); err != nil {
				return
			}
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

type replier struct {
	hub StreamHub
}

func (r *replier) Reply(
	ctx context.Context,
	sessionID string,
	message gateway.InboundMessage,
	events <-chan reply.Event,
) error {
	var publishErr error
	publish := true
	for event := range events {
		if publish {
			select {
			case <-ctx.Done():
				publish = false
				publishErr = ctx.Err()
			default:
			}
		}
		if publish {
			if err := r.hub.Publish(sessionID, message.SenderID, event); err != nil {
				publish = false
				publishErr = err
			}
		}
	}
	return publishErr
}

func ensureSenderCookie(w http.ResponseWriter, r *http.Request) (string, error) {
	if cookie, err := r.Cookie(cookieName); err == nil && cookie.Value != "" {
		return cookie.Value, nil
	}
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}
	value := hex.EncodeToString(token[:])
	http.SetCookie(w, &http.Cookie{
		Name:     cookieName,
		Value:    value,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   int((7 * 24 * time.Hour).Seconds()),
	})
	return value, nil
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
