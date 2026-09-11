package channels

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// WebChat is a browser-based channel for local self-verification (the
// mentor-endorsed deepseek-style web page). It implements the same Adapter
// contract as real IMs: inbound via POST callback, outbound via SSE push.
type WebChat struct {
	mu      sync.Mutex
	streams map[string]chan *OutboundMessage // key: tenantID + ":" + userID

	// accept, when set, switches the callback into durable mode: the message
	// is persisted before the 202 is written. A nil accept keeps the legacy
	// in-process flow.
	accept func(ctx context.Context, in *InboundMessage) error
}

// NewWebChat builds the webchat adapter.
func NewWebChat() *WebChat {
	return &WebChat{streams: make(map[string]chan *OutboundMessage)}
}

// WithDurableAccept turns the callback into the durable receive path: once
// an acceptor is set, Callback persists the normalized message through it
// and only then answers 202. An acceptor error is returned to the HTTP layer
// with no 202 written, so a client (or its retry) sees a real failure.
func (c *WebChat) WithDurableAccept(accept func(ctx context.Context, in *InboundMessage) error) *WebChat {
	c.accept = accept
	return c
}

// Type implements Adapter.
func (c *WebChat) Type() Type { return TypeWebChat }

type webchatRequest struct {
	User  string `json:"user"`
	MsgID string `json:"msg_id"`
	Text  string `json:"text"`
}

// Callback implements Adapter: accepts one chat message and ACKs with 202.
func (c *WebChat) Callback(w http.ResponseWriter, r *http.Request) ([]*InboundMessage, error) {
	if r.Method != http.MethodPost {
		return nil, fmt.Errorf("webchat callback requires POST")
	}
	var req webchatRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		return nil, fmt.Errorf("decode body: %w", err)
	}
	if req.User == "" || req.Text == "" {
		return nil, fmt.Errorf("user and text are required")
	}
	if req.MsgID == "" {
		req.MsgID = fmt.Sprintf("%s-%d", req.User, time.Now().UnixNano())
	}
	// The tenant is parsed here, not inherited from the caller: the durable
	// accept hook runs inside Callback, before any gateway wrapping could
	// attach the path's tenant.
	in := &InboundMessage{
		TenantID: strings.Trim(strings.TrimPrefix(r.URL.Path, "/callback/"+string(TypeWebChat)+"/"), "/"),
		Channel:  TypeWebChat,
		UserID:   req.User,
		MsgID:    req.MsgID,
		Text:     req.Text,
	}
	if c.accept != nil {
		// Durable mode: persist before the 202 so a failed write is a
		// retryable error, not a silently accepted message.
		if err := c.accept(r.Context(), in); err != nil {
			return nil, fmt.Errorf("webchat: accept message: %w", err)
		}
	}
	w.WriteHeader(http.StatusAccepted)
	return []*InboundMessage{in}, nil
}

// Send implements Adapter: pushes one chunk/final to the user's SSE stream.
// Messages for users without an open stream are dropped, like an offline IM
// user. The channel is never closed while registered sends may race it; it
// is simply unregistered and left to GC.
func (c *WebChat) Send(ctx context.Context, msg *OutboundMessage) error {
	key := msg.Target.TenantID + ":" + msg.Target.UserID
	c.mu.Lock()
	ch, ok := c.streams[key]
	c.mu.Unlock()
	if !ok {
		return nil
	}
	select {
	case ch <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Routes exposes the SSE stream endpoint consumed by the chat page.
func (c *WebChat) Routes() map[string]http.Handler {
	return map[string]http.Handler{"/webchat/stream": http.HandlerFunc(c.stream)}
}

type sseEvent struct {
	Text  string `json:"text"`
	Chunk bool   `json:"chunk"`
	Done  bool   `json:"done"`
}

func (c *WebChat) stream(w http.ResponseWriter, r *http.Request) {
	tenant := r.URL.Query().Get("tenant")
	user := r.URL.Query().Get("user")
	if tenant == "" || user == "" {
		http.Error(w, "tenant and user are required", http.StatusBadRequest)
		return
	}
	key := tenant + ":" + user
	ch := make(chan *OutboundMessage, 64)

	c.mu.Lock()
	if _, exists := c.streams[key]; exists {
		c.mu.Unlock()
		http.Error(w, "stream already open for this user", http.StatusConflict)
		return
	}
	c.streams[key] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.streams, key)
		c.mu.Unlock()
	}()

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-ch:
			data, err := json.Marshal(sseEvent{Text: msg.Text, Chunk: msg.Chunk, Done: msg.Done})
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}
