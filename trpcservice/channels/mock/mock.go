// Package mock provides a Mock Channel that depends on no real IM platform.
//
// It exercises the full pipeline (callback → normalization → handling →
// reply) without external IM accounts; a real adapter implements the same
// channels.Channel interface and slots in.
package mock

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// ChannelName is the channel identifier, stamped on every message this
// channel serves and keyed by in the channel_binding rows.
const ChannelName = "mock"

// CallbackPath is the webhook path mounted on the platform mux; it must match
// the channel_binding.webhook_path row for tenant routing. Exported so a
// caller-supplied webhook_path can be validated against the served routes.
const CallbackPath = "/mock/callback"

// callbackRequest is the callback payload pushed by the simulated IM platform.
type callbackRequest struct {
	MsgID  string `json:"msg_id"`  // message ID, used for idempotency
	UserID string `json:"user_id"` // sender
	ChatID string `json:"chat_id"` // group ID; empty means a direct chat
	Text   string `json:"text"`
}

// Channel is the mock implementation of channels.Channel.
type Channel struct {
	mu   sync.Mutex
	sent []channels.OutboundMessage // inbox of sent replies, for inspection in tests
}

// New creates a mock Channel.
func New() *Channel {
	return &Channel{}
}

// Name implements channels.Channel.
func (c *Channel) Name() string { return ChannelName }

// RegisterRoutes implements channels.Channel.
// POST /mock/callback simulates the IM webhook callback (dev only: the mock
// is an unauthenticated injector).
func (c *Channel) RegisterRoutes(mux *http.ServeMux, h channels.Handler) {
	handler, err := c.CallbackHandler(h, channels.BindingCredentials{})
	if err != nil {
		plog.Errorf("mock callback mount failed: %v", err)
		return
	}
	mux.HandleFunc(http.MethodPost+" "+CallbackPath, handler)
}

// CallbackHandler implements channels.BindingAware: the mock has no
// credentials, but binding-scoped paths let multi-tenant integration tests
// exercise the dispatcher with two tenants on one channel.
func (c *Channel) CallbackHandler(h channels.Handler, _ channels.BindingCredentials) (http.HandlerFunc, error) {
	return func(w http.ResponseWriter, r *http.Request) {
		// 1. Decode the IM callback payload (a real implementation verifies
		//    the signature and decrypts before this step).
		var req callbackRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}
		if req.MsgID == "" || req.UserID == "" {
			http.Error(w, "msg_id and user_id are required", http.StatusBadRequest)
			return
		}

		// 2. Normalize the external payload into the platform-wide InboundMessage.
		msg := channels.InboundMessage{
			Channel:     c.Name(),
			MsgID:       req.MsgID,
			SessionKey:  channels.SessionKey(c.Name(), req.UserID, req.ChatID),
			UserID:      req.UserID,
			ChatID:      req.ChatID,
			Text:        req.Text,
			WebhookPath: r.URL.Path, // the gateway routes tenant/app through it
			TraceID:     req.MsgID,  // the gateway stamps the real trace ID over it
			ReceivedAt:  time.Now(),
		}

		// 3. Hand over to the upper layer (the gateway dedups, enqueues to
		//    stream:inbound and returns an empty "accepted" reply; sync/debug
		//    path: the handler returns the reply inline).
		out, err := h.Handle(r.Context(), msg)
		if errors.Is(err, channels.ErrDuplicate) {
			zap.L().Warn("duplicate message dropped",
				zap.String(plog.FieldChannel, c.Name()), zap.String(plog.FieldMsgID, req.MsgID))
			// Duplicates still get a success reply so the IM stops retrying.
			writeReply(w, "duplicate", "")
			return
		}
		if err != nil {
			http.Error(w, "handle error: "+err.Error(), http.StatusInternalServerError)
			return
		}

		// An empty reply text means the reply is delivered asynchronously.
		if out.Text == "" {
			writeReply(w, "accepted", "")
			return
		}

		// 5. Sync reply path (debug): record it in the in-memory inbox.
		if err := c.Send(r.Context(), out); err != nil {
			http.Error(w, "send error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		writeReply(w, "ok", out.Text)
	}, nil
}

// Send implements channels.Channel. The mock calls no IM API; it only records
// the reply in memory for tests and debugging.
func (c *Channel) Send(_ context.Context, msg channels.OutboundMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sent = append(c.sent, msg)
	zap.L().Info("reply sent",
		zap.String(plog.FieldChannel, msg.Channel),
		zap.String(plog.FieldSessionKey, msg.SessionKey),
		zap.String(plog.FieldTraceID, msg.TraceID),
		zap.Int("text_len", len(msg.Text)))
	return nil
}

// Sent returns all recorded replies, for test assertions.
func (c *Channel) Sent() []channels.OutboundMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]channels.OutboundMessage(nil), c.sent...)
}

func writeReply(w http.ResponseWriter, status, text string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": status, "reply": text})
}
