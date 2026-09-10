// Package fake provides an HTTP channel for the P0 two-tenant vertical slice.
package fake

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	messagingmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type Binding struct {
	Locator           string
	Tenant            tenant.Context
	IdentityKey       []byte
	ExternalAccountID string
}

type Handler struct {
	Dispatcher gateway.Dispatcher
	Inbox      messaging.InboxClaimer
	Payloads   messaging.PayloadStore
	bindings   map[string]Binding
}

func NewHandler(dispatcher gateway.Dispatcher, bindings ...Binding) *Handler {
	store := messagingmemory.New()
	h := &Handler{Dispatcher: dispatcher, Inbox: store, Payloads: store, bindings: make(map[string]Binding)}
	for _, binding := range bindings {
		h.bindings[binding.Locator] = binding
	}
	return h
}

type inbound struct {
	ExternalMessageID string `json:"external_message_id"`
	ExternalUserID    string `json:"external_user_id"`
	ExternalChatID    string `json:"external_chat_id"`
	Text              string `json:"text"`
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	locator := strings.TrimPrefix(r.URL.Path, "/bindings/")
	binding, ok := h.bindings[locator]
	if !ok {
		http.Error(w, "binding not found", http.StatusNotFound)
		return
	}
	if err := binding.Tenant.Validate(); err != nil {
		http.Error(w, "invalid binding", http.StatusServiceUnavailable)
		return
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	var in inbound
	if err := dec.Decode(&in); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if in.ExternalMessageID == "" || in.ExternalUserID == "" || in.ExternalChatID == "" || in.Text == "" {
		http.Error(w, "missing field", http.StatusBadRequest)
		return
	}
	externalAccountID := binding.ExternalAccountID
	if externalAccountID == "" {
		externalAccountID = binding.Locator
	}
	payload, _ := json.Marshal(in)
	userID := hmacID("u1", binding.IdentityKey, binding.Tenant.TenantID, in.ExternalUserID)
	sessionID := hmacID("s1", binding.IdentityKey, binding.Tenant.TenantID, in.ExternalChatID)
	digest := sha256.Sum256(payload)
	if h.Payloads == nil || h.Inbox == nil {
		http.Error(w, "payload store unavailable", http.StatusServiceUnavailable)
		return
	}
	claimed, err := h.Inbox.ClaimInbox(r.Context(), messaging.ClaimInboxRequest{
		InboxKey: messaging.InboxKey{
			TenantID: binding.Tenant.TenantID, Channel: binding.Tenant.Channel,
			ExternalAccountID: externalAccountID, ExternalMessageID: in.ExternalMessageID,
		},
		AgentAppID: binding.Tenant.AgentAppID, SessionID: sessionID,
		ExternalChatID: in.ExternalChatID, ExternalUserID: in.ExternalUserID,
		PayloadDigest: hex.EncodeToString(digest[:]), KeyVersion: 1,
		InitialState: messaging.InboxDispatchPending,
	})
	if err != nil {
		if errors.Is(err, runtime.ErrIdempotencyCollision) {
			http.Error(w, "idempotency collision", http.StatusConflict)
			return
		}
		http.Error(w, "inbox claim failed", http.StatusInternalServerError)
		return
	}
	requestID, payloadRef := claimed.RequestID, claimed.PayloadRef
	if err := h.Payloads.PutPayload(r.Context(), messaging.PayloadRecord{TenantID: binding.Tenant.TenantID, RequestID: requestID, PayloadRef: payloadRef, ContentDigest: hex.EncodeToString(digest[:]), Content: payload, KeyVersion: 1}); err != nil {
		if errors.Is(err, runtime.ErrIdempotencyCollision) {
			http.Error(w, "idempotency collision", http.StatusConflict)
			return
		}
		http.Error(w, "payload persistence failed", http.StatusInternalServerError)
		return
	}
	handle, err := h.Dispatcher.Dispatch(r.Context(), gateway.DispatchRequest{Tenant: binding.Tenant, RequestID: requestID, SessionID: sessionID, UserID: userID, PayloadRef: payloadRef})
	if err != nil {
		http.Error(w, "dispatch failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(handle)
}

// Payload returns a defensive copy of the locally durable normalized payload.
func (h *Handler) Payload(requestID string) ([]byte, bool) {
	for _, binding := range h.bindings {
		record, err := h.Payloads.GetPayload(context.Background(), binding.Tenant.TenantID, requestID)
		if err == nil {
			return record.Content, true
		}
	}
	return nil, false
}

// stableID remains for identity compatibility tests; request identity used by
// the handler itself comes only from InboxClaimer.
func stableID(prefix string, parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(len(part)))
		_, _ = h.Write(b[:])
		_, _ = h.Write([]byte(part))
	}
	return prefix + "_" + hex.EncodeToString(h.Sum(nil)[:16])
}

func hmacID(prefix string, key []byte, parts ...string) string {
	m := hmac.New(sha256.New, key)
	for _, part := range parts {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(len(part)))
		_, _ = m.Write(b[:])
		_, _ = m.Write([]byte(part))
	}
	return fmt.Sprintf("%s_%s", prefix, hex.EncodeToString(m.Sum(nil)[:16]))
}
