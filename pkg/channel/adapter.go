//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package channel

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"reflect"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

// ErrIgnoredInbound marks a valid provider update that the installed Adapter
// deliberately does not translate into an Agent invocation. Gateway must
// acknowledge it so providers do not retry unsupported events forever.
var ErrIgnoredInbound = errors.New("inbound provider update ignored")

// ErrInvalidInboundMessage identifies an adapter result that cannot safely
// enter the durable Inbox. The gateway treats this as a bad provider update
// rather than allowing a buggy/custom adapter to panic the HTTP process.
var ErrInvalidInboundMessage = errors.New("inbound provider message is invalid")

// ErrStreamingUnsupported identifies a channel capability mismatch. It is a
// permanent delivery failure: retrying a stream operation on an adapter that
// explicitly does not implement streaming cannot make progress.
var ErrStreamingUnsupported = errors.New("channel streaming is not supported")

// ChannelType represents the type of IM channel.
type ChannelType string

const (
	// ChannelTypeWeWork represents WeChat Work.
	ChannelTypeWeWork ChannelType = "wework"
	// ChannelTypeWeComBot is the authenticated local bridge for a WeCom smart bot.
	ChannelTypeWeComBot ChannelType = "wecom_bot"
	// ChannelTypeTelegram represents Telegram.
	ChannelTypeTelegram ChannelType = "telegram"
	// ChannelTypeWeChatService represents WeChat Customer Service.
	ChannelTypeWeChatService ChannelType = "wechat_service"
	// ChannelTypeWeChatMP represents WeChat Official Account.
	ChannelTypeWeChatMP ChannelType = "wechat_mp"
)

// Adapter defines the interface for IM channel adapters.
type Adapter interface {
	// VerifySignature validates the incoming webhook signature.
	VerifySignature(req *http.Request, binding *tenant.ChannelBinding) error

	// ParseInbound converts IM webhook payload to internal message.
	ParseInbound(req *http.Request, binding *tenant.ChannelBinding) (*InboundMessage, error)

	// SendReply sends agent response back to IM platform.
	SendReply(ctx context.Context, binding *tenant.ChannelBinding, msg *OutboundMessage) error

	// SendStreamChunk sends partial streaming response.
	SendStreamChunk(ctx context.Context, binding *tenant.ChannelBinding, chunk *StreamChunk) error

	// SupportsStreaming indicates if this channel supports streaming.
	SupportsStreaming() bool

	// HandleRateLimit implements channel-specific backoff.
	HandleRateLimit(err error) time.Duration
}

// URLVerifier is implemented by channels such as WeCom that challenge a
// webhook URL before enabling callbacks.
type URLVerifier interface {
	VerifyURL(req *http.Request, binding *tenant.ChannelBinding) (string, error)
}

// InboundMessage represents a message received from an IM platform.
type InboundMessage struct {
	TenantID         string `json:"tenantId"`
	ChannelType      string `json:"channelType"`
	ChannelAccountID string `json:"channelAccountId"`
	ExternalUserID   string `json:"externalUserId"` // IM platform's user ID
	ConversationID   string `json:"conversationId"` // Group chat ID or 1:1 conversation ID
	// SessionOwnerID is the framework-owned Session identity used by Runner.
	// It is deliberately separate from ExternalUserID: a group conversation
	// has one shared session owner while each message still retains its actor.
	// Adapters must not set this value; the trusted gateway derives it.
	SessionOwnerID string            `json:"sessionOwnerId,omitempty"`
	MessageID      string            `json:"messageId"`
	ReplyToID      string            `json:"replyToId,omitempty"` // Provider-native reply/thread target.
	Content        string            `json:"content"`
	Attachments    []Attachment      `json:"attachments,omitempty"`
	Timestamp      time.Time         `json:"timestamp"`
	IsGroupChat    bool              `json:"isGroupChat"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// OutboundMessage represents a message to send to an IM platform.
type OutboundMessage struct {
	ConversationID string       `json:"conversationId"`
	Content        string       `json:"content"`
	ContentType    string       `json:"contentType"` // text, markdown, card
	Attachments    []Attachment `json:"attachments,omitempty"`
	ReplyToID      string       `json:"replyToId,omitempty"` // For threading
	// DeliveryID is stable for one durable Outbox chunk across retries. Adapters
	// may forward it to a provider or an idempotency-aware egress proxy. It is
	// intentionally not serialized as user-visible message data.
	DeliveryID string            `json:"-"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// StreamChunk represents a partial streaming response.
type StreamChunk struct {
	ConversationID string `json:"conversationId"`
	MessageID      string `json:"messageId"` // Message ID to update
	Content        string `json:"content"`   // Current accumulated content
	IsComplete     bool   `json:"isComplete"`
	// DeliveryID is stable for one durable streaming update across retries. It
	// is not rendered by the provider and is forwarded only as an optional
	// idempotency key to an egress layer that understands it.
	DeliveryID string `json:"-"`
}

// Attachment represents a file attachment.
type Attachment struct {
	Type     string `json:"type"` // image, file, audio, video
	URL      string `json:"url"`
	Name     string `json:"name,omitempty"`
	Size     int64  `json:"size,omitempty"`
	MimeType string `json:"mimeType,omitempty"`
}

// SessionIDGenerator generates session IDs from inbound messages.
type SessionIDGenerator interface {
	Generate(msg *InboundMessage) string
}

// DefaultSessionIDGenerator implements the default session ID generation logic.
type DefaultSessionIDGenerator struct{}

// Generate creates a session ID based on conversation context.
func (g *DefaultSessionIDGenerator) Generate(msg *InboundMessage) string {
	if msg == nil {
		return ""
	}
	return generateSessionID(msg)
}

func generateSessionID(msg *InboundMessage) string {
	scope := "direct"
	subject := msg.ExternalUserID
	if msg.IsGroupChat {
		scope = "group"
		subject = msg.ConversationID
	}
	if !validSessionComponent(msg.TenantID) ||
		!validSessionComponent(msg.ChannelType) ||
		!validSessionComponent(msg.ChannelAccountID) ||
		!validSessionComponent(subject) {
		return ""
	}
	// Hash the complete tenant/channel/account/scope subject tuple. This keeps
	// identifiers bounded for SQL/index limits and avoids exposing raw IM user
	// or group IDs in URLs and logs while preserving deterministic routing.
	digest := sha256.Sum256([]byte(
		msg.TenantID + "\x00" + msg.ChannelType + "\x00" + msg.ChannelAccountID + "\x00" + scope + "\x00" + subject,
	))
	return "sess_" + hex.EncodeToString(digest[:])
}

// validSessionComponent keeps the legacy hash format stable for ordinary
// identifiers while rejecting values that would make the NUL tuple separator
// ambiguous or leak control characters into logs and persistence.
func validSessionComponent(value string) bool {
	if value == "" || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf) {
			return false
		}
	}
	return true
}

// AdapterRegistry manages channel adapters.
type AdapterRegistry struct {
	mu       sync.RWMutex
	adapters map[ChannelType]Adapter
}

// NewAdapterRegistry creates a new adapter registry.
func NewAdapterRegistry() *AdapterRegistry {
	return &AdapterRegistry{
		adapters: make(map[ChannelType]Adapter),
	}
}

// Register registers an adapter for a channel type.
func (r *AdapterRegistry) Register(channelType ChannelType, adapter Adapter) {
	if r == nil || isNilAdapter(adapter) {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.adapters[channelType] = adapter
}

func isNilAdapter(adapter Adapter) bool {
	if adapter == nil {
		return true
	}
	v := reflect.ValueOf(adapter)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

// Get retrieves an adapter for a channel type.
func (r *AdapterRegistry) Get(channelType ChannelType) (Adapter, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	adapter, ok := r.adapters[channelType]
	return adapter, ok
}
