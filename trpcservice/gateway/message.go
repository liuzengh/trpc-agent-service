// Package gateway defines transport-neutral message contracts.
package gateway

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// InboundMessage is a verified and tenant-scoped external user message.
type InboundMessage struct {
	TenantID, AppID, BindingID string
	ExternalMessageID          string
	ExternalUserID             string
	ConversationID             string
	UserID, SessionID          string
	Text                       string
	Media                      *MediaReference
	Attachments                []Attachment
	TraceID                    string
	ConfigVersion              tenant.ConfigVersion
	ReceivedAt                 time.Time
	TraceContext               map[string]string
}

// RunRequest is the durable unit dispatched to an Agent Worker.
type RunRequest struct {
	InboxID                    string
	TenantID, AppID, BindingID string
	ExternalMessageID          string
	ExternalUserID             string
	ConversationID             string
	UserID, SessionID, Text    string
	Attachments                []Attachment
	TraceID                    string
	ConfigVersion              tenant.ConfigVersion
	ClaimOwner, ClaimToken     string
	ClaimAttempt               int
	InboxSeq                   uint64
	ClaimLeaseUntil            time.Time
	TraceContext               map[string]string
}

// OutboundMessage is a durable reply awaiting channel delivery.
type OutboundMessage struct {
	TenantID, AppID, BindingID   string
	ConfigVersion                tenant.ConfigVersion
	OutboxID, DedupeKey          string
	UserID, SessionID            string
	ExternalUserID               string
	ConversationID               string
	Text, TraceID                string
	ReplyFormat                  string
	TraceContext                 map[string]string
	SourceInboxID, SourceEventID string
	CreatedAt                    time.Time
}

// MediaReference is an opaque provider reference that exists only while an
// authenticated callback downloads media. It must never be logged, audited,
// or copied into a durable RunRequest.
type MediaReference struct {
	Kind, Key, MessageID, Name string
}

// Attachment is validated media safe to hand to the pinned Runtime. Images
// carry bytes for a multimodal model; documents carry bounded extracted text.
type Attachment struct {
	Kind, Name, MIME, ExtractedText string
	Data                            []byte
}

// AcceptedMessage is the transport-neutral durable ingress acknowledgement.
type AcceptedMessage struct {
	RequestID string
	SessionID string
	TraceID   string
	Duplicate bool
}

// InboundAcceptor persists an already authenticated and tenant-scoped message.
// Channel adapters must verify their provider callback before calling it.
type InboundAcceptor interface {
	AcceptInbound(context.Context, InboundMessage) (AcceptedMessage, error)
}

// SessionKey is the complete tenant-scoped session identity.
type SessionKey struct{ TenantID, AppID, UserID, SessionID string }

// Key returns the session identity carried by request.
func (request RunRequest) Key() SessionKey {
	return SessionKey{request.TenantID, request.AppID, request.UserID, request.SessionID}
}

// RunEvent is a transport-neutral projection of an Agent event.
type RunEvent struct {
	Type, RequestID, TraceID string
	TenantID, BindingID      string
	WorkerID                 string
	SessionID                string
	Delta, Message, ToolName string
	Stage, ToolStatus        string
	ToolCallID               string
	Error                    string
	Terminal                 bool
	PromptTokens             int
	CompletionTokens         int
	TotalTokens              int
}

// EventPublisher observes execution without owning persistence or routing.
type EventPublisher interface {
	Publish(RunEvent)
}

// EventPublisherFunc adapts a function to EventPublisher.
type EventPublisherFunc func(RunEvent)

// Publish implements EventPublisher.
func (publish EventPublisherFunc) Publish(event RunEvent) { publish(event) }
