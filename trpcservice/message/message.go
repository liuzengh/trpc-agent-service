// Package message defines the channel-neutral request and reply contracts used
// between adapters and the Agent execution runtime.
package message

import "time"

// ConversationType identifies the platform conversation scope. The values are
// deliberately small and platform-neutral so that session ownership remains
// independent from an adapter's native payload shape.
type ConversationType string

const (
	ConversationDirect ConversationType = "direct"
	ConversationGroup  ConversationType = "group"
)

func (t ConversationType) Valid() bool {
	return t == ConversationDirect || t == ConversationGroup
}

type InboundMessage struct {
	Channel           string
	BindingID         string
	ExternalAccountID string
	PlatformMessageID string
	MessageID         string // Deprecated internal alias for PlatformMessageID.
	ActorUserID       string
	ExternalUserID    string // Deprecated internal alias for ActorUserID.
	ConversationID    string
	ConversationType  ConversationType
	ReplyToMessageID  string
	Text              string
	RequestID         string
	TraceID           string
	TraceParent       string
	DigestVersion     int
	ReceivedAt        time.Time
	PlatformRequestID string // Adapter correlation data, for example WeCom req_id.
}

// DeliveryTarget is created from an immutable ExecutionTask. Executors may
// provide text only; they never choose the destination for an external reply.
type DeliveryTarget struct {
	Channel           string           `json:"channel"`
	ChannelBindingID  string           `json:"channel_binding_id"`
	ExternalAccountID string           `json:"external_account_id"`
	ConversationID    string           `json:"conversation_id"`
	ConversationType  ConversationType `json:"conversation_type"`
	ReplyToMessageID  string           `json:"reply_to_message_id,omitempty"`
	PlatformRequestID string           `json:"platform_request_id,omitempty"`
}

func (t DeliveryTarget) Valid() bool {
	return t.Channel != "" && t.ChannelBindingID != "" && t.ExternalAccountID != "" &&
		t.ConversationID != "" && t.ConversationType.Valid()
}

func (t DeliveryTarget) Apply(reply OutboundMessage) OutboundMessage {
	reply.Channel = t.Channel
	reply.BindingID = t.ChannelBindingID
	reply.ExternalAccountID = t.ExternalAccountID
	reply.ConversationID = t.ConversationID
	reply.ConversationType = t.ConversationType
	reply.ReplyToMessageID = t.ReplyToMessageID
	reply.PlatformRequestID = t.PlatformRequestID
	return reply
}

type OutboundMessage struct {
	Channel           string           `json:"-"`
	BindingID         string           `json:"-"`
	ExternalAccountID string           `json:"-"`
	ConversationID    string           `json:"-"`
	ConversationType  ConversationType `json:"-"`
	ReplyToMessageID  string           `json:"-"`
	PlatformRequestID string           `json:"-"`
	RequestID         string           `json:"request_id"`
	TraceID           string           `json:"trace_id"`
	TraceParent       string           `json:"trace_parent,omitempty"`
	DigestVersion     int              `json:"digest_version,omitempty"`
	SessionID         string           `json:"session_id"`
	Text              string           `json:"text"`
}
