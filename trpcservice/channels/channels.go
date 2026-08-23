// Package channels adapts IM platforms into normalized tRPC-Agent-Go inputs.
package channels

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"time"
)

type ConversationType string

const (
	ConversationP2P   ConversationType = "p2p"
	ConversationGroup ConversationType = "group"
)

type InboundEnvelope struct {
	TenantID               string           `json:"tenant_id"`
	BindingID              string           `json:"binding_id"`
	Channel                string           `json:"channel"`
	ExternalMessageID      string           `json:"external_message_id"`
	ExternalUserID         string           `json:"external_user_id"`
	ExternalConversationID string           `json:"external_conversation_id"`
	ConversationType       ConversationType `json:"conversation_type"`
	Content                string           `json:"content"`
	ReceivedAt             time.Time        `json:"received_at"`
	TraceID                string           `json:"trace_id"`
	ReplyToken             string           `json:"reply_token,omitempty"`
	MentionedBot           bool             `json:"mentioned_bot,omitempty"`
}

type OutboundEnvelope struct {
	ID                     string           `json:"id"`
	TenantID               string           `json:"tenant_id"`
	BindingID              string           `json:"binding_id"`
	Channel                string           `json:"channel"`
	ExternalConversationID string           `json:"external_conversation_id"`
	ConversationType       ConversationType `json:"conversation_type"`
	ReplyToMessageID       string           `json:"reply_to_message_id,omitempty"`
	ReplyToken             string           `json:"reply_token,omitempty"`
	Content                string           `json:"content"`
	Final                  bool             `json:"final"`
	TraceID                string           `json:"trace_id"`
	Attempts               int              `json:"attempts"`
}

type ChannelHealth struct {
	Ready       bool      `json:"ready"`
	State       string    `json:"state"`
	LastError   string    `json:"last_error,omitempty"`
	LastChanged time.Time `json:"last_changed"`
}

type InboundHandler func(context.Context, InboundEnvelope) error

type ChannelAdapter interface {
	ID() string
	Run(context.Context) error
	Send(context.Context, OutboundEnvelope) error
	Health() ChannelHealth
}

func (m InboundEnvelope) Validate() error {
	if m.TenantID == "" || m.BindingID == "" || m.Channel == "" {
		return errors.New("tenant_id, binding_id and channel are required")
	}
	if m.ExternalMessageID == "" || m.ExternalUserID == "" {
		return errors.New("external message and user ids are required")
	}
	if strings.TrimSpace(m.Content) == "" {
		return errors.New("message content is empty")
	}
	if m.ConversationType != ConversationP2P && m.ConversationType != ConversationGroup {
		return errors.New("unsupported conversation type")
	}
	return nil
}

func (m InboundEnvelope) IdempotencyKey() string {
	return strings.Join([]string{m.TenantID, m.BindingID, m.Channel, m.ExternalMessageID}, "|")
}

func (m InboundEnvelope) SessionID() string {
	identity := m.ExternalUserID
	if m.ConversationType == ConversationGroup {
		identity = m.ExternalConversationID
	}
	sum := sha256.Sum256([]byte(strings.Join([]string{
		m.TenantID, m.BindingID, m.Channel, string(m.ConversationType), identity,
	}, "|")))
	return hex.EncodeToString(sum[:])
}

func ShouldHandleGroup(conversationType ConversationType, mentionedBot, mentionAll bool) bool {
	if conversationType != ConversationGroup {
		return true
	}
	return mentionedBot && !mentionAll
}
