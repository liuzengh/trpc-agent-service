// Package channels defines IM-agnostic message and delivery contracts.
package channels

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	occhannel "trpc.group/trpc-go/trpc-agent-go/openclaw/channel"
)

// Channel identifies a supported external IM protocol.
type Channel string

const (
	// Telegram identifies the Telegram Bot API channel.
	Telegram Channel = "telegram"
	// WeCom identifies the WeCom Smart Bot WebSocket channel.
	WeCom Channel = "wecom"
	// Feishu identifies the Lark/Feishu open-platform channel.
	Feishu Channel = "feishu"
	// Web identifies the authenticated browser-console channel.
	Web Channel = "web"
)

var (
	// ErrInvalidSessionKey indicates an invalid session-key segment.
	ErrInvalidSessionKey = errors.New("invalid session key")
	// ErrInvalidInboundMessage indicates a malformed normalized inbound message.
	ErrInvalidInboundMessage = errors.New("invalid inbound message")
)

// InboundHandler receives one normalized external message.
type InboundHandler func(context.Context, InboundMessage) error

// Receiver is the protocol-independent ingress seam for one long-lived
// external channel binding. The concrete implementation owns polling,
// WebSocket reconnects, provider authentication and protocol decoding.
type Receiver interface {
	Run(context.Context, InboundHandler) error
}

// OpenClawChannel is the framework-native lifecycle contract used by every
// long-lived IM adapter. Provider-specific receivers are bound to an ingress
// handler before they are handed to the connector manager.
type OpenClawChannel = occhannel.Channel

// BoundChannel adapts an existing provider receiver to OpenClaw's minimal
// Channel lifecycle without duplicating gateway/session semantics.
type BoundChannel struct {
	id  string
	run func(context.Context) error
}

func NewBoundChannel(id string, run func(context.Context) error) (*BoundChannel, error) {
	id = strings.TrimSpace(id)
	if id == "" || run == nil {
		return nil, fmt.Errorf("openclaw channel id and runner are required")
	}
	return &BoundChannel{id: id, run: run}, nil
}

func (c *BoundChannel) ID() string                    { return c.id }
func (c *BoundChannel) Run(ctx context.Context) error { return c.run(ctx) }

var _ occhannel.Channel = (*BoundChannel)(nil)

// Sender delivers a normalized response to one external channel.
type Sender interface {
	Send(context.Context, ReplyTarget, OutboundMessage) (SendReceipt, error)
}

// BindingKey identifies one concrete external channel account. Sender
// credentials are registered at this scope, never globally per protocol.
type BindingKey struct {
	Channel   Channel
	BindingID string
}

func (k BindingKey) Validate() error {
	if !k.Channel.Supported() {
		return fmt.Errorf("unsupported binding channel %q", k.Channel)
	}
	if strings.TrimSpace(k.BindingID) == "" && k.Channel != Web {
		return fmt.Errorf("binding ID is required for channel %q", k.Channel)
	}
	if strings.ContainsAny(k.BindingID, "/\\") {
		return fmt.Errorf("binding ID contains a reserved separator")
	}
	return nil
}

// ConversationScope declares whether a message belongs to a personal or shared
// group conversation. The zero value represents a direct conversation.
type ConversationScope string

const (
	ConversationDirect ConversationScope = "direct"
	ConversationGroup  ConversationScope = "group"
)

// TriggerType records why one inbound message was eligible for execution.
// It is routing metadata, not message content.
type TriggerType string

const (
	TriggerDirect  TriggerType = "direct"
	TriggerMention TriggerType = "mention"
	TriggerCommand TriggerType = "command"
	TriggerAction  TriggerType = "action"
)

// InboundMessage is the protocol-independent result of decoding a webhook.
type InboundMessage struct {
	MessageID string
	// ProviderRequestID is the external platform's correlation identifier. It
	// is routing metadata for traces only and must never be used as identity or
	// authorization input.
	ProviderRequestID string `json:"provider_request_id,omitempty"`
	Channel           Channel
	ConversationID    string
	SenderID          string
	ConversationScope ConversationScope
	// SubjectID is the canonical framework UserID selected by ingress. For a
	// resolved direct user it is platform_user_id; for an unlinked direct user
	// it is a stable binding-scoped fallback; for groups it is the group route.
	SubjectID string
	// OwnerPlatformUserID is an authorization fact for direct sessions only.
	OwnerPlatformUserID string
	// ActorPlatformUserID identifies the person who sent this message when the
	// channel identity can be resolved. Groups keep this separate from SubjectID.
	ActorPlatformUserID string
	TriggerType         TriggerType
	// WebOwnerID is the authenticated console user that submitted a Web
	// request. It is intentionally distinct from SenderID: SenderID remains
	// the agent subject, while WebOwnerID gates private reply-stream access.
	WebOwnerID string
	Text       string
	Files      []InboundFile
	// ReceivedFiles exists only between a channel receiver and the Gateway
	// ingress coordinator. The Gateway stages these bytes into Artifact before
	// constructing the Kafka envelope, so raw provider media never enters Kafka.
	ReceivedFiles []ReceivedFile `json:"-"`
	ReceivedAt    time.Time
	// Action is populated for provider-native button/card interactions. These
	// events are consumed by platform control handlers before they can reach an
	// LLM.
	Action *InboundAction `json:"action,omitempty"`
	// ProgressMessageID is a provider message created at ingress and later
	// updated by streamed deltas/final Outbox delivery.
	ProgressMessageID string `json:"progress_message_id,omitempty"`
	// ProviderReplyToken is an opaque short-lived provider handle required by
	// reply protocols such as WeCom streaming. Only the channel adapter may
	// interpret it; platform transport merely preserves it.
	ProviderReplyToken string `json:"provider_reply_token,omitempty"`
}

// InboundAction is the provider-neutral form of a button/card callback.
type InboundAction struct {
	ActionID        string            `json:"action_id"`
	OriginMessageID string            `json:"origin_message_id,omitempty"`
	CallbackID      string            `json:"callback_id,omitempty"`
	Token           string            `json:"token,omitempty"`
	Values          map[string]string `json:"values,omitempty"`
}

// ReceivedFile is one provider attachment downloaded by a channel receiver.
// It is deliberately transient; durable ingress uses InboundFile references.
type ReceivedFile struct {
	Name     string
	MimeType string
	Data     []byte
}

// InboundFile points at one file already staged in the tenant artifact store.
// Kafka carries only this small reference; the Runtime resolves the bytes just
// before constructing the framework user message.
type InboundFile struct {
	Name         string
	MimeType     string
	ArtifactName string
	Version      int
	SizeBytes    int64
}

// IsGroupConversation reports whether the message must remain in the
// source-channel group session and never enter a personal active session.
func (m InboundMessage) IsGroupConversation() bool {
	return m.ConversationScope == ConversationGroup
}

// Validate verifies the identifiers required before routing a message.
func (m InboundMessage) Validate() error {
	if strings.TrimSpace(m.MessageID) == "" {
		return fmt.Errorf("%w: message_id is required", ErrInvalidInboundMessage)
	}
	if requestID := strings.TrimSpace(m.ProviderRequestID); len(requestID) > 256 || strings.IndexFunc(requestID, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0 {
		return fmt.Errorf("%w: provider_request_id is invalid", ErrInvalidInboundMessage)
	}
	if !m.Channel.Supported() {
		return fmt.Errorf("%w: unsupported channel %q", ErrInvalidInboundMessage, m.Channel)
	}
	if m.ConversationScope != "" && m.ConversationScope != ConversationDirect && m.ConversationScope != ConversationGroup {
		return fmt.Errorf("%w: unsupported conversation scope %q", ErrInvalidInboundMessage, m.ConversationScope)
	}
	if m.TriggerType != "" && m.TriggerType != TriggerDirect && m.TriggerType != TriggerMention && m.TriggerType != TriggerCommand && m.TriggerType != TriggerAction {
		return fmt.Errorf("%w: unsupported trigger type %q", ErrInvalidInboundMessage, m.TriggerType)
	}
	if strings.TrimSpace(m.ConversationID) == "" {
		return fmt.Errorf("%w: conversation_id is required", ErrInvalidInboundMessage)
	}
	if strings.TrimSpace(m.SenderID) == "" {
		return fmt.Errorf("%w: sender_id is required", ErrInvalidInboundMessage)
	}
	for _, file := range m.Files {
		if strings.TrimSpace(file.Name) == "" || strings.TrimSpace(file.ArtifactName) == "" || file.Version < 0 || file.SizeBytes < 0 {
			return fmt.Errorf("%w: invalid file input", ErrInvalidInboundMessage)
		}
	}
	for _, file := range m.ReceivedFiles {
		if strings.TrimSpace(file.Name) == "" || len(file.Data) == 0 {
			return fmt.Errorf("%w: invalid received file", ErrInvalidInboundMessage)
		}
	}
	return nil
}

// Supported reports whether the channel is implemented by the platform contract.
func (c Channel) Supported() bool {
	return c == Telegram || c == WeCom || c == Feishu || c == Web
}

// ExternalSubjectID returns the stable anonymous Agent subject for one external
// identity. Binding scope is mandatory because the same provider user ID on a
// different bot must not be treated as the same person before explicit trust.
func ExternalSubjectID(channel Channel, bindingID, externalUserID string) (string, error) {
	if !channel.Supported() || channel == Web {
		return "", fmt.Errorf("unsupported external subject channel %q", channel)
	}
	bindingID = strings.TrimSpace(bindingID)
	externalUserID = strings.TrimSpace(externalUserID)
	if bindingID == "" || externalUserID == "" {
		return "", fmt.Errorf("external binding and user ID are required")
	}
	return "external:" + string(channel) + ":" + bindingID + ":" + externalUserID, nil
}

// GroupSubjectID keeps group state on the stable channel route instead of the
// latest actor. The actor remains message metadata only.
func GroupSubjectID(channel Channel, bindingID, conversationID string) (string, error) {
	if !channel.Supported() || channel == Web {
		return "", fmt.Errorf("unsupported group subject channel %q", channel)
	}
	bindingID, conversationID = strings.TrimSpace(bindingID), strings.TrimSpace(conversationID)
	if bindingID == "" || conversationID == "" {
		return "", fmt.Errorf("group binding and conversation ID are required")
	}
	return "group:" + string(channel) + ":" + bindingID + ":" + conversationID, nil
}

// ReplyTarget identifies an external conversation that can receive a response.
type ReplyTarget struct {
	TenantID           string
	Channel            Channel
	BindingID          string
	ConversationID     string
	ConversationScope  ConversationScope
	ProviderReplyToken string
	// WebOwnerID binds Web replies to their authenticated submitter. It is
	// ignored by IM senders.
	WebOwnerID string
}

// OutboundFile reuses OpenClaw's cross-channel file contract.
type OutboundFile = occhannel.OutboundFile

// OutboundArtifact is durable file metadata carried to browser clients. IM
// senders receive materialized OutboundFile values instead, so this struct
// never contains local paths or file bytes.
type OutboundArtifact struct {
	Filename string `json:"filename"`
	Version  int    `json:"version"`
	Name     string `json:"name,omitempty"`
	MimeType string `json:"mime_type,omitempty"`
}

// OutboundMessage embeds OpenClaw's text/file payload and adds only the
// enterprise-IM capabilities that OpenClaw's public channel contract does not
// currently model: interactive cards, durable idempotency and in-place update.
type OutboundMessage struct {
	Text      string             `json:"text,omitempty"`
	Files     []OutboundFile     `json:"files,omitempty"`
	Artifacts []OutboundArtifact `json:"artifacts,omitempty"`
	Card      *InteractiveCard   `json:"card,omitempty"`
	// UpdateMessageID asks a capable sender to replace an existing progress or
	// card message instead of creating a second reply.
	UpdateMessageID string `json:"update_message_id,omitempty"`
	IdempotencyKey  string `json:"idempotency_key,omitempty"`
}

// OpenClawMessage projects the common text/file subset onto the framework
// contract. Rich cards and provider update IDs remain platform extensions.
func (m OutboundMessage) OpenClawMessage() occhannel.OutboundMessage {
	return occhannel.OutboundMessage{Text: m.Text, Files: append([]occhannel.OutboundFile(nil), m.Files...)}
}

// InteractiveCard is the common subset supported by Telegram inline
// keyboards, WeCom template cards and Feishu interactive cards.
type InteractiveCard struct {
	Title   string       `json:"title,omitempty"`
	Body    string       `json:"body"`
	Actions []CardAction `json:"actions,omitempty"`
	State   string       `json:"state,omitempty"`
}

// CardAction describes one provider-native button. Exactly one of ActionID or
// URL should normally be set.
type CardAction struct {
	ActionID string `json:"action_id,omitempty"`
	Label    string `json:"label"`
	Style    string `json:"style,omitempty"`
	URL      string `json:"url,omitempty"`
}

// ProgressSender is an optional provider capability used to avoid a long blank
// period while the Agent is running. The returned receipt is carried through
// Kafka so the durable final reply can update the same provider message.
type ProgressSender interface {
	StartProgress(context.Context, ReplyTarget) (SendReceipt, error)
	UpdateProgress(context.Context, ReplyTarget, string, string) error
}

// ProgressCardSender upgrades an existing provider progress message with an
// interactive card without creating a second visible message.
type ProgressCardSender interface {
	UpdateProgressCard(context.Context, ReplyTarget, string, InteractiveCard) error
}

// MessageDeleter is implemented by providers that can retract a bot message.
type MessageDeleter interface {
	DeleteMessage(context.Context, ReplyTarget, string) error
}

// CardUpdater covers providers such as WeCom whose template-card update is
// tied to the short-lived callback token rather than a stable message ID.
type CardUpdater interface {
	UpdateCard(context.Context, ReplyTarget, string, InteractiveCard) error
}

// SendReceipt records the external identifier returned after a successful send.
type SendReceipt struct {
	ExternalMessageID string
}

// BuildSessionKey creates a channel-neutral tenant-scoped Agent Session key.
// External channel/conversation identifiers are persisted separately as
// SessionConversation records and must never be encoded into this identity.
func BuildSessionKey(tenantID, appCode, sessionID string) (string, error) {
	if err := validateSessionSegment("tenant_id", tenantID); err != nil {
		return "", err
	}
	if err := validateSessionSegment("app_code", appCode); err != nil {
		return "", err
	}
	if err := validateSessionSegment("session_id", sessionID); err != nil {
		return "", err
	}
	return strings.Join([]string{tenantID, appCode, "session", sessionID}, "/"), nil
}

func validateSessionSegment(name, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%w: %s is required", ErrInvalidSessionKey, name)
	}
	if strings.ContainsAny(value, "/\\") {
		return fmt.Errorf("%w: %s contains a reserved separator", ErrInvalidSessionKey, name)
	}
	return nil
}
