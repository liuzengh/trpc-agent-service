package channels

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// MessageType identifies the platform-neutral shape of an inbound channel
// message.
type MessageType string

const (
	// MessageTypeText identifies a text-only message.
	MessageTypeText MessageType = "text"
	// MessageTypeImage identifies an image message.
	MessageTypeImage MessageType = "image"
	// MessageTypeFile identifies a file message.
	MessageTypeFile MessageType = "file"
	// MessageTypeMixed identifies a message containing text and media.
	MessageTypeMixed MessageType = "mixed"
	// MessageTypeCard identifies a platform card or interactive message.
	MessageTypeCard MessageType = "card"
	// MessageTypeEvent identifies a provider event normalized to a channel input.
	MessageTypeEvent MessageType = "event"
	// MessageTypeUnsupported identifies a verified message not supported by the
	// current channel capability.
	MessageTypeUnsupported MessageType = "unsupported"
)

// Validate checks that the message type is part of the platform contract.
func (t MessageType) Validate() error {
	switch t {
	case MessageTypeText, MessageTypeImage, MessageTypeFile, MessageTypeMixed,
		MessageTypeCard, MessageTypeEvent, MessageTypeUnsupported:
		return nil
	default:
		return errors.New("message type is invalid")
	}
}

// ChannelInput is the provider-neutral input handed to Gateway admission.
// Its tenant and application scope must have already been copied from a
// verified Binding. The private mapping field is consumed only by the
// controlled PostgreSQL mapping boundary and is never serialized into an
// execution command.
type ChannelInput struct {
	TenantID          string
	AppID             string
	Channel           Channel
	BindingID         string
	BindingRevision   int64
	ExternalMessageID string
	Sender            ChannelSender
	Conversation      ChannelConversation
	MessageType       MessageType
	Text              string
	ArtifactRefs      []string
	ProviderTimestamp time.Time
	ReceivedAt        time.Time
	RejectReason      string

	mapping            *ChannelMappingInput
	messageReplyTarget *MessageReplyTarget
}

// ChannelSender contains platform-level sender fields. ExternalKeyHash is a
// binding-scoped lookup value; it is optional before the admission mapper
// computes the active HMAC candidate.
type ChannelSender struct {
	UserID          string
	ExternalKeyHash string
}

// ChannelConversation contains platform-level conversation fields. Direct
// messages leave the resolved and external conversation fields empty.
type ChannelConversation struct {
	Kind                ConversationKind
	ConversationID      string
	SessionPrincipalID  string
	ExternalChatKeyHash string
	ThreadKeyHash       string
}

// ChannelMappingInput contains normalized-boundary identifiers used only to
// resolve or create the platform Identity and Conversation. Provider DTOs
// must be converted to this provider-neutral shape before construction.
type ChannelMappingInput struct {
	ExternalSenderID           string
	ExternalChatID             string
	ExternalThreadID           string
	ProviderSenderTarget       string
	ProviderConversationTarget string
	ProviderThreadTarget       string
}

// MessageReplyTarget is a short-lived provider target for replying to the
// inbound message. It is retained only as protected admission material and is
// never serialized into a Gateway message or execution command.
type MessageReplyTarget struct {
	ProviderTarget string
	ExpiresAt      time.Time
}

// Validate checks the target value and its provider validity deadline.
func (t MessageReplyTarget) Validate() error {
	if strings.TrimSpace(t.ProviderTarget) == "" || !utf8String(t.ProviderTarget) {
		return errors.New("message reply target is required")
	}
	if t.ExpiresAt.IsZero() {
		return errors.New("message reply target expiry is required")
	}
	return nil
}

// Validate checks the identifiers and target shape required by a conversation
// kind. Provider targets are opaque UTF-8 values and are not rewritten here.
func (m ChannelMappingInput) Validate(kind ConversationKind) error {
	if err := requireMappingExternalID(m.ExternalSenderID, "external sender id"); err != nil {
		return err
	}
	if err := requireMappingTarget(m.ProviderSenderTarget, "provider sender target"); err != nil {
		return err
	}
	switch kind {
	case ConversationDirect:
		if m.ExternalChatID != "" || m.ExternalThreadID != "" ||
			m.ProviderConversationTarget != "" || m.ProviderThreadTarget != "" {
			return errors.New("direct mapping contains conversation fields")
		}
	case ConversationGroup:
		if err := requireMappingExternalID(m.ExternalChatID, "external chat id"); err != nil {
			return err
		}
		if m.ExternalThreadID != "" || m.ProviderThreadTarget != "" {
			return errors.New("group mapping contains topic fields")
		}
		if err := requireMappingTarget(m.ProviderConversationTarget, "provider conversation target"); err != nil {
			return err
		}
	case ConversationTopic:
		if err := requireMappingExternalID(m.ExternalChatID, "external chat id"); err != nil {
			return err
		}
		if err := requireMappingExternalID(m.ExternalThreadID, "external thread id"); err != nil {
			return err
		}
		if err := requireMappingTarget(m.ProviderThreadTarget, "provider thread target"); err != nil {
			return err
		}
		if m.ProviderConversationTarget != "" {
			if err := requireMappingTarget(m.ProviderConversationTarget, "provider conversation target"); err != nil {
				return err
			}
		}
	default:
		return errors.New("conversation kind is invalid")
	}
	return nil
}

// NewChannelInput creates an unresolved input with its controlled mapping
// material. The mapping material is retained in memory only until Store.Admit
// resolves the internal principals in the same transaction.
func NewChannelInput(input ChannelInput, mapping ChannelMappingInput) (ChannelInput, error) {
	if err := input.Validate(); err != nil {
		return ChannelInput{}, err
	}
	if err := mapping.Validate(input.Conversation.Kind); err != nil {
		return ChannelInput{}, err
	}
	input.mapping = &mapping
	return input, nil
}

// WithMessageReplyTarget attaches an opaque, message-scoped reply target to
// an input. The target is sealed by admission with the request ID as its AAD
// identity; callers cannot place it in the execution command.
func WithMessageReplyTarget(input ChannelInput, target MessageReplyTarget) (ChannelInput, error) {
	if err := input.Validate(); err != nil {
		return ChannelInput{}, err
	}
	if err := target.Validate(); err != nil {
		return ChannelInput{}, err
	}
	input = input.Clone()
	input.messageReplyTarget = &target
	return input, nil
}

// MappingInput returns the controlled mapping material attached to an
// unresolved input. Callers outside the mapping boundary should not persist or
// log the returned provider targets.
func (i ChannelInput) MappingInput() (ChannelMappingInput, bool) {
	if i.mapping == nil {
		return ChannelMappingInput{}, false
	}
	return *i.mapping, true
}

// SealMessageReplyTarget seals the optional message-scoped reply target for
// the Inbox transaction. The returned envelope is safe to persist; the raw
// provider target is never returned.
func (i ChannelInput) SealMessageReplyTarget(
	ctx context.Context,
	protector TargetProtector,
	scope tenant.Scope,
	requestID string,
) (*TargetEnvelope, *time.Time, error) {
	if i.messageReplyTarget == nil {
		return nil, nil, nil
	}
	if protector == nil {
		return nil, nil, errors.New("target protector is required")
	}
	if err := scope.Validate(); err != nil {
		return nil, nil, fmt.Errorf("message reply target scope: %w", err)
	}
	if requestID == "" {
		return nil, nil, errors.New("message reply target request_id is required")
	}
	if err := i.Validate(); err != nil {
		return nil, nil, err
	}
	if err := i.messageReplyTarget.Validate(); err != nil {
		return nil, nil, err
	}
	if !time.Now().UTC().Before(i.messageReplyTarget.ExpiresAt) {
		return nil, nil, errors.New("message reply target is expired")
	}
	envelope, err := protector.Seal(
		ctx,
		TargetContext{
			Scope:            scope,
			BindingID:        i.BindingID,
			Channel:          i.Channel,
			EntityType:       TargetEntityInbox,
			InternalEntityID: requestID,
		},
		TargetPurposeReplyMessage,
		TargetPlaintext{
			Version:        TargetVersion,
			Channel:        i.Channel,
			TargetKind:     TargetKindMessage,
			ProviderTarget: i.messageReplyTarget.ProviderTarget,
		},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("seal message reply target: %w", err)
	}
	if err := envelope.Validate(); err != nil {
		return nil, nil, fmt.Errorf("message reply target envelope: %w", err)
	}
	expiresAt := i.messageReplyTarget.ExpiresAt.UTC()
	return &envelope, &expiresAt, nil
}

// Clone returns a copy safe for passing across the Gateway boundary.
func (i ChannelInput) Clone() ChannelInput {
	clone := i
	clone.ArtifactRefs = append([]string(nil), i.ArtifactRefs...)
	if i.mapping != nil {
		mapping := *i.mapping
		clone.mapping = &mapping
	}
	if i.messageReplyTarget != nil {
		target := *i.messageReplyTarget
		clone.messageReplyTarget = &target
	}
	return clone
}

// Validate checks the stable fields required before admission. Internal IDs
// are intentionally optional because IM-05 resolves them transactionally.
func (i ChannelInput) Validate() error {
	if i.TenantID == "" {
		return errors.New("channel input tenant_id is required")
	}
	if i.AppID == "" {
		return errors.New("channel input app_id is required")
	}
	if err := i.Channel.Validate(); err != nil {
		return fmt.Errorf("channel input channel: %w", err)
	}
	if i.BindingID == "" {
		return errors.New("channel input binding_id is required")
	}
	if i.BindingRevision <= 0 {
		return errors.New("channel input binding_revision must be positive")
	}
	normalizedMessageID, err := NormalizeExternalID(i.ExternalMessageID)
	if err != nil {
		return fmt.Errorf("channel input external message id: %w", err)
	}
	if normalizedMessageID != i.ExternalMessageID {
		return errors.New("channel input external message id is not normalized")
	}
	if err := i.Conversation.Kind.Validate(); err != nil {
		return fmt.Errorf("channel input conversation: %w", err)
	}
	if err := i.MessageType.Validate(); err != nil {
		return fmt.Errorf("channel input message type: %w", err)
	}
	if i.MessageType == MessageTypeText && i.Text == "" {
		return errors.New("channel input text is required")
	}
	if !utf8String(i.Text) {
		return errors.New("channel input text is not valid utf-8")
	}
	for index, ref := range i.ArtifactRefs {
		if ref == "" {
			return fmt.Errorf("channel input artifact ref %d is required", index)
		}
		if !utf8String(ref) {
			return fmt.Errorf("channel input artifact ref %d is not valid utf-8", index)
		}
	}
	if i.RejectReason != "" && !utf8String(i.RejectReason) {
		return errors.New("channel input reject reason is not valid utf-8")
	}
	if i.Sender.UserID != "" && !utf8String(i.Sender.UserID) {
		return errors.New("channel input sender user_id is not valid utf-8")
	}
	if err := validateDigestString(i.Sender.ExternalKeyHash, "sender external key hash"); err != nil {
		return err
	}
	if err := validateDigestString(i.Conversation.ExternalChatKeyHash, "conversation external chat key hash"); err != nil {
		return err
	}
	if err := validateDigestString(i.Conversation.ThreadKeyHash, "conversation thread key hash"); err != nil {
		return err
	}
	return nil
}

// AttachmentIngestor materializes provider media references into tenant-scoped
// ArtifactRefs before ChannelInput reaches PostgreSQL admission.
type AttachmentIngestor interface {
	Prepare(ctx context.Context, input ChannelInput, media []ProviderMediaRef) (ChannelInput, error)
}

// PinnedAttachmentIngestor materializes media against the exact config
// version selected by the trusted admission boundary. The returned cleanup
// is used when admission does not commit.
type PinnedAttachmentIngestor interface {
	PreparePinned(
		ctx context.Context,
		input ChannelInput,
		media []ProviderMediaRef,
		configVersion string,
	) (ChannelInput, func(context.Context) error, error)
}

// ProviderMediaRef is an adapter-owned opaque media handle. Its value must not
// be copied into Gateway, Worker, Session, or persistence.
type ProviderMediaRef struct {
	Kind          MessageType
	Reference     string
	DecryptionKey string
}

// Validate checks the small provider-neutral media handle contract.
func (r ProviderMediaRef) Validate() error {
	if err := r.Kind.Validate(); err != nil {
		return err
	}
	if r.Kind == MessageTypeText || r.Kind == MessageTypeUnsupported {
		return errors.New("provider media ref kind is invalid")
	}
	if strings.TrimSpace(r.Reference) == "" || !utf8String(r.Reference) {
		return errors.New("provider media ref reference is required")
	}
	if r.DecryptionKey != "" && !utf8String(r.DecryptionKey) {
		return errors.New("provider media ref decryption key is invalid")
	}
	return nil
}

func requireMappingExternalID(value, field string) error {
	normalized, err := NormalizeExternalID(value)
	if err != nil {
		return fmt.Errorf("%s: %w", field, err)
	}
	if normalized != value {
		return fmt.Errorf("%s is not normalized", field)
	}
	return nil
}

func requireMappingTarget(value, field string) error {
	if value == "" {
		return fmt.Errorf("%s is required", field)
	}
	if !utf8String(value) {
		return fmt.Errorf("%s is not valid utf-8", field)
	}
	return nil
}

func validateDigestString(value, field string) error {
	if value == "" {
		return nil
	}
	if len(value) != 64 {
		return fmt.Errorf("%s must be a sha256 digest", field)
	}
	if _, err := hex.DecodeString(value); err != nil {
		return fmt.Errorf("%s must be a sha256 digest", field)
	}
	return nil
}

func utf8String(value string) bool {
	return utf8.ValidString(value)
}
