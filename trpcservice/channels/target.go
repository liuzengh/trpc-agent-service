package channels

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	// TargetVersion is the version of the canonical provider target payload.
	TargetVersion = 1
	// TargetAlgorithmAES256GCM identifies the v1 target envelope algorithm.
	TargetAlgorithmAES256GCM = "AES-256-GCM"
	// TargetNonceSize is the AES-GCM nonce size required by v1 envelopes.
	TargetNonceSize = 12
	// TargetAuthenticationTagSize is the AES-GCM authentication tag size.
	TargetAuthenticationTagSize = 16

	// TargetEntityIdentity identifies an encrypted target owned by an Identity.
	TargetEntityIdentity TargetEntityType = "channel_identity"
	// TargetEntityConversation identifies an encrypted target owned by a Conversation.
	TargetEntityConversation TargetEntityType = "channel_conversation"
	// TargetEntityInbox identifies an encrypted message-scoped target.
	TargetEntityInbox TargetEntityType = "channel_inbox"
)

// TargetPurpose identifies the owning operation for an encrypted provider target.
type TargetPurpose string

const (
	// TargetPurposeIdentityUser protects a stable external user target.
	TargetPurposeIdentityUser TargetPurpose = "im.identity.user_target"
	// TargetPurposeConversationChat protects a stable external chat target.
	TargetPurposeConversationChat TargetPurpose = "im.conversation.chat_target"
	// TargetPurposeConversationTopic protects a stable external topic target.
	TargetPurposeConversationTopic TargetPurpose = "im.conversation.topic_target"
	// TargetPurposeReplyMessage protects a message-scoped reply target.
	TargetPurposeReplyMessage TargetPurpose = "im.reply.message_target"
)

// Validate checks that the purpose is one of the supported target operations.
func (p TargetPurpose) Validate() error {
	if _, ok := targetKindForPurpose(p); !ok {
		return errors.New("target purpose is invalid")
	}
	return nil
}

// TargetKind identifies the external entity represented by a target payload.
type TargetKind string

const (
	// TargetKindUser identifies a direct-message user target.
	TargetKindUser TargetKind = "user"
	// TargetKindConversation identifies a group conversation target.
	TargetKindConversation TargetKind = "conversation"
	// TargetKindTopic identifies a topic or thread target.
	TargetKindTopic TargetKind = "topic"
	// TargetKindMessage identifies a message-scoped reply target.
	TargetKindMessage TargetKind = "message"
)

// TargetEntityType identifies the internal record that owns an encrypted
// provider target.
type TargetEntityType string

// ExternalIDKind identifies the namespace used when deriving an external ID
// lookup hash.
type ExternalIDKind string

const (
	// ExternalIDUser identifies a provider user ID.
	ExternalIDUser ExternalIDKind = "user"
	// ExternalIDChat identifies a provider chat ID.
	ExternalIDChat ExternalIDKind = "chat"
	// ExternalIDThread identifies a provider thread ID.
	ExternalIDThread ExternalIDKind = "thread"
	// ExternalIDNoThread identifies the typed sentinel for a chat without a thread.
	ExternalIDNoThread ExternalIDKind = "thread_none"
)

// Validate checks that the external ID namespace is supported.
func (k ExternalIDKind) Validate() error {
	switch k {
	case ExternalIDUser, ExternalIDChat, ExternalIDThread, ExternalIDNoThread:
		return nil
	default:
		return errors.New("external id kind is invalid")
	}
}

// ConversationKind identifies the session shape of an inbound message.
type ConversationKind string

const (
	// ConversationDirect identifies a one-to-one conversation.
	ConversationDirect ConversationKind = "direct"
	// ConversationGroup identifies a group conversation.
	ConversationGroup ConversationKind = "group"
	// ConversationTopic identifies a topic or thread within a conversation.
	ConversationTopic ConversationKind = "topic"
)

// Validate checks that the conversation kind is supported.
func (k ConversationKind) Validate() error {
	switch k {
	case ConversationDirect, ConversationGroup, ConversationTopic:
		return nil
	default:
		return errors.New("conversation kind is invalid")
	}
}

// IdentityStatus values are stored as strings.
const (
	IdentityActive    = "ACTIVE"
	IdentitySuspended = "SUSPENDED"
)

// TargetContext binds an encrypted target to its tenant, binding, channel, and
// internal owning record. It is part of the authenticated data, not the target
// plaintext.
type TargetContext struct {
	Scope            tenant.Scope
	BindingID        string
	Channel          Channel
	EntityType       TargetEntityType
	InternalEntityID string
}

// Validate checks the scope and entity identity required for target AAD.
func (c TargetContext) Validate() error {
	if err := c.Scope.Validate(); err != nil {
		return fmt.Errorf("target scope: %w", err)
	}
	if c.BindingID == "" {
		return errors.New("target binding_id is required")
	}
	if err := c.Channel.Validate(); err != nil {
		return fmt.Errorf("target channel: %w", err)
	}
	switch c.EntityType {
	case TargetEntityIdentity, TargetEntityConversation, TargetEntityInbox:
	default:
		return errors.New("target entity type is invalid")
	}
	if c.InternalEntityID == "" {
		return errors.New("target internal entity id is required")
	}
	return nil
}

// ValidateFor checks that the target entity is valid for the requested purpose.
func (c TargetContext) ValidateFor(purpose TargetPurpose) error {
	if err := c.Validate(); err != nil {
		return err
	}
	wantEntity, ok := targetEntityForPurpose(purpose)
	if !ok {
		return errors.New("target purpose is invalid")
	}
	if c.EntityType != wantEntity {
		return errors.New("target entity type does not match purpose")
	}
	return nil
}

// TargetPlaintext is the fixed-shape provider target payload encrypted into a
// TargetEnvelope. Its fields must remain stable because they are part of the
// v1 serialization contract.
type TargetPlaintext struct {
	Version          int        `json:"version"`
	Channel          Channel    `json:"channel"`
	TargetKind       TargetKind `json:"target_kind"`
	ExternalUserID   string     `json:"external_user_id"`
	ExternalChatID   string     `json:"external_chat_id"`
	ExternalThreadID string     `json:"external_thread_id"`
	ProviderTarget   string     `json:"provider_target"`
}

// Validate checks the target shape and that it matches the requested purpose.
func (t TargetPlaintext) Validate(purpose TargetPurpose) error {
	wantKind, ok := targetKindForPurpose(purpose)
	if !ok {
		return errors.New("target purpose is invalid")
	}
	if t.Version != TargetVersion {
		return errors.New("target version is invalid")
	}
	if err := t.Channel.Validate(); err != nil {
		return fmt.Errorf("target channel: %w", err)
	}
	if t.TargetKind != wantKind {
		return errors.New("target kind does not match purpose")
	}
	if !validUTF8NonEmpty(t.ProviderTarget) {
		return errors.New("provider target is required")
	}
	switch t.TargetKind {
	case TargetKindUser:
		if !validUTF8NonEmpty(t.ExternalUserID) || t.ExternalChatID != "" || t.ExternalThreadID != "" {
			return errors.New("user target fields are invalid")
		}
	case TargetKindConversation:
		if !validUTF8NonEmpty(t.ExternalChatID) || t.ExternalUserID != "" || t.ExternalThreadID != "" {
			return errors.New("conversation target fields are invalid")
		}
	case TargetKindTopic:
		if !validUTF8NonEmpty(t.ExternalChatID) || !validUTF8NonEmpty(t.ExternalThreadID) || t.ExternalUserID != "" {
			return errors.New("topic target fields are invalid")
		}
	case TargetKindMessage:
		if t.ExternalUserID != "" || t.ExternalChatID != "" || t.ExternalThreadID != "" {
			return errors.New("message target fields are invalid")
		}
	default:
		return errors.New("target kind is invalid")
	}
	return nil
}

// TargetEnvelope is the persisted AES-GCM representation of a provider target.
type TargetEnvelope struct {
	Algorithm     string `json:"algorithm"`
	KeyVersion    string `json:"key_version"`
	NonceB64      string `json:"nonce_b64"`
	CiphertextB64 string `json:"ciphertext_b64"`
}

// Validate checks the serialized envelope without attempting decryption.
func (e TargetEnvelope) Validate() error {
	if e.Algorithm != TargetAlgorithmAES256GCM {
		return errors.New("target envelope algorithm is invalid")
	}
	if e.KeyVersion == "" {
		return errors.New("target envelope key_version is required")
	}
	if decode, err := base64.RawURLEncoding.DecodeString(e.NonceB64); err != nil || len(decode) != TargetNonceSize {
		return errors.New("target envelope nonce is invalid")
	}
	if decode, err := base64.RawURLEncoding.DecodeString(e.CiphertextB64); err != nil || len(decode) < TargetAuthenticationTagSize {
		return errors.New("target envelope ciphertext is invalid")
	}
	return nil
}

// TargetProtector seals and opens provider targets within a trusted scope.
type TargetProtector interface {
	Seal(ctx context.Context, targetContext TargetContext, purpose TargetPurpose, target TargetPlaintext) (TargetEnvelope, error)
	Open(ctx context.Context, targetContext TargetContext, purpose TargetPurpose, envelope TargetEnvelope) (TargetPlaintext, error)
}

// ExternalIDHasher derives a binding-scoped lookup key without returning a raw
// provider identifier to persistence or runtime layers.
type ExternalIDHasher interface {
	Hash(ctx context.Context, scope tenant.Scope, bindingID string, kind ExternalIDKind, externalID string) (hash string, keyVersion string, err error)
	HashWithVersion(ctx context.Context, scope tenant.Scope, bindingID string, kind ExternalIDKind, externalID, keyVersion string) (hash string, err error)
}

// NormalizeExternalID trims protocol-allowed surrounding whitespace while
// preserving case and rejecting invalid or empty identifiers.
func NormalizeExternalID(externalID string) (string, error) {
	if !utf8.ValidString(externalID) {
		return "", errors.New("external id is not valid utf-8")
	}
	normalized := strings.TrimSpace(externalID)
	if normalized == "" {
		return "", errors.New("external id is required")
	}
	return normalized, nil
}

func targetKindForPurpose(purpose TargetPurpose) (TargetKind, bool) {
	switch purpose {
	case TargetPurposeIdentityUser:
		return TargetKindUser, true
	case TargetPurposeConversationChat:
		return TargetKindConversation, true
	case TargetPurposeConversationTopic:
		return TargetKindTopic, true
	case TargetPurposeReplyMessage:
		return TargetKindMessage, true
	default:
		return "", false
	}
}

func targetEntityForPurpose(purpose TargetPurpose) (TargetEntityType, bool) {
	switch purpose {
	case TargetPurposeIdentityUser:
		return TargetEntityIdentity, true
	case TargetPurposeConversationChat, TargetPurposeConversationTopic:
		return TargetEntityConversation, true
	case TargetPurposeReplyMessage:
		return TargetEntityInbox, true
	default:
		return "", false
	}
}

func validUTF8NonEmpty(value string) bool {
	return value != "" && utf8.ValidString(value)
}
