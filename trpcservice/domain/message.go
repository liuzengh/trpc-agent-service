package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
)

// Scope describes whether an IM conversation is a direct or group chat.
type Scope string

const (
	ScopeDirect Scope = "direct"
	ScopeGroup  Scope = "group"
)

// Attachment is a provider-neutral reference to an inbound or outbound file.
// URL is deliberately treated as opaque and must never contain provider tokens.
type Attachment struct {
	Type     string `json:"type"`
	Name     string `json:"name,omitempty"`
	URL      string `json:"url,omitempty"`
	FileID   string `json:"file_id,omitempty"`
	MimeType string `json:"mime_type,omitempty"`
}

// InboundMessage is the canonical input produced by every Channel Adapter.
type InboundMessage struct {
	TenantID          string       `json:"tenant_id"`
	BindingID         string       `json:"binding_id"`
	Channel           string       `json:"channel"`
	ExternalMessageID string       `json:"external_message_id"`
	ExternalUserID    string       `json:"external_user_id"`
	ConversationID    string       `json:"conversation_id"`
	ThreadID          string       `json:"thread_id,omitempty"`
	Scope             Scope        `json:"scope"`
	Text              string       `json:"text"`
	Attachments       []Attachment `json:"attachments,omitempty"`
	ReceivedAt        time.Time    `json:"received_at"`
	ReplyTarget       string       `json:"reply_target"`
}

// OutboundMessage is the canonical reply consumed by Channel Adapters.
type OutboundMessage struct {
	TenantID    string       `json:"tenant_id"`
	BindingID   string       `json:"binding_id"`
	Channel     string       `json:"channel"`
	Target      string       `json:"target"`
	ThreadID    string       `json:"thread_id,omitempty"`
	Scope       Scope        `json:"scope,omitempty"`
	Text        string       `json:"text"`
	Attachments []Attachment `json:"attachments,omitempty"`
}

// Identity derives storage-safe, tenant-scoped user and session identifiers.
// Raw IM identifiers are never used as cross-tenant storage keys.
func Identity(msg InboundMessage, appName string) (userID, sessionID string) {
	conversation := msg.ConversationID
	if msg.Scope == ScopeDirect {
		userID = digest("user", msg.TenantID, msg.BindingID, msg.Channel, msg.ExternalUserID)
		// Real IM direct messages are one ongoing conversation per provider user,
		// even when a provider changes its opaque chat id. The admin API is
		// different: its console exposes explicit, user-created conversations and
		// must honor conversation_id so two "new conversations" do not silently
		// share model context. Include the simulated user as well so callers cannot
		// collide by choosing the same conversation id.
		if msg.Channel == "api" && msg.ConversationID != "" {
			conversation = msg.ExternalUserID + "\x1f" + msg.ConversationID
		} else {
			conversation = msg.ExternalUserID
		}
	} else {
		// Runner keys include UserID. A group principal keeps all members of the
		// same group on one transcript while the sender remains available in the
		// request governance context and audit record.
		userID = digest("group", msg.TenantID, msg.BindingID, msg.Channel, conversation)
	}
	sessionID = digest("session", msg.TenantID, appName, msg.BindingID, msg.Channel,
		string(msg.Scope), conversation, msg.ThreadID)
	return userID, sessionID
}

// SessionPartitionKey is the single FIFO lane shared by durable dispatch and
// the Worker session lock. Keeping it beside Identity prevents queue ordering
// from drifting away from transcript isolation when identity rules evolve.
func SessionPartitionKey(msg InboundMessage, appName string) string {
	principalID, sessionID := Identity(msg, appName)
	return AppNamespace(msg.TenantID, appName) + ":" + principalID + ":" + sessionID
}

// AppNamespace returns a stable opaque namespace accepted by tRPC-Agent's
// AppName. It intentionally contains no slash because slash scopes framework
// EventFilterKey branches.
func AppNamespace(tenantID, appName string) string {
	return "ta_" + digest("app", tenantID, appName)
}

// SenderIdentity is a pseudonymous audit identity. Unlike Runner's group
// principal it always represents the human sender.
func SenderIdentity(msg InboundMessage) string {
	return digest("sender", msg.TenantID, msg.BindingID, msg.Channel, msg.ExternalUserID)
}

// UserPrompt renders only bounded, non-credential attachment metadata. URL and
// provider file identifiers intentionally never cross the model boundary.
// Metadata is quoted so control characters and brackets cannot escape the
// attachment annotation format.
func UserPrompt(text string, attachments []Attachment, scope Scope, senderID string) string {
	if len(attachments) == 0 && scope != ScopeGroup {
		return text
	}
	var content strings.Builder
	if scope == ScopeGroup {
		content.WriteString("[sender_id=")
		content.WriteString(senderID)
		content.WriteString("]\n")
	}
	content.WriteString(text)
	for _, attachment := range attachments {
		if content.Len() > 0 {
			content.WriteByte('\n')
		}
		content.WriteString("[attachment type=")
		content.WriteString(strconv.Quote(attachment.Type))
		if attachment.Name != "" {
			content.WriteString(" name=")
			content.WriteString(strconv.Quote(attachment.Name))
		}
		if attachment.MimeType != "" {
			content.WriteString(" mime=")
			content.WriteString(strconv.Quote(attachment.MimeType))
		}
		content.WriteByte(']')
	}
	return content.String()
}

func digest(parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "\x1f")))
	return hex.EncodeToString(h[:16])
}
