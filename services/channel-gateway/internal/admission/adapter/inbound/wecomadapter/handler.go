// Package wecomadapter maps authenticated protocol callbacks into Admission.
// The caller owns the Client lifecycle and obtains the connection-owner fence.
package wecomadapter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	wire "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/platform/im/wecom"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
)

// Acceptor returns a durable receipt; returning from a protocol Handler itself
// does not send a provider-side persistent consumer acknowledgement.
type Acceptor interface {
	AcceptInbound(context.Context, domain.Inbound) (domain.Receipt, error)
}

type Handler struct {
	accountID string
	botID     string
	fence     domain.ConnectionFence
	acceptor  Acceptor
	retry     RetryPolicy
}

var accountIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// NewHandler binds trusted configuration, never tenant/binding data from a callback.
// A fresh Handler is created for every acquired owner/configuration revision.
func NewHandler(accountID, botID string, fence domain.ConnectionFence, acceptor Acceptor, policies ...RetryPolicy) (*Handler, error) {
	if !accountIDPattern.MatchString(accountID) || !opaque(botID, 1024, true) || fence.Validate() != nil || acceptor == nil {
		return nil, domain.ErrInvalidInput
	}
	if len(policies) > 1 {
		return nil, domain.ErrInvalidInput
	}
	var policy RetryPolicy
	if len(policies) == 1 {
		policy = policies[0]
	}
	retry, err := normalizeRetry(policy)
	if err != nil {
		return nil, err
	}
	return &Handler{accountID: accountID, botID: botID, fence: fence, acceptor: acceptor, retry: retry}, nil
}

type replyContext struct {
	ChatType          string `json:"chat_type"`
	ChatIDOrUserID    string `json:"chatid_or_userid"`
	CallbackRequestID string `json:"callback_req_id"`
	ReceivedAt        string `json:"received_at"`
}

// Handle deliberately does not execute or reply to a message. Only ordinary
// nonblank text is prompt input; notices are audited interactions, and unsupported
// callbacks/blank text are ignored. disconnected_event belongs to Connection.
func (h *Handler) Handle(ctx context.Context, event wecom.Event) error {
	if event.BotID != h.botID {
		return domain.ErrInvalidInput
	}
	if event.Kind == wecom.EventNotice && event.EventType == "disconnected_event" {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if event.Generation == 0 || !opaque(event.MessageID, 256, true) || !opaque(event.RequestID, 256, true) || !opaque(event.SenderID, 256, true) || !opaque(event.ChatID, 256, false) || !opaque(event.EventType, 1024, false) || !digestPattern.MatchString(event.BodyDigest) || !utf8.ValidString(event.Text) || len(event.Text) > 65536 {
		return domain.ErrInvalidInput
	}
	conversation := ""
	switch event.ChatType {
	case "":
		// Official event callbacks may omit chat metadata. Preserve the audit
		// decision without guessing a private conversation or reply address.
		if event.Kind == wecom.EventText {
			return domain.ErrInvalidInput
		}
	case "single":
		conversation = event.SenderID
	case "group":
		if event.ChatID == "" {
			return domain.ErrInvalidInput
		}
		conversation = event.ChatID
	default:
		return domain.ErrInvalidInput
	}
	kind := "ignore"
	text := ""
	switch event.Kind {
	case wecom.EventText:
		if event.EventType != "" {
			return domain.ErrInvalidInput
		}
		if strings.TrimSpace(event.Text) != "" {
			kind = "text"
			text = event.Text
		}
	case wecom.EventNotice:
		if event.EventType == "" {
			return domain.ErrInvalidInput
		}
		kind = "interaction"
	case wecom.EventUnsupported:
		if event.EventType == "" {
			return domain.ErrInvalidInput
		}
	default:
		return domain.ErrInvalidInput
	}
	// BodyDigest covers the strictly decoded provider body, including unknown body
	// fields, but excludes callback req_id and socket generation. This second,
	// versioned digest binds it to the curated interpretation and trusted bot.
	semantic := struct {
		Version    int             `json:"version"`
		MessageID  string          `json:"message_id"`
		BotID      string          `json:"bot_id"`
		Kind       wecom.EventKind `json:"kind"`
		EventType  string          `json:"event_type"`
		ChatType   string          `json:"chat_type"`
		ChatID     string          `json:"chat_id"`
		SenderID   string          `json:"sender_id"`
		Text       string          `json:"text"`
		BodyDigest string          `json:"body_digest"`
	}{1, event.MessageID, h.botID, event.Kind, event.EventType, event.ChatType, event.ChatID, event.SenderID, event.Text, event.BodyDigest}
	canonical, err := json.Marshal(semantic)
	if err != nil {
		return domain.ErrInvalidInput
	}
	sum := sha256.Sum256(canonical)
	now := time.Now().UTC()
	var reply json.RawMessage
	if conversation != "" {
		reply, err = json.Marshal(replyContext{event.ChatType, conversation, event.RequestID, now.Format(time.RFC3339Nano)})
		if err != nil {
			return domain.ErrInvalidInput
		}
		// Reuse the schema-owned narrow codec for a known address. Chatless
		// non-text notices do not need or receive a fabricated reply context.
		if _, err = wire.DecodeReplyContext("wecom", reply); err != nil {
			return domain.ErrInvalidInput
		}
	}
	localFence := h.fence // An acceptor cannot mutate the Handler's authorization.
	in := domain.Inbound{Key: domain.EventKey{Provider: "wecom", AccountID: h.accountID, EventID: event.MessageID}, Kind: kind, ConversationID: conversation, SenderID: event.SenderID, Text: text, ReplyContext: reply, SourceDigest: hex.EncodeToString(sum[:]), ReceivedAt: now, ConnectionFence: &localFence, ReplyOrigin: &domain.ReplyOrigin{InstanceID: localFence.InstanceID, Epoch: localFence.Epoch, Revision: localFence.Revision, SocketGeneration: event.Generation}}
	if err = in.Validate(); err != nil {
		return err
	}
	return h.accept(ctx, in)
}

func opaque(s string, max int, required bool) bool {
	if s == "" {
		return !required
	}
	if len(s) > max || !utf8.ValidString(s) || strings.TrimSpace(s) != s {
		return false
	}
	for _, r := range s {
		if r <= 32 || unicode.IsControl(r) {
			return false
		}
	}
	return true
}
