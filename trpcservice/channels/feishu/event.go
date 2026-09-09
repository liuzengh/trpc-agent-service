package feishu

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// urlVerification is the plaintext URL verification handshake body.
type urlVerification struct {
	Challenge string `json:"challenge"`
	Token     string `json:"token"`
	Type      string `json:"type"`
}

// envelope is the event-subscription v2 wrapper.
type envelope struct {
	Schema string `json:"schema"`
	Header struct {
		EventID   string `json:"event_id"`
		EventType string `json:"event_type"`
		Token     string `json:"token"`
		AppID     string `json:"app_id"`
		TenantKey string `json:"tenant_key"`
	} `json:"header"`
	Event json.RawMessage `json:"event"`
}

// messageEvent is the im.message.receive_v1 payload.
type messageEvent struct {
	Sender struct {
		SenderID struct {
			OpenID  string `json:"open_id"`
			UnionID string `json:"union_id"`
			UserID  string `json:"user_id"`
		} `json:"sender_id"`
		SenderType string `json:"sender_type"`
	} `json:"sender"`
	Message struct {
		MessageID   string `json:"message_id"`
		ChatID      string `json:"chat_id"`
		ChatType    string `json:"chat_type"`
		MessageType string `json:"message_type"`
		Content     string `json:"content"`
		Mentions    []struct {
			Key  string `json:"key"`
			Name string `json:"name"`
		} `json:"mentions"`
	} `json:"message"`
}

// externalUserID returns the stable platform identity. Nicknames are never
// used; open_id is preferred because it is stable per app, union_id is the
// fallback for cross-app console configurations.
func (event *messageEvent) externalUserID() string {
	if openID := strings.TrimSpace(event.Sender.SenderID.OpenID); openID != "" {
		return openID
	}
	return strings.TrimSpace(event.Sender.SenderID.UnionID)
}

// normalize converts one verified v2 message event into the shared durable
// ingress contract. tenant/app/binding/version always come from the
// server-owned Binding, never from the callback body.
func normalize(binding Binding, env envelope, now time.Time) (gateway.InboundMessage, error) {
	if binding.TenantID == "" || binding.AppID == "" || binding.BindingID == "" || binding.FeishuAppID == "" || binding.ConfigVersion == 0 {
		return gateway.InboundMessage{}, errors.New("feishu: complete tenant binding is required")
	}
	if env.Header.AppID == "" || env.Header.AppID != binding.FeishuAppID {
		return gateway.InboundMessage{}, fmt.Errorf("%w: app_id", ErrBindingMismatch)
	}
	if env.Header.EventType != "im.message.receive_v1" {
		return gateway.InboundMessage{}, fmt.Errorf("%w: event %q", ErrUnsupportedMessage, env.Header.EventType)
	}
	var event messageEvent
	if err := json.Unmarshal(env.Event, &event); err != nil {
		return gateway.InboundMessage{}, fmt.Errorf("%w: malformed message event", ErrUnsupportedMessage)
	}
	if event.Sender.SenderType != "" && event.Sender.SenderType != "user" {
		// Messages from other bots or apps are poison inputs for the Agent
		// path; ack them so Feishu does not retry.
		return gateway.InboundMessage{}, fmt.Errorf("%w: sender %q", ErrUnsupportedMessage, event.Sender.SenderType)
	}
	externalUserID := event.externalUserID()
	if externalUserID == "" {
		return gateway.InboundMessage{}, errors.New("feishu: callback sender is required")
	}
	text, err := messageText(event)
	if err != nil {
		return gateway.InboundMessage{}, err
	}
	externalMessageID := strings.TrimSpace(env.Header.EventID)
	if externalMessageID == "" {
		externalMessageID = strings.TrimSpace(event.Message.MessageID)
	}
	if externalMessageID == "" {
		sum := sha256.Sum256([]byte(strings.Join([]string{env.Header.AppID, externalUserID, event.Message.ChatID, event.Message.MessageType, event.Message.Content}, "\x00")))
		externalMessageID = "event-" + hex.EncodeToString(sum[:16])
	}
	userID, err := tenant.CanonicalUserID(tenant.ChannelTypeFeishu, binding.BindingID, externalUserID)
	if err != nil {
		return gateway.InboundMessage{}, err
	}
	conversationID := ""
	var sessionID string
	if strings.EqualFold(strings.TrimSpace(event.Message.ChatType), "group") {
		conversationID = strings.TrimSpace(event.Message.ChatID)
		if conversationID == "" {
			return gateway.InboundMessage{}, errors.New("feishu: group callback chat is required")
		}
		sessionID, err = tenant.GroupSessionID(binding.BindingID, conversationID)
	} else {
		sessionID, err = tenant.DirectSessionID(binding.BindingID, externalUserID)
	}
	if err != nil {
		return gateway.InboundMessage{}, err
	}
	traceHash := sha256.Sum256([]byte(binding.TenantID + "\x00" + binding.BindingID + "\x00" + externalMessageID))
	inbound := gateway.InboundMessage{
		TenantID:          binding.TenantID,
		AppID:             binding.AppID,
		BindingID:         binding.BindingID,
		ExternalMessageID: externalMessageID,
		ExternalUserID:    externalUserID,
		ConversationID:    conversationID,
		UserID:            userID,
		SessionID:         sessionID,
		Text:              text,
		TraceID:           "feishu-" + hex.EncodeToString(traceHash[:8]),
		ConfigVersion:     binding.ConfigVersion,
		ReceivedAt:        now.UTC(),
	}
	if media := event.mediaReference(); media != nil {
		media.MessageID = strings.TrimSpace(event.Message.MessageID)
		inbound.Media = media
	}
	return inbound, nil
}

func (event messageEvent) mediaReference() *gateway.MediaReference {
	switch strings.ToLower(strings.TrimSpace(event.Message.MessageType)) {
	case "image":
		var content struct {
			ImageKey string `json:"image_key"`
		}
		if json.Unmarshal([]byte(event.Message.Content), &content) == nil && strings.TrimSpace(content.ImageKey) != "" {
			return &gateway.MediaReference{Kind: "image", Key: strings.TrimSpace(content.ImageKey)}
		}
	case "file":
		var content struct {
			FileKey  string `json:"file_key"`
			FileName string `json:"file_name"`
		}
		if json.Unmarshal([]byte(event.Message.Content), &content) == nil && strings.TrimSpace(content.FileKey) != "" {
			return &gateway.MediaReference{Kind: "file", Key: strings.TrimSpace(content.FileKey), Name: strings.TrimSpace(content.FileName)}
		}
	}
	return nil
}

// messageText converts the supported message types into Agent input. Images
// and files become safe metadata placeholders: provider keys never enter the
// text, and no external URL is ever fetched. Controlled downloads plug into
// the MediaDownloader seam in a later PR.
func messageText(event messageEvent) (string, error) {
	switch strings.ToLower(strings.TrimSpace(event.Message.MessageType)) {
	case "text":
		var content struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal([]byte(event.Message.Content), &content); err != nil {
			return "", fmt.Errorf("%w: malformed text content", ErrUnsupportedMessage)
		}
		text := normalizeMentions(content.Text, event.Message.Mentions)
		if text == "" {
			return "", fmt.Errorf("%w: empty text", ErrUnsupportedMessage)
		}
		return text, nil
	case "image":
		return "[Feishu image: received, pending controlled download]", nil
	case "file":
		var content struct {
			FileName string `json:"file_name"`
		}
		_ = json.Unmarshal([]byte(event.Message.Content), &content)
		name := strings.TrimSpace(content.FileName)
		if name == "" {
			return "[Feishu file: received, pending controlled download]", nil
		}
		return "[Feishu file: " + name + ", pending controlled download]", nil
	}
	return "", fmt.Errorf("%w: type %q", ErrUnsupportedMessage, event.Message.MessageType)
}

// normalizeMentions strips @_user_N placeholders (the bot invocation and any
// user mentions) and collapses the leftover whitespace, so the Agent sees
// only the user's own words.
func normalizeMentions(text string, mentions []struct {
	Key  string `json:"key"`
	Name string `json:"name"`
},
) string {
	for _, mention := range mentions {
		if key := strings.TrimSpace(mention.Key); key != "" {
			text = strings.ReplaceAll(text, key, " ")
		}
	}
	return strings.Join(strings.Fields(text), " ")
}
