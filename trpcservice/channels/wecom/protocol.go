package wecom

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

const maxProviderURLLength = 16 << 10

const wecomProviderTargetPrefix = "wecom-target-v1:"

var (
	errWeComBindingAccountMismatch  = errors.New("wecom binding account does not match event")
	errWeComMessageIDRequired       = errors.New("wecom message id is required")
	errWeComSenderRequired          = errors.New("wecom sender id is required")
	errWeComConversationInvalid     = errors.New("wecom conversation is invalid")
	errWeComMessageInvalid          = errors.New("wecom message is invalid")
	errWeComUnsupportedMediaPayload = errors.New("wecom media payload is invalid")
)

// Message is the message body delivered by the official WeCom AI Bot
// long-connection protocol. It contains only provider event fields.
type Message struct {
	MessageID   string          `json:"msgid"`
	AIBotID     string          `json:"aibotid"`
	ChatID      string          `json:"chatid"`
	ChatType    string          `json:"chattype"`
	From        MessageFrom     `json:"from"`
	MessageType string          `json:"msgtype"`
	CreateTime  int64           `json:"create_time"`
	Text        MessageText     `json:"text"`
	Image       MessageMedia    `json:"image"`
	File        MessageMedia    `json:"file"`
	Mixed       MessageMixed    `json:"mixed"`
	Stream      MessageStream   `json:"stream"`
	Event       json.RawMessage `json:"event"`
	EventType   string          `json:"eventtype"`
	// ResponseRequestID is copied from the authenticated callback frame
	// header. It is not provider input and is never persisted in plaintext;
	// channelInput seals the complete message target before admission.
	ResponseRequestID string `json:"-"`
}

type MessageFrom struct {
	UserID string `json:"userid"`
}

type MessageText struct {
	Content string `json:"content"`
}

type MessageMedia struct {
	URL    string `json:"url"`
	AESKey string `json:"aeskey,omitempty"`
}

type MessageMixed struct {
	Items []MessageMixedItem `json:"msg_item"`
}

type MessageMixedItem struct {
	MessageType string       `json:"msgtype"`
	Text        MessageText  `json:"text"`
	Image       MessageMedia `json:"image"`
	File        MessageMedia `json:"file"`
}

type MessageStream struct {
	ID string `json:"id"`
}

// ProviderEventContext keeps provider-specific event data at the adapter
// boundary. It is never copied into Gateway, Worker, Runner, or persistence.
type ProviderEventContext struct {
	StreamID     string
	EventType    string
	EventPayload []byte
}

// VerifiedProviderEnvelope is the normalized result of one authenticated
// WeCom long-connection message.
type VerifiedProviderEnvelope struct {
	TenantID          string
	AppID             string
	Channel           channels.Channel
	BindingID         string
	BindingRevision   int64
	ExternalMessageID string
	SenderID          string
	ConversationKind  channels.ConversationKind
	ChatID            string
	MessageType       channels.MessageType
	Text              string
	Media             []channels.ProviderMediaRef
	ProviderTimestamp time.Time
	Context           ProviderEventContext

	mapping     channels.ChannelMappingInput
	replyTarget string
}

// Validate checks the normalized fields needed to build ChannelInput.
func (e VerifiedProviderEnvelope) Validate() error {
	if e.TenantID == "" || e.AppID == "" {
		return errors.New("verified wecom envelope scope is required")
	}
	if e.Channel != channels.ChannelWeCom {
		return errors.New("verified wecom envelope channel is invalid")
	}
	if e.BindingID == "" || e.BindingRevision <= 0 {
		return errors.New("verified wecom envelope binding is invalid")
	}
	normalizedMessageID, err := channels.NormalizeExternalID(e.ExternalMessageID)
	if err != nil || normalizedMessageID != e.ExternalMessageID {
		return errWeComMessageIDRequired
	}
	normalizedSenderID, err := channels.NormalizeExternalID(e.SenderID)
	if err != nil || normalizedSenderID != e.SenderID {
		return errWeComSenderRequired
	}
	if err := e.ConversationKind.Validate(); err != nil {
		return err
	}
	switch e.ConversationKind {
	case channels.ConversationDirect:
		if e.ChatID != "" {
			return errWeComConversationInvalid
		}
	case channels.ConversationGroup:
		if _, err := channels.NormalizeExternalID(e.ChatID); err != nil {
			return errWeComConversationInvalid
		}
	default:
		return errWeComConversationInvalid
	}
	if err := e.MessageType.Validate(); err != nil {
		return err
	}
	if e.MessageType == channels.MessageTypeText && e.Text == "" {
		return errWeComMessageInvalid
	}
	if e.MessageType == channels.MessageTypeMixed && e.Text == "" && len(e.Media) == 0 {
		return errWeComMessageInvalid
	}
	if !utf8.ValidString(e.Text) {
		return errWeComMessageInvalid
	}
	for _, media := range e.Media {
		if err := media.Validate(); err != nil {
			return fmt.Errorf("wecom media: %w", err)
		}
	}
	if e.Context.StreamID != "" && e.Context.EventType != "" {
		return errWeComMessageInvalid
	}
	if e.MessageType == channels.MessageTypeEvent {
		if e.Context.StreamID == "" && e.Context.EventType == "" {
			return errWeComMessageInvalid
		}
		if len(e.Context.EventPayload) > 0 && !json.Valid(e.Context.EventPayload) {
			return errWeComMessageInvalid
		}
	} else if e.Context.StreamID != "" || e.Context.EventType != "" || len(e.Context.EventPayload) > 0 {
		return errWeComMessageInvalid
	}
	if err := e.mapping.Validate(e.ConversationKind); err != nil {
		return fmt.Errorf("wecom mapping: %w", err)
	}
	if e.replyTarget == "" {
		return errWeComMessageInvalid
	}
	return nil
}

func normalizeMessage(binding channels.BindingSnapshot, message Message) (VerifiedProviderEnvelope, error) {
	if message.AIBotID == "" || message.AIBotID != binding.ExternalAccount {
		return VerifiedProviderEnvelope{}, errWeComBindingAccountMismatch
	}
	messageID, err := channels.NormalizeExternalID(message.MessageID)
	if err != nil {
		return VerifiedProviderEnvelope{}, errWeComMessageIDRequired
	}
	senderID, err := channels.NormalizeExternalID(message.From.UserID)
	if err != nil {
		return VerifiedProviderEnvelope{}, errWeComSenderRequired
	}
	conversationKind, chatID, err := normalizeConversation(message.ChatType, message.ChatID)
	if err != nil {
		return VerifiedProviderEnvelope{}, err
	}
	messageType, text, media, providerContext, err := normalizeContent(message)
	if err != nil {
		return VerifiedProviderEnvelope{}, err
	}
	var providerTimestamp time.Time
	if message.CreateTime > 0 {
		providerTimestamp = time.Unix(message.CreateTime, 0).UTC()
	}
	mapping := channels.ChannelMappingInput{
		ExternalSenderID:     senderID,
		ProviderSenderTarget: senderID,
	}
	if conversationKind == channels.ConversationGroup {
		mapping.ExternalChatID = chatID
		mapping.ProviderConversationTarget = chatID
	}
	destination := senderID
	if conversationKind == channels.ConversationGroup {
		destination = chatID
	}
	replyTarget, err := encodeProviderTarget(destination, message.ResponseRequestID)
	if err != nil {
		return VerifiedProviderEnvelope{}, errWeComMessageInvalid
	}
	envelope := VerifiedProviderEnvelope{
		TenantID:          binding.TenantID,
		AppID:             binding.AppID,
		Channel:           channels.ChannelWeCom,
		BindingID:         binding.BindingID,
		BindingRevision:   binding.BindingRevision,
		ExternalMessageID: messageID,
		SenderID:          senderID,
		ConversationKind:  conversationKind,
		ChatID:            chatID,
		MessageType:       messageType,
		Text:              text,
		Media:             media,
		ProviderTimestamp: providerTimestamp,
		Context:           providerContext,
		mapping:           mapping,
		replyTarget:       replyTarget,
	}
	if err := envelope.Validate(); err != nil {
		return VerifiedProviderEnvelope{}, err
	}
	return envelope, nil
}

type providerTarget struct {
	Destination string `json:"destination"`
	RequestID   string `json:"request_id,omitempty"`
}

func encodeProviderTarget(destination, requestID string) (string, error) {
	normalized, err := channels.NormalizeExternalID(destination)
	if err != nil || normalized != destination {
		return "", errWeComMessageInvalid
	}
	encoded, err := json.Marshal(providerTarget{
		Destination: destination,
		RequestID:   requestID,
	})
	if err != nil {
		return "", err
	}
	return wecomProviderTargetPrefix + base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decodeProviderTarget(value string) (providerTarget, error) {
	if strings.HasPrefix(value, wecomProviderTargetPrefix) {
		encoded := strings.TrimPrefix(value, wecomProviderTargetPrefix)
		decoded, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil {
			return providerTarget{}, errors.New("wecom provider target is invalid")
		}
		var target providerTarget
		if err := json.Unmarshal(decoded, &target); err != nil {
			return providerTarget{}, errors.New("wecom provider target is invalid")
		}
		normalized, err := channels.NormalizeExternalID(target.Destination)
		if err != nil || normalized != target.Destination || strings.ContainsAny(target.RequestID, "\r\n") {
			return providerTarget{}, errors.New("wecom provider target is invalid")
		}
		return target, nil
	}
	normalized, err := channels.NormalizeExternalID(value)
	if err != nil || normalized != value {
		return providerTarget{}, errors.New("wecom provider target is invalid")
	}
	return providerTarget{Destination: value}, nil
}

func normalizeConversation(chatType, chatID string) (channels.ConversationKind, string, error) {
	switch strings.ToLower(strings.TrimSpace(chatType)) {
	case "single":
		if strings.TrimSpace(chatID) != "" {
			return "", "", errWeComConversationInvalid
		}
		return channels.ConversationDirect, "", nil
	case "group":
		normalizedChatID, err := channels.NormalizeExternalID(chatID)
		if err != nil {
			return "", "", errWeComConversationInvalid
		}
		return channels.ConversationGroup, normalizedChatID, nil
	default:
		return "", "", errWeComConversationInvalid
	}
}

func normalizeContent(message Message) (channels.MessageType, string, []channels.ProviderMediaRef, ProviderEventContext, error) {
	var emptyContext ProviderEventContext
	switch strings.ToLower(strings.TrimSpace(message.MessageType)) {
	case "text":
		if message.Text.Content == "" || !utf8.ValidString(message.Text.Content) {
			return "", "", nil, emptyContext, errWeComMessageInvalid
		}
		return channels.MessageTypeText, message.Text.Content, nil, emptyContext, nil
	case "image":
		kind, text, media, err := singleMedia(channels.MessageTypeImage, message.Image)
		return kind, text, media, emptyContext, err
	case "file":
		kind, text, media, err := singleMedia(channels.MessageTypeFile, message.File)
		return kind, text, media, emptyContext, err
	case "mixed":
		kind, text, media, err := normalizeMixed(message.Mixed.Items)
		return kind, text, media, emptyContext, err
	case "card":
		return channels.MessageTypeCard, "", nil, emptyContext, nil
	case "stream":
		streamID, err := channels.NormalizeExternalID(message.Stream.ID)
		if err != nil {
			return "", "", nil, emptyContext, errWeComMessageInvalid
		}
		return channels.MessageTypeEvent, "", nil, ProviderEventContext{StreamID: streamID}, nil
	case "event":
		eventType := message.EventType
		payload := message.Event
		if eventType == "" && len(payload) > 0 {
			var event struct {
				EventType string `json:"eventtype"`
			}
			if err := json.Unmarshal(payload, &event); err != nil {
				return "", "", nil, emptyContext, errWeComMessageInvalid
			}
			eventType = event.EventType
		}
		normalized, err := channels.NormalizeExternalID(eventType)
		if err != nil {
			return "", "", nil, emptyContext, errWeComMessageInvalid
		}
		if len(payload) == 0 {
			payload, err = json.Marshal(map[string]string{"eventtype": normalized})
			if err != nil {
				return "", "", nil, emptyContext, errWeComMessageInvalid
			}
		}
		return channels.MessageTypeEvent, "", nil, ProviderEventContext{EventType: normalized, EventPayload: slices.Clone(payload)}, nil
	default:
		return channels.MessageTypeUnsupported, "", nil, emptyContext, nil
	}
}

func singleMedia(kind channels.MessageType, media MessageMedia) (channels.MessageType, string, []channels.ProviderMediaRef, error) {
	if err := validateMediaURL(media.URL); err != nil {
		return "", "", nil, errWeComUnsupportedMediaPayload
	}
	return kind, "", []channels.ProviderMediaRef{{
		Kind:          kind,
		Reference:     media.URL,
		DecryptionKey: media.AESKey,
	}}, nil
}

func normalizeMixed(items []MessageMixedItem) (channels.MessageType, string, []channels.ProviderMediaRef, error) {
	if len(items) == 0 {
		return "", "", nil, errWeComMessageInvalid
	}
	var texts []string
	var media []channels.ProviderMediaRef
	for _, item := range items {
		switch strings.ToLower(strings.TrimSpace(item.MessageType)) {
		case "text":
			if item.Text.Content == "" || !utf8.ValidString(item.Text.Content) {
				return "", "", nil, errWeComMessageInvalid
			}
			texts = append(texts, item.Text.Content)
		case "image":
			if err := validateMediaURL(item.Image.URL); err != nil {
				return "", "", nil, errWeComUnsupportedMediaPayload
			}
			media = append(media, channels.ProviderMediaRef{
				Kind:          channels.MessageTypeImage,
				Reference:     item.Image.URL,
				DecryptionKey: item.Image.AESKey,
			})
		case "file":
			if err := validateMediaURL(item.File.URL); err != nil {
				return "", "", nil, errWeComUnsupportedMediaPayload
			}
			media = append(media, channels.ProviderMediaRef{
				Kind:          channels.MessageTypeFile,
				Reference:     item.File.URL,
				DecryptionKey: item.File.AESKey,
			})
		default:
			return "", "", nil, errWeComUnsupportedMediaPayload
		}
	}
	text := strings.Join(texts, "\n")
	if len(media) == 0 {
		return channels.MessageTypeText, text, nil, nil
	}
	if text == "" && len(media) == 1 {
		return media[0].Kind, text, media, nil
	}
	return channels.MessageTypeMixed, text, media, nil
}

func validateMediaURL(value string) error {
	if value == "" || len(value) > maxProviderURLLength || !utf8.ValidString(value) {
		return errors.New("wecom media url is invalid")
	}
	if !strings.HasPrefix(value, "https://") || strings.ContainsAny(value, "\r\n\t") {
		return errors.New("wecom media url is invalid")
	}
	return nil
}
