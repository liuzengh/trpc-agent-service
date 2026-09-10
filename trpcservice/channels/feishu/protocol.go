package feishu

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

const (
	feishuMessageEventType = "im.message.receive_v1"
	feishuRecallEventType  = "im.message.recalled_v1"

	feishuTargetUser         = "open_id"
	feishuTargetConversation = "chat_id"
	feishuTargetTopic        = "thread_id"
	feishuTargetMessage      = "message_id"
)

var (
	errFeishuEvent             = errors.New("feishu event is invalid")
	errFeishuBindingAccount    = errors.New("feishu binding account does not match event")
	errFeishuMessageID         = errors.New("feishu message id is required")
	errFeishuSenderID          = errors.New("feishu sender open_id is required")
	errFeishuConversation      = errors.New("feishu conversation is invalid")
	errFeishuMessageContent    = errors.New("feishu message content is invalid")
	errFeishuUnsupportedTarget = errors.New("feishu provider target is invalid")
	errFeishuTargetMessage     = errors.New("feishu message reply target is invalid")
	errFeishuRecallEvent       = errors.New("feishu recall event is invalid")
	errFeishuRecallEventID     = errors.New("feishu recall event id is required")
)

// VerifiedProviderEnvelope is the provider-neutral result of one authenticated
// Feishu long-connection event. Provider JSON stops at this boundary.
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
	ThreadID          string
	MessageType       channels.MessageType
	Text              string
	Media             []channels.ProviderMediaRef
	ProviderTimestamp time.Time

	mapping     channels.ChannelMappingInput
	replyTarget string
}

// Validate checks the normalized fields needed to build a ChannelInput.
func (e VerifiedProviderEnvelope) Validate() error {
	if e.TenantID == "" || e.AppID == "" {
		return errors.New("verified feishu envelope scope is required")
	}
	if e.Channel != channels.ChannelFeishu {
		return errors.New("verified feishu envelope channel is invalid")
	}
	if e.BindingID == "" || e.BindingRevision <= 0 {
		return errors.New("verified feishu envelope binding is invalid")
	}
	normalizedMessageID, err := channels.NormalizeExternalID(e.ExternalMessageID)
	if err != nil || normalizedMessageID != e.ExternalMessageID {
		return errFeishuMessageID
	}
	normalizedSenderID, err := channels.NormalizeExternalID(e.SenderID)
	if err != nil || normalizedSenderID != e.SenderID {
		return errFeishuSenderID
	}
	if err := e.ConversationKind.Validate(); err != nil {
		return err
	}
	switch e.ConversationKind {
	case channels.ConversationDirect:
		if e.ChatID != "" || e.ThreadID != "" {
			return errFeishuConversation
		}
	case channels.ConversationGroup:
		if !isNormalizedID(e.ChatID) || e.ThreadID != "" {
			return errFeishuConversation
		}
	case channels.ConversationTopic:
		if !isNormalizedID(e.ChatID) || !isNormalizedID(e.ThreadID) {
			return errFeishuConversation
		}
	default:
		return errFeishuConversation
	}
	if err := e.MessageType.Validate(); err != nil {
		return err
	}
	if e.MessageType == channels.MessageTypeText && e.Text == "" {
		return errFeishuMessageContent
	}
	if (e.MessageType == channels.MessageTypeImage || e.MessageType == channels.MessageTypeFile) && len(e.Media) == 0 {
		return errFeishuMessageContent
	}
	if e.MessageType == channels.MessageTypeMixed && e.Text == "" && len(e.Media) == 0 {
		return errFeishuMessageContent
	}
	if !utf8.ValidString(e.Text) {
		return errFeishuMessageContent
	}
	for _, media := range e.Media {
		if err := media.Validate(); err != nil {
			return fmt.Errorf("feishu media: %w", err)
		}
	}
	if err := e.mapping.Validate(e.ConversationKind); err != nil {
		return fmt.Errorf("feishu mapping: %w", err)
	}
	if e.replyTarget == "" {
		return errFeishuTargetMessage
	}
	targetKind, targetID, err := parseProviderTarget(e.replyTarget)
	if err != nil || targetKind != feishuTargetMessage || targetID != e.ExternalMessageID {
		return errFeishuTargetMessage
	}
	return nil
}

// normalizeMessageEvent converts the event delivered by the official Feishu
// WebSocket SDK. TenantKey is event metadata, not a required Binding input;
// Binding scope is established by the client credentials and App ID check.
func normalizeMessageEvent(binding channels.BindingSnapshot, received *larkim.P2MessageReceiveV1) (VerifiedProviderEnvelope, error) {
	if received == nil || received.EventV2Base == nil || received.EventV2Base.Header == nil || received.Event == nil || received.Event.Message == nil {
		return VerifiedProviderEnvelope{}, errFeishuEvent
	}
	header := received.EventV2Base.Header
	if header.AppID != binding.ExternalAccount {
		return VerifiedProviderEnvelope{}, errFeishuBindingAccount
	}
	if header.EventType != "" && header.EventType != feishuMessageEventType {
		return VerifiedProviderEnvelope{}, errFeishuEvent
	}
	message := received.Event.Message
	senderID, err := channels.NormalizeExternalID(senderOpenID(received.Event.Sender))
	if err != nil {
		return VerifiedProviderEnvelope{}, errFeishuSenderID
	}
	messageID, err := channels.NormalizeExternalID(valueOf(message.MessageId))
	if err != nil {
		return VerifiedProviderEnvelope{}, errFeishuMessageID
	}
	conversationKind, chatID, threadID, err := normalizeConversation(
		valueOf(message.ChatType), valueOf(message.ChatId), valueOf(message.ThreadId),
	)
	if err != nil {
		return VerifiedProviderEnvelope{}, err
	}
	messageType, text, media, err := normalizeMessageContent(valueOf(message.MessageType), valueOf(message.Content))
	if err != nil {
		return VerifiedProviderEnvelope{}, err
	}
	providerTimestamp, err := providerTimestamp(valueOf(message.CreateTime))
	if err != nil {
		return VerifiedProviderEnvelope{}, err
	}
	mapping := channels.ChannelMappingInput{
		ExternalSenderID:     senderID,
		ProviderSenderTarget: providerTarget(feishuTargetUser, senderID),
	}
	if conversationKind == channels.ConversationGroup || conversationKind == channels.ConversationTopic {
		mapping.ExternalChatID = chatID
		mapping.ProviderConversationTarget = providerTarget(feishuTargetConversation, chatID)
	}
	if conversationKind == channels.ConversationTopic {
		mapping.ExternalThreadID = threadID
		mapping.ProviderThreadTarget = providerTarget(feishuTargetTopic, threadID)
	}
	envelope := VerifiedProviderEnvelope{
		TenantID:          binding.TenantID,
		AppID:             binding.AppID,
		Channel:           channels.ChannelFeishu,
		BindingID:         binding.BindingID,
		BindingRevision:   binding.BindingRevision,
		ExternalMessageID: messageID,
		SenderID:          senderID,
		ConversationKind:  conversationKind,
		ChatID:            chatID,
		ThreadID:          threadID,
		MessageType:       messageType,
		Text:              text,
		Media:             media,
		ProviderTimestamp: providerTimestamp,
		mapping:           mapping,
		replyTarget:       providerTarget(feishuTargetMessage, messageID),
	}
	if err := envelope.Validate(); err != nil {
		return VerifiedProviderEnvelope{}, err
	}
	return envelope, nil
}

// normalizeRecallEvent converts the official Feishu recall event into the
// existing durable recall boundary. It only checks App ID and event identity;
// TenantKey remains metadata and is never a required deployment setting.
func normalizeRecallEvent(
	binding channels.BindingSnapshot,
	recalled *larkim.P2MessageRecalledV1,
	payloadHash []byte,
) (channels.RecallRequest, error) {
	if recalled == nil || recalled.EventV2Base == nil || recalled.EventV2Base.Header == nil || recalled.Event == nil {
		return channels.RecallRequest{}, errFeishuRecallEvent
	}
	header := recalled.EventV2Base.Header
	if header.EventType != "" && header.EventType != feishuRecallEventType {
		return channels.RecallRequest{}, errFeishuRecallEvent
	}
	if header.AppID != binding.ExternalAccount {
		return channels.RecallRequest{}, errFeishuBindingAccount
	}
	eventID, err := channels.NormalizeExternalID(header.EventID)
	if err != nil || eventID != header.EventID {
		return channels.RecallRequest{}, errFeishuRecallEventID
	}
	messageID, err := channels.NormalizeExternalID(valueOf(recalled.Event.MessageId))
	if err != nil {
		return channels.RecallRequest{}, errFeishuMessageID
	}
	if len(payloadHash) != sha256.Size {
		return channels.RecallRequest{}, errFeishuRecallEvent
	}
	request := channels.RecallRequest{
		TenantID:          binding.TenantID,
		AppID:             binding.AppID,
		BindingID:         binding.BindingID,
		Channel:           channels.ChannelFeishu,
		ExternalEventID:   eventID,
		ExternalMessageID: messageID,
		PayloadHash:       append([]byte(nil), payloadHash...),
	}
	if err := request.Validate(); err != nil {
		return channels.RecallRequest{}, fmt.Errorf("feishu recall: %w", err)
	}
	return request, nil
}

func senderOpenID(sender *larkim.EventSender) string {
	if sender == nil || sender.SenderId == nil || sender.SenderId.OpenId == nil {
		return ""
	}
	return *sender.SenderId.OpenId
}

func normalizeConversation(chatType, chatID, threadID string) (channels.ConversationKind, string, string, error) {
	switch chatType {
	case "p2p":
		if threadID != "" {
			return "", "", "", errFeishuConversation
		}
		return channels.ConversationDirect, "", "", nil
	case "group":
		normalizedChatID, err := channels.NormalizeExternalID(chatID)
		if err != nil {
			return "", "", "", errFeishuConversation
		}
		if threadID == "" {
			return channels.ConversationGroup, normalizedChatID, "", nil
		}
		normalizedThreadID, err := channels.NormalizeExternalID(threadID)
		if err != nil {
			return "", "", "", errFeishuConversation
		}
		return channels.ConversationTopic, normalizedChatID, normalizedThreadID, nil
	default:
		return "", "", "", errFeishuConversation
	}
}

func normalizeMessageContent(messageType, raw string) (channels.MessageType, string, []channels.ProviderMediaRef, error) {
	switch strings.ToLower(strings.TrimSpace(messageType)) {
	case "text":
		var content struct {
			Text string `json:"text"`
		}
		if err := unmarshalObject(raw, &content); err != nil || content.Text == "" || !utf8.ValidString(content.Text) {
			return "", "", nil, errFeishuMessageContent
		}
		return channels.MessageTypeText, content.Text, nil, nil
	case "post":
		return normalizePost(raw)
	case "image":
		var content struct {
			ImageKey string `json:"image_key"`
		}
		if err := unmarshalObject(raw, &content); err != nil || !isNormalizedID(content.ImageKey) {
			return "", "", nil, errFeishuMessageContent
		}
		return channels.MessageTypeImage, "", []channels.ProviderMediaRef{{Kind: channels.MessageTypeImage, Reference: content.ImageKey}}, nil
	case "file":
		var content struct {
			FileKey string `json:"file_key"`
		}
		if err := unmarshalObject(raw, &content); err != nil || !isNormalizedID(content.FileKey) {
			return "", "", nil, errFeishuMessageContent
		}
		return channels.MessageTypeFile, "", []channels.ProviderMediaRef{{Kind: channels.MessageTypeFile, Reference: content.FileKey}}, nil
	case "interactive":
		var content map[string]json.RawMessage
		if err := unmarshalObject(raw, &content); err != nil || len(content) == 0 {
			return "", "", nil, errFeishuMessageContent
		}
		return channels.MessageTypeUnsupported, "", nil, nil
	default:
		return channels.MessageTypeUnsupported, "", nil, nil
	}
}

type postLocale struct {
	Title   string          `json:"title"`
	Content [][]postElement `json:"content"`
}

type postElement struct {
	Tag      string `json:"tag"`
	Text     string `json:"text"`
	UserName string `json:"user_name"`
	ImageKey string `json:"image_key"`
}

func normalizePost(raw string) (channels.MessageType, string, []channels.ProviderMediaRef, error) {
	var locales map[string]postLocale
	if err := unmarshalObject(raw, &locales); err != nil || len(locales) == 0 {
		return "", "", nil, errFeishuMessageContent
	}
	localeNames := make([]string, 0, len(locales))
	for name := range locales {
		localeNames = append(localeNames, name)
	}
	sort.Strings(localeNames)
	selected := localeNames[0]
	for _, preferred := range []string{"zh_cn", "en_us"} {
		if _, ok := locales[preferred]; ok {
			selected = preferred
			break
		}
	}
	locale := locales[selected]
	var textParts []string
	if locale.Title != "" {
		textParts = append(textParts, locale.Title)
	}
	var media []channels.ProviderMediaRef
	for _, line := range locale.Content {
		for _, element := range line {
			switch element.Tag {
			case "img":
				if !isNormalizedID(element.ImageKey) {
					return "", "", nil, errFeishuMessageContent
				}
				media = append(media, channels.ProviderMediaRef{Kind: channels.MessageTypeImage, Reference: element.ImageKey})
			case "text", "a", "at", "emoji":
				value := element.Text
				if value == "" {
					value = element.UserName
				}
				if value != "" {
					textParts = append(textParts, value)
				}
			}
		}
	}
	text := strings.Join(textParts, "\n")
	if text == "" && len(media) == 0 {
		return "", "", nil, errFeishuMessageContent
	}
	if len(media) == 0 {
		return channels.MessageTypeText, text, nil, nil
	}
	if text == "" && len(media) == 1 {
		return channels.MessageTypeImage, text, media, nil
	}
	return channels.MessageTypeMixed, text, media, nil
}

func unmarshalObject(raw string, target any) error {
	if strings.TrimSpace(raw) == "" {
		return errFeishuMessageContent
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	var object map[string]json.RawMessage
	if err := decoder.Decode(&object); err != nil || object == nil {
		return errFeishuMessageContent
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errFeishuMessageContent
	}
	if err := json.Unmarshal([]byte(raw), target); err != nil {
		return errFeishuMessageContent
	}
	return nil
}

func providerTimestamp(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	millis, err := strconv.ParseInt(value, 10, 64)
	if err != nil || millis < 0 {
		return time.Time{}, errFeishuEvent
	}
	if millis == 0 {
		return time.Time{}, nil
	}
	return time.UnixMilli(millis).UTC(), nil
}

func providerTarget(kind, id string) string {
	return kind + ":" + id
}

func parseProviderTarget(value string) (string, string, error) {
	kind, id, ok := strings.Cut(value, ":")
	if !ok || kind == "" || id == "" || strings.Contains(id, ":") {
		return "", "", errFeishuUnsupportedTarget
	}
	switch kind {
	case feishuTargetUser, feishuTargetConversation, feishuTargetTopic, feishuTargetMessage:
	default:
		return "", "", errFeishuUnsupportedTarget
	}
	normalized, err := channels.NormalizeExternalID(id)
	if err != nil || normalized != id || !utf8.ValidString(id) {
		return "", "", errFeishuUnsupportedTarget
	}
	return kind, id, nil
}

func isNormalizedID(value string) bool {
	normalized, err := channels.NormalizeExternalID(value)
	return err == nil && normalized == value
}

func valueOf(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
