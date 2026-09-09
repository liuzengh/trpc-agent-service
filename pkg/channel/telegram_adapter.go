//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package channel

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

// TelegramAdapter implements the Adapter interface for Telegram.
type TelegramAdapter struct {
	client *http.Client
}

// TelegramUpdate represents a Telegram webhook update.
type TelegramUpdate struct {
	UpdateID int              `json:"update_id"`
	Message  *TelegramMessage `json:"message,omitempty"`
}

// TelegramMessage is the text-message subset installed by this Adapter.
type TelegramMessage struct {
	MessageID int           `json:"message_id"`
	From      *TelegramUser `json:"from,omitempty"`
	Chat      TelegramChat  `json:"chat"`
	Date      int           `json:"date"`
	Text      string        `json:"text"`
}

// TelegramUser identifies the sender of a supported text message.
type TelegramUser struct {
	ID        int64  `json:"id"`
	FirstName string `json:"first_name"`
	Username  string `json:"username"`
}

// TelegramChat identifies a Telegram conversation.
type TelegramChat struct {
	ID   int64  `json:"id"`
	Type string `json:"type"` // private, group, supergroup
}

type telegramAPIResult struct {
	OK          bool   `json:"ok"`
	ErrorCode   int    `json:"error_code"`
	Description string `json:"description"`
	Parameters  struct {
		RetryAfter int `json:"retry_after"`
	} `json:"parameters"`
}

const (
	maxTelegramResponseBytes     = 64 << 10
	maxTelegramRetryAfterSeconds = int64((1<<63 - 1) / int64(time.Second))
)

// telegramRetryAfter converts the provider's integer retry hint without
// allowing malformed negative values or an oversized value to wrap a
// time.Duration. A non-positive hint is treated as absent; the delivery
// pipeline then applies its own bounded exponential backoff.
func telegramRetryAfter(seconds int) time.Duration {
	if seconds <= 0 {
		return 0
	}
	value := int64(seconds)
	if value > maxTelegramRetryAfterSeconds {
		value = maxTelegramRetryAfterSeconds
	}
	return time.Duration(value) * time.Second
}

// NewTelegramAdapter creates a new Telegram adapter.
func NewTelegramAdapter() *TelegramAdapter {
	return &TelegramAdapter{
		client: &http.Client{
			Timeout:       10 * time.Second,
			CheckRedirect: providerRedirectPolicy("api.telegram.org"),
		},
	}
}

// VerifySignature validates the Telegram webhook secret token.
func (a *TelegramAdapter) VerifySignature(req *http.Request, binding *tenant.ChannelBinding) error {
	// Fail closed on an unconfigured secret. Without this an empty binding
	// secret compares equal to a missing header, so a misconfigured tenant
	// would silently accept unauthenticated webhooks.
	if req == nil || binding == nil || !tenant.IsValidTelegramWebhookSecret(binding.Secret) {
		return fmt.Errorf("channel binding has no valid webhook secret configured")
	}

	values := req.Header.Values("X-Telegram-Bot-Api-Secret-Token")
	if len(values) != 1 {
		return fmt.Errorf("invalid secret token")
	}
	secretToken := values[0]
	if subtle.ConstantTimeCompare([]byte(secretToken), []byte(binding.Secret)) != 1 {
		return fmt.Errorf("invalid secret token")
	}
	return nil
}

// ParseInbound converts Telegram webhook to InboundMessage.
func (a *TelegramAdapter) ParseInbound(req *http.Request, binding *tenant.ChannelBinding) (*InboundMessage, error) {
	body, err := readAdapterInboundBody(req, false)
	if err != nil {
		return nil, err
	}
	var update TelegramUpdate
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&update); err != nil {
		return nil, fmt.Errorf("failed to decode update: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("telegram update must contain exactly one JSON value")
	}
	if update.Message == nil {
		return nil, fmt.Errorf("%w: update has no supported message", ErrIgnoredInbound)
	}
	message := update.Message
	if message.Text == "" || message.From == nil {
		return nil, fmt.Errorf("%w: message is not a supported user text message", ErrIgnoredInbound)
	}
	if message.Chat.Type != "private" && message.Chat.Type != "group" && message.Chat.Type != "supergroup" {
		return nil, fmt.Errorf("%w: chat type %q is not supported", ErrIgnoredInbound, message.Chat.Type)
	}
	if update.UpdateID < 0 || message.MessageID <= 0 || message.From.ID == 0 || message.Chat.ID == 0 ||
		message.Date <= 0 || !utf8.ValidString(message.Text) {
		return nil, fmt.Errorf("telegram text message is missing required routing fields")
	}

	isGroupChat := message.Chat.Type == "group" || message.Chat.Type == "supergroup"
	messageID := strconv.Itoa(update.UpdateID)
	if update.UpdateID == 0 {
		// Telegram message_id is only unique within a chat.
		messageID = strconv.FormatInt(message.Chat.ID, 10) + ":" + strconv.Itoa(message.MessageID)
	}

	msg := &InboundMessage{
		ChannelType:    string(ChannelTypeTelegram),
		ExternalUserID: strconv.FormatInt(message.From.ID, 10),
		ConversationID: strconv.FormatInt(message.Chat.ID, 10),
		MessageID:      messageID,
		ReplyToID:      strconv.Itoa(message.MessageID),
		Content:        message.Text,
		Timestamp:      time.Unix(int64(message.Date), 0),
		IsGroupChat:    isGroupChat,
		Metadata: map[string]string{
			"username":            message.From.Username,
			"chat_type":           message.Chat.Type,
			"provider_message_id": strconv.Itoa(message.MessageID),
		},
	}

	return msg, nil
}

// SendReply sends a reply to Telegram.
func (a *TelegramAdapter) SendReply(ctx context.Context, binding *tenant.ChannelBinding, msg *OutboundMessage) error {
	if a == nil || a.client == nil {
		return retryableTransportError("Telegram")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if msg == nil || msg.ConversationID == "" || !utf8.ValidString(msg.Content) {
		return invalidOutboundMessageError("Telegram")
	}
	if err := validateTelegramDeliveryBinding(binding); err != nil {
		return permanentCredentialError("Telegram")
	}
	endpoint := telegramEndpoint(binding.Token, "sendMessage")
	cursor, err := OutboundDeliveryCursor(msg)
	if err != nil {
		return PermanentDeliveryError(err)
	}
	chunks := splitMessageByRunes(msg.Content, 4096)
	if cursor >= len(chunks) {
		return PermanentDeliveryError(fmt.Errorf("telegram delivery cursor %d exceeds %d chunks", cursor, len(chunks)))
	}
	payload := map[string]interface{}{"chat_id": msg.ConversationID, "text": chunks[cursor]}
	if msg.ContentType == "markdown" {
		payload["parse_mode"] = "Markdown"
	}
	if cursor == 0 && msg.ReplyToID != "" {
		replyID, ok := parseTelegramMessageID(msg.ReplyToID)
		if !ok {
			return invalidOutboundMessageError("Telegram reply target")
		}
		payload["reply_parameters"] = map[string]interface{}{"message_id": replyID}
	}
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return PermanentDeliveryError(fmt.Errorf("failed to marshal payload: %w", err))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return permanentRequestBuildError("Telegram")
	}
	req.Header.Set("Content-Type", "application/json")
	if err := setDeliveryIdempotencyKey(req, msg); err != nil {
		return PermanentDeliveryError(err)
	}
	telemetry.InjectHTTP(ctx, req)
	resp, requestWritten, err := doProviderRequest(a.client, req)
	if err != nil {
		// Telegram's sendMessage API has no provider-side idempotency. Only a
		// transport failure after net/http entered the write boundary is
		// outcome-unknown; DNS/connect failures before that boundary are safe to
		// retry automatically.
		return providerTransportFailure("Telegram", requestWritten)
	}
	if resp == nil || resp.Body == nil {
		return providerTransportFailure("Telegram", requestWritten)
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxTelegramResponseBytes+1))
	_ = resp.Body.Close()
	if readErr != nil {
		return UnknownDeliveryError(retryableTransportError("Telegram"))
	}
	if len(body) > maxTelegramResponseBytes {
		return UnknownDeliveryError(retryableTransportError("Telegram"))
	}
	var result telegramAPIResult
	if err := json.Unmarshal(body, &result); err != nil {
		return UnknownDeliveryError(retryableTransportError("Telegram"))
	}
	if resp.StatusCode == http.StatusTooManyRequests || result.ErrorCode == http.StatusTooManyRequests {
		delay := telegramRetryAfter(result.Parameters.RetryAfter)
		return RateLimitedDeliveryError(fmt.Errorf("telegram rate limit: status=%d code=%d", resp.StatusCode, result.ErrorCode), delay)
	}
	if resp.StatusCode >= 500 || resp.StatusCode == http.StatusRequestTimeout {
		// A 5xx/408 response is not a durable rejection signal. Telegram may
		// have committed the message before failing the HTTP request.
		return UnknownDeliveryError(retryableTransportError("Telegram"))
	}
	if resp.StatusCode >= 400 || !result.OK {
		return PermanentDeliveryError(fmt.Errorf("telegram API error status=%d code=%d", resp.StatusCode, result.ErrorCode))
	}
	SetOutboundDeliveryProgress(msg, cursor+1, cursor+1 >= len(chunks))
	return nil
}

// SendStreamChunk updates a message with streaming content.
func (a *TelegramAdapter) SendStreamChunk(ctx context.Context, binding *tenant.ChannelBinding, chunk *StreamChunk) error {
	if a == nil || a.client == nil {
		return retryableTransportError("Telegram")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateTelegramDeliveryBinding(binding); err != nil {
		return permanentCredentialError("Telegram")
	}
	if chunk == nil || chunk.ConversationID == "" || chunk.MessageID == "" ||
		!utf8.ValidString(chunk.Content) || len([]rune(chunk.Content)) > 4096 {
		return invalidOutboundMessageError("Telegram")
	}
	endpoint := telegramEndpoint(binding.Token, "editMessageText")

	payload := map[string]interface{}{
		"chat_id": chunk.ConversationID,
		"text":    chunk.Content,
	}
	messageID, ok := parseTelegramMessageID(chunk.MessageID)
	if !ok {
		return invalidOutboundMessageError("Telegram stream target")
	}
	payload["message_id"] = messageID

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return permanentRequestBuildError("Telegram")
	}
	req.Header.Set("Content-Type", "application/json")
	if err := setStreamDeliveryIdempotencyKey(req, chunk); err != nil {
		return PermanentDeliveryError(err)
	}
	telemetry.InjectHTTP(ctx, req)
	resp, requestWritten, err := doProviderRequest(a.client, req)
	if err != nil {
		return providerTransportFailure("Telegram", requestWritten)
	}
	if resp == nil || resp.Body == nil {
		return providerTransportFailure("Telegram", requestWritten)
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxTelegramResponseBytes+1))
	_ = resp.Body.Close()
	if readErr != nil {
		return UnknownDeliveryError(retryableTransportError("Telegram"))
	}
	if len(body) > maxTelegramResponseBytes {
		return UnknownDeliveryError(retryableTransportError("Telegram"))
	}
	var result telegramAPIResult
	if err := json.Unmarshal(body, &result); err != nil {
		return UnknownDeliveryError(retryableTransportError("Telegram"))
	}
	if resp.StatusCode == http.StatusTooManyRequests || result.ErrorCode == http.StatusTooManyRequests {
		delay := telegramRetryAfter(result.Parameters.RetryAfter)
		return RateLimitedDeliveryError(fmt.Errorf("telegram stream rate limit: status=%d code=%d", resp.StatusCode, result.ErrorCode), delay)
	}
	// Telegram documents this specific 400 as an idempotent success: the
	// requested content already matches the provider message. Other 4xx
	// responses must remain visible as permanent delivery failures; treating
	// them as success would advance the durable delivery cursor and lose the
	// reply permanently.
	if resp.StatusCode == http.StatusBadRequest &&
		strings.Contains(strings.ToLower(result.Description), "message is not modified") {
		return nil
	}
	if resp.StatusCode >= 500 || resp.StatusCode == http.StatusRequestTimeout {
		return RetryableDeliveryError(fmt.Errorf("telegram stream API error status=%d code=%d", resp.StatusCode, result.ErrorCode))
	}
	if resp.StatusCode >= 400 || !result.OK {
		return PermanentDeliveryError(fmt.Errorf("telegram stream API error status=%d code=%d", resp.StatusCode, result.ErrorCode))
	}
	return nil
}

func telegramEndpoint(token, method string) string {
	return (&url.URL{
		Scheme: "https",
		Host:   "api.telegram.org",
		Path:   "/bot" + token + "/" + method,
	}).String()
}

func parseTelegramMessageID(value string) (int64, bool) {
	if value == "" {
		return 0, false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return 0, false
		}
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	return parsed, err == nil && parsed > 0
}

// SupportsStreaming returns true for Telegram.
func (a *TelegramAdapter) SupportsStreaming() bool {
	return true
}

// HandleRateLimit implements rate limit backoff.
func (a *TelegramAdapter) HandleRateLimit(err error) time.Duration {
	_, delay := DeliveryFailure(err)
	return delay
}
