// Package telegram implements Telegram Bot webhook and sendMessage protocols.
package telegram

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
	"unicode/utf16"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

const defaultAPIBase = "https://api.telegram.org"

type bindingConfig struct {
	BotTokenRef       string  `json:"bot_token_ref"`
	WebhookSecretRef  string  `json:"webhook_secret_ref"`
	APIBaseURL        string  `json:"api_base_url,omitempty"`
	BotUserID         int64   `json:"bot_user_id,omitempty"`
	BotUsername       string  `json:"bot_username,omitempty"`
	AllowedChatIDs    []int64 `json:"allowed_chat_ids,omitempty"`
	RequireMention    bool    `json:"require_mention,omitempty"`
	IgnoreBotMessages bool    `json:"ignore_bot_messages,omitempty"`
}

type Adapter struct {
	secrets secret.Store
	client  *http.Client
}

func New(secrets secret.Store, client *http.Client) (*Adapter, error) {
	if secrets == nil {
		return nil, fmt.Errorf("Telegram secret store is required")
	}
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &Adapter{secrets: secrets, client: client}, nil
}

func (a *Adapter) Type() string { return "telegram" }

func (a *Adapter) Capabilities() channels.Capabilities {
	// Outbound delivery currently calls sendMessage only. Edited updates and
	// inbound file IDs are decoded, but editMessageText/sendDocument/sendPhoto
	// are intentionally not advertised until their send paths are implemented.
	return channels.Capabilities{MaxTextRunes: 4000}
}

func (a *Adapter) Callback(
	ctx context.Context,
	binding controlplane.ChannelBinding,
	request *http.Request,
) (channels.CallbackResult, error) {
	if request.Method != http.MethodPost {
		return channels.CallbackResult{}, fmt.Errorf("unsupported Telegram callback method")
	}
	cfg, err := parseBinding(binding)
	if err != nil {
		return channels.CallbackResult{}, err
	}
	webhookSecret, err := a.secrets.Resolve(ctx, binding.TenantID, secret.TelegramWebhook, cfg.WebhookSecretRef)
	if err != nil {
		return channels.CallbackResult{}, fmt.Errorf("resolve Telegram webhook secret: %w", err)
	}
	receivedSecret := request.Header.Get("X-Telegram-Bot-Api-Secret-Token")
	if subtle.ConstantTimeCompare([]byte(receivedSecret), []byte(webhookSecret)) != 1 {
		return channels.CallbackResult{}, fmt.Errorf("invalid Telegram webhook secret")
	}
	var update telegramUpdate
	if err := json.NewDecoder(io.LimitReader(request.Body, 2<<20)).Decode(&update); err != nil {
		return channels.CallbackResult{}, fmt.Errorf("decode Telegram update: %w", err)
	}
	result := channels.CallbackResult{
		StatusCode:  http.StatusOK,
		ContentType: "application/json",
		Body:        []byte(`{"ok":true}`),
	}
	message := update.Message
	if message == nil {
		message = update.EditedMessage
	}
	if message == nil || message.From == nil {
		return result, nil
	}
	if !acceptMessage(cfg, message) {
		return result, nil
	}
	messageType, text := normalizedTelegramMessage(message)
	if text == "" {
		return result, nil
	}
	chatType := "group"
	if message.Chat.Type == "private" {
		chatType = "direct"
	}
	targetJSON, _ := json.Marshal(telegramTarget{
		ChatID:          message.Chat.ID,
		MessageThreadID: message.MessageThreadID,
	})
	result.Messages = []channels.InboundEnvelope{{
		ExternalMessageID: strconv.FormatInt(update.UpdateID, 10),
		ExternalUserID:    strconv.FormatInt(message.From.ID, 10),
		ExternalChatID:    strconv.FormatInt(message.Chat.ID, 10),
		ExternalThreadID:  strconv.FormatInt(message.MessageThreadID, 10),
		ChatType:          chatType,
		MessageType:       messageType,
		Text:              text,
		ReplyTarget:       string(targetJSON),
		OccurredAt:        time.Unix(message.Date, 0).UTC(),
	}}
	return result, nil
}

type telegramUpdate struct {
	UpdateID      int64            `json:"update_id"`
	Message       *telegramMessage `json:"message"`
	EditedMessage *telegramMessage `json:"edited_message"`
}

type telegramMessage struct {
	MessageID       int64                   `json:"message_id"`
	MessageThreadID int64                   `json:"message_thread_id"`
	Date            int64                   `json:"date"`
	Text            string                  `json:"text"`
	Caption         string                  `json:"caption"`
	Entities        []telegramMessageEntity `json:"entities"`
	CaptionEntities []telegramMessageEntity `json:"caption_entities"`
	Photo           []telegramPhoto         `json:"photo"`
	Document        *telegramDocument       `json:"document"`
	From            *telegramUser           `json:"from"`
	Chat            telegramChat            `json:"chat"`
	ReplyToMessage  *telegramMessage        `json:"reply_to_message"`
}

type telegramMessageEntity struct {
	Type   string        `json:"type"`
	Offset int           `json:"offset"`
	Length int           `json:"length"`
	User   *telegramUser `json:"user,omitempty"`
}

type telegramPhoto struct {
	FileID string `json:"file_id"`
}

type telegramDocument struct {
	FileID   string `json:"file_id"`
	FileName string `json:"file_name"`
	MimeType string `json:"mime_type"`
}

func normalizedTelegramMessage(message *telegramMessage) (string, string) {
	if text := strings.TrimSpace(message.Text); text != "" {
		return "text", text
	}
	caption := strings.TrimSpace(message.Caption)
	if message.Document != nil && message.Document.FileID != "" {
		return "file", fmt.Sprintf(
			"[Telegram file name=%q mime=%q file_id=%q] %s",
			message.Document.FileName, message.Document.MimeType,
			message.Document.FileID, caption,
		)
	}
	if len(message.Photo) > 0 {
		photo := message.Photo[len(message.Photo)-1]
		if photo.FileID != "" {
			return "image", fmt.Sprintf("[Telegram image file_id=%q] %s", photo.FileID, caption)
		}
	}
	return "", ""
}

func acceptMessage(cfg bindingConfig, message *telegramMessage) bool {
	if message == nil || message.From == nil {
		return false
	}
	if cfg.IgnoreBotMessages && message.From.IsBot {
		return false
	}
	if message.Chat.Type == "private" {
		return true
	}
	if len(cfg.AllowedChatIDs) > 0 && !containsChatID(cfg.AllowedChatIDs, message.Chat.ID) {
		return false
	}
	if !cfg.RequireMention {
		return true
	}
	return messageAddressesBot(cfg, message)
}

func containsChatID(allowed []int64, chatID int64) bool {
	for _, item := range allowed {
		if item == chatID {
			return true
		}
	}
	return false
}

func messageAddressesBot(cfg bindingConfig, message *telegramMessage) bool {
	if message.ReplyToMessage != nil && userMatchesBot(cfg, message.ReplyToMessage.From) {
		return true
	}
	if entitiesAddressBot(cfg, message.Text, message.Entities) {
		return true
	}
	return entitiesAddressBot(cfg, message.Caption, message.CaptionEntities)
}

func entitiesAddressBot(
	cfg bindingConfig,
	text string,
	entities []telegramMessageEntity,
) bool {
	for _, entity := range entities {
		switch entity.Type {
		case "text_mention":
			if userMatchesBot(cfg, entity.User) {
				return true
			}
		case "mention":
			value, ok := telegramEntityText(text, entity.Offset, entity.Length)
			if ok && strings.EqualFold(
				strings.TrimPrefix(value, "@"), cfg.BotUsername,
			) {
				return true
			}
		case "bot_command":
			value, ok := telegramEntityText(text, entity.Offset, entity.Length)
			if !ok {
				continue
			}
			_, target, addressed := strings.Cut(value, "@")
			if addressed && strings.EqualFold(target, cfg.BotUsername) {
				return true
			}
		}
	}
	return false
}

func userMatchesBot(cfg bindingConfig, user *telegramUser) bool {
	if user == nil {
		return false
	}
	if cfg.BotUserID > 0 && user.ID == cfg.BotUserID {
		return true
	}
	return cfg.BotUsername != "" && strings.EqualFold(user.Username, cfg.BotUsername)
}

func telegramEntityText(text string, offset int, length int) (string, bool) {
	units := utf16.Encode([]rune(text))
	if offset < 0 || length <= 0 || offset > len(units) || length > len(units)-offset {
		return "", false
	}
	return string(utf16.Decode(units[offset : offset+length])), true
}

type telegramUser struct {
	ID       int64  `json:"id"`
	IsBot    bool   `json:"is_bot"`
	Username string `json:"username"`
}

type telegramChat struct {
	ID   int64  `json:"id"`
	Type string `json:"type"`
}

type telegramTarget struct {
	ChatID          int64 `json:"chat_id"`
	MessageThreadID int64 `json:"message_thread_id,omitempty"`
}

func (a *Adapter) Send(
	ctx context.Context,
	binding controlplane.ChannelBinding,
	message channels.OutboundMessage,
) (channels.DeliveryReceipt, error) {
	cfg, err := parseBinding(binding)
	if err != nil {
		return channels.DeliveryReceipt{}, err
	}
	botToken, err := a.secrets.Resolve(ctx, binding.TenantID, secret.TelegramBot, cfg.BotTokenRef)
	if err != nil {
		return channels.DeliveryReceipt{}, fmt.Errorf("resolve Telegram bot token: %w", err)
	}
	var target telegramTarget
	if err := json.Unmarshal([]byte(message.ReplyTarget), &target); err != nil || target.ChatID == 0 {
		return channels.DeliveryReceipt{}, fmt.Errorf("decode Telegram reply target")
	}
	payload := map[string]any{
		"chat_id": target.ChatID,
		"text":    message.Text,
	}
	if target.MessageThreadID != 0 {
		payload["message_thread_id"] = target.MessageThreadID
	}
	body, _ := json.Marshal(payload)
	endpoint := apiBase(cfg) + "/bot" + url.PathEscape(botToken) + "/sendMessage"
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response, err := a.client.Do(request)
	if err != nil {
		return channels.DeliveryReceipt{}, &channels.DeliveryError{Cause: err, Retryable: true}
	}
	defer response.Body.Close()
	var result struct {
		OK          bool   `json:"ok"`
		Description string `json:"description"`
		ErrorCode   int    `json:"error_code"`
		Result      struct {
			MessageID int64 `json:"message_id"`
		} `json:"result"`
		Parameters struct {
			RetryAfter int `json:"retry_after"`
		} `json:"parameters"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result); err != nil {
		return channels.DeliveryReceipt{}, &channels.DeliveryError{
			Cause: err, Retryable: response.StatusCode >= 500,
		}
	}
	if !result.OK {
		retryable := response.StatusCode >= 500 || result.ErrorCode == 429
		return channels.DeliveryReceipt{}, &channels.DeliveryError{
			Cause:      fmt.Errorf("Telegram send failed: code=%d message=%s", result.ErrorCode, result.Description),
			Retryable:  retryable,
			RetryAfter: time.Duration(result.Parameters.RetryAfter) * time.Second,
		}
	}
	return channels.DeliveryReceipt{
		ProviderMessageID: strconv.FormatInt(result.Result.MessageID, 10),
		SentAt:            time.Now().UTC(),
	}, nil
}

func parseBinding(binding controlplane.ChannelBinding) (bindingConfig, error) {
	if binding.ChannelType != "telegram" || binding.Status != controlplane.StatusActive {
		return bindingConfig{}, fmt.Errorf("Telegram binding is unavailable")
	}
	decoder := json.NewDecoder(bytes.NewReader(binding.Config))
	decoder.DisallowUnknownFields()
	var cfg bindingConfig
	if err := decoder.Decode(&cfg); err != nil {
		return bindingConfig{}, fmt.Errorf("decode Telegram binding config: %w", err)
	}
	if cfg.BotTokenRef == "" || cfg.WebhookSecretRef == "" {
		return bindingConfig{}, fmt.Errorf("Telegram binding config is incomplete")
	}
	cfg.BotUsername = strings.TrimPrefix(strings.TrimSpace(cfg.BotUsername), "@")
	if cfg.RequireMention && (cfg.BotUserID <= 0 || cfg.BotUsername == "") {
		return bindingConfig{}, fmt.Errorf(
			"Telegram mention filtering requires bot_user_id and bot_username",
		)
	}
	seenChatIDs := make(map[int64]struct{}, len(cfg.AllowedChatIDs))
	for _, chatID := range cfg.AllowedChatIDs {
		if chatID == 0 {
			return bindingConfig{}, fmt.Errorf("Telegram allowed_chat_ids contains zero")
		}
		if _, exists := seenChatIDs[chatID]; exists {
			return bindingConfig{}, fmt.Errorf("Telegram allowed_chat_ids contains duplicates")
		}
		seenChatIDs[chatID] = struct{}{}
	}
	return cfg, nil
}

func apiBase(cfg bindingConfig) string {
	if strings.TrimSpace(cfg.APIBaseURL) == "" {
		return defaultAPIBase
	}
	return strings.TrimRight(cfg.APIBaseURL, "/")
}

var _ channels.CallbackAdapter = (*Adapter)(nil)
