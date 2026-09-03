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

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

const defaultAPIBase = "https://api.telegram.org"

type bindingConfig struct {
	BotTokenRef      string `json:"bot_token_ref"`
	WebhookSecretRef string `json:"webhook_secret_ref"`
	APIBaseURL       string `json:"api_base_url,omitempty"`
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
	return channels.Capabilities{MaxTextRunes: 4000, SupportsEdit: true, SupportsFile: true}
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
	webhookSecret, err := a.secrets.Resolve(ctx, cfg.WebhookSecretRef)
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
	if message == nil || message.From == nil || strings.TrimSpace(message.Text) == "" {
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
		MessageType:       "text",
		Text:              strings.TrimSpace(message.Text),
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
	MessageID       int64         `json:"message_id"`
	MessageThreadID int64         `json:"message_thread_id"`
	Date            int64         `json:"date"`
	Text            string        `json:"text"`
	From            *telegramUser `json:"from"`
	Chat            telegramChat  `json:"chat"`
}

type telegramUser struct {
	ID int64 `json:"id"`
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
	botToken, err := a.secrets.Resolve(ctx, cfg.BotTokenRef)
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
	return cfg, nil
}

func apiBase(cfg bindingConfig) string {
	if strings.TrimSpace(cfg.APIBaseURL) == "" {
		return defaultAPIBase
	}
	return strings.TrimRight(cfg.APIBaseURL, "/")
}

var _ channels.CallbackAdapter = (*Adapter)(nil)
