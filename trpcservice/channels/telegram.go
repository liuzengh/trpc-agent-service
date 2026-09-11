package channels

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/delivery"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
)

const telegramSecretHeader = "X-Telegram-Bot-Api-Secret-Token"

type Telegram struct {
	client HTTPDoer
}

func NewTelegram(client HTTPDoer) *Telegram {
	if client == nil {
		client = http.DefaultClient
	}
	return &Telegram{client: client}
}

func (*Telegram) Name() string { return "telegram" }

func (*Telegram) Verify(r *http.Request, _ []byte, binding config.ChannelConfig) error {
	secret, err := config.Secret(binding.SigningSecretEnv)
	if err != nil {
		return err
	}
	got := r.Header.Get(telegramSecretHeader)
	if len(got) != len(secret) || subtle.ConstantTimeCompare([]byte(got), []byte(secret)) != 1 {
		return ErrInvalidSignature
	}
	return nil
}

type telegramUpdate struct {
	UpdateID int64 `json:"update_id"`
	Message  *struct {
		MessageID       int64  `json:"message_id"`
		MessageThreadID int64  `json:"message_thread_id"`
		Date            int64  `json:"date"`
		Text            string `json:"text"`
		Caption         string `json:"caption"`
		From            struct {
			ID int64 `json:"id"`
		} `json:"from"`
		Chat struct {
			ID   int64  `json:"id"`
			Type string `json:"type"`
		} `json:"chat"`
		Photo []struct {
			FileID string `json:"file_id"`
		} `json:"photo"`
		Document *struct {
			FileID   string `json:"file_id"`
			FileName string `json:"file_name"`
			MimeType string `json:"mime_type"`
		} `json:"document"`
	} `json:"message"`
}

func (*Telegram) Parse(body []byte, binding config.ChannelConfig) (ParsedWebhook, error) {
	var update telegramUpdate
	if err := json.Unmarshal(body, &update); err != nil {
		return ParsedWebhook{}, fmt.Errorf("decode telegram update: %w", err)
	}
	if update.Message == nil || update.Message.From.ID == 0 || update.Message.Chat.ID == 0 {
		return ParsedWebhook{}, ErrUnsupportedEvent
	}
	m := update.Message
	text := m.Text
	if text == "" {
		text = m.Caption
	}
	attachments := make([]domain.Attachment, 0, len(m.Photo)+1)
	if len(m.Photo) > 0 {
		best := m.Photo[len(m.Photo)-1]
		attachments = append(attachments, domain.Attachment{Type: "image", FileID: best.FileID})
	}
	if m.Document != nil {
		attachments = append(attachments, domain.Attachment{
			Type: "file", FileID: m.Document.FileID, Name: m.Document.FileName, MimeType: m.Document.MimeType,
		})
	}
	if strings.TrimSpace(text) == "" && len(attachments) == 0 {
		return ParsedWebhook{}, ErrUnsupportedEvent
	}
	scope := domain.ScopeGroup
	if m.Chat.Type == "private" {
		scope = domain.ScopeDirect
	}
	received := time.Unix(m.Date, 0).UTC()
	if m.Date == 0 {
		received = time.Now().UTC()
	}
	messageID := strconv.FormatInt(update.UpdateID, 10)
	if update.UpdateID == 0 {
		messageID = fmt.Sprintf("%d:%d", m.Chat.ID, m.MessageID)
	}
	threadID := ""
	if m.MessageThreadID != 0 {
		threadID = strconv.FormatInt(m.MessageThreadID, 10)
	}
	return ParsedWebhook{Messages: []domain.InboundMessage{{
		BindingID: binding.BindingID, Channel: "telegram", ExternalMessageID: messageID,
		ExternalUserID: strconv.FormatInt(m.From.ID, 10), ConversationID: strconv.FormatInt(m.Chat.ID, 10), ThreadID: threadID,
		Scope: scope, Text: text, Attachments: attachments, ReceivedAt: received,
		ReplyTarget: strconv.FormatInt(m.Chat.ID, 10),
	}}}, nil
}

func (*Telegram) Plan(binding config.ChannelConfig, msg domain.OutboundMessage) ([]delivery.Part, error) {
	limit := binding.MaxMessageLength
	if limit <= 0 {
		limit = 4096
	}
	return planRuneParts(msg, limit), nil
}

type telegramSendResponse struct {
	OK          bool   `json:"ok"`
	ErrorCode   int    `json:"error_code"`
	Description string `json:"description"`
	Parameters  struct {
		RetryAfter int64 `json:"retry_after"`
	} `json:"parameters"`
	Result struct {
		MessageID int64 `json:"message_id"`
	} `json:"result"`
}

func (t *Telegram) Deliver(ctx context.Context, binding config.ChannelConfig, request delivery.Request) delivery.Result {
	token, err := config.Secret(binding.TokenEnv)
	if err != nil {
		return delivery.Result{
			Outcome: delivery.PermanentRejected, ErrorType: "provider_auth",
			Err: errors.New("load telegram token failed"),
		}
	}
	base := strings.TrimRight(binding.APIBaseURL, "/")
	if base == "" {
		base = "https://api.telegram.org"
	}
	// The token is used only in the request URL and is never included in an error.
	url := base + "/bot" + token + "/sendMessage"
	msg := request.Message
	payload := map[string]any{"chat_id": msg.Target, "text": msg.Text}
	if threadID, parseErr := strconv.ParseInt(msg.ThreadID, 10, 64); parseErr == nil && threadID != 0 {
		payload["message_thread_id"] = threadID
	}
	exchange := postJSON(ctx, t.client, url, nil, payload)
	classified, terminal := classifyHTTP(exchange, time.Now(), "X-Request-Id")
	var response telegramSendResponse
	decodeErr := decodeJSONResponse(exchange, &response)
	if terminal {
		// A structured provider rejection can carry a more useful stable code and
		// retry delay even when the HTTP status already determines the outcome.
		if decodeErr == nil {
			if response.ErrorCode != 0 {
				classified.ProviderCode = strconv.Itoa(response.ErrorCode)
			}
			if response.Parameters.RetryAfter > 0 {
				classified.RetryAfter = largerDelay(classified.RetryAfter, secondsDuration(response.Parameters.RetryAfter))
			}
			if !response.OK && (response.ErrorCode == http.StatusTooManyRequests || response.ErrorCode >= 500) {
				classified.Outcome = delivery.RetryableNotSent
				classified.ErrorType = "provider_transient"
			}
		}
		return classified
	}
	if decodeErr != nil {
		return malformedSuccess(exchange, "X-Request-Id")
	}
	result := responseMetadata(exchange, "X-Request-Id")
	if response.OK {
		result.Outcome = delivery.Confirmed
		if response.Result.MessageID != 0 {
			result.ProviderMessageID = strconv.FormatInt(response.Result.MessageID, 10)
		}
		return result
	}
	result.ProviderCode = strconv.Itoa(response.ErrorCode)
	result.RetryAfter = largerDelay(
		parseRetryAfter(exchange.header.Get("Retry-After"), time.Now()),
		secondsDuration(response.Parameters.RetryAfter),
	)
	if response.ErrorCode == http.StatusTooManyRequests || response.ErrorCode >= 500 {
		result.Outcome = delivery.RetryableNotSent
		result.ErrorType = "provider_transient"
	} else {
		result.Outcome = delivery.PermanentRejected
		result.ErrorType = "provider_rejected"
	}
	return result
}
