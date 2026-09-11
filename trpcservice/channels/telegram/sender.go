package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

const maxResponseBytes = 1 << 20

var (
	ErrInvalidTarget = errors.New("invalid telegram reply target")
	ErrSendRejected  = errors.New("telegram send rejected")
)

type Sender struct {
	apiBase string
	client  *http.Client
}

func NewSender(botToken, baseURL string, client *http.Client) (*Sender, error) {
	if strings.TrimSpace(botToken) == "" {
		return nil, fmt.Errorf("telegram bot token is required")
	}
	parsedURL, err := url.Parse(baseURL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		return nil, fmt.Errorf("telegram base URL is invalid")
	}
	if client == nil {
		return nil, fmt.Errorf("telegram HTTP client is required")
	}
	return &Sender{apiBase: strings.TrimRight(baseURL, "/") + "/bot" + url.PathEscape(botToken), client: client}, nil
}

func (s *Sender) Send(ctx context.Context, target channels.ReplyTarget, message channels.OutboundMessage) (channels.SendReceipt, error) {
	if err := validateTarget(target); err != nil {
		return channels.SendReceipt{}, err
	}
	if strings.TrimSpace(message.Text) == "" && len(message.Files) == 0 && message.Card == nil {
		return channels.SendReceipt{}, fmt.Errorf("%w: message content is required", ErrInvalidTarget)
	}
	if message.UpdateMessageID != "" {
		if len(message.Files) > 0 {
			return channels.SendReceipt{}, fmt.Errorf("%w: Telegram cannot attach a new local file while editing a message", ErrInvalidTarget)
		}
		if err := s.edit(ctx, target, message.UpdateMessageID, displayText(message), message.Card); err != nil {
			return channels.SendReceipt{}, err
		}
		return channels.SendReceipt{ExternalMessageID: message.UpdateMessageID}, nil
	}

	var receipt channels.SendReceipt
	if strings.TrimSpace(message.Text) != "" || message.Card != nil {
		id, err := s.sendText(ctx, target, displayText(message), message.Card)
		if err != nil {
			return channels.SendReceipt{}, err
		}
		receipt.ExternalMessageID = id
	}
	for _, file := range message.Files {
		id, err := s.sendFile(ctx, target, file)
		if err != nil {
			return channels.SendReceipt{}, err
		}
		if receipt.ExternalMessageID == "" {
			receipt.ExternalMessageID = id
		}
	}
	return receipt, nil
}

func (s *Sender) StartProgress(ctx context.Context, target channels.ReplyTarget) (channels.SendReceipt, error) {
	if err := validateTarget(target); err != nil {
		return channels.SendReceipt{}, err
	}
	// "正在输入"仅是体验优化，失败不应阻塞后续发送；记录以便观测。
	if actionErr := s.callBool(ctx, "sendChatAction", map[string]any{"chat_id": target.ConversationID, "action": "typing"}); actionErr != nil {
		slog.Debug("telegram sendChatAction failed",
			"conversation_id", target.ConversationID, "error", actionErr)
	}
	id, err := s.sendText(ctx, target, "正在处理…", nil)
	if err != nil {
		return channels.SendReceipt{}, err
	}
	return channels.SendReceipt{ExternalMessageID: id}, nil
}

func (s *Sender) UpdateProgress(ctx context.Context, target channels.ReplyTarget, messageID, text string) error {
	if strings.TrimSpace(text) == "" {
		return nil
	}
	return s.edit(ctx, target, messageID, text, nil)
}

func (s *Sender) DeleteMessage(ctx context.Context, target channels.ReplyTarget, messageID string) error {
	if err := validateTarget(target); err != nil {
		return err
	}
	id, err := strconv.ParseInt(strings.TrimSpace(messageID), 10, 64)
	if err != nil || id <= 0 {
		return fmt.Errorf("%w: invalid message id", ErrInvalidTarget)
	}
	return s.callBool(ctx, "deleteMessage", map[string]any{"chat_id": target.ConversationID, "message_id": id})
}

func (s *Sender) AnswerCallback(ctx context.Context, callbackID, text string) error {
	callbackID = strings.TrimSpace(callbackID)
	if callbackID == "" {
		return fmt.Errorf("telegram callback id is required")
	}
	payload := map[string]any{"callback_query_id": callbackID}
	if text = strings.TrimSpace(text); text != "" {
		payload["text"] = text
	}
	return s.callBool(ctx, "answerCallbackQuery", payload)
}

func (s *Sender) SetCommands(ctx context.Context, commands []BotCommand) error {
	if len(commands) == 0 {
		return fmt.Errorf("telegram command list is required")
	}
	return s.callBool(ctx, "setMyCommands", map[string]any{"commands": commands})
}

// SetMenuButtonCommands keeps Telegram's native menu button focused on the
// command list so users can discover /new and /help without already knowing
// those commands exist.
func (s *Sender) SetMenuButtonCommands(ctx context.Context) error {
	return s.callBool(ctx, "setChatMenuButton", map[string]any{
		"menu_button": map[string]string{"type": "commands"},
	})
}

type BotCommand struct {
	Command     string `json:"command"`
	Description string `json:"description"`
}

func DefaultCommands() []BotCommand {
	return []BotCommand{
		{Command: "start", Description: "开始使用"},
		{Command: "new", Description: "开启新会话"},
		{Command: "help", Description: "查看帮助"},
	}
}

func (s *Sender) sendText(ctx context.Context, target channels.ReplyTarget, text string, card *channels.InteractiveCard) (string, error) {
	payload := map[string]any{"chat_id": target.ConversationID, "text": text}
	if markup := telegramKeyboard(card); markup != nil {
		payload["reply_markup"] = markup
	}
	var result sendMessageResponse
	if err := s.callJSON(ctx, "sendMessage", payload, &result); err != nil {
		return "", err
	}
	if !result.OK || result.Result.MessageID <= 0 {
		return "", fmt.Errorf("%w: response did not contain a message ID", ErrSendRejected)
	}
	return strconv.FormatInt(result.Result.MessageID, 10), nil
}

func (s *Sender) edit(ctx context.Context, target channels.ReplyTarget, messageID, text string, card *channels.InteractiveCard) error {
	id, err := strconv.ParseInt(strings.TrimSpace(messageID), 10, 64)
	if err != nil || id <= 0 {
		return fmt.Errorf("%w: invalid message id", ErrInvalidTarget)
	}
	payload := map[string]any{"chat_id": target.ConversationID, "message_id": id, "text": text}
	if markup := telegramKeyboard(card); markup != nil {
		payload["reply_markup"] = markup
	}
	var response struct {
		OK bool `json:"ok"`
	}
	if err := s.callJSON(ctx, "editMessageText", payload, &response); err != nil {
		return err
	}
	if !response.OK {
		return fmt.Errorf("%w: edit rejected", ErrSendRejected)
	}
	return nil
}

func telegramKeyboard(card *channels.InteractiveCard) any {
	if card == nil || len(card.Actions) == 0 {
		return nil
	}
	rows := make([][]map[string]string, 0, len(card.Actions))
	for _, action := range card.Actions {
		label := strings.TrimSpace(action.Label)
		if label == "" {
			continue
		}
		button := map[string]string{"text": label}
		if urlValue := strings.TrimSpace(action.URL); urlValue != "" {
			button["url"] = urlValue
		} else if actionID := strings.TrimSpace(action.ActionID); actionID != "" {
			button["callback_data"] = actionID
		} else {
			continue
		}
		rows = append(rows, []map[string]string{button})
	}
	if len(rows) == 0 {
		return nil
	}
	return map[string]any{"inline_keyboard": rows}
}

func displayText(message channels.OutboundMessage) string {
	if message.Card == nil {
		return message.Text
	}
	parts := make([]string, 0, 3)
	if title := strings.TrimSpace(message.Card.Title); title != "" {
		parts = append(parts, title)
	}
	if body := strings.TrimSpace(message.Card.Body); body != "" {
		parts = append(parts, body)
	}
	if text := strings.TrimSpace(message.Text); text != "" && text != strings.TrimSpace(message.Card.Body) {
		parts = append(parts, text)
	}
	return strings.Join(parts, "\n\n")
}

func (s *Sender) sendFile(ctx context.Context, target channels.ReplyTarget, file channels.OutboundFile) (string, error) {
	path := strings.TrimSpace(file.Path)
	if path == "" {
		return "", fmt.Errorf("%w: outbound file path is required", ErrInvalidTarget)
	}
	opened, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open telegram outbound file: %w", err)
	}
	defer opened.Close()
	name := strings.TrimSpace(file.Name)
	if name == "" {
		name = filepath.Base(path)
	}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	_ = writer.WriteField("chat_id", target.ConversationID)
	part, err := writer.CreateFormFile("document", name)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(part, opened); err != nil {
		return "", err
	}
	if err := writer.Close(); err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.apiBase+"/sendDocument", &body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	var response sendMessageResponse
	if err := s.do(req, &response); err != nil {
		return "", err
	}
	if !response.OK || response.Result.MessageID <= 0 {
		return "", fmt.Errorf("%w: file response did not contain a message ID", ErrSendRejected)
	}
	return strconv.FormatInt(response.Result.MessageID, 10), nil
}

func validateTarget(target channels.ReplyTarget) error {
	if target.Channel != channels.Telegram || strings.TrimSpace(target.ConversationID) == "" {
		return fmt.Errorf("%w: channel or conversation is invalid", ErrInvalidTarget)
	}
	return nil
}

func (s *Sender) callBool(ctx context.Context, method string, payload any) error {
	var response struct {
		OK bool `json:"ok"`
	}
	if err := s.callJSON(ctx, method, payload, &response); err != nil {
		return err
	}
	if !response.OK {
		return fmt.Errorf("%w: %s rejected", ErrSendRejected, method)
	}
	return nil
}

func (s *Sender) callJSON(ctx context.Context, method string, payload any, target any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal telegram %s request: %w", method, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.apiBase+"/"+method, bytes.NewReader(encoded))
	if err != nil {
		return fmt.Errorf("create telegram %s request: %w", method, err)
	}
	req.Header.Set("Content-Type", "application/json")
	return s.do(req, target)
}

func (s *Sender) do(request *http.Request, target any) error {
	response, err := s.client.Do(request)
	if err != nil {
		return fmt.Errorf("send telegram request: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("read telegram response: %w", err)
	}
	if len(body) > maxResponseBytes {
		return fmt.Errorf("%w: response body too large", ErrSendRejected)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("%w: HTTP status %d", ErrSendRejected, response.StatusCode)
	}
	if target != nil && json.Unmarshal(body, target) != nil {
		return fmt.Errorf("%w: invalid response body", ErrSendRejected)
	}
	return nil
}

type sendMessageResponse struct {
	OK     bool `json:"ok"`
	Result struct {
		MessageID int64 `json:"message_id"`
	} `json:"result"`
}

var _ channels.Sender = (*Sender)(nil)
var _ channels.ProgressSender = (*Sender)(nil)
