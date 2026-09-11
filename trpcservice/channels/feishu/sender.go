package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	channeltypes "github.com/larksuite/channel-sdk-go/types"
	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/redis/go-redis/v9"
)

var ErrSendRejected = errors.New("feishu send rejected")

type outboundChannel interface {
	Send(context.Context, *channeltypes.SendInput) (*channeltypes.SendResult, error)
}

type Sender struct {
	channel outboundChannel
	client  *lark.Client
}

func newSender(channel outboundChannel, client *lark.Client) *Sender {
	return &Sender{channel: channel, client: client}
}

func (s *Sender) Send(ctx context.Context, target channels.ReplyTarget, message channels.OutboundMessage) (channels.SendReceipt, error) {
	if err := validateFeishuTarget(target); err != nil {
		return channels.SendReceipt{}, err
	}
	if strings.TrimSpace(message.Text) == "" && message.Card == nil && len(message.Files) == 0 {
		return channels.SendReceipt{}, fmt.Errorf("%w: message content is required", ErrSendRejected)
	}
	if message.UpdateMessageID != "" {
		if len(message.Files) > 0 {
			return channels.SendReceipt{}, fmt.Errorf("%w: files cannot be attached while updating a card", ErrSendRejected)
		}
		content, err := buildFeishuCard(message.Card, message.Text, target.ConversationScope)
		if err != nil {
			return channels.SendReceipt{}, err
		}
		if err := s.patchCard(ctx, message.UpdateMessageID, content); err != nil {
			return channels.SendReceipt{}, err
		}
		return channels.SendReceipt{ExternalMessageID: message.UpdateMessageID}, nil
	}

	var receipt channels.SendReceipt
	if message.Card != nil {
		content, err := buildFeishuCard(message.Card, message.Text, target.ConversationScope)
		if err != nil {
			return channels.SendReceipt{}, err
		}
		id, err := s.send(ctx, &channeltypes.SendInput{ReceiveID: target.ConversationID, Card: content})
		if err != nil {
			return channels.SendReceipt{}, err
		}
		receipt.ExternalMessageID = id
	} else if strings.TrimSpace(message.Text) != "" {
		id, err := s.send(ctx, &channeltypes.SendInput{ReceiveID: target.ConversationID, Markdown: message.Text})
		if err != nil {
			return channels.SendReceipt{}, err
		}
		receipt.ExternalMessageID = id
	}
	for _, file := range message.Files {
		id, err := s.sendFile(ctx, target.ConversationID, file)
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
	if err := validateFeishuTarget(target); err != nil {
		return channels.SendReceipt{}, err
	}
	content, err := buildFeishuProgressCard(target.ConversationScope)
	if err != nil {
		return channels.SendReceipt{}, err
	}
	id, err := s.send(ctx, &channeltypes.SendInput{ReceiveID: target.ConversationID, Card: content})
	if err != nil {
		return channels.SendReceipt{}, err
	}
	return channels.SendReceipt{ExternalMessageID: id}, nil
}

func (s *Sender) UpdateProgress(ctx context.Context, target channels.ReplyTarget, messageID, text string) error {
	if err := validateFeishuTarget(target); err != nil {
		return err
	}
	content, err := buildFeishuCard(&channels.InteractiveCard{Body: text, State: "running"}, "", target.ConversationScope)
	if err != nil {
		return err
	}
	return s.patchCard(ctx, messageID, content)
}

func (s *Sender) UpdateProgressCard(ctx context.Context, target channels.ReplyTarget, messageID string, card channels.InteractiveCard) error {
	if err := validateFeishuTarget(target); err != nil {
		return err
	}
	content, err := buildFeishuCard(&card, "", target.ConversationScope)
	if err != nil {
		return err
	}
	return s.patchCard(ctx, messageID, content)
}

func (s *Sender) DeleteMessage(ctx context.Context, target channels.ReplyTarget, messageID string) error {
	if err := validateFeishuTarget(target); err != nil {
		return err
	}
	if s.client == nil {
		return fmt.Errorf("%w: raw client is unavailable", ErrSendRejected)
	}
	request := larkim.NewDeleteMessageReqBuilder().MessageId(strings.TrimSpace(messageID)).Build()
	response, err := s.client.Im.V1.Message.Delete(ctx, request)
	if err != nil {
		return fmt.Errorf("%w: delete feishu message: %v", ErrSendRejected, err)
	}
	if !response.Success() {
		return fmt.Errorf("%w: feishu delete error %d: %s", ErrSendRejected, response.Code, response.Msg)
	}
	return nil
}

func (s *Sender) send(ctx context.Context, input *channeltypes.SendInput) (string, error) {
	if s.channel == nil {
		return "", fmt.Errorf("%w: channel is unavailable", ErrSendRejected)
	}
	response, err := s.channel.Send(ctx, input)
	if err != nil {
		return "", fmt.Errorf("%w: send feishu message: %v", ErrSendRejected, err)
	}
	if response == nil {
		return "", fmt.Errorf("%w: empty send result", ErrSendRejected)
	}
	messageID := strings.TrimSpace(response.MessageID)
	if messageID == "" && len(response.ChunkIDs) > 0 {
		messageID = strings.TrimSpace(response.ChunkIDs[len(response.ChunkIDs)-1])
	}
	if messageID == "" {
		return "", fmt.Errorf("%w: empty message id", ErrSendRejected)
	}
	return messageID, nil
}

func (s *Sender) patchCard(ctx context.Context, messageID, content string) error {
	if strings.TrimSpace(messageID) == "" {
		return fmt.Errorf("%w: message id is required", ErrSendRejected)
	}
	if s.client == nil {
		return fmt.Errorf("%w: raw client is unavailable", ErrSendRejected)
	}
	request := larkim.NewPatchMessageReqBuilder().MessageId(messageID).
		Body(larkim.NewPatchMessageReqBodyBuilder().Content(content).Build()).Build()
	response, err := s.client.Im.V1.Message.Patch(ctx, request)
	if err != nil {
		return fmt.Errorf("%w: patch feishu card: %v", ErrSendRejected, err)
	}
	if !response.Success() {
		return fmt.Errorf("%w: feishu patch error %d: %s", ErrSendRejected, response.Code, response.Msg)
	}
	return nil
}

func buildFeishuCard(card *channels.InteractiveCard, fallbackText string, scope channels.ConversationScope) (string, error) {
	if card == nil {
		card = &channels.InteractiveCard{Body: fallbackText}
	}
	body := strings.TrimSpace(card.Body)
	if body == "" {
		body = strings.TrimSpace(fallbackText)
	}
	if body == "" {
		return "", fmt.Errorf("%w: card body is required", ErrSendRejected)
	}
	// Feishu rejects patching a card whose schema differs from the original
	// message, so every card this Sender emits — create or update, plain or
	// interactive — must stay on card JSON 2.0.
	return buildFeishuCardV2(card, body, scope)
}

func buildFeishuCardV2(card *channels.InteractiveCard, body string, scope channels.ConversationScope) (string, error) {
	elements := []any{map[string]any{"tag": "markdown", "content": body}}
	columns := make([]any, 0, len(card.Actions))
	for _, action := range card.Actions {
		label := strings.TrimSpace(action.Label)
		actionID := strings.TrimSpace(action.ActionID)
		url := strings.TrimSpace(action.URL)
		if label == "" || (actionID == "" && url == "") {
			continue
		}
		buttonType := "default"
		switch action.Style {
		case "primary":
			buttonType = "primary_filled"
		case "danger":
			buttonType = "danger"
		}
		behavior := map[string]any{"type": "open_url", "default_url": url}
		if actionID != "" {
			behavior = map[string]any{
				"type": "callback",
				"value": map[string]string{
					"action_id":          actionID,
					"conversation_scope": string(scope),
				},
			}
		}
		button := map[string]any{
			"tag":       "button",
			"type":      buttonType,
			"size":      "medium",
			"text":      map[string]any{"tag": "plain_text", "content": label},
			"behaviors": []any{behavior},
		}
		columns = append(columns, map[string]any{
			"tag": "column", "width": "auto", "elements": []any{button},
		})
	}
	if len(columns) > 0 {
		elements = append(elements,
			map[string]any{"tag": "hr"},
			map[string]any{
				"tag": "column_set", "flex_mode": "none", "horizontal_spacing": "8px", "columns": columns,
			},
		)
	}
	payload := map[string]any{
		"schema": "2.0",
		"body":   map[string]any{"elements": elements},
	}
	if title := strings.TrimSpace(card.Title); title != "" {
		payload["header"] = map[string]any{
			"template": "grey",
			"title":    map[string]any{"tag": "plain_text", "content": title},
		}
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal feishu card v2: %w", err)
	}
	return string(encoded), nil
}

func buildFeishuProgressCard(scope channels.ConversationScope) (string, error) {
	// Keep progress on the same card-v2 markdown path as later updates. Feishu
	// rejects the legacy `note` element inside schema 2.0 cards.
	return buildFeishuCard(&channels.InteractiveCard{
		Body:  "正在回复…",
		State: "running",
	}, "", scope)
}

func (s *Sender) sendFile(ctx context.Context, conversationID string, file channels.OutboundFile) (string, error) {
	path := strings.TrimSpace(file.Path)
	if path == "" {
		return "", fmt.Errorf("%w: outbound file path is required", ErrSendRejected)
	}
	name := strings.TrimSpace(file.Name)
	if name == "" {
		name = filepath.Base(path)
	}
	return s.send(ctx, &channeltypes.SendInput{
		ReceiveID: conversationID,
		Media: &channeltypes.UploadInput{
			Kind:       feishuMediaKind(name),
			SourcePath: path,
			FileName:   name,
		},
	})
}

func feishuMediaKind(filename string) channeltypes.MediaKind {
	switch strings.ToLower(filepath.Ext(filename)) {
	case ".jpg", ".jpeg", ".png", ".gif", ".webp":
		return channeltypes.MediaKindImage
	case ".opus":
		return channeltypes.MediaKindAudio
	case ".mp4":
		return channeltypes.MediaKindVideo
	default:
		return channeltypes.MediaKindFile
	}
}

func validateFeishuTarget(target channels.ReplyTarget) error {
	if target.Channel != channels.Feishu || strings.TrimSpace(target.ConversationID) == "" {
		return fmt.Errorf("%w: channel or recipient is invalid", ErrSendRejected)
	}
	return nil
}

var _ channels.ProgressCardSender = (*Sender)(nil)

// RedisCache shares the SDK token cache across service nodes.
type RedisCache struct{ client redis.UniversalClient }

func NewRedisCache(client redis.UniversalClient) (*RedisCache, error) {
	if client == nil {
		return nil, errors.New("feishu token cache Redis client is required")
	}
	return &RedisCache{client: client}, nil
}

func (c *RedisCache) Get(ctx context.Context, key string) (string, error) {
	value, err := c.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return value, err
}

func (c *RedisCache) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	return c.client.Set(ctx, key, value, ttl).Err()
}

var _ larkcore.Cache = (*RedisCache)(nil)
var _ channels.Sender = (*Sender)(nil)
var _ channels.ProgressSender = (*Sender)(nil)
var _ channels.MessageDeleter = (*Sender)(nil)
