package web

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/safego"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/redis/go-redis/v9"
)

const (
	webReplyStreamTTL   = 15 * time.Minute
	webReplyStreamBlock = time.Second
)

// WebStreamEvent is one replayable browser reply event. ID is the Redis Stream
// entry ID and is emitted as the SSE id so reconnects can resume after it.
type WebStreamEvent struct {
	ID        string
	Type      string
	Content   string
	Reply     string
	Code      string
	Message   string
	Card      *channels.InteractiveCard
	Artifacts []channels.OutboundArtifact
}

// WebReplySubscriber is the narrow replayable stream seam used by the HTTP
// handler. afterID is the last SSE event ID already observed by the browser;
// an empty value replays from the beginning of the short-lived event log.
type WebReplySubscriber interface {
	Subscribe(context.Context, string, string, string, string) (<-chan WebStreamEvent, func(), error)
}

// WebReplyDeltaPublisher publishes live model tokens for a web request. It is
// deliberately best-effort in Runtime: the final PostgreSQL Outbox reply
// remains the durable result if Redis is temporarily unavailable.
type WebReplyDeltaPublisher interface {
	PublishDelta(context.Context, string, string, string, string) error
}

// WebReplyFailurePublisher publishes one terminal, user-safe failure for a
// browser request after the worker has stopped retrying it.
type WebReplyFailurePublisher interface {
	PublishFailure(context.Context, string, string, string, string, string) error
}

// RedisReplyHub is the Web channel Sender, delta publisher, and replayable
// Redis Streams fanout. The durable Outbox remains the source of truth for the
// completed reply; the Redis stream provides short-lived delta replay.
type RedisReplyHub struct{ client redis.UniversalClient }

func NewRedisReplyHub(client redis.UniversalClient) (*RedisReplyHub, error) {
	if client == nil {
		return nil, fmt.Errorf("Redis client is required")
	}
	return &RedisReplyHub{client: client}, nil
}

func (h *RedisReplyHub) Send(ctx context.Context, target channels.ReplyTarget, message channels.OutboundMessage) (channels.SendReceipt, error) {
	if target.Channel != channels.Web || strings.TrimSpace(target.TenantID) == "" || strings.TrimSpace(target.WebOwnerID) == "" || strings.TrimSpace(message.IdempotencyKey) == "" ||
		(strings.TrimSpace(message.Text) == "" && message.Card == nil && len(message.Artifacts) == 0) {
		return channels.SendReceipt{}, fmt.Errorf("invalid web reply")
	}
	eventID, err := h.publishEvent(ctx, target.TenantID, target.WebOwnerID, message.IdempotencyKey, WebStreamEvent{
		Type: "done", Reply: message.Text, Card: message.Card, Artifacts: append([]channels.OutboundArtifact(nil), message.Artifacts...),
	})
	if err != nil {
		return channels.SendReceipt{}, fmt.Errorf("publish web reply: %w", err)
	}
	return channels.SendReceipt{ExternalMessageID: eventID}, nil
}

func (h *RedisReplyHub) PublishDelta(ctx context.Context, tenantID, ownerID, requestID, content string) error {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(ownerID) == "" || strings.TrimSpace(requestID) == "" || content == "" {
		return fmt.Errorf("web delta tenant, owner, request ID and content are required")
	}
	if _, err := h.publishEvent(ctx, tenantID, ownerID, requestID, WebStreamEvent{Type: "delta", Content: content}); err != nil {
		return fmt.Errorf("publish web reply delta: %w", err)
	}
	return nil
}

func (h *RedisReplyHub) PublishFailure(ctx context.Context, tenantID, ownerID, requestID, code, message string) error {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(ownerID) == "" || strings.TrimSpace(requestID) == "" || strings.TrimSpace(message) == "" {
		return fmt.Errorf("web failure tenant, owner, request ID and message are required")
	}
	if _, err := h.publishEvent(ctx, tenantID, ownerID, requestID, WebStreamEvent{
		Type: "error", Code: strings.TrimSpace(code), Message: strings.TrimSpace(message),
	}); err != nil {
		return fmt.Errorf("publish web reply failure: %w", err)
	}
	return nil
}

// NotifyPendingApproval publishes a non-terminal card into the browser's
// existing chat stream. IM approvals remain owned by the Channel reconciler.
func (h *RedisReplyHub) NotifyPendingApproval(ctx context.Context, approval governance.PendingApproval) (string, error) {
	if approval.Channel != string(channels.Web) {
		return "", nil
	}
	if strings.TrimSpace(approval.TenantID) == "" || strings.TrimSpace(approval.RequesterUserID) == "" || strings.TrimSpace(approval.RequestID) == "" {
		return "", fmt.Errorf("web approval routing is incomplete")
	}
	card := governance.ApprovalPromptCard(approval)
	eventID, err := h.PublishApprovalCard(ctx, approval.TenantID, approval.RequesterUserID, approval.RequestID, card)
	if err != nil {
		return "", err
	}
	return eventID, nil
}

func (h *RedisReplyHub) PublishApprovalCard(ctx context.Context, tenantID, ownerID, requestID string, card channels.InteractiveCard) (string, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(ownerID) == "" || strings.TrimSpace(requestID) == "" || strings.TrimSpace(card.Body) == "" {
		return "", fmt.Errorf("web approval card routing and body are required")
	}
	eventID, err := h.publishEvent(ctx, tenantID, ownerID, requestID, WebStreamEvent{Type: "card", Card: &card})
	if err != nil {
		return "", fmt.Errorf("publish web approval card: %w", err)
	}
	return eventID, nil
}

func (h *RedisReplyHub) Subscribe(ctx context.Context, tenantID, ownerID, requestID, afterID string) (<-chan WebStreamEvent, func(), error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(ownerID) == "" || strings.TrimSpace(requestID) == "" {
		return nil, nil, fmt.Errorf("web reply tenant, owner and request ID are required")
	}
	if afterID == "" {
		afterID = "0-0"
	}
	if !validStreamID(afterID) {
		return nil, nil, fmt.Errorf("invalid web reply event ID")
	}

	streamContext, cancel := context.WithCancel(ctx)
	output := make(chan WebStreamEvent, 32)
	safego.Go("web reply subscriber", func() {
		defer close(output)
		defer cancel()
		lastID := afterID
		for {
			streams, err := h.client.XRead(streamContext, &redis.XReadArgs{
				Streams: []string{webReplyStreamKey(tenantID, ownerID, requestID), lastID},
				Count:   100,
				Block:   webReplyStreamBlock,
			}).Result()
			if err == redis.Nil {
				continue
			}
			if err != nil {
				return
			}
			for _, stream := range streams {
				for _, message := range stream.Messages {
					event, decodeErr := decodeWebStreamEvent(message)
					if decodeErr != nil {
						continue
					}
					lastID = event.ID
					select {
					case <-streamContext.Done():
						return
					case output <- event:
					}
				}
			}
		}
	})
	return output, cancel, nil
}

func (h *RedisReplyHub) publishEvent(ctx context.Context, tenantID, ownerID, requestID string, event WebStreamEvent) (string, error) {
	values := map[string]any{"type": event.Type}
	if event.Content != "" {
		values["content"] = event.Content
	}
	if event.Reply != "" {
		values["reply"] = event.Reply
	}
	if event.Code != "" {
		values["code"] = event.Code
	}
	if event.Message != "" {
		values["message"] = event.Message
	}
	if event.Card != nil {
		encoded, err := json.Marshal(event.Card)
		if err != nil {
			return "", fmt.Errorf("encode web reply card: %w", err)
		}
		values["card"] = string(encoded)
	}
	if len(event.Artifacts) > 0 {
		encoded, err := json.Marshal(event.Artifacts)
		if err != nil {
			return "", fmt.Errorf("encode web reply artifacts: %w", err)
		}
		values["artifacts"] = string(encoded)
	}
	streamKey := webReplyStreamKey(tenantID, ownerID, requestID)
	eventID, err := h.client.XAdd(ctx, &redis.XAddArgs{Stream: streamKey, Values: values}).Result()
	if err != nil {
		return "", err
	}
	// TTL is best effort: keeping an entry slightly longer is safe, while
	// treating an EXPIRE blip as a send failure would create duplicate done
	// events during Outbox retries.
	_ = h.client.Expire(ctx, streamKey, webReplyStreamTTL).Err()
	return eventID, nil
}

func decodeWebStreamEvent(message redis.XMessage) (WebStreamEvent, error) {
	typeValue, ok := message.Values["type"].(string)
	if !ok || (typeValue != "delta" && typeValue != "card" && typeValue != "done" && typeValue != "error") {
		return WebStreamEvent{}, fmt.Errorf("invalid web stream event")
	}
	event := WebStreamEvent{ID: message.ID, Type: typeValue}
	if content, ok := message.Values["content"].(string); ok {
		event.Content = content
	}
	if reply, ok := message.Values["reply"].(string); ok {
		event.Reply = reply
	}
	if code, ok := message.Values["code"].(string); ok {
		event.Code = code
	}
	if failure, ok := message.Values["message"].(string); ok {
		event.Message = failure
	}
	if encoded, ok := message.Values["card"].(string); ok && encoded != "" {
		var card channels.InteractiveCard
		if err := json.Unmarshal([]byte(encoded), &card); err != nil {
			return WebStreamEvent{}, fmt.Errorf("decode web reply card: %w", err)
		}
		event.Card = &card
	}
	if encoded, ok := message.Values["artifacts"].(string); ok && encoded != "" {
		if err := json.Unmarshal([]byte(encoded), &event.Artifacts); err != nil {
			return WebStreamEvent{}, fmt.Errorf("decode web reply artifacts: %w", err)
		}
	}
	if (event.Type == "delta" && event.Content == "") ||
		(event.Type == "card" && event.Card == nil) ||
		(event.Type == "done" && event.Reply == "" && event.Card == nil && len(event.Artifacts) == 0) ||
		(event.Type == "error" && strings.TrimSpace(event.Message) == "") {
		return WebStreamEvent{}, fmt.Errorf("incomplete web stream event")
	}
	return event, nil
}

func validStreamID(value string) bool {
	parts := strings.Split(value, "-")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return false
	}
	for _, part := range parts {
		for _, character := range part {
			if character < '0' || character > '9' {
				return false
			}
		}
	}
	return true
}

func webReplyStreamKey(tenantID, ownerID, requestID string) string {
	// URL-safe Base64 never contains dots, so independently encoding each
	// segment creates an unambiguous tenant/owner/request namespace.
	encode := func(value string) string { return base64.RawURLEncoding.EncodeToString([]byte(value)) }
	return "trpc-agent:web-reply-stream:" + encode(tenantID) + "." + encode(ownerID) + "." + encode(requestID)
}

var _ channels.Sender = (*RedisReplyHub)(nil)
var _ WebReplySubscriber = (*RedisReplyHub)(nil)
var _ WebReplyDeltaPublisher = (*RedisReplyHub)(nil)
var _ WebReplyFailurePublisher = (*RedisReplyHub)(nil)
var _ governance.ApprovalNotifier = (*RedisReplyHub)(nil)
