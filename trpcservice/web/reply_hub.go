package web

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/redis/go-redis/v9"
)

const (
	webReplyStreamTTL   = 15 * time.Minute
	webReplyStreamBlock = time.Second
)

// WebStreamEvent is one replayable browser reply event. ID is the Redis Stream
// entry ID and is emitted as the SSE id so reconnects can resume after it.
type WebStreamEvent struct {
	ID      string
	Type    string
	Content string
	Reply   string
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
	if target.Channel != channels.Web || strings.TrimSpace(target.TenantID) == "" || strings.TrimSpace(target.WebOwnerID) == "" || strings.TrimSpace(message.IdempotencyKey) == "" || strings.TrimSpace(message.Text) == "" {
		return channels.SendReceipt{}, fmt.Errorf("invalid web reply")
	}
	eventID, err := h.publishEvent(ctx, target.TenantID, target.WebOwnerID, message.IdempotencyKey, WebStreamEvent{Type: "done", Reply: message.Text})
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
	go func() {
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
	}()
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
	if !ok || (typeValue != "delta" && typeValue != "done") {
		return WebStreamEvent{}, fmt.Errorf("invalid web stream event")
	}
	event := WebStreamEvent{ID: message.ID, Type: typeValue}
	if content, ok := message.Values["content"].(string); ok {
		event.Content = content
	}
	if reply, ok := message.Values["reply"].(string); ok {
		event.Reply = reply
	}
	if (event.Type == "delta" && event.Content == "") || (event.Type == "done" && event.Reply == "") {
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
