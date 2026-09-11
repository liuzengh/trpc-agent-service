package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/safego"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/redis/go-redis/v9"
)

const imProgressRedisChannel = "trpc:im-progress:v1"

// IMProgressEvent is a best-effort live projection of an in-flight Agent
// response. It deliberately carries only routing metadata and the current full
// response text; the durable final response remains the PostgreSQL Outbox.
type IMProgressEvent struct {
	TenantID           string                     `json:"tenant_id"`
	AppCode            string                     `json:"app_code"`
	ConfigVersion      uint64                     `json:"config_version"`
	Channel            channels.Channel           `json:"channel"`
	BindingID          string                     `json:"binding_id"`
	ConversationID     string                     `json:"conversation_id"`
	ConversationScope  channels.ConversationScope `json:"conversation_scope"`
	ProviderReplyToken string                     `json:"provider_reply_token,omitempty"`
	ProgressMessageID  string                     `json:"progress_message_id"`
	RequestID          string                     `json:"request_id"`
	Content            string                     `json:"content"`
}

func (e IMProgressEvent) Validate() error {
	if strings.TrimSpace(e.TenantID) == "" || strings.TrimSpace(e.AppCode) == "" || e.ConfigVersion == 0 ||
		!e.Channel.Supported() || e.Channel == channels.Web || strings.TrimSpace(e.BindingID) == "" ||
		strings.TrimSpace(e.ConversationID) == "" || strings.TrimSpace(e.ProgressMessageID) == "" ||
		strings.TrimSpace(e.RequestID) == "" || strings.TrimSpace(e.Content) == "" {
		return fmt.Errorf("invalid IM progress event")
	}
	if e.ConversationScope != channels.ConversationDirect && e.ConversationScope != channels.ConversationGroup {
		return fmt.Errorf("invalid IM progress conversation scope %q", e.ConversationScope)
	}
	return nil
}

type IMProgressPublisher interface {
	PublishProgress(context.Context, IMProgressEvent) error
}

type RedisIMProgressHub struct{ client redis.UniversalClient }

func NewRedisIMProgressHub(client redis.UniversalClient) (*RedisIMProgressHub, error) {
	if client == nil {
		return nil, fmt.Errorf("IM progress Redis client is required")
	}
	return &RedisIMProgressHub{client: client}, nil
}

func (h *RedisIMProgressHub) PublishProgress(ctx context.Context, event IMProgressEvent) error {
	if err := event.Validate(); err != nil {
		return err
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode IM progress event: %w", err)
	}
	if err := h.client.Publish(ctx, imProgressRedisChannel, payload).Err(); err != nil {
		return fmt.Errorf("publish IM progress event: %w", err)
	}
	return nil
}

// Subscribe returns a process-local best-effort feed. Redis Pub/Sub is
// intentional here: losing live progress is safe because final delivery uses
// the durable Outbox.
func (h *RedisIMProgressHub) Subscribe(ctx context.Context) (<-chan IMProgressEvent, func(), error) {
	pubsub := h.client.Subscribe(ctx, imProgressRedisChannel)
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		return nil, nil, fmt.Errorf("subscribe IM progress: %w", err)
	}
	output := make(chan IMProgressEvent, 64)
	streamCtx, cancel := context.WithCancel(ctx)
	var once sync.Once
	closeFn := func() {
		once.Do(func() {
			cancel()
			_ = pubsub.Close()
		})
	}
	safego.Go("IM progress subscriber", func() {
		defer close(output)
		defer closeFn()
		for {
			select {
			case <-streamCtx.Done():
				return
			case message, ok := <-pubsub.Channel():
				if !ok {
					return
				}
				var event IMProgressEvent
				if json.Unmarshal([]byte(message.Payload), &event) != nil || event.Validate() != nil {
					continue
				}
				select {
				case output <- event:
				case <-streamCtx.Done():
					return
				}
			}
		}
	})
	return output, closeFn, nil
}

var _ IMProgressPublisher = (*RedisIMProgressHub)(nil)
