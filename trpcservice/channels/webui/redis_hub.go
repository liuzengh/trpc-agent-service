package webui

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Violet2314/trpc-agent-service/trpcservice/reply"
)

const redisStreamTTL = 24 * time.Hour

// RedisHub carries transient SSE events across stateless platform replicas.
// Session/Memory state remains in their dedicated shared backends.
type RedisHub struct {
	client redis.UniversalClient
}

// NewRedisHub constructs a cross-node WebUI event hub.
func NewRedisHub(client redis.UniversalClient) (*RedisHub, error) {
	if client == nil {
		return nil, errors.New("WebUI Redis client is required")
	}
	return &RedisHub{client: client}, nil
}

// Ensure atomically binds a WebUI session to its browser cookie owner.
func (h *RedisHub) Ensure(sessionID, owner string) error {
	if sessionID == "" || owner == "" {
		return errors.New("WebUI session and owner are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	created, err := h.client.SetNX(
		ctx, redisOwnerKey(sessionID), owner, redisStreamTTL,
	).Result()
	if err != nil {
		return err
	}
	if created {
		return nil
	}
	existing, err := h.client.Get(ctx, redisOwnerKey(sessionID)).Result()
	if err != nil {
		return err
	}
	if existing != owner {
		return errStreamForbidden
	}
	_ = h.client.Expire(ctx, redisOwnerKey(sessionID), redisStreamTTL).Err()
	return nil
}

// Subscribe opens Redis Pub/Sub before the debounce window flushes.
func (h *RedisHub) Subscribe(
	sessionID, owner string,
) (<-chan reply.Event, func(), error) {
	if err := h.Ensure(sessionID, owner); err != nil {
		return nil, nil, err
	}
	ctx, cancelContext := context.WithCancel(context.Background())
	pubsub := h.client.Subscribe(ctx, redisChannel(sessionID))
	if _, err := pubsub.Receive(ctx); err != nil {
		cancelContext()
		_ = pubsub.Close()
		return nil, nil, err
	}
	output := make(chan reply.Event, 128)
	messages := pubsub.Channel()
	var once sync.Once
	cancel := func() {
		once.Do(func() {
			cancelContext()
			_ = pubsub.Close()
		})
	}
	go func() {
		defer close(output)
		defer cancel()
		for {
			select {
			case message, ok := <-messages:
				if !ok {
					return
				}
				var event reply.Event
				if err := json.Unmarshal([]byte(message.Payload), &event); err != nil {
					continue
				}
				select {
				case output <- event:
				case <-ctx.Done():
					return
				}
				if event.Type == "done" {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return output, cancel, nil
}

// Publish emits one event to subscribers on any platform replica.
func (h *RedisHub) Publish(sessionID, owner string, event reply.Event) error {
	if err := h.Ensure(sessionID, owner); err != nil {
		return err
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return h.client.Publish(ctx, redisChannel(sessionID), payload).Err()
}

func redisOwnerKey(sessionID string) string {
	return "webui:owner:" + sessionID
}

func redisChannel(sessionID string) string {
	return "webui:events:" + sessionID
}

var _ StreamHub = (*RedisHub)(nil)
