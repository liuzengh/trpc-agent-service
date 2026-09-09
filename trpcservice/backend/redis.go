package backend

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/cluster"
	redis "github.com/redis/go-redis/v9"
)

// Redis adapts go-redis to session fencing and the shared run-event bus.
type Redis struct{ client *redis.Client }

// Ping verifies that the shared coordination backend is currently reachable.
// It intentionally returns the driver error only to trusted in-process callers;
// public health handlers must replace it with a generic response.
func (backend *Redis) Ping(ctx context.Context) error {
	if backend == nil || backend.client == nil || ctx == nil {
		return errors.New("backend: Redis client and context are required")
	}
	return backend.client.Ping(ctx).Err()
}

// OpenRedis creates and verifies a Redis client from a redis:// URL.
func OpenRedis(ctx context.Context, rawURL string) (*Redis, error) {
	if ctx == nil || rawURL == "" {
		return nil, errors.New("backend: Redis context and URL are required")
	}
	options, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, errors.New("backend: invalid Redis URL")
	}
	client := redis.NewClient(options)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}
	return &Redis{client: client}, nil
}

func (backend *Redis) Eval(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	if backend == nil || backend.client == nil {
		return nil, errors.New("backend: nil Redis client")
	}
	return backend.client.Eval(ctx, script, keys, args...).Result()
}

func (backend *Redis) Publish(ctx context.Context, channel string, payload []byte) error {
	if backend == nil || backend.client == nil {
		return errors.New("backend: nil Redis client")
	}
	return backend.client.Publish(ctx, channel, payload).Err()
}

func (backend *Redis) Subscribe(ctx context.Context, channel string) (<-chan []byte, func() error, error) {
	if backend == nil || backend.client == nil || ctx == nil || channel == "" {
		return nil, nil, errors.New("backend: Redis subscription context and channel are required")
	}
	pubsub := backend.client.Subscribe(ctx, channel)
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		return nil, nil, err
	}
	output := make(chan []byte, 64)
	done := make(chan struct{})
	var once sync.Once
	closeSubscription := func() error {
		var closeErr error
		once.Do(func() {
			close(done)
			closeErr = pubsub.Close()
		})
		return closeErr
	}
	go func() {
		defer close(output)
		defer closeSubscription()
		messages := pubsub.Channel()
		for {
			select {
			case <-done:
				return
			case message, ok := <-messages:
				if !ok {
					return
				}
				payload := []byte(message.Payload)
				select {
				case output <- payload:
				case <-done:
					return
				}
			}
		}
	}()
	return output, closeSubscription, nil
}

// CreateConsumerGroup idempotently creates a durable worker consumer group.
func (backend *Redis) CreateConsumerGroup(ctx context.Context, stream, group string) error {
	if backend == nil || backend.client == nil || ctx == nil || stream == "" || group == "" {
		return errors.New("backend: Redis stream and group are required")
	}
	err := backend.client.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

// AddStream appends one opaque work payload with bounded approximate history.
func (backend *Redis) AddStream(ctx context.Context, stream string, payload []byte) error {
	if backend == nil || backend.client == nil || ctx == nil || stream == "" || len(payload) == 0 {
		return errors.New("backend: Redis stream and payload are required")
	}
	return backend.client.XAdd(ctx, &redis.XAddArgs{Stream: stream, MaxLen: 100000, Approx: true, Values: map[string]any{"payload": payload}}).Err()
}

// ReadGroup reads pending or new entries for one stable consumer slot.
func (backend *Redis) ReadGroup(ctx context.Context, stream, group, consumer, start string, count int64, block time.Duration) ([]cluster.StreamMessage, error) {
	if backend == nil || backend.client == nil || ctx == nil || stream == "" || group == "" || consumer == "" || (start != "0" && start != ">") || count <= 0 {
		return nil, errors.New("backend: complete Redis consumer group request is required")
	}
	result, err := backend.client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group, Consumer: consumer, Streams: []string{stream, start}, Count: count, Block: block}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var messages []cluster.StreamMessage
	for _, batch := range result {
		for _, item := range batch.Messages {
			value, ok := item.Values["payload"]
			if !ok {
				messages = append(messages, cluster.StreamMessage{ID: item.ID})
				continue
			}
			switch payload := value.(type) {
			case string:
				messages = append(messages, cluster.StreamMessage{ID: item.ID, Payload: []byte(payload)})
			case []byte:
				messages = append(messages, cluster.StreamMessage{ID: item.ID, Payload: append([]byte(nil), payload...)})
			default:
				messages = append(messages, cluster.StreamMessage{ID: item.ID})
			}
		}
	}
	return messages, nil
}

// AckStream removes one processed entry from the consumer pending list.
func (backend *Redis) AckStream(ctx context.Context, stream, group, id string) error {
	if backend == nil || backend.client == nil || stream == "" || group == "" || id == "" {
		return errors.New("backend: Redis stream acknowledgement is required")
	}
	return backend.client.XAck(ctx, stream, group, id).Err()
}

func (backend *Redis) Close() error {
	if backend == nil || backend.client == nil {
		return nil
	}
	return backend.client.Close()
}
