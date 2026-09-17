package redis

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	redisclient "github.com/redis/go-redis/v9"
)

type WakeupQueueConfig struct {
	Group       string
	ReadBlock   time.Duration
	ReclaimIdle time.Duration
}

type WakeupQueue struct {
	client    redisclient.UniversalClient
	publisher *Publisher
	config    WakeupQueueConfig
}

var deadLetterWakeupScript = redisclient.NewScript(`
local maxlen = tonumber(ARGV[5])
if maxlen and maxlen > 0 then
  redis.call('XADD', KEYS[2], 'MAXLEN', '~', maxlen, '*',
    'source_id', ARGV[1], 'error_class', ARGV[2], 'event', ARGV[3], 'failed_at', ARGV[4])
else
  redis.call('XADD', KEYS[2], '*',
    'source_id', ARGV[1], 'error_class', ARGV[2], 'event', ARGV[3], 'failed_at', ARGV[4])
end
return redis.call('XACK', KEYS[1], ARGV[6], ARGV[1])
`)

func NewWakeupQueue(client redisclient.UniversalClient, publisher *Publisher, config WakeupQueueConfig) (*WakeupQueue, error) {
	if client == nil || publisher == nil || !validSegment(config.Group) {
		return nil, runtime.ErrInvalidEnvelope
	}
	if config.ReadBlock <= 0 {
		config.ReadBlock = 250 * time.Millisecond
	}
	if config.ReclaimIdle <= 0 {
		config.ReclaimIdle = 30 * time.Second
	}
	return &WakeupQueue{client: client, publisher: publisher, config: config}, nil
}

func (q *WakeupQueue) Consume(ctx context.Context, opts relay.WakeupConsumerOptions, handle func(context.Context, relay.WakeupDelivery) error) error {
	if opts.ConsumerID == "" || handle == nil {
		return runtime.ErrInvalidEnvelope
	}
	if err := q.ensureGroup(ctx); err != nil {
		return err
	}
	for {
		streams, err := q.client.XReadGroup(ctx, &redisclient.XReadGroupArgs{Group: q.config.Group,
			Consumer: opts.ConsumerID, Streams: []string{q.publisher.WakeupStream(), ">"}, Count: 1, Block: q.config.ReadBlock}).Result()
		if errors.Is(err, redisclient.Nil) {
			continue
		}
		if err != nil {
			return err
		}
		for _, stream := range streams {
			for _, message := range stream.Messages {
				delivery, err := decodeWakeup(message)
				if err != nil {
					if deadLetterErr := q.deadLetter(ctx, message, err); deadLetterErr != nil {
						return deadLetterErr
					}
					continue
				}
				if err := handle(ctx, delivery); err != nil {
					return err
				}
			}
		}
	}
}

func (q *WakeupQueue) AckWakeup(ctx context.Context, delivery relay.WakeupDelivery) error {
	if delivery.ID == "" {
		return runtime.ErrInvalidEnvelope
	}
	return q.client.XAck(ctx, q.publisher.WakeupStream(), q.config.Group, delivery.ID).Err()
}

func (q *WakeupQueue) ReclaimWakeups(ctx context.Context, opts relay.WakeupReclaimOptions) ([]relay.WakeupDelivery, error) {
	if opts.ConsumerID == "" {
		return nil, runtime.ErrInvalidEnvelope
	}
	if err := q.ensureGroup(ctx); err != nil {
		return nil, err
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}
	messages, _, err := q.client.XAutoClaim(ctx, &redisclient.XAutoClaimArgs{Stream: q.publisher.WakeupStream(),
		Group: q.config.Group, Consumer: opts.ConsumerID, MinIdle: q.config.ReclaimIdle, Start: "0-0", Count: int64(limit)}).Result()
	if errors.Is(err, redisclient.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	result := make([]relay.WakeupDelivery, 0, len(messages))
	for _, message := range messages {
		delivery, err := decodeWakeup(message)
		if err != nil {
			if deadLetterErr := q.deadLetter(ctx, message, err); deadLetterErr != nil {
				return nil, deadLetterErr
			}
			continue
		}
		result = append(result, delivery)
	}
	return result, nil
}

func (q *WakeupQueue) deadLetter(ctx context.Context, message redisclient.XMessage, cause error) error {
	raw, _ := wakeupValueString(message.Values["event"])
	return deadLetterWakeupScript.Run(ctx, q.client,
		[]string{q.publisher.WakeupStream(), q.publisher.WakeupDeadLetterStream()},
		message.ID, cause.Error(), raw, time.Now().UTC().Format(time.RFC3339Nano), q.publisher.config.MaxLen, q.config.Group).Err()
}

func (q *WakeupQueue) ensureGroup(ctx context.Context) error {
	err := q.client.XGroupCreateMkStream(ctx, q.publisher.WakeupStream(), q.config.Group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func decodeWakeup(message redisclient.XMessage) (relay.WakeupDelivery, error) {
	raw, ok := wakeupValueString(message.Values["event"])
	if !ok {
		return relay.WakeupDelivery{}, runtime.ErrInvalidEnvelope
	}
	var event relay.WakeupEvent
	if err := json.Unmarshal([]byte(raw), &event); err != nil {
		return relay.WakeupDelivery{}, runtime.ErrInvalidEnvelope
	}
	tenantID, ok := wakeupValueString(message.Values["tenant_id"])
	idempotencyKey, idOK := wakeupValueString(message.Values["idempotency_key"])
	schemaVersion, schemaOK := wakeupValueString(message.Values["schema_version"])
	if !ok || !idOK || !schemaOK || schemaVersion != "1" || tenantID != event.TenantID ||
		idempotencyKey != event.IdempotencyKey || event.AggregateID == "" || event.IdempotencyKey == "" || event.PayloadRef == "" {
		return relay.WakeupDelivery{}, runtime.ErrVersionMismatch
	}
	return relay.WakeupDelivery{ID: message.ID, Event: event}, nil
}

func wakeupValueString(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		return typed, true
	case []byte:
		return string(typed), true
	default:
		return "", false
	}
}

var _ relay.WakeupQueue = (*WakeupQueue)(nil)
