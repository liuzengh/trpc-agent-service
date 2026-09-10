package redis

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	redisclient "github.com/redis/go-redis/v9"
)

type ExecutionControlQueueConfig struct {
	Group       string
	ReadBlock   time.Duration
	ReclaimIdle time.Duration
}

type ExecutionControlQueue struct {
	client    redisclient.UniversalClient
	publisher *Publisher
	config    ExecutionControlQueueConfig
}

func NewExecutionControlQueue(client redisclient.UniversalClient, publisher *Publisher, config ExecutionControlQueueConfig) (*ExecutionControlQueue, error) {
	if client == nil || publisher == nil || !validSegment(config.Group) {
		return nil, runtime.ErrInvalidEnvelope
	}
	if config.ReadBlock <= 0 {
		config.ReadBlock = 250 * time.Millisecond
	}
	if config.ReclaimIdle <= 0 {
		config.ReclaimIdle = 30 * time.Second
	}
	return &ExecutionControlQueue{client: client, publisher: publisher, config: config}, nil
}

func (q *ExecutionControlQueue) ConsumeExecutionControl(ctx context.Context, opts relay.ExecutionControlConsumerOptions, handle func(context.Context, relay.ExecutionControlDelivery) error) error {
	if opts.ConsumerID == "" || handle == nil {
		return runtime.ErrInvalidEnvelope
	}
	if err := q.ensureGroup(ctx); err != nil {
		return err
	}
	for {
		streams, err := q.client.XReadGroup(ctx, &redisclient.XReadGroupArgs{
			Group: q.config.Group, Consumer: opts.ConsumerID,
			Streams: []string{q.publisher.ExecutionControlStream(), ">"}, Count: 1, Block: q.config.ReadBlock,
		}).Result()
		if errors.Is(err, redisclient.Nil) {
			continue
		}
		if err != nil {
			return err
		}
		for _, stream := range streams {
			for _, message := range stream.Messages {
				delivery, decodeErr := decodeExecutionControl(message)
				if decodeErr != nil {
					if err := q.deadLetter(ctx, message, decodeErr); err != nil {
						return err
					}
					continue
				}
				if err := handle(ctx, delivery); err != nil {
					return err
				}
				if err := q.AckExecutionControl(ctx, delivery); err != nil {
					return err
				}
			}
		}
	}
}

func (q *ExecutionControlQueue) AckExecutionControl(ctx context.Context, delivery relay.ExecutionControlDelivery) error {
	if delivery.ID == "" {
		return runtime.ErrInvalidEnvelope
	}
	return q.client.XAck(ctx, q.publisher.ExecutionControlStream(), q.config.Group, delivery.ID).Err()
}

func (q *ExecutionControlQueue) ReclaimExecutionControls(ctx context.Context, opts relay.ExecutionControlConsumerOptions) ([]relay.ExecutionControlDelivery, error) {
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
	messages, _, err := q.client.XAutoClaim(ctx, &redisclient.XAutoClaimArgs{
		Stream: q.publisher.ExecutionControlStream(), Group: q.config.Group, Consumer: opts.ConsumerID,
		MinIdle: q.config.ReclaimIdle, Start: "0-0", Count: int64(limit),
	}).Result()
	if errors.Is(err, redisclient.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	result := make([]relay.ExecutionControlDelivery, 0, len(messages))
	for _, message := range messages {
		delivery, decodeErr := decodeExecutionControl(message)
		if decodeErr != nil {
			if err := q.deadLetter(ctx, message, decodeErr); err != nil {
				return nil, err
			}
			continue
		}
		result = append(result, delivery)
	}
	return result, nil
}

func (q *ExecutionControlQueue) ensureGroup(ctx context.Context) error {
	err := q.client.XGroupCreateMkStream(ctx, q.publisher.ExecutionControlStream(), q.config.Group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func (q *ExecutionControlQueue) deadLetter(ctx context.Context, message redisclient.XMessage, cause error) error {
	raw, _ := executionControlValueString(message.Values["event"])
	_, err := q.client.XAdd(ctx, &redisclient.XAddArgs{Stream: q.publisher.ExecutionControlDeadLetterStream(), Values: map[string]any{
		"source_id": message.ID, "error": cause.Error(), "event": raw,
		"failed_at": time.Now().UTC().Format(time.RFC3339Nano),
	}}).Result()
	if err != nil {
		return err
	}
	return q.client.XAck(ctx, q.publisher.ExecutionControlStream(), q.config.Group, message.ID).Err()
}

func decodeExecutionControl(message redisclient.XMessage) (relay.ExecutionControlDelivery, error) {
	raw, ok := executionControlValueString(message.Values["event"])
	if !ok {
		return relay.ExecutionControlDelivery{}, runtime.ErrInvalidEnvelope
	}
	var event relay.ExecutionControlEvent
	if err := json.Unmarshal([]byte(raw), &event); err != nil {
		return relay.ExecutionControlDelivery{}, runtime.ErrInvalidEnvelope
	}
	tenantID, tenantOK := executionControlValueString(message.Values["tenant_id"])
	kind, kindOK := executionControlValueString(message.Values["kind"])
	version, versionOK := executionControlValueString(message.Values["version"])
	idempotencyKey, idempotencyOK := executionControlValueString(message.Values["idempotency_key"])
	if !tenantOK || !kindOK || !versionOK || !idempotencyOK || tenantID != event.TenantID || kind != event.Kind || version != strconv.FormatUint(event.Version, 10) || idempotencyKey != event.IdempotencyKey || event.Kind != "execution-control" || event.TenantID == "" || event.AggregateID == "" || event.IdempotencyKey == "" || event.Version < 1 || event.PayloadRef == "" {
		return relay.ExecutionControlDelivery{}, runtime.ErrVersionMismatch
	}
	return relay.ExecutionControlDelivery{ID: message.ID, Event: event}, nil
}

func executionControlValueString(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		return typed, true
	case []byte:
		return string(typed), true
	default:
		return "", false
	}
}

func (p *Publisher) ExecutionControlDeadLetterStream() string {
	return p.ExecutionControlStream() + ":dead-letter"
}

var _ relay.ExecutionControlQueue = (*ExecutionControlQueue)(nil)
