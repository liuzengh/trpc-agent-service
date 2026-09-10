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

type TenantControlQueueConfig struct {
	Group       string
	ReadBlock   time.Duration
	ReclaimIdle time.Duration
}

type TenantControlQueue struct {
	client    redisclient.UniversalClient
	publisher *Publisher
	config    TenantControlQueueConfig
}

func NewTenantControlQueue(client redisclient.UniversalClient, publisher *Publisher, config TenantControlQueueConfig) (*TenantControlQueue, error) {
	if client == nil || publisher == nil || !validSegment(config.Group) {
		return nil, runtime.ErrInvalidEnvelope
	}
	if config.ReadBlock <= 0 {
		config.ReadBlock = 250 * time.Millisecond
	}
	if config.ReclaimIdle <= 0 {
		config.ReclaimIdle = 30 * time.Second
	}
	return &TenantControlQueue{client: client, publisher: publisher, config: config}, nil
}

func (q *TenantControlQueue) ConsumeTenantControl(ctx context.Context, opts relay.TenantControlConsumerOptions, handle func(context.Context, relay.TenantControlDelivery) error) error {
	if opts.ConsumerID == "" || handle == nil {
		return runtime.ErrInvalidEnvelope
	}
	if err := q.ensureGroup(ctx); err != nil {
		return err
	}
	for {
		streams, err := q.client.XReadGroup(ctx, &redisclient.XReadGroupArgs{Group: q.config.Group, Consumer: opts.ConsumerID, Streams: []string{q.publisher.TenantControlStream(), ">"}, Count: 1, Block: q.config.ReadBlock}).Result()
		if errors.Is(err, redisclient.Nil) {
			continue
		}
		if err != nil {
			return err
		}
		for _, stream := range streams {
			for _, message := range stream.Messages {
				delivery, err := decodeTenantControl(message)
				if err != nil {
					if err := q.deadLetter(ctx, message, err); err != nil {
						return err
					}
					continue
				}
				if err := handle(ctx, delivery); err != nil {
					return err
				}
				if err := q.AckTenantControl(ctx, delivery); err != nil {
					return err
				}
			}
		}
	}
}

func (q *TenantControlQueue) AckTenantControl(ctx context.Context, delivery relay.TenantControlDelivery) error {
	if delivery.ID == "" {
		return runtime.ErrInvalidEnvelope
	}
	return q.client.XAck(ctx, q.publisher.TenantControlStream(), q.config.Group, delivery.ID).Err()
}

func (q *TenantControlQueue) ReclaimTenantControls(ctx context.Context, opts relay.TenantControlConsumerOptions) ([]relay.TenantControlDelivery, error) {
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
	messages, _, err := q.client.XAutoClaim(ctx, &redisclient.XAutoClaimArgs{Stream: q.publisher.TenantControlStream(), Group: q.config.Group, Consumer: opts.ConsumerID, MinIdle: q.config.ReclaimIdle, Start: "0-0", Count: int64(limit)}).Result()
	if errors.Is(err, redisclient.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	result := make([]relay.TenantControlDelivery, 0, len(messages))
	for _, message := range messages {
		delivery, err := decodeTenantControl(message)
		if err != nil {
			if err := q.deadLetter(ctx, message, err); err != nil {
				return nil, err
			}
			continue
		}
		result = append(result, delivery)
	}
	return result, nil
}

func (q *TenantControlQueue) ensureGroup(ctx context.Context) error {
	err := q.client.XGroupCreateMkStream(ctx, q.publisher.TenantControlStream(), q.config.Group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}
func (q *TenantControlQueue) deadLetter(ctx context.Context, message redisclient.XMessage, cause error) error {
	raw, _ := controlValue(message.Values["event"])
	if _, err := q.client.XAdd(ctx, &redisclient.XAddArgs{Stream: q.publisher.TenantControlDeadLetterStream(), Values: map[string]any{"source_id": message.ID, "error": cause.Error(), "event": raw, "failed_at": time.Now().UTC().Format(time.RFC3339Nano)}}).Result(); err != nil {
		return err
	}
	return q.client.XAck(ctx, q.publisher.TenantControlStream(), q.config.Group, message.ID).Err()
}
func decodeTenantControl(message redisclient.XMessage) (relay.TenantControlDelivery, error) {
	raw, ok := controlValue(message.Values["event"])
	if !ok {
		return relay.TenantControlDelivery{}, runtime.ErrInvalidEnvelope
	}
	var event relay.TenantControlEvent
	if err := json.Unmarshal([]byte(raw), &event); err != nil {
		return relay.TenantControlDelivery{}, runtime.ErrInvalidEnvelope
	}
	tenant, tenantOK := controlValue(message.Values["tenant_id"])
	kind, kindOK := controlValue(message.Values["kind"])
	version, versionOK := controlValue(message.Values["version"])
	idem, idemOK := controlValue(message.Values["idempotency_key"])
	if !tenantOK || !kindOK || !versionOK || !idemOK || tenant != event.TenantID || kind != event.Kind || version != strconv.FormatUint(event.Version, 10) || idem != event.IdempotencyKey || event.TenantID == "" || event.AggregateID == "" || event.IdempotencyKey == "" || event.Version < 1 || event.PayloadRef == "" {
		return relay.TenantControlDelivery{}, runtime.ErrVersionMismatch
	}
	return relay.TenantControlDelivery{ID: message.ID, Event: event}, nil
}
func controlValue(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		return typed, true
	case []byte:
		return string(typed), true
	default:
		return "", false
	}
}
func (p *Publisher) TenantControlDeadLetterStream() string {
	return p.TenantControlStream() + ":dead-letter"
}

var _ relay.TenantControlQueue = (*TenantControlQueue)(nil)
