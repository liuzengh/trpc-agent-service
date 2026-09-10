// Package redis publishes business Relay events to Redis Streams. Session
// execution dispatch remains owned by broker/redis.
package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	redisclient "github.com/redis/go-redis/v9"
)

type Config struct {
	Environment string
	MaxLen      int64
}

type Publisher struct {
	client redisclient.UniversalClient
	config Config
}

func NewPublisher(client redisclient.UniversalClient, config Config) (*Publisher, error) {
	if client == nil || !validSegment(config.Environment) {
		return nil, runtime.ErrInvariantViolation
	}
	if config.MaxLen < 0 {
		return nil, runtime.ErrInvariantViolation
	}
	return &Publisher{client: client, config: config}, nil
}

func (p *Publisher) PublishReply(ctx context.Context, destination channel.ReplyDestination, event channel.ReplyEvent) error {
	if destination.TenantID == "" || destination.Channel == "" || destination.ChannelBindingID == "" || destination.ExternalAccountID == "" ||
		event.SchemaVersion != 1 || event.TenantID != destination.TenantID || event.ChannelBindingID != destination.ChannelBindingID || event.DeliveryKey == "" ||
		event.ConfigVersion < 1 || (destination.ConfigVersion > 0 && event.ConfigVersion != destination.ConfigVersion) ||
		event.Target.Channel != destination.Channel || event.Target.ExternalAccountID != destination.ExternalAccountID ||
		(event.Target.ExternalMessageID == "" && event.Target.ExternalChatID == "" && event.Target.ExternalUserID == "") {
		return runtime.ErrInvariantViolation
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.xadd(ctx, p.ReplyStream(destination), map[string]any{
		"schema_version":      strconv.FormatUint(uint64(event.SchemaVersion), 10),
		"event":               payload,
		"tenant_id":           event.TenantID,
		"channel":             destination.Channel,
		"binding_id":          event.ChannelBindingID,
		"config_version":      strconv.FormatInt(event.ConfigVersion, 10),
		"external_account_id": destination.ExternalAccountID,
		"delivery_key":        event.DeliveryKey,
		"traceparent":         event.TraceParent,
	})
}

func (p *Publisher) PublishWakeup(ctx context.Context, event relay.WakeupEvent) error {
	if event.TenantID == "" || event.AggregateID == "" || event.IdempotencyKey == "" || event.PayloadRef == "" {
		return runtime.ErrInvariantViolation
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.xadd(ctx, p.WakeupStream(), map[string]any{
		"schema_version": "1", "event": payload, "tenant_id": event.TenantID,
		"idempotency_key": event.IdempotencyKey, "traceparent": event.TraceParent,
	})
}

func (p *Publisher) PublishTenantControl(ctx context.Context, event relay.TenantControlEvent) error {
	if event.TenantID == "" || event.AggregateID == "" || event.Kind == "" || event.Version < 1 || event.PayloadRef == "" {
		return runtime.ErrInvariantViolation
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.xadd(ctx, p.TenantControlStream(), map[string]any{
		"schema_version": "1", "event": payload, "tenant_id": event.TenantID,
		"kind": event.Kind, "version": strconv.FormatUint(event.Version, 10),
		"idempotency_key": event.IdempotencyKey, "traceparent": event.TraceParent,
	})
}

func (p *Publisher) PublishExecutionControl(ctx context.Context, event relay.ExecutionControlEvent) error {
	if event.TenantID == "" || event.AggregateID == "" || event.Kind != "execution-control" || event.Version < 1 || event.PayloadRef == "" {
		return runtime.ErrInvariantViolation
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.xadd(ctx, p.ExecutionControlStream(), map[string]any{
		"schema_version": "1", "event": payload, "tenant_id": event.TenantID,
		"kind": event.Kind, "version": strconv.FormatUint(event.Version, 10),
		"idempotency_key": event.IdempotencyKey, "traceparent": event.TraceParent,
	})
}

func (p *Publisher) ReplyStream(destination channel.ReplyDestination) string {
	digest := sha256.Sum256([]byte(destination.TenantID + "\x00" + destination.Channel + "\x00" + destination.ChannelBindingID + "\x00" + destination.ExternalAccountID))
	return fmt.Sprintf("trpc:{%s}:reply:%s", p.config.Environment, hex.EncodeToString(digest[:16]))
}

func (p *Publisher) WakeupStream() string {
	return fmt.Sprintf("trpc:{%s}:wakeup", p.config.Environment)
}

func (p *Publisher) WakeupDeadLetterStream() string { return p.WakeupStream() + ":dead-letter" }

func (p *Publisher) TenantControlStream() string {
	return fmt.Sprintf("trpc:{%s}:tenant-control", p.config.Environment)
}

func (p *Publisher) ExecutionControlStream() string {
	return fmt.Sprintf("trpc:{%s}:execution-control", p.config.Environment)
}

func (p *Publisher) xadd(ctx context.Context, stream string, values map[string]any) error {
	values["published_at"] = time.Now().UTC().Format(time.RFC3339Nano)
	args := &redisclient.XAddArgs{Stream: stream, Values: values}
	if p.config.MaxLen > 0 {
		args.MaxLen, args.Approx = p.config.MaxLen, true
	}
	return p.client.XAdd(ctx, args).Err()
}

func validSegment(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '-' || char == '_' {
			continue
		}
		return false
	}
	return true
}

var _ channel.ReplyPublisher = (*Publisher)(nil)
var _ relay.WakeupPublisher = (*Publisher)(nil)
var _ relay.TenantControlPublisher = (*Publisher)(nil)
var _ relay.ExecutionControlPublisher = (*Publisher)(nil)
