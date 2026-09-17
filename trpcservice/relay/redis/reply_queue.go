package redis

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	redisclient "github.com/redis/go-redis/v9"
)

type ReplyQueueConfig struct {
	Group       string
	ReadBlock   time.Duration
	ReclaimIdle time.Duration
}

type ReplyQueue struct {
	client    redisclient.UniversalClient
	publisher *Publisher
	config    ReplyQueueConfig
}

var deadLetterReplyScript = redisclient.NewScript(`
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

func NewReplyQueue(client redisclient.UniversalClient, publisher *Publisher, config ReplyQueueConfig) (*ReplyQueue, error) {
	if client == nil || publisher == nil || !validSegment(config.Group) {
		return nil, runtime.ErrInvalidEnvelope
	}
	if config.ReadBlock <= 0 {
		config.ReadBlock = 250 * time.Millisecond
	}
	if config.ReclaimIdle <= 0 {
		config.ReclaimIdle = 30 * time.Second
	}
	return &ReplyQueue{client: client, publisher: publisher, config: config}, nil
}

func (q *ReplyQueue) ConsumeReplies(ctx context.Context, destination channel.ReplyDestination, opts channel.ReplyConsumerOptions, handle func(context.Context, channel.ReplyDelivery) error) error {
	if err := validateReplyConsumer(destination, opts, handle); err != nil {
		return err
	}
	stream := q.publisher.ReplyStream(destination)
	if err := q.ensureGroup(ctx, stream); err != nil {
		return err
	}
	for {
		streams, err := q.client.XReadGroup(ctx, &redisclient.XReadGroupArgs{Group: q.config.Group,
			Consumer: opts.ConsumerID, Streams: []string{stream, ">"}, Count: 1, Block: q.config.ReadBlock}).Result()
		if errors.Is(err, redisclient.Nil) {
			continue
		}
		if err != nil {
			return err
		}
		for _, messages := range streams {
			for _, message := range messages.Messages {
				delivery, decodeErr := decodeReply(destination, message)
				if decodeErr != nil {
					if err := q.deadLetter(ctx, stream, message, decodeErr); err != nil {
						return err
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

func (q *ReplyQueue) AckReply(ctx context.Context, destination channel.ReplyDestination, delivery channel.ReplyDelivery) error {
	if err := validateReplyDestination(destination); err != nil || delivery.ID == "" || delivery.Destination != destination {
		return runtime.ErrInvalidEnvelope
	}
	return q.client.XAck(ctx, q.publisher.ReplyStream(destination), q.config.Group, delivery.ID).Err()
}

func (q *ReplyQueue) ReclaimReplies(ctx context.Context, destination channel.ReplyDestination, opts channel.ReplyConsumerOptions) ([]channel.ReplyDelivery, error) {
	if err := validateReplyConsumer(destination, opts, func(context.Context, channel.ReplyDelivery) error { return nil }); err != nil {
		return nil, err
	}
	stream := q.publisher.ReplyStream(destination)
	if err := q.ensureGroup(ctx, stream); err != nil {
		return nil, err
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}
	messages, _, err := q.client.XAutoClaim(ctx, &redisclient.XAutoClaimArgs{Stream: stream, Group: q.config.Group,
		Consumer: opts.ConsumerID, MinIdle: q.config.ReclaimIdle, Start: "0-0", Count: int64(limit)}).Result()
	if errors.Is(err, redisclient.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	result := make([]channel.ReplyDelivery, 0, len(messages))
	for _, message := range messages {
		delivery, decodeErr := decodeReply(destination, message)
		if decodeErr != nil {
			if err := q.deadLetter(ctx, stream, message, decodeErr); err != nil {
				return nil, err
			}
			continue
		}
		result = append(result, delivery)
	}
	return result, nil
}

func (q *ReplyQueue) ensureGroup(ctx context.Context, stream string) error {
	err := q.client.XGroupCreateMkStream(ctx, stream, q.config.Group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func (q *ReplyQueue) deadLetter(ctx context.Context, stream string, message redisclient.XMessage, cause error) error {
	raw, _ := replyValueString(message.Values["event"])
	return deadLetterReplyScript.Run(ctx, q.client, []string{stream, stream + ":dead-letter"},
		message.ID, cause.Error(), raw, time.Now().UTC().Format(time.RFC3339Nano), q.publisher.config.MaxLen, q.config.Group).Err()
}

func decodeReply(destination channel.ReplyDestination, message redisclient.XMessage) (channel.ReplyDelivery, error) {
	raw, ok := replyValueString(message.Values["event"])
	if !ok {
		return channel.ReplyDelivery{}, runtime.ErrInvalidEnvelope
	}
	var event channel.ReplyEvent
	if err := json.Unmarshal([]byte(raw), &event); err != nil {
		return channel.ReplyDelivery{}, runtime.ErrInvalidEnvelope
	}
	schemaVersion, schemaOK := replyValueString(message.Values["schema_version"])
	tenantID, tenantOK := replyValueString(message.Values["tenant_id"])
	channelID, channelOK := replyValueString(message.Values["channel"])
	bindingID, bindingOK := replyValueString(message.Values["binding_id"])
	configVersion, versionOK := replyValueString(message.Values["config_version"])
	accountID, accountOK := replyValueString(message.Values["external_account_id"])
	deliveryKey, deliveryOK := replyValueString(message.Values["delivery_key"])
	if !schemaOK || !tenantOK || !channelOK || !bindingOK || !versionOK || !accountOK || !deliveryOK || schemaVersion != "1" ||
		tenantID != destination.TenantID || tenantID != event.TenantID || bindingID != destination.ChannelBindingID ||
		bindingID != event.ChannelBindingID || channelID != destination.Channel || accountID != destination.ExternalAccountID ||
		configVersion != strconv.FormatInt(event.ConfigVersion, 10) || event.ConfigVersion < 1 ||
		event.Target.Channel != channelID || event.Target.ExternalAccountID != accountID ||
		(event.Target.ExternalMessageID == "" && event.Target.ExternalChatID == "" && event.Target.ExternalUserID == "") ||
		deliveryKey != event.DeliveryKey || event.SchemaVersion != 1 ||
		event.RequestID == "" || event.DeliveryKey == "" || event.ContentRef == "" {
		return channel.ReplyDelivery{}, runtime.ErrVersionMismatch
	}
	return channel.ReplyDelivery{ID: message.ID, Destination: destination, Event: event}, nil
}

func validateReplyConsumer(destination channel.ReplyDestination, opts channel.ReplyConsumerOptions, handle func(context.Context, channel.ReplyDelivery) error) error {
	if err := validateReplyDestination(destination); err != nil || opts.ConsumerID == "" || handle == nil {
		return runtime.ErrInvalidEnvelope
	}
	return nil
}

func validateReplyDestination(destination channel.ReplyDestination) error {
	if destination.TenantID == "" || destination.Channel == "" || destination.ChannelBindingID == "" || destination.ExternalAccountID == "" {
		return runtime.ErrTenantScope
	}
	return nil
}

func replyValueString(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		return typed, true
	case []byte:
		return string(typed), true
	default:
		return "", false
	}
}

var _ channel.ReplyQueue = (*ReplyQueue)(nil)
