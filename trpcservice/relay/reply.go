package relay

import (
	"context"
	"errors"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

// ReplyRelay publishes committed reply facts to the durable account queue. It
// does not call a provider sender; Delivery Ledger ownership remains with the
// Channel Adapter consuming that queue.
type ReplyRelay struct {
	Outbox             messaging.OutboxStore
	Results            messaging.ResultStore
	Routes             messaging.ReplyRouteStore
	Replies            channel.ReplyPublisher
	Owner              string
	BatchSize          int
	ClaimTTL           time.Duration
	ClaimRenewInterval time.Duration
	RetryDelay         time.Duration
	PollInterval       time.Duration
	Telemetry          telemetry.Provider
}

func (r ReplyRelay) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	return r.base().Run(ctx, RecordProcessorFunc(r.publish))
}

func (r ReplyRelay) RunOnce(ctx context.Context) (int, error) {
	if err := r.validate(); err != nil {
		return 0, err
	}
	return r.base().RunOnce(ctx, RecordProcessorFunc(r.publish))
}

func (r ReplyRelay) publish(ctx context.Context, record messaging.OutboxRecord) error {
	if record.Kind != "" && record.Kind != "reply" {
		return runtime.ErrInvariantViolation
	}
	result, err := messaging.ResolveReplyContent(ctx, r.Results, record.TenantID, record.AggregateID, record.PayloadRef)
	if err != nil {
		if errors.Is(err, runtime.ErrVersionMismatch) {
			return runtime.ErrTenantScope
		}
		return err
	}
	if result.TenantID != record.TenantID || result.RequestID != record.AggregateID || result.ResultRef != record.PayloadRef {
		return runtime.ErrTenantScope
	}
	route, err := r.Routes.ResolveReplyRoute(ctx, record.TenantID, record.AggregateID)
	if err != nil {
		return err
	}
	if route.TenantID != record.TenantID || route.RequestID != record.AggregateID || route.ChannelBindingID == "" || route.ExternalAccountID == "" || route.ConfigVersion < 1 {
		return runtime.ErrTenantScope
	}
	destination := channel.ReplyDestination{TenantID: route.TenantID, Channel: route.Channel, ChannelBindingID: route.ChannelBindingID,
		ExternalAccountID: route.ExternalAccountID, ConfigVersion: route.ConfigVersion}
	event := channel.ReplyEvent{SchemaVersion: 1, TenantID: record.TenantID, RequestID: record.AggregateID,
		ChannelBindingID: route.ChannelBindingID, DeliveryKey: record.IdempotencyKey, ConfigVersion: route.ConfigVersion,
		EventSeq: record.EventSeq, Kind: "message.completed", ContentRef: result.ResultRef, ContentType: result.ContentType,
		Target: channel.DeliveryTarget{Channel: route.Channel, ExternalAccountID: route.ExternalAccountID,
			ExternalMessageID: route.ExternalMessageID, ExternalChatID: route.ExternalChatID, ExternalUserID: route.ExternalUserID},
		Final: true, TraceParent: telemetry.EffectiveTraceParent(ctx, record.TraceParent)}
	return r.Replies.PublishReply(ctx, destination, event)
}

func (r ReplyRelay) base() Base {
	return Base{Outbox: r.Outbox, Kind: "reply", Owner: r.Owner, BatchSize: r.BatchSize,
		ClaimTTL: r.ClaimTTL, ClaimRenewInterval: r.ClaimRenewInterval,
		RetryDelay: r.RetryDelay, PollInterval: r.PollInterval, Telemetry: r.Telemetry}
}

func (r ReplyRelay) validate() error {
	if r.Outbox == nil || r.Results == nil || r.Routes == nil || r.Replies == nil || r.Owner == "" {
		return runtime.ErrInvariantViolation
	}
	return nil
}
