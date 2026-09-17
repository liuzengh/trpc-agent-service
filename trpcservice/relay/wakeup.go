package relay

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

type WakeupEvent struct {
	TenantID, AggregateID, IdempotencyKey, PayloadRef, TraceParent string
	EventSeq                                                       uint64
}

type WakeupPublisher interface {
	PublishWakeup(context.Context, WakeupEvent) error
}

type WakeupRelay struct {
	Outbox             messaging.OutboxStore
	Wakeups            WakeupPublisher
	Owner              string
	BatchSize          int
	ClaimTTL           time.Duration
	ClaimRenewInterval time.Duration
	RetryDelay         time.Duration
	PollInterval       time.Duration
	Telemetry          telemetry.Provider
}

func (r WakeupRelay) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	return r.base().Run(ctx, RecordProcessorFunc(r.publish))
}

func (r WakeupRelay) RunOnce(ctx context.Context) (int, error) {
	if err := r.validate(); err != nil {
		return 0, err
	}
	return r.base().RunOnce(ctx, RecordProcessorFunc(r.publish))
}

func (r WakeupRelay) publish(ctx context.Context, record messaging.OutboxRecord) error {
	if record.TenantID == "" || record.AggregateID == "" || record.IdempotencyKey == "" || record.PayloadRef == "" {
		return runtime.ErrInvariantViolation
	}
	return r.Wakeups.PublishWakeup(ctx, WakeupEvent{TenantID: record.TenantID,
		AggregateID: record.AggregateID, IdempotencyKey: record.IdempotencyKey,
		PayloadRef: record.PayloadRef, TraceParent: telemetry.EffectiveTraceParent(ctx, record.TraceParent), EventSeq: record.EventSeq})
}

func (r WakeupRelay) base() Base {
	return Base{Outbox: r.Outbox, Kind: "wakeup", Owner: r.Owner, BatchSize: r.BatchSize,
		ClaimTTL: r.ClaimTTL, ClaimRenewInterval: r.ClaimRenewInterval,
		RetryDelay: r.RetryDelay, PollInterval: r.PollInterval, Telemetry: r.Telemetry}
}

func (r WakeupRelay) validate() error {
	if r.Outbox == nil || r.Wakeups == nil || r.Owner == "" {
		return runtime.ErrInvariantViolation
	}
	return nil
}
