package relay

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

type TenantControlEvent struct {
	TenantID, Kind, AggregateID, IdempotencyKey, PayloadRef, TraceParent string
	Version                                                              uint64
}

// ExecutionControlEvent is a broadcast acceleration hint. It never carries
// authority; consumers must reload execution status from the TaskStore before
// cancelling work.
type ExecutionControlEvent struct {
	TenantID, Kind, AggregateID, IdempotencyKey, PayloadRef, TraceParent string
	Version                                                              uint64
}

type ExecutionControlPublisher interface {
	PublishExecutionControl(context.Context, ExecutionControlEvent) error
}

type ExecutionControlDelivery struct {
	ID    string
	Event ExecutionControlEvent
}

type ExecutionControlConsumerOptions struct {
	ConsumerID string
	Limit      int
}

// ExecutionControlQueue is deliberately a per-node consumer group: every
// runtime node needs the cancellation hint, while the authoritative status
// remains in PostgreSQL if a stream delivery is missed.
type ExecutionControlQueue interface {
	ConsumeExecutionControl(context.Context, ExecutionControlConsumerOptions, func(context.Context, ExecutionControlDelivery) error) error
	AckExecutionControl(context.Context, ExecutionControlDelivery) error
	ReclaimExecutionControls(context.Context, ExecutionControlConsumerOptions) ([]ExecutionControlDelivery, error)
}

type ExecutionControlRelay struct {
	Outbox             messaging.OutboxStore
	Controls           ExecutionControlPublisher
	Owner              string
	BatchSize          int
	ClaimTTL           time.Duration
	ClaimRenewInterval time.Duration
	RetryDelay         time.Duration
	PollInterval       time.Duration
	Telemetry          telemetry.Provider
}

func (r ExecutionControlRelay) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	return r.base().Run(ctx, RecordProcessorFunc(r.publish))
}

func (r ExecutionControlRelay) RunOnce(ctx context.Context) (int, error) {
	if err := r.validate(); err != nil {
		return 0, err
	}
	return r.base().RunOnce(ctx, RecordProcessorFunc(r.publish))
}

func (r ExecutionControlRelay) publish(ctx context.Context, record messaging.OutboxRecord) error {
	if record.TenantID == "" || record.AggregateID == "" || record.EventSeq < 1 || record.PayloadRef == "" {
		return runtime.ErrInvariantViolation
	}
	return r.Controls.PublishExecutionControl(ctx, ExecutionControlEvent{
		TenantID: record.TenantID, Kind: "execution-control", AggregateID: record.AggregateID,
		IdempotencyKey: record.IdempotencyKey, PayloadRef: record.PayloadRef,
		TraceParent: telemetry.EffectiveTraceParent(ctx, record.TraceParent), Version: record.EventSeq,
	})
}

func (r ExecutionControlRelay) base() Base {
	return Base{Outbox: r.Outbox, Kind: "execution-control", Owner: r.Owner,
		BatchSize: r.BatchSize, ClaimTTL: r.ClaimTTL, ClaimRenewInterval: r.ClaimRenewInterval,
		RetryDelay: r.RetryDelay, PollInterval: r.PollInterval, Telemetry: r.Telemetry}
}

func (r ExecutionControlRelay) validate() error {
	if r.Outbox == nil || r.Controls == nil || r.Owner == "" {
		return runtime.ErrInvariantViolation
	}
	return nil
}

// TenantControlPublisher is broadcast semantics: every runtime node must see
// invalidation events. A single consumer-group delivery is not sufficient.
type TenantControlPublisher interface {
	PublishTenantControl(context.Context, TenantControlEvent) error
}

type TenantControlDelivery struct {
	ID    string
	Event TenantControlEvent
}

type TenantControlConsumerOptions struct {
	ConsumerID string
	Limit      int
}

// TenantControlQueue uses one consumer group per runtime node. Control
// broadcasts must not be load-balanced between nodes.
type TenantControlQueue interface {
	ConsumeTenantControl(context.Context, TenantControlConsumerOptions, func(context.Context, TenantControlDelivery) error) error
	AckTenantControl(context.Context, TenantControlDelivery) error
	ReclaimTenantControls(context.Context, TenantControlConsumerOptions) ([]TenantControlDelivery, error)
}

type TenantControlRelay struct {
	Outbox             messaging.OutboxStore
	Controls           TenantControlPublisher
	Kind, Owner        string
	BatchSize          int
	ClaimTTL           time.Duration
	ClaimRenewInterval time.Duration
	RetryDelay         time.Duration
	PollInterval       time.Duration
	Telemetry          telemetry.Provider
}

func (r TenantControlRelay) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	return r.base().Run(ctx, RecordProcessorFunc(r.publish))
}

func (r TenantControlRelay) RunOnce(ctx context.Context) (int, error) {
	if err := r.validate(); err != nil {
		return 0, err
	}
	return r.base().RunOnce(ctx, RecordProcessorFunc(r.publish))
}

func (r TenantControlRelay) publish(ctx context.Context, record messaging.OutboxRecord) error {
	if record.TenantID == "" || record.AggregateID == "" || record.EventSeq < 1 || record.PayloadRef == "" {
		return runtime.ErrInvariantViolation
	}
	event := TenantControlEvent{TenantID: record.TenantID, Kind: r.Kind,
		AggregateID: record.AggregateID, IdempotencyKey: record.IdempotencyKey,
		PayloadRef: record.PayloadRef, TraceParent: telemetry.EffectiveTraceParent(ctx, record.TraceParent), Version: record.EventSeq}
	return r.Controls.PublishTenantControl(ctx, event)
}

func (r TenantControlRelay) base() Base {
	return Base{Outbox: r.Outbox, Kind: r.Kind, Owner: r.Owner, BatchSize: r.BatchSize,
		ClaimTTL: r.ClaimTTL, ClaimRenewInterval: r.ClaimRenewInterval,
		RetryDelay: r.RetryDelay, PollInterval: r.PollInterval, Telemetry: r.Telemetry}
}

func (r TenantControlRelay) validate() error {
	if r.Outbox == nil || r.Controls == nil || r.Owner == "" || (r.Kind != "tenant-control" && r.Kind != "config-invalidation") {
		return runtime.ErrInvariantViolation
	}
	return nil
}
