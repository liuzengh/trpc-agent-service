package relay

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/broker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

// DispatchRelay is the only component that publishes dispatch outbox rows.
// The authoritative envelope is reloaded from the task store; outbox payload
// references are never trusted as execution input.
type DispatchRelay struct {
	Outbox             messaging.OutboxStore
	Tasks              gateway.TaskStore
	Broker             broker.Broker
	Owner              string
	ShardCount         uint32
	BatchSize          int
	ClaimTTL           time.Duration
	ClaimRenewInterval time.Duration
	RetryDelay         time.Duration
	PollInterval       time.Duration
	Telemetry          telemetry.Provider
}

func (r DispatchRelay) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	return r.base().Run(ctx, RecordProcessorFunc(r.publish))
}

func (r DispatchRelay) RunOnce(ctx context.Context) (int, error) {
	if err := r.validate(); err != nil {
		return 0, err
	}
	return r.base().RunOnce(ctx, RecordProcessorFunc(r.publish))
}

func (r DispatchRelay) publish(ctx context.Context, record messaging.OutboxRecord) error {
	status, err := r.Tasks.GetExecution(ctx, gateway.ExecutionKey{TenantID: record.TenantID, RequestID: record.AggregateID})
	if err != nil {
		return err
	}
	if status.Envelope.TenantID != record.TenantID || status.Envelope.RequestID != record.AggregateID {
		return runtime.ErrTenantScope
	}
	if err := status.Envelope.Validate(); err != nil {
		return err
	}
	shard, err := broker.ShardForSession(status.Envelope.TenantID, status.Envelope.AgentAppID, status.Envelope.SessionID, r.ShardCount)
	if err != nil {
		return err
	}
	return r.Broker.Publish(ctx, shard, status.Envelope)
}

func (r DispatchRelay) base() Base {
	return Base{Outbox: r.Outbox, Kind: "dispatch", Owner: r.Owner, BatchSize: r.BatchSize,
		ClaimTTL: r.ClaimTTL, ClaimRenewInterval: r.ClaimRenewInterval,
		RetryDelay: r.RetryDelay, PollInterval: r.PollInterval, Telemetry: r.Telemetry}
}

func (r DispatchRelay) validate() error {
	if r.Outbox == nil || r.Tasks == nil || r.Broker == nil || r.Owner == "" || r.ShardCount == 0 {
		return runtime.ErrInvariantViolation
	}
	return nil
}
