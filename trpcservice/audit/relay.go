package audit

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

type Resolver interface {
	ResolveAuditEvent(context.Context, messaging.OutboxRecord) (Event, error)
}

type Relay struct {
	Base     relay.Base
	Resolver Resolver
	Sink     Sink
	OnError  func(error)
	Observer ExportObserver
	Alerts   QuarantineObserver
}

type ExportObserver interface {
	ObserveAuditExport(success bool, duration time.Duration)
}

type QuarantineObserver interface {
	ObserveQuarantineAlert()
}

func (r Relay) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	interval := r.Base.PollInterval
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		_, err := r.RunOnce(ctx)
		if err != nil && r.OnError != nil {
			r.OnError(err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r Relay) RunOnce(ctx context.Context) (int, error) {
	if err := r.validate(); err != nil {
		return 0, err
	}
	return r.Base.RunOnce(ctx, relay.RecordProcessorFunc(r.process))
}

func (r Relay) process(ctx context.Context, record messaging.OutboxRecord) (resultErr error) {
	started := time.Now()
	success := false
	defer func() {
		if r.Observer != nil {
			r.Observer.ObserveAuditExport(success, time.Since(started))
		}
	}()
	if record.Kind != "audit" || record.TenantID == "" || record.OutboxID == "" {
		return runtime.ErrTenantScope
	}
	event, err := r.Resolver.ResolveAuditEvent(ctx, record)
	if err != nil {
		return err
	}
	if event.TenantID != record.TenantID {
		return runtime.ErrTenantScope
	}
	expectedID, err := StableID(record.TenantID, record.OutboxID)
	if err != nil || event.AuditID != expectedID {
		return runtime.ErrIdempotencyCollision
	}
	if err := Validate(event); err != nil {
		return err
	}
	if err := r.Sink.Emit(ctx, event); err != nil {
		return err
	}
	if event.Action == "artifact.quarantine" && event.Decision == "alert" && r.Alerts != nil {
		r.Alerts.ObserveQuarantineAlert()
	}
	success = true
	return nil
}

func (r Relay) validate() error {
	if r.Resolver == nil || r.Sink == nil || r.Base.Kind != "audit" {
		return runtime.ErrInvariantViolation
	}
	return nil
}
