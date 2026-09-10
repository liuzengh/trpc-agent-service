package relay

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

type RecordProcessor interface {
	Process(context.Context, messaging.OutboxRecord) error
}

type RecordProcessorFunc func(context.Context, messaging.OutboxRecord) error

func (f RecordProcessorFunc) Process(ctx context.Context, record messaging.OutboxRecord) error {
	return f(ctx, record)
}

// Base owns the common Outbox claim lease and state transitions. A processor
// performs only the kind-specific authoritative read and idempotent publish.
type Base struct {
	Outbox             messaging.OutboxStore
	Kind, Owner        string
	BatchSize          int
	ClaimTTL           time.Duration
	ClaimRenewInterval time.Duration
	RetryDelay         time.Duration
	PollInterval       time.Duration
	Telemetry          telemetry.Provider
}

func (b Base) Run(ctx context.Context, processor RecordProcessor) error {
	if err := b.validate(processor); err != nil {
		return err
	}
	interval := b.PollInterval
	if interval <= 0 {
		interval = 50 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		_, _ = b.RunOnce(ctx, processor)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (b Base) RunOnce(ctx context.Context, processor RecordProcessor) (int, error) {
	if err := b.validate(processor); err != nil {
		return 0, err
	}
	batchSize := b.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}
	claimTTL := b.ClaimTTL
	if claimTTL <= 0 {
		claimTTL = 30 * time.Second
	}
	retryDelay := b.RetryDelay
	if retryDelay <= 0 {
		retryDelay = time.Second
	}
	records, err := b.Outbox.ClaimOutbox(ctx, b.Kind, batchSize, b.Owner, time.Now().Add(claimTTL))
	if err != nil {
		return 0, err
	}
	published := 0
	var relayErr error
	type claimResult struct {
		version   uint64
		err       error
		claimLost bool
	}
	gates := make([]chan struct{}, len(records)+1)
	results := make([]chan claimResult, len(records))
	for i := range gates {
		gates[i] = make(chan struct{})
	}
	close(gates[0])
	for i, record := range records {
		results[i] = make(chan claimResult, 1)
		go func(index int, current messaging.OutboxRecord) {
			ordered := RecordProcessorFunc(func(processCtx context.Context, value messaging.OutboxRecord) error {
				defer close(gates[index+1])
				select {
				case <-processCtx.Done():
					return processCtx.Err()
				case <-gates[index]:
				}
				operation, component := relayTelemetry(b.Kind)
				processCtx, finish := telemetry.StartOperation(processCtx, b.Telemetry, value.TraceParent, operation,
					telemetry.ComponentAttribute(component))
				processErr := processor.Process(processCtx, value)
				finish(processErr)
				return processErr
			})
			version, processErr, claimLost := b.processClaim(ctx, ordered, current, claimTTL)
			results[index] <- claimResult{version: version, err: processErr, claimLost: claimLost}
		}(i, record)
	}
	for i, record := range records {
		result := <-results[i]
		version, processErr, claimLost := result.version, result.err, result.claimLost
		if claimLost {
			relayErr = errors.Join(relayErr, runtime.ErrLeaseLost)
			continue
		}
		if processErr != nil {
			markErr := b.Outbox.MarkRetry(ctx, record.TenantID, record.OutboxID, version, time.Now().Add(retryDelay))
			relayErr = errors.Join(relayErr, processErr, markErr)
			continue
		}
		if markErr := b.Outbox.MarkPublished(ctx, record.TenantID, record.OutboxID, version); markErr != nil {
			relayErr = errors.Join(relayErr, markErr)
			continue
		}
		published++
	}
	return published, relayErr
}

func relayTelemetry(kind string) (telemetry.Operation, telemetry.Component) {
	switch kind {
	case "dispatch":
		return telemetry.OperationRelayDispatch, telemetry.ComponentBusinessRelay
	case "reply":
		return telemetry.OperationRelayReply, telemetry.ComponentBusinessRelay
	case "wakeup":
		return telemetry.OperationRelayWakeup, telemetry.ComponentBusinessRelay
	case "audit":
		return telemetry.OperationAuditExport, telemetry.ComponentAuditRelay
	default:
		return telemetry.OperationRelayControl, telemetry.ComponentBusinessRelay
	}
}

func (b Base) processClaim(ctx context.Context, processor RecordProcessor, record messaging.OutboxRecord, claimTTL time.Duration) (uint64, error, bool) {
	processCtx, cancelProcess := context.WithCancel(ctx)
	defer cancelProcess()
	stopRenewal := make(chan struct{})
	renewalDone := make(chan struct{})
	var claimLost atomic.Bool
	var version atomic.Uint64
	version.Store(record.Version)
	renewInterval := b.ClaimRenewInterval
	if renewInterval <= 0 {
		renewInterval = claimTTL / 3
	}
	go func() {
		defer close(renewalDone)
		ticker := time.NewTicker(renewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopRenewal:
				return
			case <-processCtx.Done():
				return
			case <-ticker.C:
				current := version.Load()
				next, err := b.Outbox.RenewOutboxClaim(processCtx, record.TenantID, record.OutboxID, current, b.Owner, time.Now().Add(claimTTL))
				if err != nil {
					claimLost.Store(true)
					cancelProcess()
					return
				}
				version.Store(next)
			}
		}
	}()
	processErr := processor.Process(processCtx, record)
	close(stopRenewal)
	<-renewalDone
	return version.Load(), processErr, claimLost.Load()
}

func (b Base) validate(processor RecordProcessor) error {
	if b.Outbox == nil || b.Kind == "" || b.Owner == "" || processor == nil {
		return runtime.ErrInvariantViolation
	}
	return nil
}
