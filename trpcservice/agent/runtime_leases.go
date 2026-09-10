package agent

import (
	"context"
	"fmt"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type runtimeExecutionLeases struct {
	runtime          *Runtime
	snapshot         tenant.Snapshot
	inbound          channels.InboundMessage
	bindingID        string
	traceID          string
	messageLease     storage.Lease
	messageHeartbeat *idempotencyLeaseHeartbeat
	dedupActive      bool
	claimHeartbeat   *claimHeartbeat
	sessionHeartbeat *sessionLeaseHeartbeat

	mu        sync.Mutex
	completed bool
	closed    bool
}

func (r *Runtime) acquireExecutionLeases(
	ctx context.Context,
	snapshot tenant.Snapshot,
	bindingID string,
	sessionKey string,
	inbound channels.InboundMessage,
	traceID string,
) (context.Context, *runtimeExecutionLeases, error) {
	idempotencyKey, err := storage.BuildIdempotencyKey(snapshot.Config.TenantID, inbound.Channel, bindingID, inbound.MessageID)
	if err != nil {
		return ctx, nil, err
	}
	acquisition, err := r.idempotency.Acquire(ctx, idempotencyKey, r.processingTTL)
	if err != nil {
		return ctx, nil, fmt.Errorf("acquire message lease: %w", err)
	}
	switch acquisition.State {
	case storage.LeaseAlreadyCompleted:
		return ctx, nil, ErrDuplicateMessage
	case storage.LeaseInProgress:
		return ctx, nil, ErrMessageInProgress
	case storage.LeaseAcquired:
	default:
		return ctx, nil, fmt.Errorf("unsupported idempotency lease state %q", acquisition.State)
	}

	leases := &runtimeExecutionLeases{
		runtime: r, snapshot: snapshot, inbound: inbound, bindingID: bindingID,
		traceID: traceID, messageLease: acquisition.Lease,
	}
	leases.messageHeartbeat = startIdempotencyLeaseHeartbeat(ctx, r.idempotency, acquisition.Lease, r.processingTTL)
	ctx = leases.messageHeartbeat.context
	fail := func(err error) (context.Context, *runtimeExecutionLeases, error) {
		leases.Close()
		return ctx, nil, err
	}

	if r.executionDedup != nil {
		state, err := r.executionDedup.Begin(ctx, snapshot.Config.TenantID, string(inbound.Channel), bindingID, inbound.MessageID, traceID, r.processingTTL)
		if err != nil {
			return fail(fmt.Errorf("begin execution claim: %w", err))
		}
		switch state {
		case storage.ExecutionCompleted:
			return fail(ErrDuplicateMessage)
		case storage.ExecutionInProgress:
			return fail(ErrMessageInProgress)
		case storage.ExecutionFresh:
			leases.dedupActive = true
			leases.claimHeartbeat = startClaimHeartbeat(ctx, r.executionDedup, snapshot.Config.TenantID, string(inbound.Channel), bindingID, inbound.MessageID, traceID, r.processingTTL)
			ctx = leases.claimHeartbeat.context
		default:
			return fail(fmt.Errorf("unsupported execution claim state %q", state))
		}
	}

	sessionLease, err := r.stateStore.AcquireSessionExecutionLease(ctx, snapshot.Config.TenantID, sessionKey, traceID, r.processingTTL)
	if err != nil {
		return fail(fmt.Errorf("acquire session execution lease: %w", err))
	}
	leases.sessionHeartbeat = startSessionLeaseHeartbeat(ctx, r.stateStore, sessionLease, r.processingTTL)
	return leases.sessionHeartbeat.context, leases, nil
}

func (l *runtimeExecutionLeases) Check() error {
	if l == nil {
		return nil
	}
	if l.messageHeartbeat != nil && l.messageHeartbeat.Err() != nil {
		return fmt.Errorf("message idempotency lease lost: %w", l.messageHeartbeat.Err())
	}
	if l.claimHeartbeat != nil && l.claimHeartbeat.Err() != nil {
		return fmt.Errorf("%w: %v", ErrExecutionClaimLost, l.claimHeartbeat.Err())
	}
	if l.sessionHeartbeat != nil && l.sessionHeartbeat.Err() != nil {
		return fmt.Errorf("%w: %v", ErrSessionLeaseLost, l.sessionHeartbeat.Err())
	}
	return nil
}

func (l *runtimeExecutionLeases) FencingToken() uint64 {
	if l == nil || l.sessionHeartbeat == nil {
		return 0
	}
	return l.sessionHeartbeat.Lease().FencingToken
}

func (l *runtimeExecutionLeases) Complete() error {
	if l == nil {
		return nil
	}
	if l.messageHeartbeat != nil {
		l.messageHeartbeat.Stop()
		if err := l.messageHeartbeat.Err(); err != nil {
			return fmt.Errorf("message idempotency lease lost: %w", err)
		}
	}
	finalizeCtx, cancel := context.WithTimeout(context.Background(), finalizationTimeout)
	defer cancel()
	if err := l.runtime.idempotency.Complete(finalizeCtx, l.messageLease, l.runtime.completedTTL); err != nil {
		return err
	}
	l.mu.Lock()
	l.completed = true
	l.mu.Unlock()
	return nil
}

func (l *runtimeExecutionLeases) Close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	completed := l.completed
	l.mu.Unlock()
	if l.messageHeartbeat != nil {
		l.messageHeartbeat.Stop()
	}

	if l.sessionHeartbeat != nil {
		l.sessionHeartbeat.Stop()
		finalizeCtx, cancel := context.WithTimeout(context.Background(), finalizationTimeout)
		_ = l.runtime.stateStore.ReleaseSessionExecutionLease(finalizeCtx, l.sessionHeartbeat.Lease())
		cancel()
	}
	if l.claimHeartbeat != nil {
		l.claimHeartbeat.Stop()
	}
	if completed {
		return
	}
	finalizeCtx, cancel := context.WithTimeout(context.Background(), finalizationTimeout)
	defer cancel()
	_ = l.runtime.idempotency.Release(finalizeCtx, l.messageLease)
	if l.dedupActive && l.runtime.executionDedup != nil {
		_ = l.runtime.executionDedup.Abort(finalizeCtx, l.snapshot.Config.TenantID, string(l.inbound.Channel), l.bindingID, l.inbound.MessageID, l.traceID)
	}
}
