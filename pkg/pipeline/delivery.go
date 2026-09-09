package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/channel"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

// DeliveryConfig bounds delivery concurrency, claims and retries.
type DeliveryConfig struct {
	Owner               string
	Concurrency         int
	PollInterval        time.Duration
	LeaseDuration       time.Duration
	DeliveryTimeout     time.Duration
	RetryBase           time.Duration
	RetryMaximum        time.Duration
	ExpiryReapInterval  time.Duration
	ExpiryReapBatchSize int
}

// Delivery sends durable Outbox messages through tenant Channel Adapters.
type Delivery struct {
	store    reliable.Store
	tenants  tenant.Service
	adapters *channel.AdapterRegistry
	config   DeliveryConfig
}

// NewDelivery validates dependencies and applies safe defaults.
func NewDelivery(store reliable.Store, tenants tenant.Service, adapters *channel.AdapterRegistry, config DeliveryConfig) (*Delivery, error) {
	if isNilPipelineDependency(store) || isNilPipelineDependency(tenants) || isNilPipelineDependency(adapters) {
		return nil, fmt.Errorf("delivery requires store, tenant service and adapter registry")
	}
	if config.Owner == "" {
		return nil, fmt.Errorf("delivery owner is required")
	}
	if config.Concurrency <= 0 {
		config.Concurrency = 4
	}
	if err := validateDerivedOwner(config.Owner, config.Concurrency); err != nil {
		return nil, fmt.Errorf("delivery owner: %w", err)
	}
	if _, ok := store.(reliable.OutboxDispatchFence); !ok {
		return nil, fmt.Errorf("delivery store must implement outbox dispatch fence")
	}
	if config.PollInterval <= 0 {
		config.PollInterval = 250 * time.Millisecond
	}
	if config.LeaseDuration <= 0 {
		config.LeaseDuration = time.Minute
	}
	if config.DeliveryTimeout <= 0 {
		config.DeliveryTimeout = 30 * time.Second
	}
	if config.LeaseDuration < minimumSafeLease(config.DeliveryTimeout) {
		return nil, fmt.Errorf(
			"delivery lease duration %s must be at least %s for delivery timeout %s",
			config.LeaseDuration,
			minimumSafeLease(config.DeliveryTimeout),
			config.DeliveryTimeout,
		)
	}
	if config.RetryBase <= 0 {
		config.RetryBase = time.Second
	}
	if config.RetryMaximum <= 0 {
		config.RetryMaximum = 10 * time.Minute
	}
	if config.RetryMaximum < config.RetryBase {
		return nil, fmt.Errorf(
			"delivery retry maximum %s must be at least retry base %s",
			config.RetryMaximum, config.RetryBase,
		)
	}
	interval, batchSize, err := normalizeExpiryReapConfig(config.ExpiryReapInterval, config.ExpiryReapBatchSize)
	if err != nil {
		return nil, fmt.Errorf("delivery expiry reaper: %w", err)
	}
	config.ExpiryReapInterval = interval
	config.ExpiryReapBatchSize = batchSize
	return &Delivery{store: store, tenants: tenants, adapters: adapters, config: config}, nil
}

// Run starts a fixed-size delivery pool and drains claimed sends on shutdown.
func (d *Delivery) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var wg sync.WaitGroup
	for i := 0; i < d.config.Concurrency; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			d.loop(ctx, fmt.Sprintf("%s-%d", d.config.Owner, index))
		}(i)
	}
	if reaper, ok := d.store.(reliable.ExpiredWorkReaper); ok && !isNilPipelineDependency(reaper) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runExpiryReaper(ctx, "delivery", reaper, d.config.ExpiryReapInterval, d.config.ExpiryReapBatchSize)
		}()
	}
	<-ctx.Done()
	wg.Wait()
	return nil
}

func (d *Delivery) loop(ctx context.Context, owner string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		msg, err := d.store.ClaimOutbox(ctx, owner, d.config.LeaseDuration)
		if errors.Is(err, reliable.ErrNoWork) {
			if !wait(ctx, d.config.PollInterval) {
				return
			}
			continue
		}
		if err != nil {
			log.Printf("delivery owner=%s claim failed: error=%s", owner, stablePipelineError(err))
			if !wait(ctx, d.config.PollInterval) {
				return
			}
			continue
		}
		deliveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), d.config.DeliveryTimeout)
		d.deliverOne(deliveryCtx, msg)
		cancel()
	}
}

func (d *Delivery) deliverOne(ctx context.Context, msg *reliable.OutboxMessage) {
	if msg == nil {
		log.Printf("delivery received a nil Outbox message")
		return
	}
	ctx = nonNilContext(ctx)
	started, result := time.Now(), "error"
	defer func() { observePipeline("delivery", result, msg.TenantID, started, msg.CreatedAt) }()
	ctx = telemetry.ContextWithTraceParent(ctx, msg.TraceParent)
	ctx, span := telemetry.StartOperation(ctx, telemetry.OperationOutboxDeliver)
	defer func() {
		if result == "error" {
			telemetry.EndOperation(span, errors.New("outbox delivery failed"))
			return
		}
		telemetry.EndOperation(span, nil)
	}()
	// Delivery only needs the destination channel credentials. The scoped
	// capability is mandatory so model/storage and unrelated channel secrets do
	// not enter the egress process.
	t, err := resolveDeliveryTenant(ctx, d.tenants, msg)
	if err != nil || t == nil {
		if err == nil && t == nil {
			err = fmt.Errorf("tenant lookup returned nil")
		}
		if errors.Is(err, tenant.ErrTenantNotFound) {
			d.deadLetter(ctx, msg, fmt.Errorf("tenant no longer exists: %w", err))
			return
		}
		d.retry(ctx, msg, fmt.Errorf("tenant unavailable: %w", err), 0)
		return
	}
	if t.Status == tenant.TenantStatusSuspended {
		d.block(ctx, msg, fmt.Errorf("tenant is suspended"))
		return
	}
	if t.Status == tenant.TenantStatusDeleted {
		d.deadLetter(ctx, msg, fmt.Errorf("tenant is deleted"))
		return
	}
	if t.Status != tenant.TenantStatusActive {
		d.deadLetter(ctx, msg, fmt.Errorf("tenant has unsupported status %q", t.Status))
		return
	}

	var binding *tenant.ChannelBinding
	for i := range t.Channels {
		candidate := &t.Channels[i]
		if candidate.Type == msg.ChannelType && candidate.EnsureAccountID() == msg.ChannelAccountID {
			binding = candidate
			break
		}
	}
	if binding == nil {
		d.deadLetter(ctx, msg, fmt.Errorf("channel binding %s/%s not found", msg.ChannelType, msg.ChannelAccountID))
		return
	}
	adapter, ok := d.adapters.Get(channel.ChannelType(msg.ChannelType))
	if !ok {
		d.deadLetter(ctx, msg, fmt.Errorf("channel adapter %s not registered", msg.ChannelType))
		return
	}
	outbound := &channel.OutboundMessage{
		ConversationID: msg.ConversationID,
		Content:        msg.Content,
		ContentType:    msg.ContentType,
		ReplyToID:      msg.ReplyToID,
		DeliveryID:     outboxDeliveryID(msg.ID, msg.DeliveryCursor),
		Metadata: map[string]string{
			"traceparent":     msg.TraceParent,
			"delivery_cursor": strconv.Itoa(msg.DeliveryCursor),
			"delivery_id":     outboxDeliveryID(msg.ID, msg.DeliveryCursor),
		},
	}
	fence, ok := d.store.(reliable.OutboxDispatchFence)
	if !ok {
		d.block(context.WithoutCancel(ctx), msg, fmt.Errorf("delivery store lacks outbox dispatch fence"))
		return
	}
	fenceCtx, cancelFence := persistenceContext(ctx)
	fenceErr := fence.MarkDispatchStarted(fenceCtx, msg.ID, msg.Lease)
	cancelFence()
	if fenceErr != nil {
		log.Printf("delivery outbox=%d dispatch fence failed: error=%s", msg.ID, stablePipelineError(fenceErr))
		if errors.Is(fenceErr, reliable.ErrStaleLease) {
			pipelineFenceRejects.WithLabelValues("delivery").Inc()
		}
		return
	}
	if err := adapter.SendReply(ctx, binding, outbound); err != nil {
		cause := fmt.Errorf("channel delivery: %w", err)
		// Telegram and WeChat Work do not expose a provider-side idempotency
		// key for ordinary message sends. If the request may have reached the
		// provider but its outcome is unknown, ordinary retry can create a
		// duplicate user-visible message. Stop the durable row and require an
		// operator/provider-side reconciliation instead.
		if channel.DeliveryOutcomeUnknown(err) {
			d.block(context.WithoutCancel(ctx), msg, cause)
			return
		}
		permanent, retryAfter := channel.DeliveryFailure(err)
		if permanent {
			d.deadLetter(context.WithoutCancel(ctx), msg, cause)
		} else {
			d.retry(context.WithoutCancel(ctx), msg, cause, retryAfter)
		}
		return
	}
	nextCursor, complete, err := channel.OutboundDeliveryProgress(outbound)
	if err != nil {
		// The Provider call already returned nil, so an Adapter protocol error
		// cannot prove that no side effect occurred. Preserve the durable fence
		// and require reconciliation instead of treating it as a safe permanent
		// rejection.
		d.block(context.WithoutCancel(ctx), msg, fmt.Errorf("channel adapter delivery progress is unknown: %w", err))
		return
	}
	// One successful provider call must advance exactly one chunk. Without this
	// check a buggy/custom adapter could report completion (or jump ahead) and
	// cause the durable Outbox to mark unsent content as delivered. Because the
	// Provider call already returned nil, an invalid cursor is an unknown
	// outcome and must be reconciled rather than dead-lettered as safe failure.
	if nextCursor != msg.DeliveryCursor+1 {
		d.block(context.WithoutCancel(ctx), msg, fmt.Errorf(
			"channel adapter contract: delivery cursor advanced from %d to %d",
			msg.DeliveryCursor, nextCursor,
		))
		return
	}
	persistCtx, cancelPersist := persistenceContext(ctx)
	defer cancelPersist()
	if !complete {
		if err := d.store.AdvanceOutbox(persistCtx, msg.ID, msg.Lease, nextCursor); err != nil {
			log.Printf("delivery outbox=%d failed to persist chunk cursor: error=%s", msg.ID, stablePipelineError(err))
			if errors.Is(err, reliable.ErrStaleLease) {
				pipelineFenceRejects.WithLabelValues("delivery").Inc()
			}
			return
		}
		result = "chunk_sent"
		return
	}
	if err := d.store.MarkDelivered(persistCtx, msg.ID, msg.Lease); err != nil {
		// Delivery is at-least-once. A provider-side idempotency key should be
		// used when available because a crash here can cause a duplicate send.
		log.Printf("delivery outbox=%d fenced completion failed after send: error=%s", msg.ID, stablePipelineError(err))
		if errors.Is(err, reliable.ErrStaleLease) {
			pipelineFenceRejects.WithLabelValues("delivery").Inc()
		}
		return
	}
	result = "success"
}

func resolveDeliveryTenant(ctx context.Context, service tenant.Service, msg *reliable.OutboxMessage) (*tenant.Tenant, error) {
	if msg == nil {
		return nil, fmt.Errorf("outbox message is required")
	}
	if scoped, ok := service.(tenant.ChannelScopedReader); ok {
		return scoped.GetTenantChannelScoped(ctx, msg.TenantID, msg.ChannelType, msg.ChannelAccountID)
	}
	return nil, tenant.ErrScopedTenantReadUnavailable
}

// outboxDeliveryID is stable for the lifetime of one Outbox chunk. It is not
// a provider message ID; it is an opaque, non-secret correlation key that an
// idempotency-aware Adapter or egress proxy can use to collapse retries.
func outboxDeliveryID(outboxID int64, cursor int) string {
	return fmt.Sprintf("trpc-outbox-v1-%d-%d", outboxID, cursor)
}

func (d *Delivery) retry(ctx context.Context, msg *reliable.OutboxMessage, cause error, providerDelay time.Duration) {
	pipelineRetries.WithLabelValues("delivery", telemetry.MetricTenantLabel(msg.TenantID)).Inc()
	delay := providerDelay
	if delay <= 0 {
		jitter := reliable.DeterministicJitter(msg.ID, d.config.RetryBase)
		delay = reliable.Backoff(msg.AttemptCount, d.config.RetryBase, d.config.RetryMaximum, jitter)
	}
	if delay > d.config.RetryMaximum {
		delay = d.config.RetryMaximum
	}
	clockSafe, ok := d.store.(reliable.RetryAfterStore)
	if !ok {
		// A node wall clock is not an authority for a distributed retry
		// deadline. Park the row for reconciliation instead of silently
		// scheduling it using a potentially skewed local clock.
		d.block(ctx, msg, fmt.Errorf("retry-after persistence is unavailable"))
		return
	}
	persistCtx, cancelPersist := persistenceContext(ctx)
	defer cancelPersist()
	err := clockSafe.RetryOutboxAfter(persistCtx, msg.ID, msg.Lease, cause, delay, msg.DeliveryCursor)
	if err != nil {
		log.Printf("delivery outbox=%d fenced retry failed: cause=%s error=%s", msg.ID, stablePipelineError(cause), stablePipelineError(err))
		if errors.Is(err, reliable.ErrStaleLease) {
			pipelineFenceRejects.WithLabelValues("delivery").Inc()
		}
	}
}

func (d *Delivery) deadLetter(ctx context.Context, msg *reliable.OutboxMessage, cause error) {
	persistCtx, cancelPersist := persistenceContext(ctx)
	defer cancelPersist()
	if err := d.store.DeadLetterOutbox(persistCtx, msg.ID, msg.Lease, cause); err != nil {
		log.Printf("delivery outbox=%d permanent failure could not be dead-lettered: cause=%s error=%s", msg.ID, stablePipelineError(cause), stablePipelineError(err))
		if errors.Is(err, reliable.ErrStaleLease) {
			pipelineFenceRejects.WithLabelValues("delivery").Inc()
		}
		return
	}
	pipelineDeadLetters.WithLabelValues("delivery", telemetry.MetricTenantLabel(msg.TenantID)).Inc()
}

func (d *Delivery) block(ctx context.Context, msg *reliable.OutboxMessage, cause error) {
	persistCtx, cancelPersist := persistenceContext(ctx)
	defer cancelPersist()
	if blocker, ok := d.store.(reliable.OutboxBlocker); ok {
		if err := blocker.BlockOutbox(persistCtx, msg.ID, msg.Lease, cause); err != nil {
			log.Printf("delivery outbox=%d reconciliation block failed: cause=%s error=%s", msg.ID, stablePipelineError(cause), stablePipelineError(err))
			if errors.Is(err, reliable.ErrStaleLease) {
				pipelineFenceRejects.WithLabelValues("delivery").Inc()
			}
		}
		return
	}
	// External Store implementations predating the reconciliation state cannot
	// represent a blocked row. Fail closed into their existing audited DLQ, but
	// keep the reason generic because this path also handles unknown Provider
	// outcomes, not only tenant suspension.
	d.deadLetter(persistCtx, msg, fmt.Errorf("store lacks outbox reconciliation state: %w", cause))
}
