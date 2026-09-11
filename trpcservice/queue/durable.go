package queue

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/delivery"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
	"github.com/cyl6/trpc-agent-service/trpcservice/metrics"
	"github.com/cyl6/trpc-agent-service/trpcservice/sessionturn"
	"github.com/cyl6/trpc-agent-service/trpcservice/store"
	"github.com/cyl6/trpc-agent-service/trpcservice/tenant"
	"github.com/cyl6/trpc-agent-service/trpcservice/tenant/governance"
	"github.com/cyl6/trpc-agent-service/trpcservice/tooloperation"
	"github.com/cyl6/trpc-agent-service/trpcservice/worker"
)

// Durable is a Dispatcher backed by the persistent Inbox/Outbox store:
//
//	IM callback -> Submit -> Inbox INSERT (persist before ACK)
//	relay loop  -> lease inbox row -> run Agent (Deliver=false)
//	           -> atomic: inbox processed + outbox pending
//	sender loop -> lease outbox row -> adapter deliver -> sent / retry
//
// A crash between any two steps loses no acknowledged message: the inbox row
// survives, an expired lease is reclaimed, and the coordinator's pending
// result lets the re-run skip Agent execution and go straight to completion.
type Durable struct {
	store     store.Store
	processor Processor
	resolver  BindingResolver
	deliverer Deliverer
	metrics   *metrics.Metrics
	opts      DurableOptions

	cancel    context.CancelFunc
	stopped   chan struct{}
	startOnce sync.Once
	closeOnce sync.Once
	closeErr  error
	wg        sync.WaitGroup
	running   atomic.Bool
}

type inboxTurnCommitParticipant struct {
	durable   *Durable
	completer store.InboxBatchTxCompleter
	record    store.InboxRecord
	owner     string
	binding   config.ChannelConfig
	stopLease func()
	committed atomic.Bool
}

func (p *inboxTurnCommitParticipant) CompleteInTransaction(
	ctx context.Context,
	tx pgx.Tx,
	plan worker.AtomicDeliveryPlan,
) error {
	var (
		outboxes []store.OutboxRecord
		err      error
	)
	if plan.Version > 0 {
		outboxes, err = p.durable.outboxesFromMessages(ctx, &p.record, plan.Parts)
	} else {
		// Version zero is the rolling-upgrade compatibility path for replay
		// payloads written before delivery plans were persisted.
		outboxes, err = p.durable.planOutboxes(ctx, &p.record, p.binding, &plan.Outbound)
	}
	if err != nil {
		return err
	}
	return p.completer.CompleteInboxBatchTx(ctx, tx, store.InboxLeaseFence{
		InboxID: p.record.InboxID, Owner: p.owner, AttemptCount: p.record.AttemptCount,
		TenantID: p.record.TenantID, ChannelType: p.record.ChannelType,
		BindingID: p.record.BindingID, DedupKey: p.record.DedupKey,
		PartitionKey:          p.record.PartitionKey,
		PipelineSchemaVersion: p.record.PipelineSchemaVersion,
		AtomicCommitMode:      p.record.AtomicCommitMode, DatabaseIdentity: p.record.DatabaseIdentity,
	}, outboxes)
}

func (p *inboxTurnCommitParticipant) MarkCommitted() {
	p.committed.Store(true)
	if p.stopLease != nil {
		p.stopLease()
	}
}

func (p *inboxTurnCommitParticipant) Committed() bool {
	return p != nil && p.committed.Load()
}

// BindingResolver resolves the live channel binding config for an outbox row.
// It is satisfied by *tenant.Registry.
type BindingResolver interface {
	ResolveBindingForTenant(tenantID, channelType, bindingID string) (tenant.Binding, error)
}

// Deliverer plans deterministic provider operations and sends exactly one
// operation per call. It is satisfied by *worker.Service.
type Deliverer interface {
	PlanDelivery(binding config.ChannelConfig, msg domain.OutboundMessage) ([]delivery.Part, error)
	DeliverOperation(ctx context.Context, binding config.ChannelConfig, request delivery.Request) delivery.Result
}

// UncertainOutbox is the redacted administrative view of an operation whose
// provider result cannot be proven. Message content and routing target are
// intentionally absent.
type UncertainOutbox struct {
	OutboxID          string `json:"outbox_id"`
	OperationKey      string `json:"operation_key"`
	TenantID          string `json:"tenant_id"`
	ChannelType       string `json:"channel_type"`
	BindingID         string `json:"binding_id"`
	PartIndex         int    `json:"part_index"`
	PartCount         int    `json:"part_count"`
	AttemptNo         int    `json:"attempt_no"`
	StateVersion      int64  `json:"state_version"`
	ErrorType         string `json:"error_type,omitempty"`
	ProviderCode      string `json:"provider_code,omitempty"`
	ProviderMessageID string `json:"provider_message_id,omitempty"`
	ProviderRequestID string `json:"provider_request_id,omitempty"`
}

// ResolveOutboxRequest is an explicit, audited acceptance of the duplicate
// or loss trade-off for a parked unknown operation.
type ResolveOutboxRequest struct {
	ResolutionID    string `json:"resolution_id"`
	OutboxID        string `json:"-"`
	ExpectedVersion int64  `json:"expected_version"`
	ExpectedAttempt int    `json:"expected_attempt"`
	Action          string `json:"action"`
	Reason          string `json:"reason"`
	Actor           string `json:"-"`
}

// OutboxOperator is implemented only by the durable dispatcher and is used by
// the authenticated admin API.
type OutboxOperator interface {
	ListUncertainOutbox(context.Context, int) ([]UncertainOutbox, error)
	ResolveOutbox(context.Context, ResolveOutboxRequest) error
}

type DurableOptions struct {
	PollInterval      time.Duration
	LeaseTTL          time.Duration
	BatchSize         int
	InboxMaxAttempts  int
	OutboxMaxAttempts int
	RetryBase         time.Duration
	RetryMax          time.Duration
	WorkerCount       int
}

func (o *DurableOptions) fillDefaults() {
	if o.PollInterval <= 0 {
		o.PollInterval = 250 * time.Millisecond
	}
	if o.LeaseTTL <= 0 {
		o.LeaseTTL = 2 * time.Minute
	}
	if o.BatchSize <= 0 {
		o.BatchSize = 16
	}
	if o.InboxMaxAttempts <= 0 {
		o.InboxMaxAttempts = 5
	}
	if o.OutboxMaxAttempts <= 0 {
		o.OutboxMaxAttempts = 8
	}
	if o.RetryBase <= 0 {
		o.RetryBase = 500 * time.Millisecond
	}
	if o.RetryMax <= 0 {
		o.RetryMax = time.Minute
	}
	if o.WorkerCount <= 0 {
		o.WorkerCount = 2
	}
}

func NewDurable(st store.Store, processor Processor, resolver BindingResolver, deliverer Deliverer, exporter *metrics.Metrics, opts DurableOptions) *Durable {
	opts.fillDefaults()
	if exporter == nil {
		exporter = metrics.NewMetrics()
	}
	return &Durable{
		store: st, processor: processor, resolver: resolver, deliverer: deliverer,
		metrics: exporter, opts: opts,
		stopped: make(chan struct{}),
	}
}

// Submit persists the message into the Inbox and returns only after the row
// is durable, so the gateway ACKs after persistence, not after processing.
// Platform redeliveries (same dedup key) are reported as success.
func (d *Durable) Submit(ctx context.Context, task worker.Task) error {
	// Normalize trust-boundary identity before both deduplication and session
	// partitioning. Adapters are not allowed to choose a tenant or binding.
	task.Message.TenantID = task.Tenant.TenantID
	task.Message.BindingID = task.Binding.BindingID
	task.Message.Channel = task.Binding.Type
	if task.ConfigRevision == "" {
		task.ConfigRevision = task.Tenant.Version
	}
	if task.ConfigRevision != "" && task.Tenant.Version != "" && task.ConfigRevision != task.Tenant.Version {
		return worker.ErrTaskRevisionMismatch
	}
	// Delivery is owned by the Outbox sender; the relay always runs the task
	// with Deliver=false and forwards the result text via the outbox record.
	task.Deliver = false
	if err := d.stampDurablePipeline(&task); err != nil {
		return fmt.Errorf("prepare durable pipeline: %w", err)
	}
	payload, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("encode task payload: %w", err)
	}
	rec := &store.InboxRecord{
		InboxID:           uuid.NewString(),
		TenantID:          task.Tenant.TenantID,
		ChannelType:       task.Binding.Type,
		BindingID:         task.Binding.BindingID,
		ExternalMessageID: task.Message.ExternalMessageID,
		DedupKey:          worker.MessageDedupKey(task.Message),
		PartitionKey:      domain.SessionPartitionKey(task.Message, task.Tenant.App.Name),
		Payload:           payload,
		TraceCarrier:      task.TraceCarrier,
	}
	mirrorPipelineOnInbox(rec, task)
	if err := d.store.InsertInbox(ctx, rec, time.Now()); err != nil {
		if errors.Is(err, store.ErrDuplicate) {
			return nil
		}
		return err
	}
	return nil
}

// Start launches the reclaim, relay and sender loops. Call it once after
// construction; Close stops them.
func (d *Durable) Start() {
	d.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		d.cancel = cancel
		d.running.Store(true)
		relayWorkers := d.opts.WorkerCount
		if d.opts.BatchSize < relayWorkers {
			relayWorkers = d.opts.BatchSize
		}
		d.wg.Add(relayWorkers + 2)
		go func() {
			defer d.wg.Done()
			d.reclaimLoop(ctx)
		}()
		for i := 0; i < relayWorkers; i++ {
			go func() {
				defer d.wg.Done()
				d.relayLoop(ctx)
			}()
		}
		go func() {
			defer d.wg.Done()
			d.senderLoop(ctx)
		}()
		go func() {
			d.wg.Wait()
			close(d.stopped)
		}()
	})
}

func (d *Durable) Close() error {
	d.closeOnce.Do(func() {
		d.running.Store(false)
		// Close-before-Start is legal and must not block forever.
		d.startOnce.Do(func() { close(d.stopped) })
		if d.cancel != nil {
			d.cancel()
		}
		<-d.stopped
		d.closeErr = d.store.Close()
	})
	return d.closeErr
}

// Ready verifies that the queue loops are active and the backing Store can
// answer a bounded query. Depth samples are exported as gauges while avoiding
// high-cardinality tenant/message labels.
func (d *Durable) Ready(ctx context.Context) error {
	if !d.running.Load() {
		return ErrClosed
	}
	runnableInbox, deadInbox, pendingOutbox, deadOutbox, uncertainOutbox, err := d.store.Depths(ctx)
	if err != nil {
		return errors.New("durable queue store unavailable")
	}
	help := "Current durable queue records by state group."
	d.metrics.Set("queue_depth", help, float64(runnableInbox), map[string]string{"component": "inbox_runnable"})
	d.metrics.Set("queue_depth", help, float64(deadInbox), map[string]string{"component": "inbox_dead"})
	d.metrics.Set("queue_depth", help, float64(pendingOutbox), map[string]string{"component": "outbox_pending"})
	d.metrics.Set("queue_depth", help, float64(deadOutbox), map[string]string{"component": "outbox_dead"})
	d.metrics.Set("queue_depth", help, float64(uncertainOutbox), map[string]string{"component": "outbox_uncertain"})
	return nil
}

func (d *Durable) reclaimLoop(ctx context.Context) {
	ticker := time.NewTicker(d.opts.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			inboxCount, outboxCount, err := d.store.ReclaimExpired(ctx, time.Now())
			if err != nil {
				log.Printf("durable queue reclaim failed: category=store_error")
				continue
			}
			if inboxCount > 0 {
				d.metrics.Add("queue_inbox_reclaimed_total", "Inbox leases reclaimed after expiry.", float64(inboxCount), nil)
			}
			if outboxCount > 0 {
				d.metrics.Add("queue_outbox_reclaimed_total", "Outbox leases reclaimed after expiry.", float64(outboxCount), nil)
			}
		}
	}
}

func (d *Durable) relayLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		worked := d.relayOnce(ctx)
		if !worked {
			select {
			case <-ctx.Done():
				return
			case <-time.After(d.opts.PollInterval):
			}
		}
	}
}

// relayOnce leases exactly one inbox row for this worker. Leasing more than
// the immediately available worker capacity starts the lease clock while
// records wait in a local batch, which can cause valid work to be reclaimed
// before processing begins.
func (d *Durable) relayOnce(ctx context.Context) bool {
	owner := uuid.NewString()
	records, err := d.store.LeaseInbox(ctx, owner, time.Now(), d.opts.LeaseTTL, 1)
	if err != nil {
		log.Printf("durable queue lease failed: category=store_error")
		time.Sleep(d.opts.PollInterval)
		return false
	}
	if len(records) == 0 {
		return false
	}
	d.processInbox(ctx, &records[0], owner)
	return true
}

func (d *Durable) processInbox(ctx context.Context, rec *store.InboxRecord, owner string) {
	var task worker.Task
	if err := json.Unmarshal(rec.Payload, &task); err != nil {
		// A payload the current binary cannot decode is permanently
		// undecodable; park it instead of retrying forever.
		_ = d.store.DeadLetterInbox(ctx, rec.InboxID, owner, "payload_decode_failed", time.Now())
		return
	}
	atomicCommit, pipelineErr := d.prepareAtomicParticipant(rec, &task, owner)
	if pipelineErr != nil {
		if pipelineFailureShouldPark(pipelineErr) {
			d.parkIncompatiblePipeline(ctx, rec, owner, pipelineErr)
			return
		}
		_ = d.store.DeadLetterInbox(ctx, rec.InboxID, owner, pipelineFailureCategory(pipelineErr), time.Now())
		d.metrics.Add("queue_inbox_pipeline_rejected_total", "Inbox records rejected by durable pipeline validation.", 1, map[string]string{"tenant": rec.TenantID})
		return
	}
	if task.Pipeline.IsLegacy() {
		d.metrics.Add("queue_inbox_legacy_pipeline_total", "Legacy Inbox records drained without combined atomic commit.", 1, map[string]string{"tenant": rec.TenantID})
	}
	if atomicCommit != nil {
		task.TurnCommitParticipant = atomicCommit
	}
	leaseCtx, stopRenewal := d.renewLease(ctx, func(renewCtx context.Context, now time.Time) error {
		err := d.store.RenewInboxLease(renewCtx, rec.InboxID, owner, now, d.opts.LeaseTTL)
		if err != nil && atomicCommit != nil && atomicCommit.Committed() {
			// The atomic Session transaction already consumed this lease. A
			// renewal racing just after COMMIT observes the processed row and is
			// expected to lose ownership; it must not cancel post-commit cleanup.
			return nil
		}
		return err
	})
	defer stopRenewal()
	if atomicCommit != nil {
		atomicCommit.stopLease = stopRenewal
	}
	task.Deliver = false
	taskCtx := otel.GetTextMapPropagator().Extract(leaseCtx, propagation.MapCarrier(rec.TraceCarrier))
	taskCtx, span := tracer.Start(taskCtx, "queue.inbox_process")
	defer span.End()

	result, err := d.processor.Process(taskCtx, task)
	if atomicCommit.Committed() {
		if err != nil {
			span.SetStatus(codes.Error, "inbox_post_commit_cleanup_failed")
			d.metrics.Add("queue_inbox_post_commit_failures_total", "Post-commit coordinator cleanup failures.", 1, map[string]string{"tenant": rec.TenantID})
		}
		d.metrics.Add("queue_inbox_processed_total", "Inbox messages processed.", 1, map[string]string{"tenant": rec.TenantID})
		return
	}
	if leaseCause := context.Cause(leaseCtx); leaseCause != nil {
		span.SetStatus(codes.Error, "inbox_lease_lost")
		return
	}
	if err != nil {
		disposition, retryAfter := worker.ProcessDispositionOf(err)
		switch disposition {
		case worker.ProcessTerminalIgnored:
			// Policy rejection is a successful terminal consumption of the Inbox:
			// there is deliberately no Outbox reply and no Agent state to commit.
			if completeErr := d.store.CompleteInboxBatch(leaseCtx, rec.InboxID, owner, nil, time.Now()); completeErr != nil {
				if !errors.Is(completeErr, store.ErrLeaseLost) {
					d.metrics.Add("queue_inbox_complete_failures_total", "Inbox completion failures.", 1, map[string]string{"tenant": rec.TenantID})
					d.failInbox(leaseCtx, rec, owner, completeErr)
				}
				return
			}
			d.metrics.Add("queue_inbox_ignored_total", "Inbox messages consumed by terminal policy decisions.", 1, map[string]string{"tenant": rec.TenantID})
			d.metrics.Add("queue_inbox_processed_total", "Inbox messages processed.", 1, map[string]string{"tenant": rec.TenantID})
		case worker.ProcessDeadLetter:
			span.SetStatus(codes.Error, "inbox_dead_letter")
			if deadErr := d.store.DeadLetterInbox(leaseCtx, rec.InboxID, owner, errorCategory(err), time.Now()); deadErr == nil {
				d.metrics.Add("queue_inbox_dead_total", "Inbox messages dead-lettered.", 1, map[string]string{"tenant": rec.TenantID})
			}
		case worker.ProcessBlocked:
			pipelineBlocked := isPipelineBlocked(err)
			if pipelineBlocked {
				span.SetStatus(codes.Error, "inbox_pipeline_blocked")
			} else {
				span.SetStatus(codes.Error, "inbox_processing_blocked")
			}
			d.parkInboxAfter(leaseCtx, rec, owner, errorCategory(err), retryAfter, pipelineBlocked)
		default:
			span.SetStatus(codes.Error, "inbox_retry")
			d.metrics.Add("queue_inbox_retries_total", "Inbox processing failures.", 1, map[string]string{"tenant": rec.TenantID})
			d.failInboxAfter(leaseCtx, rec, owner, err, retryAfter)
		}
		return
	}

	outboxes, planErr := d.planOutboxes(taskCtx, rec, task.Binding, result.Outbound)
	if planErr != nil {
		d.failInbox(leaseCtx, rec, owner, planErr)
		return
	}
	if err := d.store.CompleteInboxBatch(leaseCtx, rec.InboxID, owner, outboxes, time.Now()); err != nil {
		// Lease lost means another worker owns the row now; nothing to do.
		if !errors.Is(err, store.ErrLeaseLost) {
			d.metrics.Add("queue_inbox_complete_failures_total", "Inbox completion failures.", 1, map[string]string{"tenant": rec.TenantID})
			// The Agent result is retained by the coordinator, so re-arm the
			// Inbox immediately instead of waiting for lease expiry. The retry
			// reconstructs the same Outbox without another model run.
			d.failInbox(leaseCtx, rec, owner, err)
		}
		return
	}
	d.metrics.Add("queue_inbox_processed_total", "Inbox messages processed.", 1, map[string]string{"tenant": rec.TenantID})
}

func (d *Durable) planOutboxes(
	ctx context.Context,
	rec *store.InboxRecord,
	binding config.ChannelConfig,
	outbound *domain.OutboundMessage,
) ([]store.OutboxRecord, error) {
	if outbound == nil {
		return nil, nil
	}
	parts, err := d.deliverer.PlanDelivery(binding, *outbound)
	if err != nil {
		return nil, err
	}
	if len(parts) == 0 {
		return nil, errors.New("delivery plan is empty")
	}
	messages := make([]domain.OutboundMessage, 0, len(parts))
	for _, part := range parts {
		messages = append(messages, part.Message)
	}
	return d.outboxesFromMessages(ctx, rec, messages)
}

func (d *Durable) outboxesFromMessages(
	ctx context.Context,
	rec *store.InboxRecord,
	messages []domain.OutboundMessage,
) ([]store.OutboxRecord, error) {
	if len(messages) == 0 {
		return nil, errors.New("delivery plan is empty")
	}
	carrier := propagation.MapCarrier{}
	// Persist only W3C trace headers. Baggage can contain application data and
	// is intentionally not copied into the durable Outbox.
	propagation.TraceContext{}.Inject(ctx, carrier)
	traceID := ""
	if spanContext := trace.SpanContextFromContext(ctx); spanContext.IsValid() {
		traceID = spanContext.TraceID().String()
	}
	outboxes := make([]store.OutboxRecord, 0, len(messages))
	for index, message := range messages {
		payload, err := json.Marshal(message)
		if err != nil {
			return nil, err
		}
		operationKey := deliveryOperationKey(rec.DedupKey, index)
		payloadDigest := sha256.Sum256(payload)
		outboxes = append(outboxes, store.OutboxRecord{
			OutboxID:     uuid.NewSHA1(uuid.NameSpaceOID, []byte(operationKey)).String(),
			OperationKey: operationKey, OperationVersion: 1,
			PartIndex: index, PartCount: len(messages), PayloadHash: fmt.Sprintf("%x", payloadDigest[:]),
			TenantID: rec.TenantID, ChannelType: rec.ChannelType, BindingID: rec.BindingID,
			DedupKey: operationKey, PartitionKey: rec.PartitionKey, Payload: payload,
			TraceID: traceID, TraceCarrier: map[string]string(carrier),
			Status: store.OutboxPending, DeliveryState: store.DeliveryPending,
		})
	}
	return outboxes, nil
}

func (d *Durable) failInbox(ctx context.Context, rec *store.InboxRecord, owner string, err error) {
	d.failInboxAfter(ctx, rec, owner, err, 0)
}

func (d *Durable) parkInboxAfter(
	ctx context.Context,
	rec *store.InboxRecord,
	owner, errType string,
	retryAfter time.Duration,
	pipelineBlocked bool,
) {
	now := time.Now()
	if retryAfter <= 0 {
		retryAfter = d.opts.RetryMax
	}
	if retryAfter <= 0 {
		retryAfter = time.Minute
	}
	if storeErr := d.store.RetryInbox(ctx, rec.InboxID, owner, errType, now, now.Add(retryAfter)); storeErr == nil {
		if pipelineBlocked {
			d.metrics.Add("queue_inbox_pipeline_blocked_total", "Inbox records parked for a compatible pipeline worker.", 1, map[string]string{"tenant": rec.TenantID})
		} else {
			d.metrics.Add("queue_inbox_processing_blocked_total", "Inbox records parked for explicit reconciliation without consuming attempt budget.", 1, map[string]string{
				"tenant": rec.TenantID, "result": errType,
			})
		}
	}
}

func isPipelineBlocked(err error) bool {
	return errors.Is(err, worker.ErrFuturePipelineVersion) ||
		errors.Is(err, worker.ErrAtomicDatabaseMismatch) ||
		errors.Is(err, worker.ErrAtomicCommitUnavailable)
}

func (d *Durable) failInboxAfter(
	ctx context.Context,
	rec *store.InboxRecord,
	owner string,
	err error,
	retryAfter time.Duration,
) {
	errType := errorCategory(err)
	if rec.AttemptCount >= d.opts.InboxMaxAttempts {
		_ = d.store.DeadLetterInbox(ctx, rec.InboxID, owner, errType, time.Now())
		d.metrics.Add("queue_inbox_dead_total", "Inbox messages dead-lettered.", 1, map[string]string{"tenant": rec.TenantID})
		return
	}
	now := time.Now()
	delay := d.backoff(rec.AttemptCount)
	if retryAfter > delay {
		delay = retryAfter
	}
	retryAt := now.Add(delay)
	if storeErr := d.store.RetryInbox(ctx, rec.InboxID, owner, errType, now, retryAt); storeErr != nil && !errors.Is(storeErr, store.ErrLeaseLost) {
		log.Printf("durable queue inbox retry failed: category=store_error")
	}
}

func (d *Durable) senderLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		owner := uuid.NewString()
		records, err := d.store.LeaseOutbox(ctx, owner, time.Now(), d.opts.LeaseTTL, 1)
		if err != nil {
			log.Printf("durable queue outbox lease failed: category=store_error")
			time.Sleep(d.opts.PollInterval)
			continue
		}
		if len(records) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(d.opts.PollInterval):
			}
			continue
		}
		d.deliverOutbox(ctx, &records[0], owner)
	}
}

func (d *Durable) deliverOutbox(ctx context.Context, rec *store.OutboxRecord, owner string) {
	leaseCtx, stopRenewal := d.renewLease(ctx, func(renewCtx context.Context, now time.Time) error {
		return d.store.RenewOutboxLease(renewCtx, rec.OutboxID, owner, now, d.opts.LeaseTTL)
	})
	defer stopRenewal()
	deliverCtx := otel.GetTextMapPropagator().Extract(leaseCtx, propagation.MapCarrier(rec.TraceCarrier))
	deliverCtx, span := tracer.Start(deliverCtx, "queue.outbox_deliver")
	defer span.End()
	var outbound domain.OutboundMessage
	if err := json.Unmarshal(rec.Payload, &outbound); err != nil {
		_ = d.store.DeadLetterOutbox(deliverCtx, rec.OutboxID, owner, "payload_decode_failed", time.Now())
		return
	}
	// Resolve the live binding so delivery uses the current token/secret; the
	// reply payload itself is immutable from the run that produced it.
	binding, err := d.resolver.ResolveBindingForTenant(rec.TenantID, rec.ChannelType, rec.BindingID)
	if err != nil {
		// A missing binding is usually configuration drift (tenant removed or
		// binding disabled). Retry within the attempt budget, then park.
		span.SetStatus(codes.Error, "binding_unresolved")
		d.failOutbox(deliverCtx, rec, owner, err)
		return
	}
	// This durable marker is the point of no automatic return. If the process
	// crashes after it commits, recovery must assume the provider may have
	// accepted the request and park the operation as unknown.
	if err := d.store.MarkOutboxDispatched(deliverCtx, rec.OutboxID, owner, rec.AttemptCount, time.Now()); err != nil {
		if !errors.Is(err, store.ErrLeaseLost) {
			log.Printf("durable queue outbox dispatch marker failed: category=store_error")
		}
		return
	}
	result := d.deliverer.DeliverOperation(deliverCtx, binding.Channel, delivery.Request{
		OperationKey: rec.OperationKey,
		AttemptNo:    rec.AttemptCount,
		Message:      outbound,
	})
	if context.Cause(leaseCtx) != nil {
		span.SetStatus(codes.Error, "outbox_lease_lost")
		return
	}
	if err := result.Validate(); err != nil {
		result = delivery.Result{Outcome: delivery.Unknown, ErrorType: "adapter_invalid_result", Err: err}
	}
	now := time.Now()
	retryDelay := d.backoff(rec.AttemptCount)
	if result.RetryAfter > retryDelay {
		retryDelay = result.RetryAfter
	}
	exhausted := result.Outcome == delivery.RetryableNotSent && rec.AttemptCount >= d.opts.OutboxMaxAttempts
	if err := d.store.FinishOutboxAttempt(
		deliverCtx, rec.OutboxID, owner, rec.AttemptCount, result,
		now, now.Add(retryDelay), exhausted,
	); err != nil {
		if !errors.Is(err, store.ErrLeaseLost) {
			log.Printf("durable queue outbox attempt finalize failed: category=store_error")
		}
		return
	}
	labels := map[string]string{"tenant": rec.TenantID, "channel": rec.ChannelType, "result": string(result.Outcome)}
	d.metrics.Add("queue_outbox_attempts_total", "Provider delivery operation attempts by outcome.", 1, labels)
	switch result.Outcome {
	case delivery.Confirmed:
		d.metrics.Add("queue_outbox_delivered_total", "Outbox replies delivered.", 1, map[string]string{"tenant": rec.TenantID})
	case delivery.RetryableNotSent:
		span.SetStatus(codes.Error, "delivery_retryable_not_sent")
		if exhausted {
			d.metrics.Add("queue_outbox_dead_total", "Outbox replies dead-lettered.", 1, map[string]string{"tenant": rec.TenantID})
		} else {
			d.metrics.Add("queue_outbox_retries_total", "Outbox delivery failures safe to retry.", 1, map[string]string{"tenant": rec.TenantID})
		}
	case delivery.PermanentRejected:
		span.SetStatus(codes.Error, "delivery_permanent_rejected")
		d.metrics.Add("queue_outbox_dead_total", "Outbox replies dead-lettered.", 1, map[string]string{"tenant": rec.TenantID})
	case delivery.Unknown:
		span.SetStatus(codes.Error, "delivery_unknown")
		d.metrics.Add("queue_outbox_uncertain_total", "Outbox operations parked because provider acceptance is unknown.", 1, map[string]string{"tenant": rec.TenantID, "channel": rec.ChannelType})
	}
}

func (d *Durable) failOutbox(ctx context.Context, rec *store.OutboxRecord, owner string, err error) {
	errType := errorCategory(err)
	if rec.AttemptCount >= d.opts.OutboxMaxAttempts {
		_ = d.store.DeadLetterOutbox(ctx, rec.OutboxID, owner, errType, time.Now())
		d.metrics.Add("queue_outbox_dead_total", "Outbox replies dead-lettered.", 1, map[string]string{"tenant": rec.TenantID})
		return
	}
	now := time.Now()
	retryAt := now.Add(d.backoff(rec.AttemptCount))
	if storeErr := d.store.RetryOutbox(ctx, rec.OutboxID, owner, errType, now, retryAt); storeErr != nil && !errors.Is(storeErr, store.ErrLeaseLost) {
		log.Printf("durable queue outbox retry failed: category=store_error")
	}
}

// renewLease keeps one in-flight record fenced for as long as processing or
// provider delivery legitimately takes. Any renewal failure cancels the work
// context so a stale attempt cannot commit after another worker takes over.
func (d *Durable) renewLease(parent context.Context, renew func(context.Context, time.Time) error) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(parent)
	stop := make(chan struct{})
	done := make(chan struct{})
	interval := d.opts.LeaseTTL / 3
	if interval <= 0 {
		interval = time.Millisecond
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-parent.Done():
				cancel(parent.Err())
				return
			case <-stop:
				return
			case <-ticker.C:
				renewCtx, cancelRenew := context.WithTimeout(parent, interval)
				err := renew(renewCtx, time.Now())
				cancelRenew()
				if err != nil {
					cancel(err)
					return
				}
			}
		}
	}()
	var once sync.Once
	return ctx, func() {
		once.Do(func() { close(stop) })
		<-done
	}
}

func (d *Durable) backoff(attempt int) time.Duration {
	shift := attempt
	if shift > 30 {
		shift = 30
	}
	delay := d.opts.RetryBase * time.Duration(math.Pow(2, float64(shift)))
	if delay > d.opts.RetryMax {
		delay = d.opts.RetryMax
	}
	return delay
}

func deliveryOperationKey(inboxDedupKey string, partIndex int) string {
	digest := sha256.Sum256([]byte("trpc.delivery.operation/v1\x1f" + inboxDedupKey + "\x1f" + strconv.Itoa(partIndex)))
	return fmt.Sprintf("op_v1_%x", digest[:])
}

// ListUncertainOutbox returns a content-free administrative projection.
func (d *Durable) ListUncertainOutbox(ctx context.Context, limit int) ([]UncertainOutbox, error) {
	records, err := d.store.ListUncertainOutbox(ctx, limit)
	if err != nil {
		return nil, err
	}
	result := make([]UncertainOutbox, 0, len(records))
	for _, rec := range records {
		result = append(result, UncertainOutbox{
			OutboxID: rec.OutboxID, OperationKey: rec.OperationKey,
			TenantID: rec.TenantID, ChannelType: rec.ChannelType, BindingID: rec.BindingID,
			PartIndex: rec.PartIndex, PartCount: rec.PartCount,
			AttemptNo: rec.AttemptCount, StateVersion: rec.StateVersion,
			ErrorType: rec.LastErrorType, ProviderCode: rec.ProviderCode,
			ProviderMessageID: rec.ProviderMessageID, ProviderRequestID: rec.ProviderRequestID,
		})
	}
	return result, nil
}

// ResolveOutbox applies the operator decision through the Store's version and
// attempt CAS, preventing two administrators from resolving the same unknown
// result differently.
func (d *Durable) ResolveOutbox(ctx context.Context, request ResolveOutboxRequest) error {
	err := d.store.ResolveOutbox(ctx, store.ResolveRequest{
		ResolutionID:    request.ResolutionID,
		OutboxID:        request.OutboxID,
		ExpectedVersion: request.ExpectedVersion,
		ExpectedAttempt: request.ExpectedAttempt,
		Action:          request.Action,
		Actor:           request.Actor,
		Reason:          request.Reason,
		Now:             time.Now(),
	})
	if err == nil {
		d.metrics.Add("queue_outbox_resolutions_total", "Manual unknown-operation resolutions.", 1, map[string]string{"result": request.Action})
	}
	return err
}

// errorCategory maps an arbitrary pipeline error to a stable label for the
// queue tables. Provider details may contain tokens and must not be stored.
func errorCategory(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	switch text := err.Error(); {
	case errors.Is(err, worker.ErrFuturePipelineVersion):
		return "future_pipeline_version"
	case errors.Is(err, worker.ErrAtomicDatabaseMismatch):
		return "atomic_database_mismatch"
	case errors.Is(err, worker.ErrAtomicCommitUnavailable):
		return "atomic_capability_unavailable"
	case errors.Is(err, worker.ErrPipelineMetadataMismatch), errors.Is(err, worker.ErrUnsupportedPipeline):
		return "unsupported_pipeline"
	case errors.Is(err, governance.ErrUserDenied):
		return "permission_denied"
	case errors.Is(err, governance.ErrInputTooLarge):
		return "input_too_large"
	case errors.Is(err, governance.ErrRateLimited):
		return "rate_limited"
	case errors.Is(err, governance.ErrBudgetExceeded):
		return "budget_exceeded"
	case errors.Is(err, sessionturn.ErrCorruptData):
		return "session_corrupt"
	case errors.Is(err, sessionturn.ErrInvalidRequest):
		return "session_invalid"
	case errors.Is(err, sessionturn.ErrTurnAborted):
		return "session_turn_aborted"
	case errors.Is(err, sessionturn.ErrVersionConflict), errors.Is(err, sessionturn.ErrFenceLost):
		return "session_conflict"
	case errors.Is(err, tooloperation.ErrOutcomeUnknown),
		errors.Is(err, tooloperation.ErrUnknownRequiresResolution):
		return "tool_outcome_unknown"
	case errors.Is(err, tooloperation.ErrOperationInProgress):
		return "tool_operation_in_progress"
	case errors.Is(err, tooloperation.ErrReplayRequired):
		return "tool_replay_required"
	case errors.Is(err, tenant.ErrBindingTenantMismatch):
		return "binding_tenant_mismatch"
	case errors.Is(err, tenant.ErrBindingNotFound):
		return "binding_unresolved"
	case strings.Contains(text, "binding_unresolved"):
		return "binding_unresolved"
	case strings.Contains(text, "token"):
		return "provider_auth"
	case strings.Contains(text, "429"):
		return "rate_limited"
	default:
		return "internal"
	}
}

var tracer = otel.Tracer("trpc-agent-service/queue")
