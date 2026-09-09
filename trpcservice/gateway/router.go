package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
	agenttrace "trpc.group/trpc-go/trpc-agent-go/telemetry/trace"

	platformlog "github.com/Violet2314/trpc-agent-service/trpcservice/log"
	platformmetrics "github.com/Violet2314/trpc-agent-service/trpcservice/metrics"
	"github.com/Violet2314/trpc-agent-service/trpcservice/reply"
	"github.com/Violet2314/trpc-agent-service/trpcservice/storage"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
	"github.com/Violet2314/trpc-agent-service/trpcservice/worker"
)

const (
	executionTimeout = 10 * time.Minute
	// markConsumedAttempts bounds retries of the consumed-marker so a transient
	// MySQL blip does not re-execute (and re-reply) a finished turn.
	markConsumedAttempts = 3
	markConsumedBackoff  = 100 * time.Millisecond
	// Inbound identifier caps sized to the tightest storage columns.
	maxMsgIDLength         = 64
	maxParticipantIDLength = 128
)

// drainReplyGrace bounds how long Drain waits for in-flight IM replies
// before cancelling them; it stays well inside main's shutdown budget.
// It is a variable so tests can shorten the grace window.
var drainReplyGrace = 10 * time.Second

// ReplyDispatcher resolves the inbound adapter and drains the Worker event
// stream into its platform-specific reply path.
//
// SPEC-GAP: the spec's Router constructor omitted the adapter registry needed
// by its flush pseudocode. This interface makes that dependency explicit.
type ReplyDispatcher interface {
	Dispatch(
		context.Context,
		tenant.Snapshot,
		string,
		InboundMessage,
		<-chan reply.Event,
	) error
}

// DrainReplyDispatcher is used until concrete channel repliers are registered.
type DrainReplyDispatcher struct{}

// Dispatch drains all events without producing an external reply.
func (DrainReplyDispatcher) Dispatch(
	ctx context.Context,
	_ tenant.Snapshot,
	_ string,
	_ InboundMessage,
	events <-chan reply.Event,
) error {
	for {
		select {
		case _, ok := <-events:
			if !ok {
				return nil
			}
		case <-ctx.Done():
			for range events {
			}
			return ctx.Err()
		}
	}
}

// Router is the shared, platform-independent inbound pipeline.
type Router struct {
	cache      tenant.ConfigCache
	dedup      Deduper
	lock       SessionLock
	debouncer  Debouncer
	events     storage.EventStore
	executor   worker.Executor
	dispatcher ReplyDispatcher
	audit      platformlog.AuditWriter
	metrics    platformmetrics.Recorder

	lifecycleCtx    context.Context
	cancelLifecycle context.CancelFunc
	replyWG         sync.WaitGroup

	stateMu        sync.Mutex
	stateChanged   *sync.Cond
	draining       bool
	activeHandlers int
}

// RouterOption configures optional Gateway integrations.
type RouterOption func(*Router)

// WithMetrics enables tenant-aware Gateway metrics.
func WithMetrics(recorder platformmetrics.Recorder) RouterOption {
	return func(router *Router) {
		router.metrics = recorder
	}
}

// NewRouter validates and assembles the inbound pipeline.
func NewRouter(
	cache tenant.ConfigCache,
	dedup Deduper,
	lock SessionLock,
	debouncer Debouncer,
	events storage.EventStore,
	executor worker.Executor,
	dispatcher ReplyDispatcher,
	audit platformlog.AuditWriter,
	options ...RouterOption,
) (*Router, error) {
	if cache == nil || dedup == nil || lock == nil || debouncer == nil ||
		events == nil || executor == nil {
		return nil, errors.New("router cache, deduper, lock, debouncer, event store, and executor are required")
	}
	if dispatcher == nil {
		dispatcher = DrainReplyDispatcher{}
	}
	if audit == nil {
		audit = platformlog.NopAuditWriter{}
	}
	lifecycleCtx, cancel := context.WithCancel(context.Background())
	router := &Router{
		cache:           cache,
		dedup:           dedup,
		lock:            lock,
		debouncer:       debouncer,
		events:          events,
		executor:        executor,
		dispatcher:      dispatcher,
		audit:           audit,
		lifecycleCtx:    lifecycleCtx,
		cancelLifecycle: cancel,
	}
	router.stateChanged = sync.NewCond(&router.stateMu)
	for _, option := range options {
		if option != nil {
			option(router)
		}
	}
	return router, nil
}

// Handle durably ingests a message and returns before Runner execution.
func (r *Router) Handle(ctx context.Context, message InboundMessage) (Result, error) {
	ctx, span := agenttrace.Tracer.Start(ctx, "gateway.handle")
	defer span.End()
	if spanContext := span.SpanContext(); spanContext.IsValid() {
		message.TraceID = spanContext.TraceID().String()
	}
	if !r.beginHandle() {
		return Result{Outcome: OutcomeAgentOffline}, nil
	}
	defer r.endHandle()
	started := time.Now()

	if err := validateInbound(message); err != nil {
		r.writeAudit(ctx, tenant.Snapshot{}, message, "", string(DropInvalidEvent), "invalid_event", time.Since(started))
		return Result{Outcome: OutcomeDropped, DropReason: DropInvalidEvent}, nil
	}

	snapshot, err := r.cache.ResolveBinding(ctx, message.Channel, message.RouteKey)
	if err != nil {
		switch {
		case errors.Is(err, tenant.ErrNotFound):
			r.writeAudit(ctx, tenant.Snapshot{}, message, "", string(DropUnboundBinding), "", time.Since(started))
			return Result{Outcome: OutcomeNeedsBinding, DropReason: DropUnboundBinding}, nil
		case errors.Is(err, tenant.ErrInactive):
			r.writeAudit(ctx, tenant.Snapshot{}, message, "", string(DropRevokedBinding), "", time.Since(started))
			return Result{Outcome: OutcomeDropped, DropReason: DropRevokedBinding}, nil
		default:
			return Result{}, fmt.Errorf("resolve channel binding: %w", err)
		}
	}
	span.SetAttributes(
		attribute.String("tenant.id", snapshot.Tenant.ID),
		attribute.String("channel", message.Channel),
	)
	if message.ChatType == "group" && !message.AddressedToBot {
		r.writeAudit(ctx, snapshot, message, "", string(DropNotAddressedInGroup), "", time.Since(started))
		return Result{Outcome: OutcomeDropped, DropReason: DropNotAddressedInGroup}, nil
	}

	sessionID := DeriveSessionID(snapshot.Tenant.ID, message.Channel, message)
	dedupKey := BuildDedupKey(snapshot.Tenant.ID, snapshot.Binding.ID, message.MsgID)
	token, err := r.dedup.Claim(ctx, dedupKey)
	if err != nil {
		if errors.Is(err, ErrDuplicate) {
			r.writeAudit(ctx, snapshot, message, sessionID, string(DropDuplicate), "", time.Since(started))
			return Result{Outcome: OutcomeDropped, DropReason: DropDuplicate, SessionID: sessionID}, nil
		}
		return Result{}, fmt.Errorf("claim inbound message: %w", err)
	}

	userEvent := storage.UserEvent{
		SessionID: sessionID,
		TenantID:  snapshot.Tenant.ID,
		AppID:     snapshot.App.ID,
		Channel:   message.Channel,
		MsgID:     message.MsgID,
		SenderID:  message.SenderID,
		Text:      message.Text,
		TraceID:   message.TraceID,
	}
	if message.ChatType == "group" {
		userEvent.UserRef = message.GroupID
	} else {
		userEvent.UserRef = message.SenderID
	}
	if err := r.events.AppendUserEvent(ctx, userEvent); err != nil {
		if errors.Is(err, storage.ErrDuplicateEvent) {
			if markErr := r.dedup.Mark(ctx, dedupKey, token); markErr != nil {
				return Result{}, fmt.Errorf("mark database-deduplicated message: %w", markErr)
			}
			r.writeAudit(ctx, snapshot, message, sessionID, string(DropDuplicate), "", time.Since(started))
			return Result{Outcome: OutcomeDropped, DropReason: DropDuplicate, SessionID: sessionID}, nil
		}
		releaseErr := r.dedup.Release(ctx, dedupKey, token)
		return Result{}, errors.Join(
			fmt.Errorf("persist inbound message: %w", err),
			wrapOptionalError("release failed inbound claim", releaseErr),
		)
	}
	if err := r.dedup.Mark(ctx, dedupKey, token); err != nil {
		return Result{}, fmt.Errorf("mark persisted inbound message: %w", err)
	}

	r.writeAudit(ctx, snapshot, message, sessionID, string(OutcomeIngested), "", time.Since(started))
	r.scheduleFlush(
		snapshot,
		sessionID,
		message,
		oteltrace.SpanContextFromContext(ctx),
	)
	return Result{Outcome: OutcomeIngested, SessionID: sessionID}, nil
}

func (r *Router) scheduleFlush(
	snapshot tenant.Snapshot,
	sessionID string,
	message InboundMessage,
	parent oteltrace.SpanContext,
) {
	r.stateMu.Lock()
	draining := r.draining
	r.stateMu.Unlock()
	if draining {
		return
	}
	r.debouncer.Schedule(sessionID, func() {
		r.flush(snapshot, sessionID, message, parent)
	})
}

func (r *Router) flush(
	snapshot tenant.Snapshot,
	sessionID string,
	message InboundMessage,
	parent oteltrace.SpanContext,
) {
	started := time.Now()
	base := oteltrace.ContextWithSpanContext(r.lifecycleCtx, parent)
	executionCtx, cancel := context.WithTimeout(base, executionTimeout)
	defer cancel()
	ctx, span := agenttrace.Tracer.Start(
		executionCtx,
		"gateway.flush",
		oteltrace.WithAttributes(
			attribute.String("tenant.id", snapshot.Tenant.ID),
			attribute.String("session.id", sessionID),
		),
	)
	defer span.End()

	lease, err := r.lock.Acquire(ctx, sessionID)
	if err != nil {
		if errors.Is(err, ErrHeld) {
			r.scheduleFlush(snapshot, sessionID, message, parent)
			return
		}
		r.writeAudit(ctx, snapshot, message, sessionID, "execute_failed", "lock_acquire", time.Since(started))
		r.dispatchFailureReply(ctx, snapshot, sessionID, message, "lock_acquire")
		return
	}
	defer func() {
		if err := lease.Release(context.Background()); err != nil {
			r.writeAudit(
				context.Background(), snapshot, message, sessionID,
				"execute_failed", "lock_release", time.Since(started),
			)
		}
	}()
	// Abort execution when the lease is lost (renewal failure or max hold
	// exceeded) instead of writing concurrently with a new lock owner.
	if lost := lease.Lost(); lost != nil {
		go func() {
			select {
			case err, ok := <-lost:
				if ok && err != nil {
					cancel()
				}
			case <-executionCtx.Done():
			}
		}()
	}

	messages, err := r.events.PendingUserEvents(ctx, sessionID, 0)
	if err != nil {
		r.writeAudit(ctx, snapshot, message, sessionID, "execute_failed", "event_read", time.Since(started))
		r.dispatchFailureReply(ctx, snapshot, sessionID, message, "event_read")
		return
	}
	if len(messages) == 0 {
		return
	}
	source, err := r.executor.Execute(ctx, snapshot, sessionID, messages)
	if err != nil {
		r.writeAudit(ctx, snapshot, message, sessionID, "execute_failed", "executor_start", time.Since(started))
		// The inbound message stays pending and retries on the next delivery,
		// but the user must not be left without any reply.
		r.dispatchFailureReply(ctx, snapshot, sessionID, message, "executor_start")
		return
	}
	replyEvents := make(chan reply.Event, 128)
	replyCtx := oteltrace.ContextWithSpanContext(
		r.lifecycleCtx,
		oteltrace.SpanContextFromContext(ctx),
	)
	r.replyWG.Add(1)
	go func() {
		defer r.replyWG.Done()
		if err := r.dispatcher.Dispatch(replyCtx, snapshot, sessionID, message, replyEvents); err != nil {
			r.writeAudit(
				context.Background(), snapshot, message, sessionID,
				"reply_failed", "im_delivery", time.Since(started),
			)
		}
	}()

	forward := true
	for event := range source {
		if event.Decision != "" {
			r.writeAudit(
				ctx, snapshot, message, sessionID,
				event.Decision, "", time.Since(started),
			)
		}
		if !forward {
			continue
		}
		select {
		case replyEvents <- event:
		case <-ctx.Done():
			forward = false
		}
	}
	close(replyEvents)

	eventIDs := make([]int64, 0, len(messages))
	for _, pending := range messages {
		eventIDs = append(eventIDs, pending.ID)
	}
	if err := r.markConsumedWithRetry(ctx, eventIDs); err != nil {
		r.writeAudit(ctx, snapshot, message, sessionID, "execute_failed", "event_commit", time.Since(started))
		return
	}
	r.writeAudit(ctx, snapshot, message, sessionID, "executed", "", time.Since(started))
}

// dispatchFailureReply delivers a terminal error event to the user when a
// turn fails after the inbound message was already ACKed and marked done, so
// the two-phase deduplication cannot be undone.
func (r *Router) dispatchFailureReply(
	ctx context.Context,
	snapshot tenant.Snapshot,
	sessionID string,
	message InboundMessage,
	errorType string,
) {
	events := make(chan reply.Event, 1)
	events <- reply.Event{Type: "error", Error: "agent execution failed: " + errorType}
	close(events)
	replyCtx := oteltrace.ContextWithSpanContext(
		r.lifecycleCtx,
		oteltrace.SpanContextFromContext(ctx),
	)
	r.replyWG.Add(1)
	go func() {
		defer r.replyWG.Done()
		if err := r.dispatcher.Dispatch(replyCtx, snapshot, sessionID, message, events); err != nil {
			r.writeAudit(
				context.Background(), snapshot, message, sessionID,
				"reply_failed", "im_delivery", 0,
			)
		}
	}()
}

// markConsumedWithRetry retries transient storage failures before leaving a
// finished turn pending (which would duplicate the reply on the next message).
func (r *Router) markConsumedWithRetry(ctx context.Context, eventIDs []int64) error {
	var err error
	for attempt := 0; attempt < markConsumedAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(markConsumedBackoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err = r.events.MarkConsumed(ctx, eventIDs); err == nil {
			return nil
		}
	}
	return err
}

// Drain stops new ingestion, flushes pending sessions, and waits for
// in-flight replies. Replies get a grace period to finish; only stuck
// repliers (e.g. a hanging IM API) are cancelled so shutdown stays bounded.
func (r *Router) Drain() {
	r.stateMu.Lock()
	r.draining = true
	for r.activeHandlers > 0 {
		r.stateChanged.Wait()
	}
	r.stateMu.Unlock()

	r.debouncer.FlushAll()
	replies := make(chan struct{})
	go func() {
		r.replyWG.Wait()
		close(replies)
	}()
	select {
	case <-replies:
	case <-time.After(drainReplyGrace):
		r.cancelLifecycle()
		<-replies
		return
	}
	r.cancelLifecycle()
}

func (r *Router) beginHandle() bool {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	if r.draining {
		return false
	}
	r.activeHandlers++
	return true
}

func (r *Router) endHandle() {
	r.stateMu.Lock()
	r.activeHandlers--
	if r.activeHandlers == 0 {
		r.stateChanged.Broadcast()
	}
	r.stateMu.Unlock()
}

func (r *Router) writeAudit(
	ctx context.Context,
	snapshot tenant.Snapshot,
	message InboundMessage,
	sessionID string,
	decision string,
	errorType string,
	latency time.Duration,
) {
	r.audit.Write(ctx, platformlog.AuditRecord{
		TenantID:  snapshot.Tenant.ID,
		Channel:   message.Channel,
		UserID:    truncateRunes(message.SenderID, maxParticipantIDLength),
		SessionID: sessionID,
		AgentName: snapshot.App.AppName,
		Decision:  decision,
		Latency:   latency,
		ErrorType: errorType,
		TraceID:   message.TraceID,
		RequestID: truncateRunes(message.MsgID, maxMsgIDLength),
		TS:        time.Now(),
	})
	if r.metrics != nil {
		r.metrics.ObserveRequest(
			snapshot.Tenant.ID,
			message.Channel,
			decision,
			errorType,
			latency,
		)
	}
}

func validateInbound(message InboundMessage) error {
	if message.Channel == "" || message.RouteKey == "" || message.MsgID == "" ||
		message.SenderID == "" {
		return errors.New("channel, route key, message ID, and sender ID are required")
	}
	// Keep inbound identifiers within the tightest storage columns so a
	// hostile or buggy client cannot force failed inserts downstream
	// (message_event.msg_id and audit_log.request_id are the smallest).
	if len(message.MsgID) > maxMsgIDLength {
		return errors.New("message ID is too long")
	}
	if len(message.SenderID) > maxParticipantIDLength {
		return errors.New("sender ID is too long")
	}
	if len(message.GroupID) > maxParticipantIDLength {
		return errors.New("group ID is too long")
	}
	if message.ChatType != "p2p" && message.ChatType != "group" {
		return errors.New("chat type must be p2p or group")
	}
	if message.ChatType == "group" && message.GroupID == "" {
		return errors.New("group ID is required for group messages")
	}
	return nil
}

func wrapOptionalError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", operation, err)
}

// truncateRunes bounds audit fields to their column width without splitting a
// multi-byte character, so an oversized hostile identifier still produces a
// durable audit row instead of a silently dropped insert.
func truncateRunes(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}
