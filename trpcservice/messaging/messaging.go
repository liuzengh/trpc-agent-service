// Package messaging defines the durable Outbox-to-Kafka delivery contract.
package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"go.opentelemetry.io/otel/propagation"
)

var (
	// ErrRetryScheduled indicates a retryable delivery was intentionally left
	// uncommitted for the broker retry path.
	ErrRetryScheduled = errors.New("message retry scheduled")
	// ErrWorkerDraining means the worker accepted no new business execution
	// because shutdown draining has started. The Kafka delivery is deliberately
	// left uncommitted so another consumer can receive it after rebalance.
	ErrWorkerDraining = errors.New("worker is draining")
	// ErrInvalidEnvelope indicates a malformed or incompatible broker message.
	ErrInvalidEnvelope = errors.New("invalid messaging envelope")
)

// CurrentEnvelopeVersion is the only repository-internal Kafka envelope version
// accepted by the current development build.
const CurrentEnvelopeVersion uint16 = 1

// Envelope is the versioned, channel-neutral Kafka message contract.
type Envelope struct {
	Version     uint16             `json:"version"`
	EventID     string             `json:"event_id"`
	TenantID    string             `json:"tenant_id"`
	SessionKey  string             `json:"session_key"`
	Type        string             `json:"type"`
	Payload     json.RawMessage    `json:"payload"`
	Attempt     int                `json:"attempt"`
	TraceParent string             `json:"traceparent,omitempty"`
	Manifest    *ExecutionManifest `json:"execution_manifest,omitempty"`
}

// PartitionKey keeps one session ordered within Kafka while allowing different
// sessions to be consumed in parallel.
func (e Envelope) PartitionKey() []byte {
	return []byte(e.SessionKey)
}

// Validate rejects envelopes that could weaken tenant isolation or ordering.
func (e Envelope) Validate() error {
	if e.Version != CurrentEnvelopeVersion {
		return fmt.Errorf("%w: unsupported version %d", ErrInvalidEnvelope, e.Version)
	}
	if strings.TrimSpace(e.EventID) == "" || strings.TrimSpace(e.TenantID) == "" {
		return fmt.Errorf("%w: event and tenant IDs are required", ErrInvalidEnvelope)
	}
	if strings.TrimSpace(e.SessionKey) == "" || !strings.HasPrefix(e.SessionKey, e.TenantID+"/") {
		return fmt.Errorf("%w: session key is not tenant-scoped", ErrInvalidEnvelope)
	}
	if strings.TrimSpace(e.Type) == "" || !json.Valid(e.Payload) {
		return fmt.Errorf("%w: type and JSON payload are required", ErrInvalidEnvelope)
	}
	if e.Attempt < 0 {
		return fmt.Errorf("%w: attempt cannot be negative", ErrInvalidEnvelope)
	}
	return nil
}

// Producer publishes an envelope to the configured Kafka topic.
type Producer interface {
	Publish(context.Context, Envelope) error
}

// OutboxDispatcher publishes committed database outbox records and marks them
// delivered only after the producer acknowledges the publish.
type OutboxDispatcher struct {
	store    storage.StateStore
	producer Producer
}

// NewOutboxDispatcher constructs a transactional-outbox dispatcher.
func NewOutboxDispatcher(store storage.StateStore, producer Producer) (*OutboxDispatcher, error) {
	if store == nil {
		return nil, fmt.Errorf("outbox state store is required")
	}
	if producer == nil {
		return nil, fmt.Errorf("Kafka producer is required")
	}
	return &OutboxDispatcher{store: store, producer: producer}, nil
}

// DispatchTenant attempts pending events in their store order. The first
// failed publish remains pending and stops the batch to preserve per-session
// causal ordering until a retry succeeds.
func (d *OutboxDispatcher) DispatchTenant(ctx context.Context, tenantID string, limit int) (int, error) {
	events, err := d.store.ListPendingOutbox(ctx, tenantID, limit)
	if err != nil {
		return 0, err
	}
	dispatched := 0
	for _, event := range events {
		carrier := propagation.MapCarrier{}
		propagation.TraceContext{}.Inject(ctx, carrier)
		envelope := Envelope{
			Version:     CurrentEnvelopeVersion,
			EventID:     event.ID,
			TenantID:    event.TenantID,
			SessionKey:  event.AggregateKey,
			Type:        event.Type,
			Payload:     append(json.RawMessage(nil), event.Payload...),
			TraceParent: carrier.Get("traceparent"),
		}
		if err := envelope.Validate(); err != nil {
			return dispatched, err
		}
		if err := d.producer.Publish(ctx, envelope); err != nil {
			return dispatched, fmt.Errorf("publish outbox event %q: %w", event.ID, err)
		}
		if err := d.store.MarkOutboxDelivered(ctx, event.TenantID, event.ID); err != nil {
			return dispatched, fmt.Errorf("mark outbox event %q delivered: %w", event.ID, err)
		}
		dispatched++
	}
	return dispatched, nil
}

// Delivery carries a broker record and its decoded envelope. Opaque is owned
// by the Kafka adapter and is passed back unchanged on commit.
type Delivery struct {
	Envelope    Envelope
	RawPayload  []byte
	DecodeError error
	Opaque      any
}

// Consumer represents the ordered, manually committed Kafka boundary.
type Consumer interface {
	Receive(context.Context) (Delivery, error)
	Commit(context.Context, Delivery) error
	PublishDLQ(context.Context, DeadLetter) error
}

// DeadLetter preserves the original envelope and a safe error class for an
// explicit replay workflow.
type DeadLetter struct {
	Envelope   Envelope
	RawPayload []byte
	ErrorClass string
}

// Processor executes one decoded envelope through the governed agent path.
type Processor interface {
	Process(context.Context, Envelope) error
}

// ProcessorFunc adapts a function to Processor.
type ProcessorFunc func(context.Context, Envelope) error

func (f ProcessorFunc) Process(ctx context.Context, envelope Envelope) error {
	return f(ctx, envelope)
}

// Worker processes one broker delivery at a time. Kafka partition assignment
// supplies session ordering; offsets are committed only after the business
// result is durable or a DLQ handoff succeeds.
type Worker struct {
	consumer     Consumer
	processor    Processor
	maxAttempts  int
	retry        *Delivery
	retryTracker storage.RetryTracker

	lifecycleMu sync.Mutex
	draining    bool
	active      int
	drainDone   chan struct{}
	drainClose  sync.Once
}

// NewWorker constructs a worker with an explicit bounded retry policy.
func NewWorker(consumer Consumer, processor Processor, maxAttempts int) (*Worker, error) {
	return NewWorkerWithRetryTracker(consumer, processor, maxAttempts, storage.NewMemoryRetryTracker())
}

// NewWorkerWithRetryTracker constructs a Worker with durable retry accounting.
func NewWorkerWithRetryTracker(consumer Consumer, processor Processor, maxAttempts int, tracker storage.RetryTracker) (*Worker, error) {
	if consumer == nil {
		return nil, fmt.Errorf("Kafka consumer is required")
	}
	if processor == nil {
		return nil, fmt.Errorf("message processor is required")
	}
	if maxAttempts <= 0 {
		return nil, fmt.Errorf("maximum attempts must be positive")
	}
	if tracker == nil {
		return nil, fmt.Errorf("retry tracker is required")
	}
	return &Worker{
		consumer: consumer, processor: processor, maxAttempts: maxAttempts, retryTracker: tracker,
		drainDone: make(chan struct{}),
	}, nil
}

// BeginDrain prevents new Kafka deliveries from entering business processing.
// A delivery that already passed the admission point is allowed to finish all
// the way through its commit/DLQ handoff so shutdown never creates a false
// duplicate after durable side effects were already written.
func (w *Worker) BeginDrain() {
	if w == nil {
		return
	}
	w.lifecycleMu.Lock()
	w.draining = true
	active := w.active
	w.lifecycleMu.Unlock()
	if active == 0 {
		w.drainClose.Do(func() { close(w.drainDone) })
	}
}

// WaitDrain waits until every delivery admitted before BeginDrain has reached
// its terminal broker handoff. It does not wait for an idle Kafka poll; the
// caller can cancel that poll after this method returns.
func (w *Worker) WaitDrain(ctx context.Context) error {
	if w == nil {
		return nil
	}
	w.lifecycleMu.Lock()
	draining := w.draining
	done := w.drainDone
	w.lifecycleMu.Unlock()
	if !draining {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

// ActiveDeliveries reports deliveries that have been admitted and have not yet
// completed their broker commit/DLQ handoff.
func (w *Worker) ActiveDeliveries() int {
	if w == nil {
		return 0
	}
	w.lifecycleMu.Lock()
	defer w.lifecycleMu.Unlock()
	return w.active
}

func (w *Worker) beginDelivery() bool {
	w.lifecycleMu.Lock()
	defer w.lifecycleMu.Unlock()
	if w.draining {
		return false
	}
	w.active++
	return true
}

func (w *Worker) finishDelivery() {
	w.lifecycleMu.Lock()
	if w.active > 0 {
		w.active--
	}
	shouldClose := w.draining && w.active == 0
	w.lifecycleMu.Unlock()
	if shouldClose {
		w.drainClose.Do(func() { close(w.drainDone) })
	}
}

// RunOnce processes one record. Retryable errors retain the source offset.
func (w *Worker) RunOnce(ctx context.Context) error {
	delivery, err := w.receive(ctx)
	if err != nil {
		return err
	}
	if !w.beginDelivery() {
		return ErrWorkerDraining
	}
	defer w.finishDelivery()
	if delivery.DecodeError != nil {
		return w.finishWithDLQ(ctx, delivery, "invalid_json")
	}
	if err := delivery.Envelope.Validate(); err != nil {
		return w.finishWithDLQ(ctx, delivery, "invalid_envelope")
	}
	if delivery.Envelope.TraceParent != "" {
		ctx = propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": delivery.Envelope.TraceParent})
	}
	if err := w.processor.Process(ctx, delivery.Envelope); err != nil {
		if IsRetryable(err) {
			attempt, trackErr := w.retryTracker.Increment(ctx, delivery.Envelope.TenantID, delivery.Envelope.SessionKey, delivery.Envelope.EventID)
			if trackErr != nil {
				return fmt.Errorf("persist retry attempt: %w", trackErr)
			}
			if attempt <= delivery.Envelope.Attempt {
				attempt = delivery.Envelope.Attempt + 1
			}
			delivery.Envelope.Attempt = attempt
			slog.Warn("retryable Kafka processing failure",
				"tenant_id", delivery.Envelope.TenantID,
				"session_key", delivery.Envelope.SessionKey,
				"event_id", delivery.Envelope.EventID,
				"attempt", attempt,
				"error", err)
			if attempt < w.maxAttempts {
				w.retry = &delivery
				return fmt.Errorf("%w: %v", ErrRetryScheduled, err)
			}
		}
		return w.finishWithDLQ(ctx, delivery, errorClass(err))
	}
	if err := w.consumer.Commit(ctx, delivery); err != nil {
		return fmt.Errorf("commit Kafka delivery: %w", err)
	}
	w.retry = nil
	_ = w.retryTracker.Clear(ctx, delivery.Envelope.TenantID, delivery.Envelope.SessionKey, delivery.Envelope.EventID)
	return nil
}

func (w *Worker) receive(ctx context.Context) (Delivery, error) {
	if w.retry != nil {
		return *w.retry, nil
	}
	return w.consumer.Receive(ctx)
}

func (w *Worker) finishWithDLQ(ctx context.Context, delivery Delivery, class string) error {
	slog.Warn("dead-lettering Kafka delivery",
		"tenant_id", delivery.Envelope.TenantID,
		"session_key", delivery.Envelope.SessionKey,
		"event_id", delivery.Envelope.EventID,
		"class", class)
	if err := w.consumer.PublishDLQ(ctx, DeadLetter{Envelope: delivery.Envelope, RawPayload: append([]byte(nil), delivery.RawPayload...), ErrorClass: class}); err != nil {
		return fmt.Errorf("publish Kafka dead letter: %w", err)
	}
	if err := w.consumer.Commit(ctx, delivery); err != nil {
		return fmt.Errorf("commit Kafka delivery after DLQ: %w", err)
	}
	w.retry = nil
	_ = w.retryTracker.Clear(ctx, delivery.Envelope.TenantID, delivery.Envelope.SessionKey, delivery.Envelope.EventID)
	return nil
}

type classifiedError struct {
	error
	retryable bool
}

// Retryable classifies an error as safe for bounded retry.
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return classifiedError{error: err, retryable: true}
}

// Permanent classifies an error as non-retryable.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return classifiedError{error: err, retryable: false}
}

// IsRetryable reports the explicit retry classification; unclassified errors
// fail closed into DLQ rather than risk uncontrolled duplicate side effects.
func IsRetryable(err error) bool {
	var classified classifiedError
	if errors.As(err, &classified) {
		return classified.retryable
	}
	return false
}

func errorClass(err error) string {
	if IsRetryable(err) {
		return "retry_exhausted"
	}
	return "permanent"
}
