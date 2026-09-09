package telemetry

import (
	"context"
	"errors"

	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func errorsNew(text string) error { return errors.New(text) }

func jobAttrOptions(jobID, executionID, workerID string, attempt int) []attribute.KeyValue {
	// Only bounded fingerprints may decorate spans; raw business identifiers
	// never become span attributes.
	return []attribute.KeyValue{
		attribute.String("job_fingerprint", Fingerprint(jobID)),
		attribute.String("execution_fingerprint", Fingerprint(executionID)),
		attribute.String("worker_fingerprint", Fingerprint(workerID)),
		attribute.Int("attempt", attempt),
	}
}

// Runtime implements the narrow gateway producer surface.
func (r *Runtime) StartProducerSpan(ctx context.Context, operation string) (context.Context, func(outcome string)) {
	if r == nil {
		return ctx, func(string) {}
	}
	producerCtx, span := r.Tracer().Start(ctx, "queue "+operation)
	return producerCtx, func(outcome string) {
		if outcome != "" && outcome != "accepted" {
			span.RecordError(errorsNew("outcome:" + outcome))
		}
		span.End()
	}
}

// InjectCarrier captures the current producer span as a durable carrier.
func (r *Runtime) InjectCarrier(ctx context.Context) *queue.TelemetryCarrier {
	if r == nil {
		return nil
	}
	internal := InjectCarrier(ctx)
	if internal == nil {
		return nil
	}
	return &queue.TelemetryCarrier{Traceparent: internal.Traceparent}
}

// QueueProducerMetric counts producer outcomes with fixed attributes.
func (r *Runtime) QueueProducerMetric(outcome string) {
	if r == nil {
		return
	}
	r.Metrics().QueueOperation("enqueue", outcome)
}

// workerTelemetry implements the worker's optional observability surface.
func (r *Runtime) StartAttemptSpan(ctx context.Context, jobID, executionID, workerID string, carrier *queue.TelemetryCarrier, attempt int) (context.Context, func(outcome string)) {
	if r == nil {
		return ctx, func(string) {}
	}
	spanCtx := ctx
	if carrier != nil {
		linked, state := Extract(ctx, &TraceCarrier{Traceparent: carrier.Traceparent})
		_ = state
		spanCtx = linked
	}
	attemptCtx, span := r.Tracer().Start(spanCtx, "worker execution attempt",
		trace.WithAttributes(jobAttrOptions(jobID, executionID, workerID, attempt)...))
	return attemptCtx, func(outcome string) {
		if outcome != "completed" {
			span.RecordError(errorsNew("outcome:" + outcome))
		}
		span.End()
	}
}

// JobStarted counts an accepted job.
func (r *Runtime) JobStarted() {
	if r == nil {
		return
	}
	r.Metrics().WorkerJob("started")
}

// JobFinished records the terminal job outcome.
func (r *Runtime) JobFinished(outcome string, seconds float64) {
	if r == nil {
		return
	}
	r.Metrics().WorkerJob(outcome)
	r.Metrics().HTTPRequestDuration(Attrs{Component: "worker", Outcome: outcome}, seconds)
}

// Inflight adjusts the worker in-flight gauge.
func (r *Runtime) Inflight(delta int) {
	if r == nil {
		return
	}
	r.Metrics().HTTPInflight(delta, Attrs{Component: "worker"})
}
