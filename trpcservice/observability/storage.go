// Package observability provides the common low-cardinality storage tracing
// contract.  Storage adapters call StartStorage at their boundary so a trace
// can show the durable path without copying user content or credentials into
// telemetry.
package observability

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/cyl6/trpc-agent-service/trpcservice/metrics"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"
)

var storageMetrics atomic.Pointer[metrics.Metrics]

// SetStorageMetrics connects storage boundaries to the local /metrics exporter.
// A nil exporter disables metrics. The returned restore function is for teardown.
func SetStorageMetrics(exporter *metrics.Metrics) func() {
	previous := storageMetrics.Swap(exporter)
	return func() { storageMetrics.Store(previous) }
}

var storageTracer = otel.Tracer("trpc-agent-service/storage")

// StartStorage starts a span with only bounded, non-sensitive attributes.
// tenantID is immediately hashed; callers must not pass prompts, response
// text, IDs containing user input, DSNs, or provider errors here.
func StartStorage(ctx context.Context, operation, backend, tenantID, revision string) (context.Context, func(error)) {
	if ctx == nil {
		ctx = context.Background()
	}
	if operation == "" {
		operation = "unknown"
	}
	if backend == "" {
		backend = "unknown"
	}
	ctx, span := storageTracer.Start(ctx, "storage."+operation,
		oteltrace.WithAttributes(
			attribute.String("storage.operation", bounded(operation)),
			attribute.String("storage.backend", bounded(backend)),
			attribute.String("tenant.hash", HashTenant(tenantID)),
			attribute.String("config.cohort", bounded(revision)),
		),
	)
	started := time.Now()
	var once sync.Once
	return ctx, func(err error) {
		once.Do(func() {
			span.SetAttributes(attribute.Int64("storage.duration_ms", time.Since(started).Milliseconds()))
			if err != nil {
				span.SetStatus(codes.Error, ErrorCategory(err))
				span.SetAttributes(attribute.String("storage.result", "error"))
			} else {
				span.SetStatus(codes.Ok, "ok")
				span.SetAttributes(attribute.String("storage.result", "ok"))
			}
			if exporter := storageMetrics.Load(); exporter != nil {
				exporter.Observe("storage_operation_duration_seconds", "Storage operation latency.", time.Since(started).Seconds(), map[string]string{
					"tenant": HashTenant(tenantID), "component": bounded(operation), "backend": bounded(backend), "result": ErrorCategory(err),
				})
			}
			span.End()
		})
	}
}

func HashTenant(tenantID string) string {
	sum := sha256.Sum256([]byte(tenantID))
	return hex.EncodeToString(sum[:8])
}

func ErrorCategory(err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	text := strings.ToLower(err.Error())
	switch {
	case strings.Contains(text, "lease"):
		return "lease_lost"
	case strings.Contains(text, "conflict"):
		return "conflict"
	case strings.Contains(text, "not found"):
		return "not_found"
	default:
		return "unavailable"
	}
}

func bounded(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 64 {
		return fmt.Sprintf("%x", sha256.Sum256([]byte(value)))[:16]
	}
	return value
}
