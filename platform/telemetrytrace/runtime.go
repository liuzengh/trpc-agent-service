package telemetrytrace

import (
	"context"
	"errors"
	"runtime/debug"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type Identity struct {
	Service, Version, Instance, Environment string
}

func BuildVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" && len(s.Value) == 40 {
				return s.Value
			}
		}
	}
	return "development"
}

type Stats struct {
	Finished, Exported, ExportFailures uint64
}

type counters struct{ finished, exported, failures atomic.Uint64 }

// Runtime is one process's provider, never one provider per tenant/attempt.
// No package globals or OTEL environment variables select the export target.
type Runtime struct {
	resource *resource.Resource
	provider trace.TracerProvider
	sdk      *sdktrace.TracerProvider
	counters *counters
}

func New(ctx context.Context, config *Config, id Identity) (*Runtime, error) {
	if config == nil {
		return &Runtime{provider: noop.NewTracerProvider(), counters: &counters{}}, nil
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if id.Service != "agent-worker" && id.Service != "channel-gateway" {
		return nil, errors.New("invalid tracing service identity")
	}
	if id.Version == "" {
		id.Version = BuildVersion()
	}
	if id.Environment == "" {
		id.Environment = "unspecified"
	}
	if !identifier(id.Instance) || !identifier(id.Version) || !identifier(id.Environment) {
		return nil, errors.New("invalid tracing resource identity")
	}
	res := resource.NewSchemaless(
		attribute.String("service.name", id.Service), attribute.String("service.version", id.Version),
		attribute.String("service.instance.id", id.Instance), attribute.String("service.namespace", "agent-platform"),
		attribute.String("deployment.environment.name", id.Environment))
	timeout, _ := time.ParseDuration(config.ExportTimeout)
	batchTimeout, _ := time.ParseDuration(config.BatchTimeout)
	client := newHTTPClient(config.TracesEndpoint, timeout)
	exporter, err := otlptrace.New(ctx, client)
	if err != nil {
		return nil, errors.New("initialize trace exporter")
	}
	stats := &counters{}
	filtered := &filteredExporter{next: exporter, resource: res, counters: stats}
	batch := sdktrace.NewBatchSpanProcessor(filtered,
		sdktrace.WithMaxQueueSize(config.MaxQueueSize), sdktrace.WithMaxExportBatchSize(config.MaxExportBatchSize),
		sdktrace.WithBatchTimeout(batchTimeout), sdktrace.WithExportTimeout(timeout))
	p := sdktrace.NewTracerProvider(sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(config.SamplingRatio))),
		sdktrace.WithSpanProcessor(&countProcessor{SpanProcessor: batch, counters: stats}))
	return &Runtime{provider: p, sdk: p, counters: stats, resource: res}, nil
}

func (r *Runtime) Provider() trace.TracerProvider   { return r.provider }
func (r *Runtime) Tracer(scope string) trace.Tracer { return r.provider.Tracer(scope) }
func (r *Runtime) Stats() Stats {
	return Stats{r.counters.finished.Load(), r.counters.exported.Load(), r.counters.failures.Load()}
}
func (r *Runtime) ForceFlush(ctx context.Context) error {
	if r == nil || r.sdk == nil {
		return nil
	}
	if err := r.sdk.ForceFlush(ctx); err != nil {
		return errors.New("trace flush incomplete")
	}
	return nil
}
func (r *Runtime) Shutdown(ctx context.Context) error {
	if r == nil || r.sdk == nil {
		return nil
	}
	if err := r.sdk.Shutdown(ctx); err != nil {
		return errors.New("trace shutdown incomplete")
	}
	return nil
}

type countProcessor struct {
	sdktrace.SpanProcessor
	counters *counters
}

func (p *countProcessor) OnEnd(s sdktrace.ReadOnlySpan) {
	if s.SpanContext().IsSampled() {
		p.counters.finished.Add(1)
	}
	p.SpanProcessor.OnEnd(s)
}

// Resource is shared with other enabled telemetry signals in this process.
func (r *Runtime) Resource() *resource.Resource { return r.resource }
