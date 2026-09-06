// Package telemetry configures OpenTelemetry and HTTP trace propagation.
package telemetry

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	agentmetric "trpc.group/trpc-go/trpc-agent-go/telemetry/metric"
	agenttrace "trpc.group/trpc-go/trpc-agent-go/telemetry/trace"
)

type Shutdown func(context.Context) error

func Setup(ctx context.Context, cfg config.TelemetryConfig) (Shutdown, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	if !cfg.Enabled {
		return func(context.Context) error { return nil }, nil
	}
	options := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.Endpoint)}
	if cfg.Insecure {
		options = append(options, otlptracegrpc.WithInsecure())
	}
	exporter, err := otlptracegrpc.New(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
	}
	metricOptions := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(cfg.Endpoint)}
	if cfg.Insecure {
		metricOptions = append(metricOptions, otlpmetricgrpc.WithInsecure())
	}
	metricExporter, err := otlpmetricgrpc.New(ctx, metricOptions...)
	if err != nil {
		_ = exporter.Shutdown(ctx)
		return nil, fmt.Errorf("create OTLP metric exporter: %w", err)
	}
	provider, err := NewTracerProvider(exporter, cfg)
	if err != nil {
		_ = metricExporter.Shutdown(ctx)
		_ = exporter.Shutdown(ctx)
		return nil, err
	}
	otel.SetTracerProvider(provider)
	BindFrameworkTracing(provider)
	res := metricsResource(cfg.ServiceName)
	meterProvider := metricsdk.NewMeterProvider(
		metricsdk.WithResource(res),
		metricsdk.WithView(frameworkMetricView),
		metricsdk.WithReader(metricsdk.NewPeriodicReader(metricExporter)),
	)
	if err := agentmetric.InitMeterProvider(meterProvider); err != nil {
		_ = meterProvider.Shutdown(ctx)
		_ = provider.Shutdown(ctx)
		return nil, fmt.Errorf("initialize framework metrics: %w", err)
	}
	otel.SetMeterProvider(meterProvider)
	return func(ctx context.Context) error {
		return errors.Join(meterProvider.Shutdown(ctx), provider.Shutdown(ctx))
	}, nil
}

func metricsResource(serviceName string) *resource.Resource {
	// A random process identity separates cumulative counters across nodes.
	// Do not copy host/process/environment resource attributes into metrics.
	return resource.NewSchemaless(semconv.ServiceName(serviceName), semconv.ServiceInstanceID(uuid.NewString()), semconv.TelemetrySDKLanguageGo, semconv.TelemetrySDKName("opentelemetry"))
}

// NewTracerProvider also serves isolated preflight tests. All exports go through
// the same metadata-only privacy boundary used by the live service.
func NewTracerProvider(exporter tracesdk.SpanExporter, cfg config.TelemetryConfig) (*tracesdk.TracerProvider, error) {
	res, err := telemetryResource(cfg.ServiceName)
	if err != nil {
		return nil, fmt.Errorf("create telemetry resource: %w", err)
	}
	return tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(metadataExporter{exporter}), tracesdk.WithResource(res),
		tracesdk.WithSampler(tracesdk.ParentBased(tracesdk.TraceIDRatioBased(cfg.SampleRatio))),
	), nil
}

// BindFrameworkTracing must run once at startup, before any Agent goroutines.
// The framework has its own global noop tracer; setting otel's provider alone
// does not enable LLMAgent/chat/execute_tool spans in the pinned version.
func BindFrameworkTracing(provider trace.TracerProvider) {
	agenttrace.TracerProvider = provider
	agenttrace.Tracer = provider.Tracer("trpc-agent-go")
	var policy agenttrace.SpanAttributePolicy
	for _, op := range []agenttrace.SpanOperation{agenttrace.OperationChat, agenttrace.OperationInvokeAgent, agenttrace.OperationExecuteTool, agenttrace.OperationWorkflow} {
		for _, key := range []agenttrace.AttributeKey{
			agenttrace.AttrLLMRequest, agenttrace.AttrLLMResponse, agenttrace.AttrInputMessages,
			agenttrace.AttrInputMessagesOTel, agenttrace.AttrOutputMessages, agenttrace.AttrOutputMessagesOTel,
			"gen_ai.tool.call.arguments", "gen_ai.tool.call.result", "gen_ai.workflow.request", "gen_ai.workflow.response",
			"trpc.go.agent.runner.input", "trpc.go.agent.runner.output",
		} {
			agenttrace.WithAttributeRule(op, key, agenttrace.Drop())(&policy)
		}
	}
	agenttrace.SetSpanAttributePolicy(policy)
}

func telemetryResource(serviceName string) (*resource.Resource, error) {
	// resource.Default may use a newer OpenTelemetry schema than the semconv
	// package imported by this module. A schemaless service.name attribute can
	// be merged without producing a conflicting-schema startup failure.
	return resource.Merge(
		resource.Default(),
		resource.NewSchemaless(semconv.ServiceName(serviceName)),
	)
}
