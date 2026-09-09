package telemetry

import (
	"context"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"strings"
)

type Carrier map[string]string

func (c Carrier) Get(key string) string { return c[key] }
func (c Carrier) Set(key, value string) { c[key] = value }
func (c Carrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for key := range c {
		keys = append(keys, key)
	}
	return keys
}
func ExtractTraceParent(ctx context.Context, value string) context.Context {
	value = strings.TrimSpace(value)
	if value == "" {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, Carrier{"traceparent": value})
}
func InjectTraceParent(ctx context.Context) string {
	c := Carrier{}
	otel.GetTextMapPropagator().Inject(ctx, c)
	return c["traceparent"]
}
func Start(ctx context.Context, name string) (context.Context, trace.Span) {
	return otel.Tracer("trpc-agent-service").Start(ctx, name)
}

var _ propagation.TextMapCarrier = Carrier{}
