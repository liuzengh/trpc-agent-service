package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// metadataExporter is the final export boundary, including native framework
// spans. Never export prompts, tool arguments/results, exceptions, DB queries,
// arbitrary callback attributes, or status descriptions from provider errors.
type metadataExporter struct{ sdktrace.SpanExporter }

func (e metadataExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	safe := make([]sdktrace.ReadOnlySpan, len(spans))
	for i, span := range spans {
		safe[i] = metadataSpan{span}
	}
	return e.SpanExporter.ExportSpans(ctx, safe)
}

type metadataSpan struct{ sdktrace.ReadOnlySpan }

func (s metadataSpan) Attributes() []attribute.KeyValue {
	var safe []attribute.KeyValue
	for _, attr := range s.ReadOnlySpan.Attributes() {
		if safeAttribute(string(attr.Key)) {
			safe = append(safe, attr)
		}
	}
	return safe
}
func (s metadataSpan) Events() []sdktrace.Event { return nil }
func (s metadataSpan) Status() sdktrace.Status {
	return sdktrace.Status{Code: s.ReadOnlySpan.Status().Code}
}
func (s metadataSpan) Links() []sdktrace.Link {
	links := append([]sdktrace.Link(nil), s.ReadOnlySpan.Links()...)
	for i := range links {
		links[i].Attributes = nil
	}
	return links
}
func (s metadataSpan) Resource() *resource.Resource {
	var attrs []attribute.KeyValue
	for _, attr := range s.ReadOnlySpan.Resource().Attributes() {
		switch string(attr.Key) {
		case "service.name", "telemetry.sdk.name", "telemetry.sdk.version", "telemetry.sdk.language":
			attrs = append(attrs, attr)
		}
	}
	return resource.NewSchemaless(attrs...)
}

func safeAttribute(key string) bool {
	switch key {
	case "tenant.id", "agent.app.id", "agent.revision.id", "channel.type", "channel.binding.id",
		"gen_ai.request.id", "messaging.message.id", "approval.id", "approval.decision", "approval.origin_request_id",
		"http.request.method", "http.route", "http.response.status_code",
		"storage.backend", "storage.resource", "storage.operation", "db.system", "error.type", "delivery.error.kind", "delivery.phase",
		"gen_ai.system", "gen_ai.operation.name", "gen_ai.provider.name",
		"gen_ai.request.model", "gen_ai.response.model", "gen_ai.request.is_stream",
		"gen_ai.request.max_tokens", "gen_ai.request.temperature", "gen_ai.request.top_p",
		"gen_ai.usage.input_tokens", "gen_ai.usage.output_tokens",
		"gen_ai.agent.id", "gen_ai.agent.name", "gen_ai.tool.name", "gen_ai.tool.call.id",
		"trpc.go.agent.invocation_id", "trpc.go.agent.event_id", "trpc.go.agent.runner.name",
		"trpc_agent_go.client.time_to_first_token":
		return true
	default:
		return false
	}
}
