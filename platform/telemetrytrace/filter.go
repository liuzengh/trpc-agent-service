package telemetrytrace

import (
	"context"
	"regexp"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$`)

func identifier(s string) bool { return identifierPattern.MatchString(s) && !strings.Contains(s, "//") }

func spanName(name string) string {
	switch name {
	case "gateway.im.callback", "gateway.im.receive", "gateway.run.admit", "gateway.reply.deliver", "gateway.reply.verify", "gateway.im.send", "worker.reply.verify",
		"worker.run.attempt", "worker.run.claim", "worker.manifest.resolve", "worker.runtime.prepare",
		"worker.credential.resolve", "worker.runner.run", "worker.run.complete", "worker.session.open",
		"worker.session.load", "worker.session.stage", "worker.session.commit", "worker.session.verify", "worker.run.find_completion", "worker.run.terminalize", "session.overlay.get",
		"session.overlay.create", "session.overlay.append", "memory.search", "memory.read", "memory.write", "memory.delete":
		return name
	}
	for _, prefix := range []string{"invoke_agent ", "chat ", "execute_tool ", "workflow "} {
		if suffix, ok := strings.CutPrefix(name, prefix); ok {
			if identifier(suffix) {
				return name
			}
			return strings.TrimSpace(prefix)
		}
	}
	for _, op := range []string{"create ", "publish ", "process "} {
		for _, subject := range []string{"execution.run-requested.v1", "execution.reply-intent.v1"} {
			if name == op+subject {
				return name
			}
		}
	}
	return "operation"
}

func allowedAttributes(attrs []attribute.KeyValue) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(attrs))
	for _, a := range attrs {
		switch string(a.Key) {
		case "app.run.id", "app.attempt.id", "app.session.id", "app.intent.id", "app.binding.id",
			"gen_ai.request.model", "gen_ai.response.model", "gen_ai.agent.name", "gen_ai.tool.name",
			"gen_ai.tool.call.id", "gen_ai.conversation.id":
			if a.Value.Type() == attribute.STRING && identifier(a.Value.AsString()) {
				out = append(out, a)
			}
		case "app.run.status":
			if a.Value.Type() == attribute.STRING && (a.Value.AsString() == "SUCCEEDED" || a.Value.AsString() == "FAILED") {
				out = append(out, a)
			}
		case "gen_ai.operation.name":
			if a.Value.Type() == attribute.STRING {
				switch a.Value.AsString() {
				case "chat", "invoke_agent", "execute_tool", "workflow":
					out = append(out, a)
				}
			}
		case "messaging.system":
			if a.Value.AsString() == "nats" {
				out = append(out, a)
			}
		case "messaging.destination.name":
			if a.Value.AsString() == "execution.run-requested.v1" || a.Value.AsString() == "execution.reply-intent.v1" {
				out = append(out, a)
			}
		case "messaging.operation.name":
			switch a.Value.AsString() {
			case "create", "publish", "process", "ack", "nack":
				out = append(out, a)
			}
		case "messaging.message.id":
			if a.Value.Type() == attribute.STRING && identifier(a.Value.AsString()) {
				out = append(out, a)
			}
		case "error.type", "app.outcome", "app.stage":
			if a.Value.Type() == attribute.STRING {
				switch a.Value.AsString() {
				case "ok", "failed", "dependency", "cancelled", "deadline", "fenced", "conflict", "capacity", "invalid",
					"manifest_wait", "session_wait", "credential_denied", "session_preparation", "session_invalid",
					"ACCEPTED", "NOT_SENT", "UNKNOWN", "REJECTED", "candidate", "accepted", "not_found", "parse_config", "connect", "target_query", "namespace_query", "table_query", "candidate_acl":
					out = append(out, a)
				}
			}
		case "app.retry.number", "app.part.index", "app.storage.records", "app.storage.bytes", "app.session.overlay.appends", "app.session.overlay.bytes",
			"gen_ai.usage.input_tokens", "gen_ai.usage.output_tokens", "gen_ai.usage.total_tokens",
			"http.response.status_code":
			if a.Value.Type() == attribute.INT64 && a.Value.AsInt64() >= 0 {
				out = append(out, a)
			}
		case "trpc_agent_go.client.time_to_first_token", "gen_ai.client.time_to_first_token":
			if a.Value.Type() == attribute.FLOAT64 && a.Value.AsFloat64() >= 0 {
				out = append(out, a)
			}
		case "gen_ai.response.finish_reasons":
			if a.Value.Type() == attribute.STRINGSLICE {
				clean := []string{}
				for _, v := range a.Value.AsStringSlice() {
					switch v {
					case "stop", "length", "tool_calls", "content_filter":
						clean = append(clean, v)
					}
				}
				if len(clean) > 0 {
					out = append(out, attribute.StringSlice(string(a.Key), clean))
				}
			}
		}
	}
	return out
}

func cleanContext(sc trace.SpanContext) trace.SpanContext {
	return sc.WithTraceState(trace.TraceState{})
}

// Embedding preserves the SDK's private ReadOnlySpan interface method. Only
// sanitized views are handed to the OTLP serializer; original snapshots are
// never mutated and may be shared with other in-process processors.
type filteredSpan struct {
	sdktrace.ReadOnlySpan
	resource *resource.Resource
}

func (s filteredSpan) Name() string { return spanName(s.ReadOnlySpan.Name()) }
func (s filteredSpan) Attributes() []attribute.KeyValue {
	return allowedAttributes(s.ReadOnlySpan.Attributes())
}
func (s filteredSpan) Resource() *resource.Resource { return s.resource }
func (s filteredSpan) Status() sdktrace.Status {
	v := s.ReadOnlySpan.Status()
	v.Description = ""
	return v
}
func (s filteredSpan) SpanContext() trace.SpanContext {
	return cleanContext(s.ReadOnlySpan.SpanContext())
}
func (s filteredSpan) Parent() trace.SpanContext { return cleanContext(s.ReadOnlySpan.Parent()) }
func (s filteredSpan) Events() []sdktrace.Event {
	// SDK exception/message events may contain arbitrary content. V1 exports
	// durations and allowlisted operation attributes instead of raw span events.
	return nil
}
func (s filteredSpan) Links() []sdktrace.Link {
	links := s.ReadOnlySpan.Links()
	out := make([]sdktrace.Link, 0, len(links))
	for _, v := range links {
		v.SpanContext = cleanContext(v.SpanContext)
		v.Attributes = allowedAttributes(v.Attributes)
		out = append(out, v)
	}
	return out
}
func (s filteredSpan) InstrumentationScope() instrumentation.Scope {
	v := s.ReadOnlySpan.InstrumentationScope()
	switch v.Name {
	case "trpc.agent.go", "agent-worker/execution-v1", "channel-gateway", "agent-worker":
	default:
		v.Name = "application"
	}
	v.Version = ""
	v.SchemaURL = ""
	return v
}
func (s filteredSpan) InstrumentationLibrary() instrumentation.Library {
	return s.InstrumentationScope()
}

type filteredExporter struct {
	next     sdktrace.SpanExporter
	resource *resource.Resource
	counters *counters
}

func (e *filteredExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	clean := make([]sdktrace.ReadOnlySpan, 0, len(spans))
	for _, s := range spans {
		clean = append(clean, filteredSpan{s, e.resource})
	}
	if err := e.next.ExportSpans(ctx, clean); err != nil {
		e.counters.failures.Add(1)
		return errExport
	}
	e.counters.exported.Add(uint64(len(spans)))
	return nil
}
func (e *filteredExporter) Shutdown(ctx context.Context) error {
	if err := e.next.Shutdown(ctx); err != nil {
		return errExport
	}
	return nil
}
