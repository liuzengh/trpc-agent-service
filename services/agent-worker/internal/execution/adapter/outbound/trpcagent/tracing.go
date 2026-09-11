package trpcagent

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	semconv "trpc.group/trpc-go/trpc-agent-go/telemetry/semconv/trace"
	agenttrace "trpc.group/trpc-go/trpc-agent-go/telemetry/trace"
)

// BindTracing is bootstrap-only: call once before starting any SDK executions.
// The SDK's exported Tracer is independent of otel's global provider. Platform
// operations use explicitly injected tracers; this is the sole SDK compatibility
// bridge. The content policy is process-wide and is not changed at runtime.
func BindTracing(provider trace.TracerProvider) {
	policy := agenttrace.SpanAttributePolicy{}
	keys := []agenttrace.AttributeKey{
		agenttrace.AttrLLMRequest, agenttrace.AttrLLMResponse,
		agenttrace.AttrInputMessages, agenttrace.AttrInputMessagesOTel,
		agenttrace.AttrOutputMessages, agenttrace.AttrOutputMessagesOTel,
		agenttrace.AttributeKey(semconv.KeyGenAIToolCallArguments),
		agenttrace.AttributeKey(semconv.KeyGenAIToolCallResult),
		agenttrace.AttributeKey(semconv.KeyGenAIWorkflowRequest),
		agenttrace.AttributeKey(semconv.KeyGenAIWorkflowResponse),
	}
	for _, op := range []agenttrace.SpanOperation{agenttrace.OperationChat, agenttrace.OperationInvokeAgent, agenttrace.OperationWorkflow, agenttrace.OperationExecuteTool} {
		for _, key := range keys {
			agenttrace.WithAttributeRule(op, key, agenttrace.Drop())(&policy)
		}
	}
	agenttrace.SetSpanAttributePolicy(policy)
	agenttrace.TracerProvider = provider
	agenttrace.Tracer = cancellationTracer{Tracer: provider.Tracer("trpc.agent.go")}
}

// SDK cancellation paths can return before populating Tool response attributes.
// Reflect the actual context cancellation on End without changing SDK execution.
type cancellationTracer struct{ trace.Tracer }

func (t cancellationTracer) Start(ctx context.Context, name string, options ...trace.SpanStartOption) (context.Context, trace.Span) {
	child, span := t.Tracer.Start(ctx, name, options...)
	wrapped := cancellationSpan{Span: span, ctx: child}
	return trace.ContextWithSpan(child, wrapped), wrapped
}

type cancellationSpan struct {
	trace.Span
	ctx context.Context
}

func (s cancellationSpan) End(options ...trace.SpanEndOption) {
	if err := s.ctx.Err(); err != nil {
		kind := "cancelled"
		if errors.Is(err, context.DeadlineExceeded) {
			kind = "deadline"
		}
		s.Span.SetStatus(codes.Error, "")
		s.Span.SetAttributes(attribute.String("error.type", kind))
	}
	s.Span.End(options...)
}
