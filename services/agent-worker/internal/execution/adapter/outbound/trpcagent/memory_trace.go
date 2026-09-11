package trpcagent

import (
	"context"
	"errors"

	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	"go.opentelemetry.io/otel/trace"
	"trpc.group/trpc-go/trpc-agent-go/memory"
)

// TraceMemoryService instruments the final scoped Attempt service, not a raw
// database adapter. SDK tools resolve this outer service from Invocation, so
// tool calls and preloading keep their current trace parent. This wrapper never
// exports memory content, search queries, identity keys or dependency errors.
// Lifecycle and automatic-extraction behavior remain owned by the wrapped service.
func TraceMemoryService(service memory.Service, tracer trace.Tracer) (memory.Service, error) {
	if nilCapabilityService(service) {
		return nil, errors.New("memory service required for tracing")
	}
	return &tracedMemory{Service: service, tracer: tracer}, nil
}

type tracedMemory struct {
	memory.Service
	tracer trace.Tracer
}

func (s *tracedMemory) AddMemory(ctx context.Context, key memory.UserKey, value string, topics []string, opts ...memory.AddOption) (err error) {
	ctx, span := telemetrytrace.Start(s.tracer, ctx, "memory.write")
	defer func() { telemetrytrace.End(span, err) }()
	return s.Service.AddMemory(ctx, key, value, topics, opts...)
}

func (s *tracedMemory) UpdateMemory(ctx context.Context, key memory.Key, value string, topics []string, opts ...memory.UpdateOption) (err error) {
	ctx, span := telemetrytrace.Start(s.tracer, ctx, "memory.write")
	defer func() { telemetrytrace.End(span, err) }()
	return s.Service.UpdateMemory(ctx, key, value, topics, opts...)
}

func (s *tracedMemory) DeleteMemory(ctx context.Context, key memory.Key) (err error) {
	ctx, span := telemetrytrace.Start(s.tracer, ctx, "memory.delete")
	defer func() { telemetrytrace.End(span, err) }()
	return s.Service.DeleteMemory(ctx, key)
}

func (s *tracedMemory) ClearMemories(ctx context.Context, key memory.UserKey) (err error) {
	ctx, span := telemetrytrace.Start(s.tracer, ctx, "memory.delete")
	defer func() { telemetrytrace.End(span, err) }()
	return s.Service.ClearMemories(ctx, key)
}

func (s *tracedMemory) ReadMemories(ctx context.Context, key memory.UserKey, limit int) (entries []*memory.Entry, err error) {
	ctx, span := telemetrytrace.Start(s.tracer, ctx, "memory.read")
	defer func() { telemetrytrace.End(span, err) }()
	return s.Service.ReadMemories(ctx, key, limit)
}

func (s *tracedMemory) SearchMemories(ctx context.Context, key memory.UserKey, query string, opts ...memory.SearchOption) (entries []*memory.Entry, err error) {
	ctx, span := telemetrytrace.Start(s.tracer, ctx, "memory.search")
	defer func() { telemetrytrace.End(span, err) }()
	return s.Service.SearchMemories(ctx, key, query, opts...)
}
