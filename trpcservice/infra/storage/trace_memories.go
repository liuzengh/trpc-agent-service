package storage

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// tracingMemories decorates a memory service with one span per storage
// operation, so the long-term memory half of a turn (preload read, semantic
// search, agent-driven writes) is visible in the same trace as agent.run.
type tracingMemories struct {
	inner   memory.Service
	backend Backend
}

// withTracingMemories wraps a memory service so its operations appear in the
// trace. memory.Service has no optional capability interfaces (the framework
// only defines Reader, a subset of Service), so — unlike the session decorator
// — delegation cannot hide anything.
func withTracingMemories(inner memory.Service, backend Backend) memory.Service {
	if inner == nil {
		return nil
	}
	return &tracingMemories{inner: inner, backend: backend}
}

func (m *tracingMemories) start(ctx context.Context, op string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return startStoreSpan(ctx, "memory", op, m.backend, attrs...)
}

func (m *tracingMemories) ReadMemories(ctx context.Context, userKey memory.UserKey, limit int) ([]*memory.Entry, error) {
	ctx, span := m.start(ctx, "read",
		attribute.String("memory.app_name", userKey.AppName),
		attribute.String("memory.user_id", userKey.UserID),
		attribute.Int("memory.limit", limit))
	entries, err := m.inner.ReadMemories(ctx, userKey, limit)
	if err == nil {
		// The preload path depends on this count: zero means "no long-term
		// context injected", which is worth seeing next to the model span.
		span.SetAttributes(attribute.Int("memory.count", len(entries)))
	}
	finish(span, err)
	return entries, err
}

func (m *tracingMemories) SearchMemories(ctx context.Context, userKey memory.UserKey, query string, opts ...memory.SearchOption) ([]*memory.Entry, error) {
	ctx, span := m.start(ctx, "search",
		attribute.String("memory.app_name", userKey.AppName),
		attribute.String("memory.user_id", userKey.UserID),
		attribute.Int("memory.query_len", len(query)))
	entries, err := m.inner.SearchMemories(ctx, userKey, query, opts...)
	if err == nil {
		span.SetAttributes(attribute.Int("memory.count", len(entries)))
	}
	finish(span, err)
	return entries, err
}

func (m *tracingMemories) AddMemory(ctx context.Context, userKey memory.UserKey, text string, topics []string, opts ...memory.AddOption) error {
	ctx, span := m.start(ctx, "add",
		attribute.String("memory.app_name", userKey.AppName),
		attribute.String("memory.user_id", userKey.UserID),
		attribute.Int("memory.topics", len(topics)),
		attribute.Int("memory.text_len", len(text)))
	err := m.inner.AddMemory(ctx, userKey, text, topics, opts...)
	finish(span, err)
	return err
}

func (m *tracingMemories) UpdateMemory(ctx context.Context, key memory.Key, text string, topics []string, opts ...memory.UpdateOption) error {
	ctx, span := m.start(ctx, "update",
		attribute.String("memory.app_name", key.AppName),
		attribute.String("memory.user_id", key.UserID),
		attribute.String("memory.id", key.MemoryID))
	err := m.inner.UpdateMemory(ctx, key, text, topics, opts...)
	finish(span, err)
	return err
}

func (m *tracingMemories) DeleteMemory(ctx context.Context, key memory.Key) error {
	ctx, span := m.start(ctx, "delete",
		attribute.String("memory.app_name", key.AppName),
		attribute.String("memory.user_id", key.UserID),
		attribute.String("memory.id", key.MemoryID))
	err := m.inner.DeleteMemory(ctx, key)
	finish(span, err)
	return err
}

func (m *tracingMemories) ClearMemories(ctx context.Context, userKey memory.UserKey) error {
	ctx, span := m.start(ctx, "clear",
		attribute.String("memory.app_name", userKey.AppName),
		attribute.String("memory.user_id", userKey.UserID))
	err := m.inner.ClearMemories(ctx, userKey)
	finish(span, err)
	return err
}

func (m *tracingMemories) EnqueueAutoMemoryJob(ctx context.Context, sess *session.Session) error {
	ctx, span := m.start(ctx, "enqueue_auto_job", attribute.String("session.id", sessionIDOf(sess)))
	err := m.inner.EnqueueAutoMemoryJob(ctx, sess)
	finish(span, err)
	return err
}

// Tools is a cheap local factory call, deliberately not spanned: a span per
// turn for a slice copy would only add noise.
func (m *tracingMemories) Tools() []tool.Tool { return m.inner.Tools() }

func (m *tracingMemories) Close() error { return m.inner.Close() }
