package trpcagent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"trpc.group/trpc-go/trpc-agent-go/memory"
)

func TestMemoryTraceParentContentAndFailure(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	tracer := provider.Tracer("memory-test")
	ctx, parent := tracer.Start(context.Background(), "execute_tool memory_add")
	key := memory.UserKey{AppName: "PRIVATE_APP", UserID: "PRIVATE_USER"}
	attempt, err := NewMemoryAttempt(ctx, key, memory.UserKey{AppName: "bound", UserID: "scope"}, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = attempt.Close() })
	service, err := TraceMemoryService(attempt, tracer)
	if err != nil {
		t.Fatal(err)
	}
	if err = service.AddMemory(ctx, key, "PRIVATE_CONTENT tea", []string{"PRIVATE_TOPIC"}); err != nil {
		t.Fatal(err)
	}
	entries, err := service.ReadMemories(ctx, key, 0)
	if err != nil || len(entries) != 1 {
		t.Fatalf("read: %v %v", entries, err)
	}
	entryKey := memory.Key{AppName: key.AppName, UserID: key.UserID, MemoryID: entries[0].ID}
	updated := memory.UpdateResult{}
	if err = service.UpdateMemory(ctx, entryKey, "PRIVATE_CONTENT tea again", nil, memory.WithUpdateResult(&updated)); err != nil {
		t.Fatal(err)
	}
	if _, err = service.SearchMemories(ctx, key, "PRIVATE_QUERY tea"); err != nil {
		t.Fatal(err)
	}
	entryKey.MemoryID = updated.MemoryID
	if err = service.DeleteMemory(ctx, entryKey); err != nil {
		t.Fatal(err)
	}
	if err = service.ClearMemories(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err = service.AddMemory(ctx, memory.UserKey{AppName: "FOREIGN", UserID: key.UserID}, "PRIVATE_FAILURE", nil); !errors.Is(err, ErrMemoryScope) {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = service.ReadMemories(cancelled, key, 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	parent.End()
	spans := exporter.GetSpans()
	if len(spans) != 9 {
		t.Fatalf("spans=%d", len(spans))
	}
	want := []string{"memory.write", "memory.read", "memory.write", "memory.search", "memory.delete", "memory.delete", "memory.write", "memory.read"}
	for i, name := range want {
		s := spans[i]
		if s.Name != name || s.Parent.SpanID() != parent.SpanContext().SpanID() || s.SpanContext.TraceID() != parent.SpanContext().TraceID() {
			t.Fatalf("bad span %d: %+v", i, s)
		}
		if len(s.Events) != 0 || s.Status.Description != "" {
			t.Fatalf("unfiltered error: %+v", s)
		}
		if i >= 6 && s.Status.Code != codes.Error {
			t.Fatal("failure not marked")
		}
	}
	encoded, err := json.Marshal(spans)
	if err != nil || strings.Contains(string(encoded), "PRIVATE_") || strings.Contains(string(encoded), "FOREIGN") {
		t.Fatalf("content export: %s %v", encoded, err)
	}
}

func TestMemoryTraceNoExporterPreservesParentAndRejectsNil(t *testing.T) {
	var typedNil *MemoryAttempt
	if _, err := TraceMemoryService(typedNil, nil); err == nil {
		t.Fatal("typed nil accepted")
	}
	parent := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled})
	ctx := trace.ContextWithSpanContext(context.Background(), parent)
	inner := &memoryTraceContextObserver{}
	wrapped, err := TraceMemoryService(inner, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = wrapped.AddMemory(ctx, memory.UserKey{}, "value", nil); err != nil {
		t.Fatal(err)
	}
	if !inner.seen.Equal(parent) {
		t.Fatal("parent changed")
	}
}

type memoryTraceContextObserver struct {
	memory.Service
	seen trace.SpanContext
}

func (s *memoryTraceContextObserver) AddMemory(ctx context.Context, _ memory.UserKey, _ string, _ []string, _ ...memory.AddOption) error {
	s.seen = trace.SpanContextFromContext(ctx)
	return nil
}
