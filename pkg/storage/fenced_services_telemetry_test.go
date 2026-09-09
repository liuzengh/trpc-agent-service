//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmem "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func installFencedServiceSpanRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(recorder),
	)
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(previous)
	})
	return recorder
}

func TestFencedServicesEmitOnlySanitizedOperationSpans(t *testing.T) {
	recorder := installFencedServiceSpanRecorder(t)
	const secret = "raw-user-content-and-memory-query-must-not-reach-otel"

	authorizer := &countingFenceAuthorizer{}
	sessionService, err := NewStrictFencedSessionService(sessioninmem.NewSessionService(), authorizer, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	defer sessionService.Close()
	ctx := strictFenceContext(nil)
	sessionKey := session.Key{
		AppName: "tsa1:8:tenant-a:support", UserID: "user-a", SessionID: "session-a",
	}
	if _, err := sessionService.CreateSession(ctx, sessionKey, session.StateMap{"private": []byte(secret)}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := sessionService.GetSession(ctx, sessionKey); err != nil {
		t.Fatalf("get session: %v", err)
	}

	memoryService, err := NewStrictFencedMemoryService(inmemory.NewMemoryService(), authorizer, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	defer memoryService.Close()
	memoryKey := memory.UserKey{AppName: sessionKey.AppName, UserID: "user-a"}
	if err := memoryService.AddMemory(ctx, memoryKey, secret, []string{"private-topic"}); err != nil {
		t.Fatalf("add memory: %v", err)
	}
	if _, err := memoryService.SearchMemories(ctx, memoryKey, secret); err != nil {
		t.Fatalf("search memories: %v", err)
	}

	spans := recorder.Ended()
	if len(spans) != 4 {
		t.Fatalf("ended spans = %d, want 4", len(spans))
	}
	wantNames := []string{"session.write", "session.read", "memory.write", "memory.read"}
	for i, span := range spans {
		if span.Name() != wantNames[i] {
			t.Errorf("span %d name = %q, want %q", i, span.Name(), wantNames[i])
		}
		attributes := span.Attributes()
		if len(attributes) != 1 || attributes[0].Key != "error.code" || attributes[0].Value.AsString() != "none" {
			t.Errorf("span %d attributes = %#v, want only error.code=none", i, attributes)
		}
		if dump := fmt.Sprintf("%+v", span); strings.Contains(dump, secret) {
			t.Errorf("span %d leaked storage payload: %s", i, dump)
		}
	}
}

type traceAwareSessionService struct {
	session.Service
	spanContext trace.SpanContext
}

func (s *traceAwareSessionService) CreateSession(ctx context.Context, key session.Key, state session.StateMap, opts ...session.Option) (*session.Session, error) {
	s.spanContext = trace.SpanFromContext(ctx).SpanContext()
	return s.Service.CreateSession(ctx, key, state, opts...)
}

func TestFencedSessionServicePropagatesStorageSpanContext(t *testing.T) {
	recorder := installFencedServiceSpanRecorder(t)
	inner := &traceAwareSessionService{Service: sessioninmem.NewSessionService()}
	service, err := NewStrictFencedSessionService(inner, &countingFenceAuthorizer{}, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()

	key := session.Key{AppName: "tsa1:8:tenant-a:support", UserID: "user-a", SessionID: "session-a"}
	if _, err := service.CreateSession(strictFenceContext(nil), key, nil); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if !inner.spanContext.IsValid() {
		t.Fatal("backend did not receive an active storage span context")
	}
	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(spans))
	}
	if got := spans[0].SpanContext(); got.TraceID() != inner.spanContext.TraceID() || got.SpanID() != inner.spanContext.SpanID() {
		t.Fatalf("backend span context = %s/%s, want storage span = %s/%s", inner.spanContext.TraceID(), inner.spanContext.SpanID(), got.TraceID(), got.SpanID())
	}
}
