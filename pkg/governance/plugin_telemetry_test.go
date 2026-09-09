//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package governance

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

func installPluginSpanRecorder(t *testing.T) *tracetest.SpanRecorder {
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

func TestPluginToolSpanDoesNotExposeArgumentsOrRawErrors(t *testing.T) {
	recorder := installPluginSpanRecorder(t)
	const secret = "postgres://user:password@example.invalid/db?token=live"
	plugin := NewPlugin(NewGovernanceFilter(&tenant.Tenant{ToolPolicy: tenant.ToolPolicy{
		Mode: "whitelist", Allowed: []string{"safe"},
	}}), "test")

	before, err := plugin.beforeTool(context.Background(), &tool.BeforeToolArgs{
		ToolCallID: "call-1",
		ToolName:   "safe",
		Arguments:  []byte(`{"credential":"` + secret + `"}`),
	})
	if err != nil {
		t.Fatalf("before tool: %v", err)
	}
	if _, err := plugin.afterTool(before.Context, &tool.AfterToolArgs{
		ToolCallID: "call-1",
		ToolName:   "safe",
		Error:      errors.New(secret),
	}); err == nil {
		t.Fatal("tool execution error was unexpectedly suppressed")
	}

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(spans))
	}
	span := spans[0]
	if span.Name() != "tool.invoke" {
		t.Fatalf("span name = %q, want tool.invoke", span.Name())
	}
	attributes := span.Attributes()
	if len(attributes) != 1 || attributes[0].Key != "error.code" || attributes[0].Value.AsString() != "internal_error" {
		t.Fatalf("span attributes = %#v, want only stable error code", attributes)
	}
	if dump := fmt.Sprintf("%+v", span); strings.Contains(dump, secret) {
		t.Fatalf("tool span leaked sensitive input or error: %s", dump)
	}
}

func TestPluginDeniedToolFinishesItsSpan(t *testing.T) {
	recorder := installPluginSpanRecorder(t)
	const secret = "sensitive-tool-argument"
	plugin := NewPlugin(NewGovernanceFilter(&tenant.Tenant{ToolPolicy: tenant.ToolPolicy{
		Mode: "whitelist", Allowed: []string{"safe"},
	}}), "test")
	if _, err := plugin.beforeTool(context.Background(), &tool.BeforeToolArgs{
		ToolName:  "denied",
		Arguments: []byte(`{"payload":"` + secret + `"}`),
	}); err == nil {
		t.Fatal("denied tool was allowed")
	}

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(spans))
	}
	span := spans[0]
	if span.Name() != "tool.invoke" {
		t.Fatalf("span name = %q, want tool.invoke", span.Name())
	}
	attributes := span.Attributes()
	if len(attributes) != 1 || attributes[0].Key != "error.code" || attributes[0].Value.AsString() != "internal_error" {
		t.Fatalf("span attributes = %#v, want only stable error code", attributes)
	}
	if dump := fmt.Sprintf("%+v", span); strings.Contains(dump, secret) {
		t.Fatalf("denied tool span leaked arguments: %s", dump)
	}
}
