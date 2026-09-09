//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package telemetry

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func installOperationSpanRecorder(t *testing.T) *tracetest.SpanRecorder {
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

func TestOperationSpanUsesAllowlistedNameAndStableErrorCode(t *testing.T) {
	recorder := installOperationSpanRecorder(t)
	secret := "postgres://user:password@example.invalid/db?token=live"
	_, span := StartOperation(context.Background(), OperationToolInvoke)
	EndOperation(span, errors.New(secret))

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(spans))
	}
	got := spans[0]
	if got.Name() != string(OperationToolInvoke) {
		t.Fatalf("span name = %q, want %q", got.Name(), OperationToolInvoke)
	}
	if got.Status().Code.String() != "Error" || got.Status().Description != "internal_error" {
		t.Fatalf("span status = %+v, want stable internal error", got.Status())
	}
	attributes := got.Attributes()
	if len(attributes) != 1 || attributes[0] != attribute.String("error.code", "internal_error") {
		t.Fatalf("span attributes = %#v, want only stable error code", attributes)
	}
	if dump := fmt.Sprintf("%+v", got); containsSensitiveValue(dump, secret) {
		t.Fatalf("span contains raw error text: %s", dump)
	}
}

func TestOperationSpanNormalizesUnknownName(t *testing.T) {
	recorder := installOperationSpanRecorder(t)
	secret := "tenant-a/user-a/session-a/tool-secret"
	_, span := StartOperation(context.Background(), Operation(secret))
	EndOperation(span, nil)

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(spans))
	}
	got := spans[0]
	if got.Name() != string(operationUnknown) {
		t.Fatalf("span name = %q, want %q", got.Name(), operationUnknown)
	}
	attributes := got.Attributes()
	if len(attributes) != 1 || attributes[0] != attribute.String("error.code", "none") {
		t.Fatalf("span attributes = %#v, want only stable success code", attributes)
	}
	if dump := fmt.Sprintf("%+v", got); containsSensitiveValue(dump, secret) {
		t.Fatalf("span contains caller-controlled operation: %s", dump)
	}
}

func containsSensitiveValue(value, secret string) bool {
	return secret != "" && strings.Contains(value, secret)
}
