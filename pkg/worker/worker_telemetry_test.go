//
// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func installWorkerSpanRecorder(t *testing.T) *tracetest.SpanRecorder {
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

func TestWorkerProcessEmitsSanitizedWorkerAndRunnerSpans(t *testing.T) {
	recorder := installWorkerSpanRecorder(t)
	const secret = "sensitive-user-and-model-content"
	runner := &scriptedRunner{events: completedModelEvents("provider-response", secret, nil)}
	value := newBudgetProcessWorker(t, runner, &recordingBudgetController{})

	response, err := value.Process(context.Background(), &Request{
		UserID: "user", SessionID: "session", ChannelType: "telegram", Content: secret,
	})
	if err != nil || response == nil || response.Content != secret {
		t.Fatalf("Process response=%+v err=%v", response, err)
	}

	spans := recorder.Ended()
	if len(spans) != 2 {
		t.Fatalf("ended spans = %d, want 2", len(spans))
	}
	runnerSpan, workerSpan := spans[0], spans[1]
	if runnerSpan.Name() != "runner.run" || workerSpan.Name() != "worker.process" {
		t.Fatalf("span names = [%q, %q], want [runner.run, worker.process]", runnerSpan.Name(), workerSpan.Name())
	}
	if runnerSpan.Parent().SpanID() != workerSpan.SpanContext().SpanID() {
		t.Fatalf("runner span parent = %s, want Worker span %s", runnerSpan.Parent().SpanID(), workerSpan.SpanContext().SpanID())
	}
	for _, span := range spans {
		attrs := span.Attributes()
		if len(attrs) != 1 || attrs[0].Key != "error.code" || attrs[0].Value.AsString() != "none" {
			t.Errorf("span %q attributes = %#v, want only error.code=none", span.Name(), attrs)
		}
		if dump := fmt.Sprintf("%+v", span); strings.Contains(dump, secret) {
			t.Errorf("span %q leaked request or model content: %s", span.Name(), dump)
		}
	}
}

func TestWorkerProcessReportsRunnerFailureOnBothSpans(t *testing.T) {
	recorder := installWorkerSpanRecorder(t)
	const secret = "provider-error-containing-a-secret"
	runErr := errors.New(secret)
	value := newBudgetProcessWorker(t, &scriptedRunner{runErr: runErr}, &recordingBudgetController{})

	_, err := value.Process(context.Background(), budgetProcessRequest())
	if !errors.Is(err, runErr) {
		t.Fatalf("Process error=%v, want wrapped runner error", err)
	}

	spans := recorder.Ended()
	if len(spans) != 2 {
		t.Fatalf("ended spans = %d, want 2", len(spans))
	}
	for _, span := range spans {
		attrs := span.Attributes()
		if len(attrs) != 1 || attrs[0].Key != "error.code" || attrs[0].Value.AsString() != "internal_error" {
			t.Errorf("span %q attributes = %#v, want only error.code=internal_error", span.Name(), attrs)
		}
		if dump := fmt.Sprintf("%+v", span); strings.Contains(dump, secret) {
			t.Errorf("span %q leaked provider error: %s", span.Name(), dump)
		}
	}
}
