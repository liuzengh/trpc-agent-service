// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package worker

import (
	"strings"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestCollectRunnerResponseRejectsTerminalErrorAfterPartialOutput(t *testing.T) {
	events := make(chan *event.Event, 3)
	events <- responseEvent("partial answer")
	events <- event.NewErrorEvent("invocation", "agent", "model_error", "provider stream failed")
	events <- runnerCompletionEvent()
	close(events)

	result, err := collectRunnerResponse(events)
	if err == nil || !strings.Contains(err.Error(), "model_error") {
		t.Fatalf("terminal error was accepted: content=%q err=%v", result.Content, err)
	}
	if strings.Contains(err.Error(), "provider stream failed") {
		t.Fatalf("provider error detail leaked through worker error: %v", err)
	}
}

func TestCollectRunnerResponseRequiresRunnerCompletion(t *testing.T) {
	events := make(chan *event.Event, 1)
	events <- responseEvent("orphaned answer")
	close(events)

	result, err := collectRunnerResponse(events)
	if err == nil || !strings.Contains(err.Error(), "completion") {
		t.Fatalf("incomplete stream was accepted: content=%q err=%v", result.Content, err)
	}
}

func TestCollectRunnerResponseRejectsNilEventStream(t *testing.T) {
	if _, err := collectRunnerResponse(nil); err == nil || !strings.Contains(err.Error(), "nil event stream") {
		t.Fatalf("nil event stream was accepted: %v", err)
	}
}

func TestCollectRunnerResponseReturnsCompletedContent(t *testing.T) {
	events := make(chan *event.Event, 3)
	events <- responseEvent("hello ")
	events <- responseEvent("world")
	events <- runnerCompletionEvent()
	close(events)

	result, err := collectRunnerResponse(events)
	if err != nil {
		t.Fatal(err)
	}
	if result.Content != "hello world" {
		t.Fatalf("content=%q, want %q", result.Content, "hello world")
	}
}

func TestCollectRunnerResponseUsesMaximumCumulativeUsagePerResponse(t *testing.T) {
	events := make(chan *event.Event, 4)
	events <- usageResponseEvent("response-1", 10)
	events <- usageResponseEvent("response-1", 15)
	events <- usageResponseEvent("response-1", 12)
	events <- runnerCompletionEvent()
	close(events)

	result, err := collectRunnerResponse(events)
	if err != nil {
		t.Fatal(err)
	}
	if !result.UsageReliable || result.TotalTokens != 15 {
		t.Fatalf("usage=%+v, want reliable total 15", result)
	}
}

func TestCollectRunnerResponseSumsDistinctProviderResponses(t *testing.T) {
	events := make(chan *event.Event, 3)
	events <- usageResponseEvent("response-1", 15)
	events <- usageResponseEvent("response-2", 7)
	events <- runnerCompletionEvent()
	close(events)

	result, err := collectRunnerResponse(events)
	if err != nil {
		t.Fatal(err)
	}
	if !result.UsageReliable || result.TotalTokens != 22 {
		t.Fatalf("usage=%+v, want reliable total 22", result)
	}
}

func TestCollectRunnerResponseRejectsUsageWithoutStableIdentity(t *testing.T) {
	events := make(chan *event.Event, 2)
	events <- usageResponseEvent("", 9)
	events <- runnerCompletionEvent()
	close(events)

	result, err := collectRunnerResponse(events)
	if err != nil {
		t.Fatal(err)
	}
	if result.UsageReliable {
		t.Fatalf("anonymous provider usage was treated as reliable: %+v", result)
	}
}

func TestCollectRunnerResponseTreatsAllZeroUsageAsUnreliable(t *testing.T) {
	events := make(chan *event.Event, 2)
	events <- usageResponseEvent("response-1", 0)
	events <- runnerCompletionEvent()
	close(events)

	result, err := collectRunnerResponse(events)
	if err != nil {
		t.Fatal(err)
	}
	if result.UsageReliable {
		t.Fatalf("zero provider usage was treated as reliable: %+v", result)
	}
}

func TestCollectRunnerResponseRejectsNegativeUsage(t *testing.T) {
	events := make(chan *event.Event, 2)
	events <- usageResponseEvent("response-1", -1)
	events <- runnerCompletionEvent()
	close(events)

	if _, err := collectRunnerResponse(events); err == nil {
		t.Fatal("negative provider usage was accepted")
	}
}

func TestCollectRunnerResponseDrainsProducerAfterTerminalError(t *testing.T) {
	events := make(chan *event.Event)
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		events <- event.NewErrorEvent("invocation", "agent", "model_error", "provider detail")
		events <- responseEvent("must be drained but ignored")
		events <- runnerCompletionEvent()
		close(events)
	}()

	result, err := collectRunnerResponse(events)
	if err == nil || !strings.Contains(err.Error(), "model_error") || result.Content != "" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	select {
	case <-producerDone:
	case <-time.After(time.Second):
		t.Fatal("runner event producer remained blocked after terminal error")
	}
}

func TestCollectRunnerResponseDrainsProducerAfterNegativeUsage(t *testing.T) {
	events := make(chan *event.Event)
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		events <- usageResponseEvent("response-1", -1)
		events <- responseEvent("must be drained but ignored")
		events <- runnerCompletionEvent()
		close(events)
	}()

	result, err := collectRunnerResponse(events)
	if err == nil || result.Content != "" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	select {
	case <-producerDone:
	case <-time.After(time.Second):
		t.Fatal("runner event producer remained blocked after invalid usage")
	}
}

func TestCollectRunnerResponseBoundsModelOutput(t *testing.T) {
	events := make(chan *event.Event, 3)
	events <- responseEvent(strings.Repeat("x", maxRunnerResponseBytes))
	events <- responseEvent("overflow")
	events <- runnerCompletionEvent()
	close(events)
	if _, err := collectRunnerResponse(events); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized response was accepted: %v", err)
	}
}

func responseEvent(content string) *event.Event {
	return event.NewResponseEvent("invocation", "agent", &model.Response{
		Choices: []model.Choice{{Delta: model.Message{Content: content}}},
	})
}

func usageResponseEvent(responseID string, total int) *event.Event {
	return event.NewResponseEvent("invocation", "agent", &model.Response{
		ID:    responseID,
		Usage: &model.Usage{TotalTokens: total},
	})
}

func runnerCompletionEvent() *event.Event {
	return event.NewResponseEvent("invocation", "runner", &model.Response{
		Object: model.ObjectTypeRunnerCompletion,
		Done:   true,
	})
}
