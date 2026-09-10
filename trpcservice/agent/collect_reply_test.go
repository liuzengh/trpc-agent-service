package agent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func responseEvent(done bool, object string, messageContent, delta string) *event.Event {
	choices := make([]model.Choice, 0, 1)
	if messageContent != "" || delta != "" {
		choice := model.Choice{}
		if messageContent != "" {
			choice.Message = model.NewAssistantMessage(messageContent)
		}
		if delta != "" {
			choice.Delta = model.Message{Role: model.RoleAssistant, Content: delta}
		}
		choices = append(choices, choice)
	}
	return &event.Event{Response: &model.Response{Done: done, Object: object, Choices: choices}}
}

func TestCollectReplyPrefersFinalMessageAndStopsAtCompletion(t *testing.T) {
	t.Parallel()
	events := make(chan *event.Event, 4)
	events <- responseEvent(false, model.ObjectTypeChatCompletionChunk, "", "h")
	events <- responseEvent(false, model.ObjectTypeChatCompletionChunk, "", "i")
	events <- responseEvent(false, model.ObjectTypeChatCompletion, "hi", "")
	events <- responseEvent(true, model.ObjectTypeRunnerCompletion, "", "")
	close(events)

	outcome := collectRun(context.Background(), events, nil)
	if outcome.err != nil {
		t.Fatalf("collectRun() error = %v, want nil", outcome.err)
	}
	if outcome.reply != "hi" {
		t.Fatalf("collectRun() reply = %q, want %q", outcome.reply, "hi")
	}
}

func TestCollectReplyFallsBackToStreamedDeltas(t *testing.T) {
	t.Parallel()
	events := make(chan *event.Event, 3)
	events <- responseEvent(false, model.ObjectTypeChatCompletionChunk, "", "hello ")
	events <- responseEvent(false, model.ObjectTypeChatCompletionChunk, "", "world")
	events <- responseEvent(true, model.ObjectTypeRunnerCompletion, "", "")
	close(events)

	outcome := collectRun(context.Background(), events, nil)
	if outcome.err != nil {
		t.Fatalf("collectRun() error = %v, want nil", outcome.err)
	}
	if outcome.reply != "hello world" {
		t.Fatalf("collectRun() reply = %q, want %q", outcome.reply, "hello world")
	}
}

func TestCollectReplyTerminalErrorFailsFast(t *testing.T) {
	t.Parallel()
	events := make(chan *event.Event, 2)
	events <- responseEvent(false, model.ObjectTypeChatCompletionChunk, "", "partial")
	events <- &event.Event{Response: &model.Response{Done: true, Object: model.ObjectTypeError, Error: &model.ResponseError{Message: "boom"}}}
	close(events)

	outcome := collectRun(context.Background(), events, nil)
	if outcome.err == nil {
		t.Fatal("collectRun() error = nil, want terminal failure")
	} else if !errors.Is(outcome.err, ErrAgentExecutionFailed) {
		t.Fatalf("collectRun() error = %v, want ErrAgentExecutionFailed", outcome.err)
	}
}

func TestCollectReplyEmptyRunFails(t *testing.T) {
	t.Parallel()
	events := make(chan *event.Event, 1)
	events <- responseEvent(true, model.ObjectTypeRunnerCompletion, "", "")
	close(events)

	outcome := collectRun(context.Background(), events, nil)
	if outcome.err == nil {
		t.Fatal("collectRun() error = nil, want empty-reply failure")
	} else if !errors.Is(outcome.err, ErrAgentProducedNoReply) {
		t.Fatalf("collectRun() error = %v, want ErrAgentProducedNoReply", outcome.err)
	}
}

func TestCollectReplyPublishesEveryDelta(t *testing.T) {
	t.Parallel()
	events := make(chan *event.Event, 3)
	events <- responseEvent(false, model.ObjectTypeChatCompletionChunk, "", "hello ")
	events <- responseEvent(false, model.ObjectTypeChatCompletionChunk, "", "world")
	events <- responseEvent(true, model.ObjectTypeRunnerCompletion, "", "")
	close(events)

	var published strings.Builder
	outcome := collectRun(context.Background(), events, func(content string) { _, _ = published.WriteString(content) })
	if outcome.err != nil {
		t.Fatalf("collectRun() error = %v", outcome.err)
	}
	if outcome.reply != "hello world" || published.String() != "hello world" {
		t.Fatalf("reply=%q published=%q, want both %q", outcome.reply, published.String(), "hello world")
	}
}

func TestCollectReplyDrainsRemainingEventsAfterCompletion(t *testing.T) {
	t.Parallel()
	events := make(chan *event.Event)
	producerDone := make(chan struct{})

	go func() {
		defer close(producerDone)
		events <- responseEvent(false, model.ObjectTypeChatCompletion, "done message", "")
		events <- responseEvent(true, model.ObjectTypeRunnerCompletion, "", "")
		events <- responseEvent(false, model.ObjectTypeChatCompletionChunk, "", "trailing event")
		close(events)
	}()

	outcome := collectRun(context.Background(), events, nil)
	if outcome.err != nil {
		t.Fatalf("collectRun() error = %v, want nil", outcome.err)
	}
	if outcome.reply != "done message" {
		t.Fatalf("reply = %q, want %q", outcome.reply, "done message")
	}

	select {
	case <-producerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("producer goroutine was blocked; channel draining failed")
	}
}

func TestCollectReplyPreservesCachedPromptUsage(t *testing.T) {
	t.Parallel()
	events := make(chan *event.Event, 2)
	response := responseEvent(false, model.ObjectTypeChatCompletion, "done", "")
	response.Response.Usage = &model.Usage{
		PromptTokens: 100, CompletionTokens: 20, TotalTokens: 120,
		PromptTokensDetails: model.PromptTokensDetails{CachedTokens: 70, CacheReadTokens: 60},
	}
	events <- response
	events <- responseEvent(true, model.ObjectTypeRunnerCompletion, "", "")
	close(events)

	outcome := collectRun(context.Background(), events, nil)
	if outcome.err != nil {
		t.Fatal(outcome.err)
	}
	if outcome.usage.promptTokens != 100 || outcome.usage.cachedPromptTokens != 70 {
		t.Fatalf("usage = %#v, want 100 prompt / 70 cached", outcome.usage)
	}
}

func TestCollectReplyContextCancellationDrainsAndAborts(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan *event.Event)
	producerDone := make(chan struct{})

	go func() {
		defer close(producerDone)
		events <- responseEvent(false, model.ObjectTypeChatCompletionChunk, "", "first chunk")
		for i := 0; i < 5; i++ {
			events <- responseEvent(false, model.ObjectTypeChatCompletionChunk, "", "more chunk")
		}
		close(events)
	}()

	outcomeChan := make(chan runOutcome, 1)
	go func() {
		outcome := collectRun(ctx, events, func(delta string) {
			if delta == "first chunk" {
				cancel()
			}
		})
		outcomeChan <- outcome
	}()

	select {
	case outcome := <-outcomeChan:
		if !errors.Is(outcome.err, context.Canceled) {
			t.Fatalf("outcome.err = %v, want context.Canceled", outcome.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("collectRun did not abort on context cancellation")
	}

	select {
	case <-producerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("producer goroutine leaked; channel draining failed after context cancellation")
	}
}
