package agent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	tagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

// fakeRunner implements runner.Runner with a scripted behavior per call.
type fakeRunner struct {
	mu    sync.Mutex
	calls int
	run   func(ctx context.Context, call int) (<-chan *event.Event, error)
}

func (f *fakeRunner) Run(ctx context.Context, _, _ string, _ model.Message, _ ...tagent.RunOption) (<-chan *event.Event, error) {
	f.mu.Lock()
	f.calls++
	call := f.calls
	f.mu.Unlock()
	return f.run(ctx, call)
}

func (f *fakeRunner) Close() error { return nil }

func (f *fakeRunner) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func finalEvent(text string) *event.Event {
	return &event.Event{Response: &model.Response{
		Done:    true,
		Choices: []model.Choice{{Message: model.NewAssistantMessage(text)}},
		Usage:   &model.Usage{PromptTokens: 10, CompletionTokens: 5},
	}}
}

func errorEvent(msg string) *event.Event {
	return &event.Event{Response: &model.Response{
		Error: &model.ResponseError{Message: msg},
	}}
}

func scriptedEvents(events ...*event.Event) func(context.Context, int) (<-chan *event.Event, error) {
	return func(context.Context, int) (<-chan *event.Event, error) {
		ch := make(chan *event.Event, len(events))
		for _, e := range events {
			ch <- e
		}
		close(ch)
		return ch, nil
	}
}

func TestRunnerProcessorSuccess(t *testing.T) {
	p := newRunnerProcessor(&fakeRunner{run: scriptedEvents(finalEvent("你好"))}, "test-model", time.Second, 1)
	out, err := p.Process(context.Background(), testMsg("hi"))
	if err != nil {
		t.Fatal(err)
	}
	if out.Text != "你好" {
		t.Fatalf("unexpected reply: %q", out.Text)
	}
	// The outbound shell must keep the routing fields for the sender.
	if out.SessionKey != "dm:mock:u1" || out.UserID != "u1" || out.TenantID != "t1" {
		t.Fatalf("routing fields lost: %+v", out)
	}
}

func TestRunnerProcessorRetriesThenSucceeds(t *testing.T) {
	r := &fakeRunner{run: func(ctx context.Context, call int) (<-chan *event.Event, error) {
		if call == 1 {
			return scriptedEvents(errorEvent("rate limited"))(ctx, call)
		}
		return scriptedEvents(finalEvent("ok"))(ctx, call)
	}}
	p := newRunnerProcessor(r, "test-model", time.Second, 1)
	out, err := p.Process(context.Background(), testMsg("hi"))
	if err != nil {
		t.Fatal(err)
	}
	if out.Text != "ok" {
		t.Fatalf("unexpected reply: %q", out.Text)
	}
	if r.callCount() != 2 {
		t.Fatalf("want 2 attempts, got %d", r.callCount())
	}
}

func TestRunnerProcessorModelErrorAfterRetries(t *testing.T) {
	r := &fakeRunner{run: scriptedEvents(errorEvent("boom"))}
	p := newRunnerProcessor(r, "test-model", time.Second, 1)
	_, err := p.Process(context.Background(), testMsg("hi"))
	var mErr *ModelError
	if !errors.As(err, &mErr) {
		t.Fatalf("want ModelError, got %v", err)
	}
	if r.callCount() != 2 {
		t.Fatalf("want 1 retry = 2 attempts, got %d", r.callCount())
	}
}

func TestRunnerProcessorTimeout(t *testing.T) {
	// Mimic the framework: the event channel closes only after ctx cancel.
	r := &fakeRunner{run: func(ctx context.Context, _ int) (<-chan *event.Event, error) {
		ch := make(chan *event.Event)
		go func() {
			<-ctx.Done()
			close(ch)
		}()
		return ch, nil
	}}
	p := newRunnerProcessor(r, "test-model", 50*time.Millisecond, 0)
	started := time.Now()
	_, err := p.Process(context.Background(), testMsg("hi"))
	var mErr *ModelError
	if !errors.As(err, &mErr) {
		t.Fatalf("want ModelError, got %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline exceeded cause, got %v", err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("timeout not enforced, took %s", elapsed)
	}
}

func TestRunnerProcessorInfraErrorNotRetried(t *testing.T) {
	infra := errors.New("session store down")
	r := &fakeRunner{run: func(context.Context, int) (<-chan *event.Event, error) {
		return nil, infra
	}}
	p := newRunnerProcessor(r, "test-model", time.Second, 1)
	_, err := p.Process(context.Background(), testMsg("hi"))
	if !errors.Is(err, infra) {
		t.Fatalf("want the raw infra error, got %v", err)
	}
	var mErr *ModelError
	if errors.As(err, &mErr) {
		t.Fatal("infra errors must not degrade into ModelError")
	}
	if r.callCount() != 1 {
		t.Fatalf("infra errors are not retried inline, got %d attempts", r.callCount())
	}
}

func TestRunnerProcessorEmptyResponseIsModelError(t *testing.T) {
	// Channel closes without a final response and without an error event.
	r := &fakeRunner{run: scriptedEvents()}
	p := newRunnerProcessor(r, "test-model", time.Second, 0)
	_, err := p.Process(context.Background(), testMsg("hi"))
	var mErr *ModelError
	if !errors.As(err, &mErr) {
		t.Fatalf("want ModelError, got %v", err)
	}
	if !strings.Contains(err.Error(), "no final response") {
		t.Fatalf("unexpected cause: %v", err)
	}
}

// Drain/shutdown cancellation is infrastructure, not a model failure:
// Process returns the raw cancel error instead of a ModelError, so
// the guardrail does not degrade into a busy reply and the worker leaves the
// message pending for a surviving replica.
func TestRunnerProcessorCancelReturnsRawError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &fakeRunner{run: func(ctx context.Context, _ int) (<-chan *event.Event, error) {
		cancel() // the drain deadline fires mid-run
		return scriptedEvents(errorEvent("shutting down"))(ctx, 0)
	}}
	p := newRunnerProcessor(r, "test-model", time.Second, 1)
	_, err := p.Process(ctx, channels.InboundMessage{Channel: "mock", SessionKey: "s1", UserID: "u1", Text: "hi"})
	if err == nil {
		t.Fatal("canceled run must return an error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want the raw cancel error, got %v", err)
	}
	var me *ModelError
	if errors.As(err, &me) {
		t.Fatal("cancellation must not be wrapped as ModelError")
	}
}

// The backoff sleeps ~2^attempt scaled with jitter and aborts on a canceled
// context.
func TestRetryBackoff(t *testing.T) {
	start := time.Now()
	if err := retryBackoff(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	el := time.Since(start)
	if el < 200*time.Millisecond || el > 2*time.Second {
		t.Fatalf("first backoff = %v, want within [250ms, 750ms] jittered bounds", el)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := retryBackoff(ctx, 3); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled backoff must return the cancel error, got %v", err)
	}
}
