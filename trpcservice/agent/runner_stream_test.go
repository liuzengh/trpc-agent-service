package agent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

type runnerStreamTestRunner struct {
	events    <-chan *trpcevent.Event
	err       error
	requestID string
}

func (runner *runnerStreamTestRunner) Run(_ context.Context, _ string, _ string, _ trpcmodel.Message, options ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
	settings := trpcagent.RunOptions{}
	for _, option := range options {
		option(&settings)
	}
	runner.requestID = settings.RequestID
	return runner.events, runner.err
}

func (*runnerStreamTestRunner) Close() error { return nil }

func TestInvokeMapsAndStopsAtTerminalExternalEvent(t *testing.T) {
	source := make(chan *trpcevent.Event, 3)
	source <- &trpcevent.Event{Response: &trpcmodel.Response{Choices: []trpcmodel.Choice{{Delta: trpcmodel.Message{Content: "hello"}}}}}
	source <- &trpcevent.Event{Response: &trpcmodel.Response{Done: true}}
	source <- &trpcevent.Event{Response: &trpcmodel.Response{Choices: []trpcmodel.Choice{{Delta: trpcmodel.Message{Content: "ignored"}}}}}
	runner := &runnerStreamTestRunner{events: source}
	stream, err := Invoke(context.Background(), runner, Invocation{UserID: "user", SessionID: "session", RequestID: "request"}, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	events := collectRunnerStreamEvents(stream)
	if len(events) != 2 || events[0].Type != RunnerEventMessage || events[0].Text != "hello" || events[1].Type != RunnerEventDone || !events[1].Done {
		t.Fatalf("events=%+v", events)
	}
	if runner.requestID != "request" {
		t.Fatalf("upstream request ID=%q", runner.requestID)
	}
}

func TestInvokeRedactsExternalRunnerErrors(t *testing.T) {
	source := make(chan *trpcevent.Event, 1)
	source <- &trpcevent.Event{Response: &trpcmodel.Response{Error: &trpcmodel.ResponseError{Message: "provider secret"}}}
	runner := &runnerStreamTestRunner{events: source}
	stream, err := Invoke(context.Background(), runner, Invocation{UserID: "user", SessionID: "session", RequestID: "request"}, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	events := collectRunnerStreamEvents(stream)
	if len(events) != 2 || events[0].Type != RunnerEventError || !errors.Is(events[0].Err, ErrRunnerExecution) || events[1].Type != RunnerEventDone {
		t.Fatalf("events=%+v", events)
	}
	if strings.Contains(events[0].Err.Error(), "provider secret") {
		t.Fatal("provider error escaped Agent boundary")
	}
}

func TestInvokeCancellationDrainsExternalStream(t *testing.T) {
	source := make(chan *trpcevent.Event)
	runner := &runnerStreamTestRunner{events: source}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := Invoke(ctx, runner, Invocation{UserID: "user", SessionID: "session", RequestID: "request"}, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case _, ok := <-stream:
		if ok {
			t.Fatal("canceled invocation emitted an event")
		}
	case <-time.After(time.Second):
		t.Fatal("canceled invocation did not close its stream")
	}
}

func TestInvokeRejectsInvalidInput(t *testing.T) {
	runner := &runnerStreamTestRunner{events: make(chan *trpcevent.Event)}
	tests := []struct {
		name   string
		ctx    context.Context
		runner Runner
		input  Invocation
	}{
		{name: "nil context", runner: runner, input: Invocation{UserID: "user", SessionID: "session", RequestID: "request"}},
		{name: "nil runner", ctx: context.Background(), input: Invocation{UserID: "user", SessionID: "session", RequestID: "request"}},
		{name: "missing identity", ctx: context.Background(), runner: runner, input: Invocation{RequestID: "request"}},
		{name: "missing request ID", ctx: context.Background(), runner: runner, input: Invocation{UserID: "user", SessionID: "session"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Invoke(test.ctx, test.runner, test.input, time.Millisecond); !errors.Is(err, ErrInvalid) {
				t.Fatalf("Invoke() error=%v", err)
			}
		})
	}
}

func TestInvokePropagatesRunnerFailureAndRejectsNilStream(t *testing.T) {
	providerErr := errors.New("runner provider detail")
	for _, test := range []struct {
		name   string
		runner *runnerStreamTestRunner
	}{
		{name: "runner failure", runner: &runnerStreamTestRunner{err: providerErr}},
		{name: "nil event stream", runner: &runnerStreamTestRunner{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := Invoke(context.Background(), test.runner, Invocation{UserID: "user", SessionID: "session", RequestID: "request"}, time.Millisecond)
			if test.runner.err != nil {
				if !errors.Is(err, providerErr) {
					t.Fatalf("Invoke() error=%v, want provider error", err)
				}
				return
			}
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("Invoke() error=%v, want invalid stream error", err)
			}
		})
	}
}

func TestMapExternalRunnerEventNormalizesResponseVariants(t *testing.T) {
	tests := []struct {
		name       string
		event      *trpcevent.Event
		want       []RunnerEvent
		wantClosed bool
	}{
		{
			name:  "nil event",
			event: nil,
			want:  []RunnerEvent{{Type: RunnerEventStatus, Status: "progress"}},
		},
		{
			name:  "nil response",
			event: &trpcevent.Event{},
			want:  []RunnerEvent{{Type: RunnerEventStatus, Status: "progress"}},
		},
		{
			name:  "partial progress",
			event: &trpcevent.Event{Response: &trpcmodel.Response{IsPartial: true}},
			want:  []RunnerEvent{{Type: RunnerEventStatus, Status: "partial"}},
		},
		{
			name: "message fallback and delta",
			event: &trpcevent.Event{Response: &trpcmodel.Response{Choices: []trpcmodel.Choice{
				{Delta: trpcmodel.Message{Content: "hello"}},
				{Message: trpcmodel.Message{Content: " world"}},
			}}},
			want: []RunnerEvent{{Type: RunnerEventMessage, Text: "hello world"}},
		},
		{
			name: "message followed by terminal",
			event: &trpcevent.Event{Response: &trpcmodel.Response{
				Choices: []trpcmodel.Choice{{Message: trpcmodel.Message{Content: "done"}}}, Done: true,
			}},
			want:       []RunnerEvent{{Type: RunnerEventMessage, Text: "done"}, {Type: RunnerEventDone, Status: "complete", Done: true}},
			wantClosed: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, closed := mapExternalRunnerEvent(test.event)
			if closed != test.wantClosed {
				t.Fatalf("terminal=%v, want %v", closed, test.wantClosed)
			}
			if len(got) != len(test.want) {
				t.Fatalf("events=%+v, want %+v", got, test.want)
			}
			for index := range got {
				if got[index].Type != test.want[index].Type || got[index].Text != test.want[index].Text || got[index].Status != test.want[index].Status || got[index].Done != test.want[index].Done {
					t.Fatalf("event[%d]=%+v, want %+v", index, got[index], test.want[index])
				}
			}
		})
	}
}

func TestForwardRunnerEventsHandlesSourceClosure(t *testing.T) {
	source := make(chan *trpcevent.Event)
	close(source)
	output := make(chan RunnerEvent, 1)
	forwardRunnerEvents(context.Background(), source, output, time.Millisecond)
	if _, ok := <-output; ok {
		t.Fatal("closed source produced an event")
	}
}

func TestRunnerStreamHelpersHonorCancellationAndDrainBounds(t *testing.T) {
	if sendRunnerEvent(nil, make(chan RunnerEvent, 1), RunnerEvent{Type: RunnerEventStatus}) {
		t.Fatal("nil context unexpectedly accepted an event")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if sendRunnerEvent(ctx, make(chan RunnerEvent, 1), RunnerEvent{Type: RunnerEventStatus}) {
		t.Fatal("canceled context unexpectedly accepted an event")
	}

	drainRunnerEvents(nil, time.Millisecond)
	drainRunnerEvents(make(chan *trpcevent.Event), 0)
	closed := make(chan *trpcevent.Event)
	close(closed)
	drainRunnerEvents(closed, time.Millisecond)
	drainRunnerEvents(make(chan *trpcevent.Event), time.Millisecond)
}

func collectRunnerStreamEvents(stream <-chan RunnerEvent) []RunnerEvent {
	events := make([]RunnerEvent, 0, 4)
	for event := range stream {
		events = append(events, event)
	}
	return events
}
