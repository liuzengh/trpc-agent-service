package worker

import (
	"context"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// Shared recorder for the whole test process: otel's global provider resolves
// its delegate once, so a per-test provider would be ignored after the first.
var (
	traceRecOnce sync.Once
	traceRec     *tracetest.SpanRecorder
)

func spanRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	traceRecOnce.Do(func() {
		traceRec = tracetest.NewSpanRecorder()
		otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(traceRec)))
	})
	traceRec.Reset()
	return traceRec
}

// TestTurnTraceCoversSessionAndMemory closes the "trace must span every
// component" gap end to end: a real turn (runner + tools + session + memory)
// must produce session/memory spans as children of `agent.run`, so Jaeger shows
// one chain from the IM callback down to the shared state layer instead of
// jumping from the agent straight to the next tool call.
func TestTurnTraceCoversSessionAndMemory(t *testing.T) {
	rec := spanRecorder(t)
	ctx := context.Background()
	w, agents, _, _ := memoryFixture(t)
	agents.SetPreloadMemory(5) // memory on: preload read happens every turn

	msg := inbound("m-trace-1", "t-mem", "a-mem", "s-trace-1", "u-1", "hello")
	if _, _, _, err := w.run(ctx, "a-mem", msg, "lock-1"); err != nil {
		t.Fatalf("turn: %v", err)
	}

	spans := rec.Ended()
	if len(spans) == 0 {
		t.Fatal("no spans recorded: the turn produced no trace at all")
	}

	var runSpanID string
	names := map[string]bool{}
	for _, s := range spans {
		names[s.Name()] = true
		if s.Name() == "agent.run" {
			runSpanID = s.SpanContext().SpanID().String()
		}
	}
	if runSpanID == "" {
		t.Fatalf("no agent.run span; got %v", names)
	}

	// The shared state layer must be visible...
	var sawSession, sawMemory bool
	for _, s := range spans {
		switch {
		case len(s.Name()) > len("session.") && s.Name()[:len("session.")] == "session.":
			sawSession = true
			assertChildOfRun(t, s, runSpanID)
		case len(s.Name()) > len("memory.") && s.Name()[:len("memory.")] == "memory.":
			sawMemory = true
			assertChildOfRun(t, s, runSpanID)
		}
	}
	if !sawSession {
		t.Errorf("no session.* span in %v: the session store is invisible in the trace", names)
	}
	if !sawMemory {
		t.Errorf("no memory.* span in %v: the memory store is invisible in the trace", names)
	}
}

// assertChildOfRun requires the span to hang off agent.run, which is what makes
// one readable chain in Jaeger rather than disconnected fragments.
func assertChildOfRun(t *testing.T, s sdktrace.ReadOnlySpan, runSpanID string) {
	t.Helper()
	if got := s.Parent().SpanID().String(); got != runSpanID {
		t.Errorf("span %q parent = %s, want agent.run %s", s.Name(), got, runSpanID)
	}
}
