package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestWorkerTracingConfigIndependentOfMetrics(t *testing.T) {
	c := testConfig(t)
	cfg := telemetrytrace.Config{TracesEndpoint: "http://127.0.0.1:4318/v1/traces", SamplingRatio: 1, ExportTimeout: "1s", BatchTimeout: "1s", MaxQueueSize: 32, MaxExportBatchSize: 8}
	c.Tracing = &cfg
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "worker.json")
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	var loaded Config
	if err = readJSONFile(path, false, &loaded); err != nil {
		t.Fatal(err)
	}
	loaded.DatabaseURL = c.DatabaseURL
	loaded.MigrationDatabaseURL = c.MigrationDatabaseURL
	if loaded.Validate() != nil || loaded.Telemetry != nil || loaded.Tracing == nil {
		t.Fatal("traces-only configuration rejected")
	}
	for _, body := range []string{strings.Replace(string(raw), `"tracing":{`, `"tracing":null,"duplicate":{`, 1), strings.Replace(string(raw), `"sampling_ratio":1`, `"sampling_ratio":null`, 1)} {
		if err = os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if readJSONFile(path, false, &loaded) == nil {
			t.Fatal("malformed tracing accepted")
		}
	}
}

type traceProcessorFunc func(context.Context, domain.Run) error

func (f traceProcessorFunc) Advance(ctx context.Context, r domain.Run) error { return f(ctx, r) }
func TestTracedProcessorPreservesOutcomeAndContext(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer provider.Shutdown(context.Background())
	expected := errors.New("private execution error")
	calls := 0
	p := tracedProcessor{tracer: provider.Tracer("agent-worker"), delegate: traceProcessorFunc(func(ctx context.Context, r domain.Run) error {
		calls++
		if !trace.SpanContextFromContext(ctx).IsValid() {
			t.Fatal("no execution context")
		}
		return expected
	})}
	if p.Advance(context.Background(), domain.Run{}) != expected || calls != 1 {
		t.Fatal("changed business result")
	}
	spans := recorder.Ended()
	if len(spans) != 1 || spans[0].Status().Description != "" {
		t.Fatal("span outcome")
	}
}

type tracedReady struct{ item application.ScheduledRun }

func (s tracedReady) Scheduled(context.Context, int) ([]application.ScheduledRun, error) {
	return []application.ScheduledRun{s.item}, nil
}
func TestSchedulerRestoresOnlyDurableSpanContext(t *testing.T) {
	c := testConfig(t)
	c.Limits.MaxActiveAttempts = 1
	c.Timing.PollInterval = Duration(time.Millisecond)
	processor := &heldProcessor{entered: make(chan context.Context, 1), release: make(chan struct{})}
	app := &App{config: c, executor: processor}
	app.storageHealthy.Store(true)
	carrier := tracecontext.Carrier{Traceparent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-00"}
	ready := tracedReady{application.ScheduledRun{Run: domain.Run{Request: domain.Requested{RunID: "run", Route: domain.Route{TenantID: "tenant"}}}, Carrier: carrier}}
	intake, stopIntake := context.WithCancel(context.Background())
	defer stopIntake()
	active, stopActive := context.WithCancel(context.Background())
	defer stopActive()
	done := make(chan struct{})
	go func() { app.schedule(intake, active, ready); close(done) }()
	var ctx context.Context
	select {
	case ctx = <-processor.entered:
	case <-time.After(time.Second):
		t.Fatal("not scheduled")
	}
	if tracecontext.Capture(ctx) != carrier {
		t.Fatal("durable carrier lost")
	}
	stopIntake()
	<-done
	if ctx.Err() != nil {
		t.Fatal("ended fetch lifecycle leaked into execution")
	}
	stopActive()
	if ctx.Err() != context.Canceled {
		t.Fatal("active cancellation lost")
	}
	close(processor.release)
	app.attempts.Wait()
}
