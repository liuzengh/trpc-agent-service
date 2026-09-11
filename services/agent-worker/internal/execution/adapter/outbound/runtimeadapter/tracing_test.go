package runtimeadapter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/sessionstore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestRuntimeSessionPhasesAndCandidateRecoveryTrace(t *testing.T) {
	ex := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(ex))
	defer provider.Shutdown(context.Background())
	g, p := runtimeFixture()
	f, _ := newFixtureFactory(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(responseFixture(g, p))
	})
	f.options.Tracer = provider.Tracer("worker")
	store := &fakeStore{putErr: errors.New("lost response canary")}
	f.openStore = func(context.Context, string, sessionstore.Target, int) (candidateStore, error) { return store, nil }
	ctx, root := f.options.Tracer.Start(context.Background(), "worker.run.attempt")
	defer root.End()
	runtime, err := f.Prepare(ctx, g, p, func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if _, err = runtime.Load(ctx, g.Parent); err != nil {
		t.Fatal(err)
	}
	a := runtime.(*attempt)
	body := []byte(`{"fixture":"canary"}`)
	a.executed = true
	a.resultDigest = domain.Digest(body)
	if _, err = runtime.Stage(ctx, body); err != nil {
		t.Fatal(err)
	}
	names := map[string]tracetest.SpanStub{}
	for _, s := range ex.GetSpans() {
		names[s.Name] = s
		if s.SpanContext.TraceID() != root.SpanContext().TraceID() {
			t.Fatal("phase trace disconnected")
		}
	}
	for _, name := range []string{"worker.runtime.prepare", "worker.credential.resolve", "worker.session.open", "worker.session.load", "worker.session.stage", "worker.session.verify"} {
		if _, ok := names[name]; !ok {
			t.Fatal("missing", name)
		}
	}
	if names["worker.session.verify"].Parent.SpanID() != names["worker.session.stage"].SpanContext.SpanID() {
		t.Fatal("verification not child of candidate stage")
	}
	if _, ok := names["worker.session.commit"]; ok {
		t.Fatal("candidate write pretended accepted commit")
	}
	if store.puts != 1 || store.loads != 1 {
		t.Fatal("changed recovery calls", store.puts, store.loads)
	}
	t.Log("RUNTIME_SESSION_TRACE=PASS prepare_load_stage_verify=true candidate_not_commit=true unchanged_calls=true")
}
