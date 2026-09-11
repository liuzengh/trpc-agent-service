package postgresadapter_test

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	admissionpg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/adapter/outbound/postgres"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestAdmissionDurableCreationTrace(t *testing.T) {
	store, pool, routes := setup(t)
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer provider.Shutdown(context.Background())
	store = store.WithTracing(provider.Tracer("channel-gateway"))
	ctx, parent := provider.Tracer("channel-gateway").Start(context.Background(), "gateway.im.callback")
	first := acceptance("trace-first")
	if _, err := store.Commit(ctx, first); err != nil {
		t.Fatal(err)
	}
	parent.End()
	// A new adapter instance simulates restoration after process ownership ends.
	reopened := admissionpg.NewStore(pool, routes)
	msg, found, err := reopened.Claim(context.Background())
	if err != nil || !found || msg.Carrier.Traceparent == "" {
		t.Fatal(err, found, msg.Carrier)
	}
	creation := trace.SpanContextFromContext(msg.Carrier.Restore(context.Background()))
	if creation.TraceID() != parent.SpanContext().TraceID() || creation.SpanID() == parent.SpanContext().SpanID() {
		t.Fatal("creation parent not persisted")
	}
	spans := exporter.GetSpans()
	matched := false
	for _, s := range spans {
		if s.Name == "create execution.run-requested.v1" {
			matched = s.SpanContext.SpanID() == creation.SpanID() && s.Parent.SpanID() == parent.SpanContext().SpanID()
		}
	}
	if !matched {
		t.Fatal("actual producer relationship missing")
	}
	saved := msg.Carrier
	payload := string(msg.Payload)
	other, span := provider.Tracer("channel-gateway").Start(context.Background(), "gateway.im.callback")
	if _, err = store.Commit(other, first); err != nil {
		t.Fatal(err)
	}
	span.End()
	replaySpans := exporter.GetSpans()
	replay := replaySpans[len(replaySpans)-1]
	if len(replay.Links) != 1 || replay.Links[0].SpanContext.SpanID() != creation.SpanID() {
		t.Fatal("duplicate did not link original creation")
	}
	if err = reopened.Retry(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(context.Background(), `UPDATE gateway_outbox SET next_attempt_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	msg, found, err = reopened.Claim(context.Background())
	if err != nil || !found || msg.Carrier != saved || string(msg.Payload) != payload {
		t.Fatal("replay changed immutable context or payload", err)
	}
	if err = reopened.Published(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	// A first NULL context remains NULL when a later replay is traced.
	empty := acceptance("trace-empty")
	if _, err = reopened.Commit(context.Background(), empty); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Commit(other, empty); err != nil {
		t.Fatal(err)
	}
	msg, found, err = reopened.Claim(context.Background())
	if err != nil || !found || msg.Carrier != (tracecontext.Carrier{}) {
		t.Fatal("empty first context overwritten", err)
	}
	// Invalid carrier normalization cannot cause business rejection.
	c := tracecontext.Carrier{Traceparent: "invalid"}.Restore(context.Background())
	timeout, cancel := context.WithTimeout(c, time.Second)
	defer cancel()
	if _, err = reopened.Commit(timeout, acceptance("trace-invalid")); err != nil {
		t.Fatal(err)
	}

	if _, err = pool.Exec(context.Background(), `CREATE FUNCTION trace_commit_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture commit failure'; END $$; CREATE CONSTRAINT TRIGGER fail_trace_commit AFTER INSERT ON gateway_outbox DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION trace_commit_failure()`); err != nil {
		t.Fatal(err)
	}
	failureCtx, failureRoot := provider.Tracer("channel-gateway").Start(context.Background(), "gateway.im.callback")
	if _, err = store.Commit(failureCtx, acceptance("trace-commit-fail")); err == nil {
		t.Fatal("expected deferred commit failure")
	}
	failureRoot.End()
	latest := exporter.GetSpans()
	created := latest[len(latest)-2]
	if created.Name != "create execution.run-requested.v1" || created.Status.Code != codes.Error {
		t.Fatal("creation pretended failed commit succeeded", created.Name, created.Status)
	}
	var count int
	if err = pool.QueryRow(context.Background(), `SELECT count(*) FROM gateway_outbox WHERE event_id='admission-trace-commit-fail'`).Scan(&count); err != nil || count != 0 {
		t.Fatal("failed transaction leaked outbox", err, count)
	}
	t.Log("ADMISSION_TRACE=PASS actual_pg=true restart_adapter=true first_context_immutable=true payload_unchanged=true")
}
