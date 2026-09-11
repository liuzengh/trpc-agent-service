package httpadapter

import (
	"context"
	proof "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"net/http/httptest"
	"testing"
)

func TestProofTraceInheritedOnlyAfterCommittedFinalAuthorization(t *testing.T) {
	ex := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(ex))
	defer tp.Shutdown(context.Background())
	h, _, f := fixture(t)
	h.tracer = tp.Tracer("worker")
	carrier := tracecontext.Carrier{Traceparent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"}
	for _, identity := range []string{workerID, gatewayID, controlID} {
		allowed := identity != workerID
		r := request(proof.FinalVerifyPath, finalBody(), identity)
		carrier.Inject(r.Header)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if allowed && w.Code != 200 || !allowed && w.Code != 403 {
			t.Fatal(w.Code)
		}
	}
	spans := ex.GetSpans()
	parent := trace.SpanContextFromContext(carrier.Restore(context.Background()))
	if f.calls != 2 || len(spans) != 2 || spans[0].Name != "worker.reply.verify" || spans[0].Parent.SpanID() != parent.SpanID() || spans[0].SpanKind != trace.SpanKindServer || spans[1].Parent.SpanID() != parent.SpanID() || spans[1].SpanKind != trace.SpanKindServer {
		t.Fatal("unauthorized request traced or authorized parent lost")
	}
}
