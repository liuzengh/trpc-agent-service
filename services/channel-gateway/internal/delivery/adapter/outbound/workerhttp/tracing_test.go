package workerhttp

import (
	"context"
	wire "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestProofTraceOverControlledMTLS(t *testing.T) {
	ex := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(ex))
	defer tp.Shutdown(context.Background())
	var received tracecontext.Carrier
	c, _ := mtls(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received = tracecontext.FromHeaders(r.Header)
		if r.Header.Get("Baggage") != "" || r.Header.Get("Authorization") != "" {
			t.Error("unexpected propagation")
		}
		raw, _ := io.ReadAll(r.Body)
		q, err := wire.DecodeFinalRequest(raw)
		if err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		body, err := wire.EncodeFinalResponse(wire.FinalResponse{FinalRequest: q, TenantID: "tenant", ManifestDigest: "sha256:" + strings.Repeat("b", 64)})
		if err != nil {
			t.Error(err)
		}
		w.Write(body)
	}))
	c.Tracer = tp.Tracer("gateway")
	ctx, parent := c.Tracer.Start(context.Background(), "process execution.reply-intent.v1")
	defer parent.End()
	i := intent()
	digest, _ := d.IntentDigest(i)
	if _, err := c.VerifyCommittedFinal(ctx, i, digest); err != nil {
		t.Fatal(err)
	}
	spans := ex.GetSpans()
	if len(spans) != 1 || spans[0].Name != "gateway.reply.verify" || spans[0].Parent.SpanID() != parent.SpanContext().SpanID() || trace.SpanContextFromContext(received.Restore(context.Background())).SpanID() != spans[0].SpanContext.SpanID() {
		t.Fatal("mTLS proof propagation mismatch")
	}
	t.Log("PROOF_HTTP=PASS actual_mtls=true dedicated_destination=true w3c_parent=true")
}
