package application_test

import (
	"context"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"testing"
)

type traceClaims struct {
	*dispatchLedger
	carrier tracecontext.Carrier
}

func (l *traceClaims) ClaimTraced(context.Context, d.ClaimRequest) ([]app.TracedClaim, error) {
	return []app.TracedClaim{{Claim: l.claims[0], Carrier: l.carrier}}, nil
}
func TestDeliveryTraceOnlyActualSendAndCertainty(t *testing.T) {
	for _, kind := range []string{"ACCEPTED", "NOT_SENT", "UNKNOWN", "reserve", "mark"} {
		t.Run(kind, func(t *testing.T) {
			ex := tracetest.NewInMemoryExporter()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(ex))
			defer tp.Shutdown(context.Background())
			carrier := tracecontext.Carrier{Traceparent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"}
			l := &traceClaims{dispatchLedger: &dispatchLedger{claims: []d.Claim{claim()}}, carrier: carrier}
			h := &reserved{result: d.Result{Certainty: d.CertaintyAccepted, ProviderMessageID: "message"}}
			provider := &senders{handle: h}
			switch kind {
			case "NOT_SENT":
				h.result = d.Result{Certainty: d.CertaintyNotSent, ErrorClass: d.ErrorTemporary}
			case "UNKNOWN":
				h.result = d.Result{Certainty: d.CertaintyUnknown, ErrorClass: d.ErrorTemporary}
			case "reserve":
				provider.err = d.ErrUnavailable
			case "mark":
				l.markErr = d.ErrUnavailable
			}
			dispatcher, err := app.NewDispatcher(l, provider, app.DispatchOptions{Tracer: tp.Tracer("gateway")})
			if err != nil {
				t.Fatal(err)
			}
			_, err = dispatcher.DispatchAccount(context.Background(), d.ClaimRequest{})
			if (err != nil) != (kind == "mark") {
				t.Fatal(err)
			}
			var delivery, send tracetest.SpanStub
			for _, s := range ex.GetSpans() {
				switch s.Name {
				case "gateway.reply.deliver":
					delivery = s
				case "gateway.im.send":
					send = s
				}
			}
			parent := trace.SpanContextFromContext(carrier.Restore(context.Background()))
			if delivery.Parent.SpanID() != parent.SpanID() {
				t.Fatal("lost durable parent")
			}
			if kind == "reserve" || kind == "mark" {
				if send.SpanContext.IsValid() || h.calls != 0 {
					t.Fatal("phantom provider call")
				}
				return
			}
			if send.Parent.SpanID() != delivery.SpanContext.SpanID() || h.calls != 1 || l.finished != 1 || l.observed != 1 {
				t.Fatal("send/evidence sequencing changed")
			}
			for _, s := range []tracetest.SpanStub{send, delivery} {
				found := false
				for _, a := range s.Attributes {
					if string(a.Key) == "app.outcome" {
						found = a.Value.AsString() == kind
					}
				}
				if !found {
					t.Fatal("certainty mislabeled", kind)
				}
			}
		})
	}
}
