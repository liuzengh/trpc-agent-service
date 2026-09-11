package natsadapter

import (
	"context"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	"github.com/nats-io/nats.go"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"testing"
)

type tracedHandler struct{ ctx context.Context }

func (h *tracedHandler) Handle(ctx context.Context, _ []byte) (d.Receipt, error) {
	h.ctx = ctx
	return d.Receipt{IntentID: "intent", RunID: "run", PartCount: 1}, nil
}
func TestReplyProcessTraceBeforeACKAndReplay(t *testing.T) {
	for _, flags := range []string{"01", "00", "invalid"} {
		t.Run(flags, func(t *testing.T) {
			ex := tracetest.NewInMemoryExporter()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(ex))
			defer tp.Shutdown(context.Background())
			c, _, receipts, m := fixture()
			c.Tracer = tp.Tracer("gateway")
			h := &tracedHandler{}
			c.handler = h
			carrier := tracecontext.Carrier{Traceparent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-" + flags}
			m.headers = nats.Header{}
			carrier.Inject(m.headers)
			if flags == "invalid" {
				m.headers["traceparent"] = []string{"bad"}
			}
			body := string(m.data)
			m.onAck = func() {
				if h.ctx == nil || !trace.SpanContextFromContext(h.ctx).IsValid() || len(receipts.rows) != 1 {
					t.Fatal("ACK before traced durable handoff")
				}
			}
			if err := c.process(context.Background(), m); err != nil {
				t.Fatal(err)
			}
			sc := trace.SpanContextFromContext(h.ctx)
			if flags != "invalid" {
				parent := trace.SpanContextFromContext(carrier.Restore(context.Background()))
				if sc.TraceID() != parent.TraceID() || sc.SpanID() == parent.SpanID() || sc.IsSampled() != (flags == "01") {
					t.Fatal("process parent/sampling mismatch")
				}
			}
			if err := c.process(context.Background(), m); err != nil {
				t.Fatal(err)
			}
			if m.acks != 2 || receipts.writes != 1 || string(m.data) != body {
				t.Fatal("transport replay changed")
			}
			if flags == "01" {
				spans := ex.GetSpans()
				if len(spans) != 2 || len(spans[0].Links) != 1 || spans[0].Parent.SpanID() != spans[1].Parent.SpanID() {
					t.Fatal("delivery process linkage")
				}
			}
		})
	}
}

func TestReplyTraceOutcomeAfterDurableReceiptOnly(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "accepted", true: "receipt_failed"}[failed], func(t *testing.T) {
			ex := tracetest.NewInMemoryExporter()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(ex))
			defer tp.Shutdown(context.Background())
			c, _, receipts, m := fixture()
			c.Tracer = tp.Tracer("gateway")
			if failed {
				receipts.writeErr = d.ErrUnavailable
			}
			err := c.process(context.Background(), m)
			if (err != nil) != failed {
				t.Fatal(err)
			}
			spans := ex.GetSpans()
			if len(spans) != 1 {
				t.Fatal(len(spans))
			}
			attrs := map[string]string{}
			for _, a := range spans[0].Attributes {
				attrs[string(a.Key)] = a.Value.AsString()
			}
			if failed {
				if attrs["app.outcome"] == "ACCEPTED" || m.acks != 0 {
					t.Fatal("uncommitted receipt labeled accepted")
				}
			} else {
				if attrs["app.outcome"] != "ACCEPTED" || attrs["app.run.id"] != "run" || attrs["app.intent.id"] != "intent" || m.acks != 1 {
					t.Fatal("missing durable correlation")
				}
			}
		})
	}
}
