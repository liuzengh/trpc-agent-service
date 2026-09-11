package natsadapter

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	"github.com/nats-io/nats.go"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestRunIntakeTraceBeforeACK(t *testing.T) {
	for _, sampled := range []string{"01", "00"} {
		t.Run(sampled, func(t *testing.T) {
			ex := tracetest.NewInMemoryExporter()
			p := sdktrace.NewTracerProvider(sdktrace.WithSyncer(ex))
			defer p.Shutdown(context.Background())
			c, m, intake, _, _ := fixture(t, false)
			c.Tracer = p.Tracer("worker")
			original := tracecontext.Carrier{Traceparent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-" + sampled}
			m.headers = nats.Header{}
			original.Inject(m.headers)
			bytes := string(m.raw)
			m.onAck = func() {
				if intake.ctx == nil || tracecontext.Capture(intake.ctx).Traceparent == "" {
					t.Fatal("ACK before traced owner handoff")
				}
			}
			if err := c.Handle(context.Background(), m); err != nil {
				t.Fatal(err)
			}
			sc := trace.SpanContextFromContext(intake.ctx)
			parent := trace.SpanContextFromContext(original.Restore(context.Background()))
			if sc.TraceID() != parent.TraceID() || sc.SpanID() == parent.SpanID() || string(m.raw) != bytes || m.acks != 1 {
				t.Fatal("context/wire/ack changed")
			}
			if sc.IsSampled() != (sampled == "01") {
				t.Fatal("sampling decision changed")
			}
			if sampled == "01" {
				s := ex.GetSpans()
				if len(s) != 1 || s[0].Parent.SpanID() != parent.SpanID() || len(s[0].Links) != 1 {
					t.Fatal("consumer relationship")
				}
			}
		})
	}
}

func TestRunConsumerKeepsAmbientOnlyAsLink(t *testing.T) {
	for _, wireParent := range []string{"00-0123456789abcdef0123456789abcdef-0123456789abcdef-01", "", "malformed"} {
		t.Run(wireParent, func(t *testing.T) {
			exporter := tracetest.NewInMemoryExporter()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
			defer provider.Shutdown(context.Background())
			tracer := provider.Tracer("review")
			ambientCtx, ambientSpan := tracer.Start(context.Background(), "poll.ambient")
			defer ambientSpan.End()
			ambient := ambientSpan.SpanContext()
			c, m, _, _, _ := fixture(t, false)
			c.Tracer = tracer
			m.headers = nats.Header{}
			if wireParent != "" {
				m.headers["traceparent"] = []string{wireParent}
			}
			if err := c.Handle(ambientCtx, m); err != nil {
				t.Fatal(err)
			}
			found := false
			for _, s := range exporter.GetSpans() {
				if s.Name != "process execution.run-requested.v1" {
					continue
				}
				found = true
				linkedAmbient := false
				for _, link := range s.Links {
					if link.SpanContext.TraceID() == ambient.TraceID() && link.SpanContext.SpanID() == ambient.SpanID() {
						linkedAmbient = true
					}
				}
				t.Logf("parent_is_ambient=%t linked_ambient=%t links=%d", s.Parent.SpanID() == ambient.SpanID(), linkedAmbient, len(s.Links))
				if wireParent == "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01" {
					creation := trace.SpanContextFromContext((tracecontext.Carrier{Traceparent: wireParent}).Restore(context.Background()))
					if !s.Parent.Equal(creation) || len(s.Links) != 2 {
						t.Error("creation parent and both links required")
					}
				}
				if !linkedAmbient {
					t.Error("spec 154-155: ambient context not linked")
				}
				if wireParent != "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01" && s.Parent.IsValid() {
					t.Error("spec 181-183: legacy/invalid message inherited polling parent instead of diagnostic root")
				}
			}
			if !found {
				t.Fatal("consumer span missing")
			}
		})
	}
}
