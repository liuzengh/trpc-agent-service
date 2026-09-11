package postgresadapter_test

import (
	"context"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	pg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/postgres"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"strings"
	"testing"
)

func TestDeliveryCarrierImmutableAcrossPoolsAndParts(t *testing.T) {
	p1, p2 := deliveryDatabase(t)
	s1, err := pg.NewStore(p1, nil, pg.Options{})
	if err != nil {
		t.Fatal(err)
	}
	s2, err := pg.NewStore(p2, nil, pg.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ex := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(ex))
	defer tp.Shutdown(context.Background())
	ctx, process := tp.Tracer("gateway").Start(context.Background(), "process execution.reply-intent.v1")
	carrier := tracecontext.Capture(ctx)
	p := prepared("trace-parts")
	p.Intent.Text = strings.Repeat("a", 4096) + "second"
	refresh(&p)
	if _, err = s1.Accept(ctx, p); err != nil {
		t.Fatal(err)
	}
	process.End()
	other, replay := tp.Tracer("gateway").Start(context.Background(), "process execution.reply-intent.v1")
	if _, err = s2.Accept(other, p); err != nil {
		t.Fatal(err)
	}
	replay.End()
	spans := ex.GetSpans()
	if len(spans) != 2 || len(spans[1].Links) != 1 || spans[1].Links[0].SpanContext.SpanID() != process.SpanContext().SpanID() {
		t.Fatal("replay must link first accepted process")
	}
	var stored tracecontext.Carrier
	if err = p2.QueryRow(ctx, `SELECT COALESCE(traceparent,''),COALESCE(tracestate,'') FROM gateway_delivery_intents WHERE intent_id=$1`, p.Intent.ID).Scan(&stored.Traceparent, &stored.Tracestate); err != nil {
		t.Fatal(err)
	}
	if stored != carrier {
		t.Fatal("repeat replaced carrier")
	}

	for part := 0; part < 2; part++ {
		rows, e := s2.ClaimTraced(context.Background(), claimRequest())
		if e != nil || len(rows) != 1 {
			t.Fatal(e, len(rows))
		}
		if rows[0].Carrier != carrier || rows[0].Part.Index != part {
			t.Fatal("part ordering/context changed")
		}
		a := calling(t, s2, rows[0].Claim)
		if e = s2.Finish(context.Background(), a, acceptedResult()); e != nil {
			t.Fatal(e)
		}
	}
	legacy := prepared("trace-legacy")
	if _, err = s1.Accept(context.Background(), legacy); err != nil {
		t.Fatal(err)
	}
	rows, err := s2.ClaimTraced(context.Background(), claimRequest())
	if err != nil || len(rows) != 1 || rows[0].Carrier != (tracecontext.Carrier{}) {
		t.Fatal("legacy context changed")
	}
	t.Log("DELIVERY_CONTEXT=PASS independent_pools=true immutable=true ordered_parts=2 legacy_null=true")
}
