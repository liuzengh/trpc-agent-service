package natsadapter

import (
	"bytes"
	"context"
	"errors"
	codec "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	app "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"os"
	"strings"
	"testing"
	"time"
)

type tracedOutbox struct {
	*outbox
	carrier tracecontext.Carrier
}

func (o *tracedOutbox) PendingTracedReplies(ctx context.Context, n int) ([]app.TracedReply, error) {
	rows, err := o.outbox.PendingTracedReplies(ctx, n)
	for i := range rows {
		rows[i].Carrier = o.carrier
	}
	return rows, err
}
func TestReplyTraceActualJetStreamRetryAndFrameCapacity(t *testing.T) {
	url := os.Getenv("TRACING_TEST_NATS_URL")
	if url == "" {
		t.Skip("TRACING_TEST_NATS_URL requires isolated broker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { nc.Close() }()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: ReplyStream, Subjects: []string{ReplySubject}, Storage: jetstream.FileStorage, MaxMsgSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if e := js.DeleteStream(cleanup, ReplyStream); e != nil {
			t.Error(e)
		}
	}()
	_, base, _ := relayFixture(t)
	event, err := codec.DecodeReplyIntent(base.rows[0].Payload)
	if err != nil {
		t.Fatal(err)
	}
	// Control characters maximize JSON escaping (six wire bytes per text byte).
	event.Content.Text = "x" + strings.Repeat("\x01", codec.MaxFinalTextBytes-1)
	raw, err := codec.EncodeReplyIntent(event)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := codec.ReplyIntentDigest(event)
	if err != nil {
		t.Fatal(err)
	}
	base.rows[0].Payload = raw
	base.rows[0].Digest = digest
	carrier := tracecontext.Carrier{Traceparent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01", Tracestate: "vendor=" + strings.Repeat("v", 249) + ",other=" + strings.Repeat("v", 249)}
	source := &tracedOutbox{outbox: base, carrier: carrier}
	relay, err := NewReplyRelay(source, js, 1)
	if err != nil {
		t.Fatal(err)
	}
	ex := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(ex))
	defer tp.Shutdown(context.Background())
	relay.Tracer = tp.Tracer("worker")
	base.markErr = errors.New("fixture lost mark")
	if _, err = relay.Tick(ctx); err == nil {
		t.Fatal("mark uncertainty missing")
	}
	nc.Close()
	nc, err = nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	js, err = jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	relay.publisher = js
	base.markErr = nil
	if n, err := relay.Tick(ctx); err != nil || n != 1 {
		t.Fatal(err, n)
	}
	stream, err = js.Stream(ctx, ReplyStream)
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil || info.State.Msgs != 1 {
		t.Fatal("deduplication changed", err)
	}
	msg, err := stream.GetMsg(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(msg.Data, raw) || msg.Header.Get("Nats-Msg-Id") != event.IntentID || tracecontext.FromHeaders(msg.Header) != carrier {
		t.Fatal("immutable bytes/id/creation context changed")
	}
	spans := ex.GetSpans()
	if len(spans) != 2 || spans[0].SpanContext.SpanID() == spans[1].SpanContext.SpanID() || spans[0].Parent.SpanID() != spans[1].Parent.SpanID() || len(spans[0].Links) != 1 {
		t.Fatal("publish attempts relationship")
	}
	t.Logf("REPLY_NATS=PASS reconnect=true retry_dedup=true body_bytes=%d text_bytes=%d max_frame=1048576", len(raw), len(event.Content.Text))
}
