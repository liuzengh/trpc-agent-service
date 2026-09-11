package natsadapter

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestTraceHeadersActualJetStream(t *testing.T) {
	url := os.Getenv("TRACING_TEST_NATS_URL")
	if url == "" {
		t.Skip("TRACING_TEST_NATS_URL requires dedicated empty broker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: RunStream, Subjects: []string{RunSubject}, Storage: jetstream.FileStorage})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if e := js.DeleteStream(cleanup, RunStream); e != nil {
			t.Error("cleanup fixture stream", e)
		}
	}()
	transport := &Transport{JS: js}
	body := []byte(`{"fixture":"immutable bytes"}`)
	carrier := tracecontext.Carrier{Traceparent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01", Tracestate: "vendor=value"}
	for i := 0; i < 2; i++ {
		if err = transport.PublishMessage(ctx, RunSubject, "same-event-id", body, carrier); err != nil {
			t.Fatal(err)
		}
	}
	info, err := stream.Info(ctx)
	if err != nil || info.State.Msgs != 1 {
		t.Fatal("deduplication changed", err)
	}
	// Reconnect: verify broker storage, not a local publisher buffer.
	nc.Close()
	nc, err = nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}

	js, err = jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	stream, err = js.Stream(ctx, RunStream)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := stream.GetMsg(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, msg.Data) || msg.Header.Get("Nats-Msg-Id") != "same-event-id" || tracecontext.FromHeaders(msg.Header) != carrier {
		t.Fatal("broker changed wire/header/id")
	}
	t.Log("NATS_TRACE=PASS actual_jetstream=true reconnect=true payload_identical=true msg_id_dedup=true carrier_preserved=true")
}
