package natsadapter

import (
	"bytes"
	"context"
	"errors"
	codec "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"github.com/nats-io/nats.go/jetstream"
	"os"
	"testing"
)

func relayFixture(t *testing.T) (*ReplyRelay, *outbox, *publisher) {
	t.Helper()
	raw, err := os.ReadFile("../../../../../api/events/execution/v1/fixtures/reply-intent-final.valid.json")
	if err != nil {
		t.Fatal(err)
	}
	e, err := codec.DecodeReplyIntent(raw)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := codec.ReplyIntentDigest(e)
	if err != nil {
		t.Fatal(err)
	}
	o := &outbox{rows: []domain.OutboxItem{{IntentID: e.IntentID, Digest: digest, Payload: raw}}}
	p := &publisher{ack: &jetstream.PubAck{Stream: ReplyStream}}
	r, err := NewReplyRelay(o, p, 10)
	if err != nil {
		t.Fatal(err)
	}
	return r, o, p
}
func TestReplyRelayMarksOnlyAfterPubAck(t *testing.T) {
	r, o, p := relayFixture(t)
	p.onPublish = func() {
		if o.marks != 0 {
			t.Fatal("mark before PubAck")
		}
	}
	n, e := r.Tick(context.Background())
	if e != nil || n != 1 || o.marks != 1 || p.calls != 1 {
		t.Fatal(n, e, o.marks, p.calls)
	}
}
func TestReplyPubAckMarkUncertaintyReusesExactBytes(t *testing.T) {
	r, o, p := relayFixture(t)
	o.markErr = errors.New("database connection lost")
	if _, e := r.Tick(context.Background()); e == nil {
		t.Fatal("mark uncertainty lost")
	}
	o.markErr = nil
	if n, e := r.Tick(context.Background()); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	if p.calls != 2 || !bytes.Equal(p.payloads[0], p.payloads[1]) || !bytes.Equal(p.payloads[0], o.rows[0].Payload) {
		t.Fatal("retry rewrote immutable Final")
	}
}
func TestFailedOrUnexpectedReplyPubAckDoesNotMark(t *testing.T) {
	for _, kind := range []string{"network", "wrong stream", "nil ack"} {
		r, o, p := relayFixture(t)
		switch kind {
		case "network":
			p.err = errors.New("lost PubAck")
		case "wrong stream":
			p.ack.Stream = RunStream
		case "nil ack":
			p.ack = nil
		}
		if _, e := r.Tick(context.Background()); e == nil || o.marks != 0 {
			t.Fatal(kind, e, o.marks)
		}
	}
}
func TestCorruptReplyOutboxNeverPublishesOrMarks(t *testing.T) {
	for _, kind := range []string{"digest", "id", "wire"} {
		r, o, p := relayFixture(t)
		switch kind {
		case "digest":
			o.rows[0].Digest = domain.Digest([]byte("bad"))
		case "id":
			o.rows[0].IntentID = "wrong"
		case "wire":
			o.rows[0].Payload = []byte(`{}`)
		}
		if _, e := r.Tick(context.Background()); !errors.Is(e, ErrIntegrity) || o.marks != 0 || p.calls != 0 {
			t.Fatal(kind, e, o.marks, p.calls)
		}
	}
}
