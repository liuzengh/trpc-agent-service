package natsadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	controlwire "github.com/liuzengh/trpc-agent-service/api/events/control/v1"
	runwire "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Uses pre-provisioned declared streams and runtime ACLs. The test does not
// create/delete streams or purge existing broker state; payload IDs are unique.
func TestWorkerNATSRuntimeHandoffAndReplyReplayIntegration(t *testing.T) {
	address := os.Getenv("WORKER_TEST_NATS_URL")
	if address == "" {
		t.Skip("WORKER_TEST_NATS_URL required; V1 topology already reconciled")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connect := func(user string) jetstream.JetStream {
		t.Helper()
		options := []nats.Option{nats.CustomInboxPrefix("_INBOX." + user)}
		if password := os.Getenv("NATS_" + strings.ToUpper(user) + "_PASSWORD"); password != "" {
			options = append(options, nats.UserInfo(user, password))
		}
		nc, e := nats.Connect(address, options...)
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(nc.Close)
		js, e := jetstream.New(nc)
		if e != nil {
			t.Fatal(e)
		}
		return js
	}
	worker, control, gateway := connect("worker"), connect("control"), connect("gateway")
	i, p, reject := &intake{}, &projection{}, &rejector{}
	run, e := BindRun(ctx, worker, i, reject)
	if e != nil {
		t.Fatal(e)
	}
	manifests, e := BindManifest(ctx, worker, p, reject, 100)
	if e != nil {
		t.Fatal(e)
	}
	raw, e := os.ReadFile("../../../../../api/events/execution/v1/fixtures/valid/telegram-text.json")
	if e != nil {
		t.Fatal(e)
	}
	event, e := runwire.DecodeRunRequested(raw)
	if e != nil {
		t.Fatal(e)
	}
	id := fmt.Sprintf("nats-adapter-%d", time.Now().UnixNano())
	event.EventID = id
	event.AdmissionID = id
	event.RunID = id
	raw, e = runwire.EncodeRunRequested(event)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = gateway.Publish(ctx, RunSubject, raw); e != nil {
		t.Fatal(e)
	}
	for i.last.EventID != id && ctx.Err() == nil {
		if _, e = run.Poll(ctx); e != nil {
			t.Fatal(e)
		}
	}
	if i.last.EventID != id {
		t.Fatal("Run was not durably handed off")
	}
	raw, e = os.ReadFile("../../../../../api/events/control/v1/examples/valid/runtime-manifest-published.json")
	if e != nil {
		t.Fatal(e)
	}
	m, e := controlwire.DecodeRuntimeManifestPublishedEvent(raw)
	if e != nil {
		t.Fatal(e)
	}
	m.EventID = id
	raw, e = json.Marshal(m)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = control.Publish(ctx, ManifestSubject, raw); e != nil {
		t.Fatal(e)
	}
	for p.last.EventID != id && ctx.Err() == nil {
		if _, e = manifests.Poll(ctx); e != nil {
			t.Fatal(e)
		}
	}
	if p.last.EventID != id {
		t.Fatal("Manifest was not durably handed off")
	}
	// A broker PubAck followed by an unknown SQL mark reuses the same MsgID and
	// original bytes. Deduplication is visible as a single new retained message.
	_, out, _ := relayFixture(t)
	replyEvent, e := runwire.DecodeReplyIntent(out.rows[0].Payload)
	if e != nil {
		t.Fatal(e)
	}
	replyEvent.IntentID = id
	replyEvent.RunID = id
	replyEvent.AdmissionID = id
	raw, e = runwire.EncodeReplyIntent(replyEvent)
	if e != nil {
		t.Fatal(e)
	}
	digest, e := runwire.ReplyIntentDigest(replyEvent)
	if e != nil {
		t.Fatal(e)
	}
	out.rows[0].IntentID = id
	out.rows[0].Digest = digest
	out.rows[0].Payload = raw
	out.markErr = errors.New("injected SQL mark uncertainty")
	replies, e := worker.Stream(ctx, ReplyStream)
	if e != nil {
		t.Fatal(e)
	}
	before, e := replies.Info(ctx)
	if e != nil {
		t.Fatal(e)
	}
	relay, e := NewReplyRelay(out, worker, 1)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = relay.Tick(ctx); e == nil {
		t.Fatal("mark fault not injected")
	}
	out.markErr = nil
	if n, e := relay.Tick(ctx); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	after, e := replies.Info(ctx)
	if e != nil || after.State.LastSeq != before.State.LastSeq+1 {
		t.Fatal("retry changed broker message identity", e)
	}
	delivery, e := gateway.Consumer(ctx, ReplyStream, "channel-gateway-replies-v1")
	if e != nil {
		t.Fatal(e)
	}
	seen := false
	for !seen && ctx.Err() == nil {
		message, e := delivery.Next(jetstream.FetchMaxWait(time.Second))
		if e != nil {
			t.Fatal(e)
		}
		decoded, e := runwire.DecodeReplyIntent(message.Data())
		if e == nil && decoded.IntentID == id {
			if string(message.Data()) != string(raw) || message.Headers().Get("Nats-Msg-Id") != id {
				t.Fatal("Reply transport rewrote immutable payload/id")
			}
			seen = true
		}
		if e = message.DoubleAck(ctx); e != nil {
			t.Fatal(e)
		}
	}
	if !seen {
		t.Fatal("Reply not delivered")
	}
	t.Log("WORKER_NATS_PASS: runtime ACL pre-bound Run+Manifest; strict wire durable handoff; original Reply payload+MsgID; uncertain mark replay yields single broker message; Gateway ACK")
}
