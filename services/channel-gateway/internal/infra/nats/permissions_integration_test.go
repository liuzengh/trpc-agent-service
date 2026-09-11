package natsadapter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	wire "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// This test is also compiled as a small Linux test binary and run on the
// dedicated Compose network; no broker ports need to be exposed to the host.
func TestBrokerPermissionsIntegration(t *testing.T) {
	url := os.Getenv("GATEWAY_TEST_AUTH_NATS_URL")
	if url == "" {
		t.Skip("GATEWAY_TEST_AUTH_NATS_URL required")
	}
	topology, err := LoadTopology(os.Getenv("GATEWAY_TEST_TOPOLOGY_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	runtime, err := Connect(url, topology, Auth{User: "gateway", Password: os.Getenv("NATS_GATEWAY_PASSWORD"), InboxPrefix: "_INBOX.gateway"})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if err = runtime.Verify(ctx); err != nil {
		t.Fatalf("runtime read-only topology verification: %v", err)
	}
	admin, err := Connect(url, topology, Auth{User: "reconciler", Password: os.Getenv("NATS_RECONCILER_PASSWORD"), InboxPrefix: "_INBOX.reconciler"})
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err = admin.Reconcile(ctx); err != nil {
		t.Fatalf("administration permissions: %v", err)
	}
	fixturePath := os.Getenv("GATEWAY_TEST_RUN_FIXTURE_FILE")
	if fixturePath == "" {
		fixturePath = "../../../../../api/events/execution/v1/fixtures/valid/telegram-text.json"
	}
	fixture, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	event, err := wire.DecodeRunRequested(fixture)
	if err != nil {
		t.Fatal(err)
	}
	event.EventID = fmt.Sprintf("acl-admission-%d", time.Now().UnixNano())
	event.AdmissionID = event.EventID
	event.RunID = "run-" + event.EventID
	payload, err := wire.EncodeRunRequested(event)
	if err != nil {
		t.Fatal(err)
	}
	if err = runtime.Publish(ctx, RunSubject, event.EventID, payload); err != nil {
		t.Fatalf("runtime publication/PubAck permissions: %v", err)
	}
	denied := make(chan error, 8)
	c, err := nats.Connect(url, nats.UserInfo("gateway", os.Getenv("NATS_GATEWAY_PASSWORD")), nats.CustomInboxPrefix("_INBOX.gateway"), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, e error) { denied <- e }))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, subject := range []string{RouteSubject, ManifestSubject, ReplySubject, "$JS.API.CONSUMER.MSG.NEXT." + RunStream + "." + RunConsumer, "$JS.API.STREAM.UPDATE." + RunStream} {
		if err = c.Publish(subject, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
		if err = c.FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-denied:
			if !errors.Is(err, nats.ErrPermissionViolation) {
				t.Fatalf("expected permission denial: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("no permission denial for %s", subject)
		}
	}
	if unexpected, connectErr := nats.Connect(url, nats.UserInfo("gateway", "wrong-password"), nats.Timeout(time.Second), nats.NoReconnect()); connectErr == nil {
		unexpected.Close()
		t.Fatal("wrong credentials accepted")
	}
	t.Log("ACL_VERIFIED: runtime read-only topology; schema-valid RunRequested PubAck; reconciler idempotence; Gateway Control-publish denied; Gateway topology-write denied; incorrect credential denied")
}

// Worker is a publisher only for ReplyIntent and a consumer only of its Run and
// Manifest durables. Server-enforced denials are stronger than config inspection.
func TestWorkerBrokerPermissionsIntegration(t *testing.T) {
	address := os.Getenv("GATEWAY_TEST_AUTH_NATS_URL")
	if address == "" {
		t.Skip("GATEWAY_TEST_AUTH_NATS_URL required")
	}
	topology, err := LoadTopology(os.Getenv("GATEWAY_TEST_TOPOLOGY_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := Connect(address, topology, Auth{User: "reconciler", Password: os.Getenv("NATS_RECONCILER_PASSWORD"), InboxPrefix: "_INBOX.reconciler"})
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err = admin.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	denied := make(chan error, 16)
	nc, err := nats.Connect(address, nats.UserInfo("worker", os.Getenv("NATS_WORKER_PASSWORD")), nats.CustomInboxPrefix("_INBOX.worker"), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, e error) { denied <- e }))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{RunStream, ManifestStream, ReplyStream} {
		stream, e := js.Stream(ctx, name)
		if e != nil {
			t.Fatal("Worker stream info", e)
		}
		if _, e = stream.Info(ctx); e != nil {
			t.Fatal(e)
		}
	}
	for _, item := range []struct{ stream, durable string }{{RunStream, RunConsumer}, {ManifestStream, ManifestConsumer}} {
		consumer, e := js.Consumer(ctx, item.stream, item.durable)
		if e != nil {
			t.Fatal("Worker durable info", e)
		}
		if _, e = consumer.Info(ctx); e != nil {
			t.Fatal(e)
		}
	}
	if ack, e := js.Publish(ctx, ReplySubject, []byte(`{}`)); e != nil || ack.Stream != ReplyStream {
		t.Fatal("Worker Reply publication", ack, e)
	}

	// Exercise pull request and ACK permissions, not just Consumer.Info. The
	// opposite owners publish one fixture each through their own runtime ACL.
	for _, item := range []struct{ user, password, subject, stream, durable string }{{"gateway", os.Getenv("NATS_GATEWAY_PASSWORD"), RunSubject, RunStream, RunConsumer}, {"control", os.Getenv("NATS_CONTROL_PASSWORD"), ManifestSubject, ManifestStream, ManifestConsumer}} {
		owner, e := nats.Connect(address, nats.UserInfo(item.user, item.password), nats.CustomInboxPrefix("_INBOX."+item.user))
		if e != nil {
			t.Fatal(e)
		}
		ownerJS, e := jetstream.New(owner)
		if e != nil {
			owner.Close()
			t.Fatal(e)
		}
		if _, e = ownerJS.Publish(ctx, item.subject, []byte(`{}`)); e != nil {
			owner.Close()
			t.Fatal(e)
		}
		owner.Close()
		consumer, e := js.Consumer(ctx, item.stream, item.durable)
		if e != nil {
			t.Fatal(e)
		}
		message, e := consumer.Next(jetstream.FetchMaxWait(time.Second))
		if e != nil {
			t.Fatal("Worker pull permissions", e)
		}
		if e = message.DoubleAck(ctx); e != nil {
			t.Fatal("Worker ACK permissions", e)
		}
	}
	// Gateway must be able to consume and ACK the Worker publication as well.
	gateway, e := nats.Connect(address, nats.UserInfo("gateway", os.Getenv("NATS_GATEWAY_PASSWORD")), nats.CustomInboxPrefix("_INBOX.gateway"))
	if e != nil {
		t.Fatal(e)
	}
	defer gateway.Close()
	gatewayJS, e := jetstream.New(gateway)
	if e != nil {
		t.Fatal(e)
	}
	replies, e := gatewayJS.Consumer(ctx, ReplyStream, ReplyConsumer)
	if e != nil {
		t.Fatal(e)
	}
	reply, e := replies.Next(jetstream.FetchMaxWait(time.Second))
	if e != nil {
		t.Fatal("Gateway Reply pull permissions", e)
	}
	if e = reply.DoubleAck(ctx); e != nil {
		t.Fatal("Gateway Reply ACK permissions", e)
	}
	for _, subject := range []string{RouteSubject, RunSubject, ManifestSubject, "$JS.API.STREAM.INFO." + RouteStream, "$JS.API.CONSUMER.MSG.NEXT." + ReplyStream + "." + ReplyConsumer, "$JS.API.STREAM.UPDATE." + RunStream, "$JS.API.CONSUMER.CREATE." + RunStream + ".rogue"} {
		if err = nc.Publish(subject, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
		if err = nc.FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
		select {
		case e := <-denied:
			if !errors.Is(e, nats.ErrPermissionViolation) {
				t.Fatal(e)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("missing Worker permission denial", subject)
		}
	}
	t.Log("WORKER_ACL_PASS: Reply-only publisher; Run/Manifest pull+ACK; Gateway Reply pull+ACK; cross-owner publication, Reply consumption and topology writes denied")
}
