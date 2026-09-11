package natsadapter

import (
	"context"
	"encoding/json"
	"errors"
	controleventsv1 "github.com/liuzengh/trpc-agent-service/api/events/control/v1"
	"github.com/nats-io/nats.go"
	"os"
	"testing"
	"time"
)

func TestManifestPublisherAgainstNATS(t *testing.T) {
	address := os.Getenv("CONTROL_MANIFEST_TEST_NATS_URL")
	if address == "" {
		t.Skip("CONTROL_MANIFEST_TEST_NATS_URL is not set")
	}
	nc, err := nats.Connect(address)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = js.StreamInfo(controleventsv1.ManifestStream); !errors.Is(err, nats.ErrStreamNotFound) {
		t.Fatalf("requires isolated broker with no existing Manifest stream: %v", err)
	}
	raw, err := os.ReadFile("../../../../../../../api/events/control/v1/examples/valid/runtime-manifest-worker-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	event, err := controleventsv1.DecodeRuntimeManifestPublishedEvent(raw)
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewManifestPublisher(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err = p.PublishManifest(ctx, event.EventID, raw); err == nil {
		t.Fatal("publish without durable stream acknowledged")
	}
	_, err = js.AddStream(&nats.StreamConfig{Name: controleventsv1.ManifestStream, Subjects: []string{controleventsv1.ManifestSubject}, Storage: nats.FileStorage, Retention: nats.LimitsPolicy, MaxAge: 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer js.DeleteStream(controleventsv1.ManifestStream)
	if err = p.PublishManifest(ctx, event.EventID, raw); err != nil {
		t.Fatal(err)
	}
	if err = p.PublishManifest(ctx, event.EventID, raw); err != nil {
		t.Fatal(err)
	}
	info, err := js.StreamInfo(controleventsv1.ManifestStream)
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("durable dedup: %#v %v", info, err)
	}
	stored, err := js.GetMsg(controleventsv1.ManifestStream, 1)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := controleventsv1.DecodeRuntimeManifestPublishedEvent(stored.Data)
	if err != nil || decoded.Manifest.ContentDigest != event.Manifest.ContentDigest {
		t.Fatal("stored publication changed")
	}
	var bad map[string]any
	json.Unmarshal(raw, &bad)
	bad["private_secret"] = "canary"
	invalid, _ := json.Marshal(bad)
	if err = p.PublishManifest(ctx, event.EventID, invalid); err == nil {
		t.Fatal("invalid event published")
	}
}
