package messaging

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/redis/go-redis/v9"
)

func TestRedisPendingAttachmentStoreAppendsIdempotentlyAndDrainsInOrder(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store, err := NewRedisPendingAttachmentStore(client)
	if err != nil {
		t.Fatal(err)
	}
	key := PendingAttachmentKey{
		TenantID: "tenant-a", AppCode: "support", Channel: channels.Feishu, BindingID: "bot-a",
		SessionKey: "tenant-a/support/session/1", SenderID: "user-a",
	}
	later := time.Date(2026, 9, 11, 10, 0, 2, 0, time.UTC)
	earlier := later.Add(-time.Second)
	if err := store.Append(context.Background(), key, pendingAttachmentBatch("m2", later, "b.png", 2)); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(context.Background(), key, pendingAttachmentBatch("m1", earlier, "a.png", 1)); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(context.Background(), key, pendingAttachmentBatch("m1", earlier, "a-new.png", 3)); err != nil {
		t.Fatal(err)
	}
	batches, err := store.Drain(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) != 2 || batches[0].MessageID != "m1" || batches[0].Files[0].Name != "a-new.png" || batches[1].MessageID != "m2" {
		t.Fatalf("drained batches = %#v", batches)
	}
	if again, err := store.Drain(context.Background(), key); err != nil || len(again) != 0 {
		t.Fatalf("second drain = %#v, %v", again, err)
	}
}

func TestRedisPendingAttachmentStoreEnforcesAggregateLimitAndExpires(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store, _ := NewRedisPendingAttachmentStore(client)
	key := PendingAttachmentKey{TenantID: "tenant", AppCode: "app", Channel: channels.Web, BindingID: "web-console", SessionKey: "session", SenderID: "user"}
	files := make([]channels.InboundFile, 0, MaxPendingAttachmentFiles)
	for index := 0; index < MaxPendingAttachmentFiles; index++ {
		files = append(files, channels.InboundFile{Name: "f", ArtifactName: "input/f", Version: index, SizeBytes: 1})
	}
	if err := store.Append(context.Background(), key, PendingAttachmentBatch{MessageID: "full", Files: files}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(context.Background(), key, pendingAttachmentBatch("overflow", time.Now(), "x", 1)); !errors.Is(err, ErrPendingAttachmentLimit) {
		t.Fatalf("overflow error = %v", err)
	}
	server.FastForward(PendingAttachmentTTL + time.Second)
	if batches, err := store.Drain(context.Background(), key); err != nil || len(batches) != 0 {
		t.Fatalf("expired drain = %#v, %v", batches, err)
	}
}

func TestRedisPendingAttachmentStoreRestoreMakesDrainedBatchAvailableAgain(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store, _ := NewRedisPendingAttachmentStore(client)
	key := PendingAttachmentKey{TenantID: "tenant", AppCode: "app", Channel: channels.WeCom, BindingID: "bot", SessionKey: "session", SenderID: "user"}
	want := pendingAttachmentBatch("m1", time.Now(), "proof.pdf", 5)
	if err := store.Append(context.Background(), key, want); err != nil {
		t.Fatal(err)
	}
	batches, err := store.Drain(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Restore(context.Background(), key, batches); err != nil {
		t.Fatal(err)
	}
	got, err := store.Drain(context.Background(), key)
	if err != nil || len(got) != 1 || got[0].MessageID != want.MessageID || got[0].Files[0].Name != "proof.pdf" {
		t.Fatalf("restored batches = %#v, %v", got, err)
	}
}

func TestRedisPendingAttachmentStoreIsolatesSendersInsideSharedSession(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store, _ := NewRedisPendingAttachmentStore(client)
	base := PendingAttachmentKey{
		TenantID: "tenant", AppCode: "app", Channel: channels.Feishu, BindingID: "bot", SessionKey: "tenant/app/group/group-1",
	}
	alice := base
	alice.SenderID = "alice"
	bob := base
	bob.SenderID = "bob"
	if err := store.Append(context.Background(), alice, pendingAttachmentBatch("alice-file", time.Now(), "alice.png", 1)); err != nil {
		t.Fatal(err)
	}
	if got, err := store.Drain(context.Background(), bob); err != nil || len(got) != 0 {
		t.Fatalf("bob drained alice attachments: %#v, %v", got, err)
	}
	if got, err := store.Drain(context.Background(), alice); err != nil || len(got) != 1 || got[0].MessageID != "alice-file" {
		t.Fatalf("alice drain = %#v, %v", got, err)
	}
}

func pendingAttachmentBatch(messageID string, receivedAt time.Time, name string, size int64) PendingAttachmentBatch {
	return PendingAttachmentBatch{
		MessageID: messageID, ReceivedAt: receivedAt,
		Files: []channels.InboundFile{{Name: name, MimeType: "application/octet-stream", ArtifactName: "input/" + messageID + "/" + name, Version: 0, SizeBytes: size}},
	}
}
