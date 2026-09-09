package knowledgeplane

import (
	"context"
	"github.com/qdrant/go-client/qdrant"
	"testing"
	qdrantstorage "trpc.group/trpc-go/trpc-agent-go/storage/qdrant"
)

type migrationWriteClient struct {
	qdrantstorage.Client
	wait   bool
	status qdrant.UpdateStatus
}

func (c *migrationWriteClient) Upsert(_ context.Context, r *qdrant.UpsertPoints) (*qdrant.UpdateResult, error) {
	c.wait = r.GetWait()
	return &qdrant.UpdateResult{Status: c.status}, nil
}
func (c *migrationWriteClient) Delete(_ context.Context, r *qdrant.DeletePoints) (*qdrant.UpdateResult, error) {
	c.wait = r.GetWait()
	return &qdrant.UpdateResult{Status: c.status}, nil
}
func (c *migrationWriteClient) SetPayload(_ context.Context, r *qdrant.SetPayloadPoints) (*qdrant.UpdateResult, error) {
	c.wait = r.GetWait()
	return &qdrant.UpdateResult{Status: c.status}, nil
}
func TestMigrationQdrantWritesWaitForCompletion(t *testing.T) {
	client := &migrationWriteClient{status: qdrant.UpdateStatus_Completed}
	wrapped := synchronousQdrantClient{Client: client}
	request := &qdrant.UpsertPoints{}
	if _, err := wrapped.Upsert(context.Background(), request); err != nil || !client.wait || request.Wait != nil {
		t.Fatalf("upsert completion err=%v wait=%v original=%v", err, client.wait, request.Wait)
	}
	client.wait = false
	if _, err := wrapped.Delete(context.Background(), &qdrant.DeletePoints{}); err != nil || !client.wait {
		t.Fatalf("delete err=%v wait=%v", err, client.wait)
	}
	client.wait = false
	if _, err := wrapped.SetPayload(context.Background(), &qdrant.SetPayloadPoints{}); err != nil || !client.wait {
		t.Fatalf("metadata err=%v wait=%v", err, client.wait)
	}
	client.status = qdrant.UpdateStatus_Acknowledged
	if _, err := wrapped.Upsert(context.Background(), request); err == nil {
		t.Fatal("accepted unapplied source write")
	}
}
