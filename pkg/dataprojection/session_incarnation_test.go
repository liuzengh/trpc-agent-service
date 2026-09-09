package dataprojection

import (
	"context"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestSessionProjectionPreservesIncarnationAcrossBackendCreation(t *testing.T) {
	ctx := context.Background()
	source, target := inmemory.NewSessionService(), inmemory.NewSessionService()
	key := session.Key{AppName: "tsa1:8:tenant-a:support", UserID: "owner", SessionID: "session"}
	id := "00000000-0000-4000-8000-000000000001"
	if _, err := source.CreateSession(ctx, key, session.StateMap{storage.SessionIncarnationStateKey: []byte(id)}); err != nil {
		t.Fatal(err)
	}
	record, err := NewSessionRecord(ctx, source, key, 1, 100)
	if err != nil {
		t.Fatal(err)
	}
	projector, err := NewSessionProjector(func(context.Context, string, string) (session.Service, error) { return target, nil }, 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := projector.Apply(ctx, "tenant-a", record); err != nil {
		t.Fatal(err)
	}
	value, err := target.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := storage.SessionIncarnationID(value); err != nil || got != id {
		t.Fatalf("migration changed Session incarnation: %q %v", got, err)
	}
}
