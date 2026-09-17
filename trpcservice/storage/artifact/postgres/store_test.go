package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact"
)

func TestStoreWithoutDatabaseFailsClosed(t *testing.T) {
	store := New(nil)
	if _, err := store.PutArtifact(context.Background(), artifact.Record{}); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("put err=%v", err)
	}
	if _, err := store.GetArtifact(context.Background(), "tenant", "artifact"); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("get err=%v", err)
	}
}
