package inmemory_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact"
	artifactmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact/inmemory"
)

func TestStoreImmutableTenantScopedArtifact(t *testing.T) {
	source := sha256.Sum256([]byte("source"))
	sourceDigest := hex.EncodeToString(source[:])
	id, ref, err := artifact.StableIdentity("tenant-a", "request-1", 0, sourceDigest)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("safe text")
	digest := sha256.Sum256(content)
	record := artifact.Record{TenantID: "tenant-a", RequestID: "request-1", ArtifactID: id, ArtifactRef: ref,
		Ordinal: 0, SourceDigest: sourceDigest, ContentDigest: hex.EncodeToString(digest[:]), MediaType: "text/plain",
		Kind: "file", Content: content, MalwareScanVersion: "av-1", DLPVersion: "dlp-1"}
	store := artifactmemory.New()
	first, err := store.PutArtifact(context.Background(), record)
	if err != nil {
		t.Fatal(err)
	}
	record.Content[0] = 'X'
	loaded, err := store.GetArtifact(context.Background(), "tenant-a", id)
	if err != nil || string(loaded.Content) != "safe text" || first.ArtifactRef != ref {
		t.Fatalf("loaded=%#v err=%v", loaded, err)
	}
	if _, err := store.GetArtifact(context.Background(), "tenant-b", id); !errors.Is(err, runtime.ErrNotFound) {
		t.Fatalf("cross-tenant err=%v", err)
	}
	collision := loaded
	collision.Content = []byte("other")
	other := sha256.Sum256(collision.Content)
	collision.ContentDigest = hex.EncodeToString(other[:])
	if _, err := store.PutArtifact(context.Background(), collision); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("collision err=%v", err)
	}
	invalid := loaded
	invalid.ContentDigest = hex.EncodeToString(make([]byte, sha256.Size))
	if _, err := artifactmemory.New().PutArtifact(context.Background(), invalid); !errors.Is(err, runtime.ErrInvalidEnvelope) {
		t.Fatalf("invalid digest err=%v", err)
	}
}
