// Package contracttest contains the backend-neutral immutable ObjectStore suite.
package contracttest

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore"
)

type Factory func(testing.TB) objectstore.Store

func Run(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("immutable_idempotent_and_tenant_scoped", func(t *testing.T) {
		ctx := context.Background()
		store := factory(t)
		value := object(t, "tenant-a", "safe object")
		first, err := store.PutObject(ctx, value)
		if err != nil {
			t.Fatal(err)
		}
		value.Content[0] = 'X'
		loaded, err := store.GetObject(ctx, first.TenantID, first.ObjectKey)
		if err != nil || string(loaded.Content) != "safe object" {
			t.Fatalf("loaded=%+v err=%v", loaded, err)
		}
		again, err := store.PutObject(ctx, loaded)
		if err != nil || string(again.Content) != "safe object" {
			t.Fatalf("idempotent put=%+v err=%v", again, err)
		}
		collision := loaded
		collision.Content = []byte("other object")
		sum := sha256.Sum256(collision.Content)
		collision.ContentDigest = hex.EncodeToString(sum[:])
		if _, err := store.PutObject(ctx, collision); !errors.Is(err, runtime.ErrIdempotencyCollision) {
			t.Fatalf("collision err=%v", err)
		}
		if _, err := store.GetObject(ctx, "tenant-b", loaded.ObjectKey); !errors.Is(err, runtime.ErrTenantScope) {
			t.Fatalf("cross tenant err=%v", err)
		}
	})

	t.Run("digest_guarded_idempotent_delete", func(t *testing.T) {
		ctx := context.Background()
		store := factory(t)
		value := object(t, "tenant-a", "delete object")
		stored, err := store.PutObject(ctx, value)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.DeleteObject(ctx, stored.TenantID, stored.ObjectKey, hex.EncodeToString(make([]byte, sha256.Size))); !errors.Is(err, runtime.ErrVersionMismatch) {
			t.Fatalf("wrong digest delete err=%v", err)
		}
		if _, err := store.GetObject(ctx, stored.TenantID, stored.ObjectKey); err != nil {
			t.Fatalf("wrong digest removed object: %v", err)
		}
		if err := store.DeleteObject(ctx, stored.TenantID, stored.ObjectKey, stored.ContentDigest); err != nil {
			t.Fatal(err)
		}
		if err := store.DeleteObject(ctx, stored.TenantID, stored.ObjectKey, stored.ContentDigest); err != nil {
			t.Fatalf("idempotent delete err=%v", err)
		}
		if _, err := store.GetObject(ctx, stored.TenantID, stored.ObjectKey); !errors.Is(err, runtime.ErrNotFound) {
			t.Fatalf("deleted get err=%v", err)
		}
	})
}

func object(t testing.TB, tenantID, content string) objectstore.Object {
	t.Helper()
	artifactSum := sha256.Sum256([]byte(content))
	artifactID := "a1_" + base64.RawURLEncoding.EncodeToString(artifactSum[:])
	key, err := objectstore.StableKey(tenantID, artifactID)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(content))
	return objectstore.Object{TenantID: tenantID, ObjectKey: key, ContentDigest: hex.EncodeToString(digest[:]), Content: []byte(content)}
}
