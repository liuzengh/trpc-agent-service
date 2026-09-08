package storage

import (
	"context"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
)

func TestImmutableArtifactRetriesDoNotCreateNewVersions(t *testing.T) {
	repo := controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData())
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(repo)
	r, _ := NewArtifactRouter(repo, secret.StaticStore{})
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(r)
	ctx := context.Background()
	info := artifact.SessionInfo{AppName: "t/tutorial-tenant/a/tutorial-app", UserID: "user", SessionID: "session"}
	value := &artifact.Artifact{Data: []byte("same"), MimeType: "text/plain"}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := r.SaveArtifactOnce(ctx, info, "immutable", value); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	versions, err := r.ListVersions(ctx, info, "immutable")
	if err != nil || len(versions) != 1 {
		t.Fatalf("versions=%v err=%v", versions, err)
	}
	if _, err := r.SaveArtifactOnce(ctx, info, "immutable", &artifact.Artifact{Data: []byte("changed"), MimeType: "text/plain"}); err == nil {
		t.Fatal("immutable overwrite accepted")
	}
}
