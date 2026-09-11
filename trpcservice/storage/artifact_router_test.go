package storage

import (
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
)

func TestArtifactRouterVersionsAndTenantScope(t *testing.T) {
	repository := controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData())
	router, err := NewArtifactRouter(repository, secret.StaticStore{})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	t.Cleanup(func() {
		_ = router.Close()
		_ = repository.Close()
	})
	info := artifact.SessionInfo{
		AppName: "t/tutorial-tenant/a/tutorial-app", UserID: "alice", SessionID: "session-a",
	}
	for index, data := range []string{"version-zero", "version-one"} {
		version, err := router.SaveArtifact(storageTestContext(), info, "report.txt", &artifact.Artifact{
			Data: []byte(data), MimeType: "text/plain",
		})
		if err != nil || version != index {
			t.Fatalf("version=%d err=%v", version, err)
		}
	}
	loaded, err := router.LoadArtifact(storageTestContext(), info, "report.txt", nil)
	if err != nil || string(loaded.Data) != "version-one" {
		t.Fatalf("artifact=%+v err=%v", loaded, err)
	}
	if _, err := router.ListArtifactKeys(storageTestContext(), artifact.SessionInfo{
		AppName: "tutorial-app", UserID: "alice", SessionID: "session-a",
	}); err == nil || !strings.Contains(err.Error(), "storage scope") {
		t.Fatalf("forged scope error=%v", err)
	}
}
