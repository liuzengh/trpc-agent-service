package storage

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
)

func TestArtifactRouterS3Integration(t *testing.T) {
	endpoint := os.Getenv("TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("TEST_S3_ENDPOINT is not set")
	}
	data := controlplane.BootstrapData{BackendBindings: []controlplane.BackendBinding{{
		ID: "s3-artifact", TenantID: "tenant-s3", AppID: "app-s3",
		ResourceType: "artifact", BackendType: "s3", MigrationState: "active", Version: 1,
		Config:    json.RawMessage(`{"bucket":"trpc-agent-artifacts","endpoint":"` + endpoint + `","region":"us-east-1","path_style":true}`),
		SecretRef: "secret://s3",
	}}}
	repository := controlplane.NewMemoryRepository(data)
	router, err := NewArtifactRouter(repository, secret.StaticStore{
		"secret://s3": `{"access_key_id":"minioadmin","secret_access_key":"minioadmin"}`,
	})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	t.Cleanup(func() {
		_ = router.Close()
		_ = repository.Close()
	})
	info := artifact.SessionInfo{
		AppName: "t/tenant-s3/a/app-s3", UserID: "integration-user", SessionID: "session",
	}
	if _, err := router.SaveArtifact(context.Background(), info, "integration.txt", &artifact.Artifact{
		Data: []byte("s3-compatible"), MimeType: "text/plain",
	}); err != nil {
		t.Fatalf("save artifact: %v", err)
	}
	loaded, err := router.LoadArtifact(context.Background(), info, "integration.txt", nil)
	if err != nil || string(loaded.Data) != "s3-compatible" {
		t.Fatalf("artifact=%+v err=%v", loaded, err)
	}
}
