package backend

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/DocJlm/trpc-agent-service/trpcservice/config"
	"github.com/DocJlm/trpc-agent-service/trpcservice/secrets"
	"github.com/DocJlm/trpc-agent-service/trpcservice/tenant"
	"github.com/google/uuid"
)

func TestBackendIsolationIntegration(t *testing.T) {
	qdrantURL := os.Getenv("TEST_QDRANT_URL")
	minioEndpoint := os.Getenv("TEST_MINIO_ENDPOINT")
	if qdrantURL == "" || minioEndpoint == "" {
		t.Skip("TEST_QDRANT_URL and TEST_MINIO_ENDPOINT are required")
	}
	access := os.Getenv("TEST_MINIO_ACCESS_KEY")
	secret := os.Getenv("TEST_MINIO_SECRET_KEY")
	if access == "" || secret == "" {
		t.Skip("TEST_MINIO_ACCESS_KEY and TEST_MINIO_SECRET_KEY are required")
	}
	t.Setenv("BACKEND_TEST_ACCESS", access)
	t.Setenv("BACKEND_TEST_SECRET", secret)
	prefix := "integration-" + uuid.NewString()
	tenants := []tenant.Tenant{
		{ID: prefix + "-a", Backend: tenant.BackendProfile{Namespace: prefix + "-a"}},
		{ID: prefix + "-b", Backend: tenant.BackendProfile{Namespace: prefix + "-b"}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	router, err := NewRouter(ctx, config.ExternalBackends{
		QdrantURL: qdrantURL, MinIOEndpoint: minioEndpoint,
		MinIOAccessKeyRef: "env:BACKEND_TEST_ACCESS", MinIOSecretKeyRef: "env:BACKEND_TEST_SECRET",
		MinIOBucket: "trpc-agent-integration",
	}, tenants, secrets.FileEnvProvider{})
	if err != nil {
		t.Fatal(err)
	}
	results, err := Smoke(ctx, router, tenants)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || !results[0].QdrantIsolated || !results[1].MinIOIsolated {
		t.Fatalf("results=%+v", results)
	}
}
