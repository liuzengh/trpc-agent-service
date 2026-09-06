package storage

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

// These checks run before a backend can connect to any external service.
func TestStorageSecretScopeAndNoAmbientCredentialFallback(t *testing.T) {
	ctx := context.Background()
	store, _ := secret.NewEnvStore([]secret.Grant{{TenantID: "tenant-b", Purpose: secret.Session, Reference: "env://BACKEND_CREDENTIAL"}})
	t.Setenv("BACKEND_CREDENTIAL", "sensitive-canary")
	sessionRouter := &SessionRouter{secrets: store}
	if _, err := sessionRouter.endpoint(ctx, "tenant-a", "", "env://BACKEND_CREDENTIAL"); !errors.Is(err, secret.ErrForbidden) {
		t.Fatalf("session: %v", err)
	}
	memoryRouter := &MemoryRouter{secrets: store}
	if _, err := memoryRouter.endpoint(ctx, "tenant-b", "", "env://BACKEND_CREDENTIAL"); !errors.Is(err, secret.ErrForbidden) {
		t.Fatalf("memory purpose: %v", err)
	}
	knowledgeRouter := &KnowledgeRouter{secrets: store}
	for _, ref := range []string{"", "env://BACKEND_CREDENTIAL"} {
		if _, err := knowledgeRouter.buildEmbedder(ctx, "tenant-a", knowledgeEmbeddingConfig{Provider: "openai", Model: "example", SecretRef: ref}); err == nil {
			t.Fatal("embedding accepted ambient/unassigned credential")
		}
	}
	artifactRouter := &ArtifactRouter{secrets: store}
	for _, ref := range []string{"", "env://BACKEND_CREDENTIAL"} {
		_, err := artifactRouter.build(ctx, controlplane.BackendBinding{TenantID: "tenant-a", BackendType: "s3", Config: json.RawMessage(`{"bucket":"example"}`), SecretRef: ref})
		if err == nil {
			t.Fatal("S3 accepted ambient/unassigned credential")
		}
	}
}
