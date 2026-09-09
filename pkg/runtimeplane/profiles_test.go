package runtimeplane

import (
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

const validProfileManifest = `[
  {
    "id":"knowledge-local","backend":"qdrant","endpoint":"127.0.0.1:6334",
    "tls":false,"allowInsecure":true,"collection":"agent_knowledge","dimension":3,
    "apiKeyEnv":"QDRANT_API_KEY","embeddingEndpoint":"http://127.0.0.1:18080/v1",
    "embeddingModel":"embedding-test","embeddingAPIKeyEnv":"EMBEDDING_API_KEY",
    "tenantIds":["tenant-a"]
  },
  {
    "id":"artifact-local","backend":"s3","endpoint":"127.0.0.1:9000",
    "tls":false,"allowInsecure":true,"bucket":"agent-artifacts","region":"us-east-1",
    "accessKeyEnv":"MINIO_ACCESS_KEY","secretKeyEnv":"MINIO_SECRET_KEY",
    "createBucket":true,"maxBytes":1048576,"tenantIds":["tenant-a"]
  }
]`

func configuredDataPlane() tenant.StorageConfig {
	return tenant.StorageConfig{
		SessionBackend: "postgres", SessionProfile: "session-primary",
		MemoryBackend: "redis", MemoryProfile: "memory-primary",
		KnowledgeBackend: "qdrant", KnowledgeProfile: "knowledge-local",
		ArtifactBackend: "s3", ArtifactProfile: "artifact-local",
	}
}

func TestProfileValidatorAdmitsExactTenantBackendBindings(t *testing.T) {
	validator, err := LoadProfileValidator(validProfileManifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := validator.ValidateTenantStorage("tenant-a", configuredDataPlane()); err != nil {
		t.Fatalf("valid profile bindings rejected: %v", err)
	}
	if err := validator.ValidateTenantStorage("tenant-b", configuredDataPlane()); !errors.Is(err, ErrProfileNotFound) {
		t.Fatalf("tenant allowlist error=%v, want ErrProfileNotFound", err)
	}
	wrong := configuredDataPlane()
	wrong.KnowledgeProfile = "artifact-local"
	if err := validator.ValidateTenantStorage("tenant-a", wrong); !errors.Is(err, ErrProfileTypeMismatch) {
		t.Fatalf("profile type error=%v, want ErrProfileTypeMismatch", err)
	}
}

func TestProfileManifestRejectsSecretsUnknownFieldsAndUnsafeRemotePlaintext(t *testing.T) {
	tests := []string{
		`[{"id":"knowledge-local","backend":"qdrant","endpoint":"127.0.0.1:6334","tls":false,"allowInsecure":true,"collection":"kb","dimension":3,"embeddingEndpoint":"http://127.0.0.1:18080/v1","embeddingModel":"e","embeddingAPIKeyEnv":"EMBEDDING_API_KEY","apiKey":"raw-secret"}]`,
		`[{"id":"knowledge-remote","backend":"qdrant","endpoint":"qdrant.internal:6334","tls":false,"allowInsecure":true,"collection":"kb","dimension":3,"embeddingEndpoint":"https://embedding.internal/v1","embeddingModel":"e","embeddingAPIKeyEnv":"EMBEDDING_API_KEY"}]`,
		`[{"id":"artifact-remote","backend":"s3","endpoint":"minio.internal:9000","tls":false,"allowInsecure":true,"bucket":"agent-artifacts","accessKeyEnv":"MINIO_ACCESS_KEY","secretKeyEnv":"MINIO_SECRET_KEY","maxBytes":1024}]`,
	}
	for _, manifest := range tests {
		if _, err := LoadProfileValidator(manifest); !errors.Is(err, ErrProfileManifestInvalid) {
			t.Fatalf("unsafe manifest error=%v, want ErrProfileManifestInvalid", err)
		}
	}
}

func TestLoadProfilesResolvesSecretsOnlyInWorkerCatalog(t *testing.T) {
	values := map[string]string{
		"QDRANT_API_KEY": "qdrant-secret", "EMBEDDING_API_KEY": "embedding-secret",
		"MINIO_ACCESS_KEY": "minio-access", "MINIO_SECRET_KEY": "minio-secret",
	}
	catalog, err := LoadProfiles(validProfileManifest, func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.ValidateTenantStorage("tenant-a", configuredDataPlane()); err != nil {
		t.Fatalf("resolved catalog rejected: %v", err)
	}
	delete(values, "EMBEDDING_API_KEY")
	if _, err := LoadProfiles(validProfileManifest, func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}); !errors.Is(err, ErrProfileSecretUnavailable) {
		t.Fatalf("missing secret error=%v, want ErrProfileSecretUnavailable", err)
	}
}
