package storage

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

type knowledgeSecretResolver struct {
	values map[string]string
	err    error
}

func (r knowledgeSecretResolver) Resolve(_ context.Context, reference string) (string, error) {
	if r.err != nil {
		return "", r.err
	}
	value, ok := r.values[reference]
	if !ok {
		return "", errors.New("secret not found")
	}
	return value, nil
}

func TestFrameworkKnowledgeQdrantConfigurationValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("required reference", func(t *testing.T) {
		framework := frameworkKnowledgeConfig{secrets: knowledgeSecretResolver{}}
		if _, err := framework.resolveQdrantConfig(ctx, " "); err == nil {
			t.Fatal("resolveQdrantConfig() error = nil")
		}
	})

	t.Run("secret resolution", func(t *testing.T) {
		want := errors.New("secret backend unavailable")
		framework := frameworkKnowledgeConfig{secrets: knowledgeSecretResolver{err: want}}
		if _, err := framework.resolveQdrantConfig(ctx, "env:QDRANT"); !errors.Is(err, want) {
			t.Fatalf("resolveQdrantConfig() error = %v, want %v", err, want)
		}
	})

	t.Run("invalid json", func(t *testing.T) {
		framework := frameworkKnowledgeConfig{secrets: knowledgeSecretResolver{values: map[string]string{"env:QDRANT": "{"}}}
		if _, err := framework.resolveQdrantConfig(ctx, "env:QDRANT"); err == nil || !strings.Contains(err.Error(), "decode Qdrant") {
			t.Fatalf("resolveQdrantConfig() error = %v", err)
		}
	})

	for _, test := range []struct {
		name  string
		value string
	}{
		{name: "missing host", value: `{"port":6334}`},
		{name: "port below range", value: `{"host":"localhost","port":-1}`},
		{name: "port above range", value: `{"host":"localhost","port":70000}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			framework := frameworkKnowledgeConfig{secrets: knowledgeSecretResolver{values: map[string]string{"env:QDRANT": test.value}}}
			if _, err := framework.resolveQdrantConfig(ctx, "env:QDRANT"); err == nil {
				t.Fatal("resolveQdrantConfig() error = nil")
			}
		})
	}

	t.Run("defaults port and trims host", func(t *testing.T) {
		framework := frameworkKnowledgeConfig{secrets: knowledgeSecretResolver{values: map[string]string{
			"env:QDRANT": `{"host":" 127.0.0.1 ","api_key":"test-key","tls":true}`,
		}}}
		connection, err := framework.resolveQdrantConfig(ctx, "env:QDRANT")
		if err != nil {
			t.Fatal(err)
		}
		if connection.Host != "127.0.0.1" || connection.Port != 6334 || connection.APIKey != "test-key" || !connection.TLS {
			t.Fatalf("connection = %+v", connection)
		}
	})
}

func TestFrameworkKnowledgeElasticsearchConfigurationValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for _, test := range []struct {
		name      string
		reference string
		value     string
	}{
		{name: "required reference"},
		{name: "invalid json", reference: "env:ELASTIC", value: "{"},
		{name: "missing addresses", reference: "env:ELASTIC", value: `{"username":"user"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			values := map[string]string{}
			if test.reference != "" {
				values[test.reference] = test.value
			}
			framework := frameworkKnowledgeConfig{secrets: knowledgeSecretResolver{values: values}}
			if _, err := framework.resolveElasticsearchConfig(ctx, test.reference); err == nil {
				t.Fatal("resolveElasticsearchConfig() error = nil")
			}
		})
	}

	framework := frameworkKnowledgeConfig{secrets: knowledgeSecretResolver{values: map[string]string{
		"env:ELASTIC": `{"addresses":[" https://es-a.example.test ","","https://es-b.example.test"],"username":" user ","password":" pass ","api_key":" key ","certificate_fingerprint":" fp "}`,
	}}}
	connection, err := framework.resolveElasticsearchConfig(ctx, "env:ELASTIC")
	if err != nil {
		t.Fatal(err)
	}
	if len(connection.Addresses) != 2 || connection.Addresses[0] != "https://es-a.example.test" || connection.Username != "user" || connection.Password != "pass" || connection.APIKey != "key" || connection.CertificateFingerprint != "fp" {
		t.Fatalf("connection = %+v", connection)
	}
}

func TestFrameworkKnowledgeBackendNormalizationAndCollectionScope(t *testing.T) {
	t.Parallel()
	backend := normalizedKnowledgeBackendConfig(config.BackendConfig{Driver: "  ", ConnectionRef: " env:KNOWLEDGE "})
	if backend.Driver != "pgvector" || backend.ConnectionRef != "env:KNOWLEDGE" {
		t.Fatalf("normalized backend = %+v", backend)
	}
	backend = normalizedKnowledgeBackendConfig(config.BackendConfig{Driver: " QDRANT "})
	if backend.Driver != "qdrant" {
		t.Fatalf("normalized driver = %q", backend.Driver)
	}

	first := knowledgeQdrantCollection("tenant-a", "support")
	second := knowledgeQdrantCollection("tenant-a", "support")
	other := knowledgeQdrantCollection("tenant-b", "support")
	if first != second || first == other || !strings.HasPrefix(first, "trpc_knowledge_") {
		t.Fatalf("collection names first=%q second=%q other=%q", first, second, other)
	}
}

func TestFrameworkKnowledgeEmbedderAndRerankerConfiguration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	base := frameworkKnowledgeConfig{
		knowledge: config.KnowledgeConfig{
			EmbeddingProviderID: "embedding",
			EmbeddingModel:      "text-embedding-test",
			EmbeddingDimensions: 1536,
		},
		providers: map[string]config.ModelProviderConfig{
			"embedding": {ID: "embedding", BaseURL: "https://models.example.test/v1", APIKeyRef: "env:EMBEDDING_KEY"},
		},
		secrets:    knowledgeSecretResolver{values: map[string]string{"env:EMBEDDING_KEY": "test-key", "env:RERANK_KEY": "rerank-key"}},
		httpClient: http.DefaultClient,
	}

	if _, err := base.newEmbedder(ctx); err != nil {
		t.Fatalf("newEmbedder() error = %v", err)
	}

	missingProvider := base
	missingProvider.knowledge.EmbeddingProviderID = "missing"
	if _, err := missingProvider.newEmbedder(ctx); err == nil {
		t.Fatal("newEmbedder() accepted missing provider")
	}

	secretErr := errors.New("credential unavailable")
	failingSecret := base
	failingSecret.secrets = knowledgeSecretResolver{err: secretErr}
	if _, err := failingSecret.newEmbedder(ctx); !errors.Is(err, secretErr) {
		t.Fatalf("newEmbedder() error = %v, want %v", err, secretErr)
	}

	for _, rerankerConfig := range []config.RerankerConfig{
		{},
		{Type: "topk", TopN: 4},
		{Type: "cohere", Endpoint: "https://rerank.example.test", Model: "rerank-test", APIKeyRef: "env:RERANK_KEY", TopN: 5},
		{Type: "infinity", Endpoint: "http://127.0.0.1:7997", Model: "rerank-test", APIKeyRef: "env:RERANK_KEY", TopN: 6},
	} {
		framework := base
		framework.knowledge.Reranker = rerankerConfig
		if _, err := framework.newReranker(ctx); err != nil {
			t.Fatalf("newReranker(%q) error = %v", rerankerConfig.Type, err)
		}
	}

	unsupported := base
	unsupported.knowledge.Reranker.Type = "unsupported"
	if _, err := unsupported.newReranker(ctx); err == nil {
		t.Fatal("newReranker() accepted unsupported type")
	}

	rererankSecretFailure := base
	rererankSecretFailure.knowledge.Reranker = config.RerankerConfig{Type: "cohere", APIKeyRef: "env:RERANK_KEY"}
	rererankSecretFailure.secrets = knowledgeSecretResolver{err: secretErr}
	if _, err := rererankSecretFailure.newReranker(ctx); !errors.Is(err, secretErr) {
		t.Fatalf("newReranker() error = %v, want %v", err, secretErr)
	}

	if value, err := base.resolveOptionalSecret(ctx, " "); err != nil || value != "" {
		t.Fatalf("resolveOptionalSecret(blank) = %q, %v", value, err)
	}
	if value, err := base.resolveOptionalSecret(ctx, "env:RERANK_KEY"); err != nil || value != "rerank-key" {
		t.Fatalf("resolveOptionalSecret() = %q, %v", value, err)
	}
}

func TestScopedKnowledgeDSNIncludesTenantAndIngestFence(t *testing.T) {
	t.Parallel()
	dsn, err := scopedKnowledgeDSN(
		"postgres://user:password@127.0.0.1:5432/test?sslmode=disable&options=-c%20statement_timeout%3D5000",
		"tenant-a",
		"support",
		map[string]any{knowledgeJobMetadataKey: "job-1", knowledgeOwnerMetadataKey: "worker-1"},
	)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	options := parsed.Query().Get("options")
	for _, expected := range []string{"statement_timeout=5000", "role=trpc_tenant", "app.tenant_id=tenant-a", "app.app_code=support", "app.knowledge_ingest_job=job-1", "app.knowledge_ingest_owner=worker-1"} {
		if !strings.Contains(options, expected) {
			t.Fatalf("options %q missing %q", options, expected)
		}
	}

	for _, test := range []struct {
		name     string
		database string
		tenant   string
		app      string
		metadata map[string]any
	}{
		{name: "missing database", tenant: "tenant-a", app: "support"},
		{name: "invalid scheme", database: "https://127.0.0.1/test", tenant: "tenant-a", app: "support"},
		{name: "missing host", database: "postgres:///test", tenant: "tenant-a", app: "support"},
		{name: "tenant whitespace", database: "postgres://127.0.0.1/test", tenant: "tenant a", app: "support"},
		{name: "job whitespace", database: "postgres://127.0.0.1/test", tenant: "tenant-a", app: "support", metadata: map[string]any{knowledgeJobMetadataKey: "job 1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := scopedKnowledgeDSN(test.database, test.tenant, test.app, test.metadata); err == nil {
				t.Fatal("scopedKnowledgeDSN() error = nil")
			}
		})
	}
}

func TestFrameworkKnowledgeRejectsUnsupportedVectorBackend(t *testing.T) {
	t.Parallel()
	framework := frameworkKnowledgeConfig{}
	if _, err := framework.newVectorStore(context.Background(), "tenant-a", "support", config.BackendConfig{Driver: "unsupported"}, nil); err == nil {
		t.Fatal("newVectorStore() accepted unsupported backend")
	}
}
