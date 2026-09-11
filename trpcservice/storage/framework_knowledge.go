package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/backendhealth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credential"
	agentknowledge "trpc.group/trpc-go/trpc-agent-go/knowledge"
	embedderopenai "trpc.group/trpc-go/trpc-agent-go/knowledge/embedder/openai"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/query"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/reranker"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/reranker/cohere"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/reranker/infinity"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/reranker/topk"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
	frameworkelasticsearch "trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/elasticsearch"
	frameworkpgvector "trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/pgvector"
	frameworkqdrant "trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/qdrant"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type frameworkKnowledgeConfig struct {
	databaseURL string
	knowledge   config.KnowledgeConfig
	providers   map[string]config.ModelProviderConfig
	secrets     credential.SecretResolver
	httpClient  *http.Client
	health      *backendhealth.Registry
}

func (c frameworkKnowledgeConfig) newKnowledge(ctx context.Context, tenantConfig config.TenantConfig, backend config.BackendConfig, llm model.Model, profileIDs ...string) (*agentknowledge.BuiltinKnowledge, error) {
	profileID := ""
	if len(profileIDs) > 0 {
		profileID = strings.TrimSpace(profileIDs[0])
	}
	store, err := c.newVectorStoreWithProfile(ctx, tenantConfig.TenantID, tenantConfig.AppCode, normalizedKnowledgeBackendConfig(backend), profileID, nil)
	if err != nil {
		return nil, err
	}
	embedder, err := c.newEmbedder(ctx)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	rank, err := c.newReranker(ctx)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	options := []agentknowledge.Option{
		agentknowledge.WithVectorStore(store),
		agentknowledge.WithEmbedder(embedder),
		agentknowledge.WithReranker(rank),
	}
	if llm != nil {
		options = append(options, agentknowledge.WithQueryEnhancer(query.NewLLMEnhancer(llm)))
	}
	return agentknowledge.New(options...), nil
}

func (c frameworkKnowledgeConfig) newVectorStore(ctx context.Context, tenantID, appCode string, backend config.BackendConfig, metadata map[string]any) (vectorstore.VectorStore, error) {
	return c.newVectorStoreWithProfile(ctx, tenantID, appCode, backend, "", metadata)
}

func (c frameworkKnowledgeConfig) newVectorStoreWithProfile(ctx context.Context, tenantID, appCode string, backend config.BackendConfig, profileID string, metadata map[string]any) (vectorstore.VectorStore, error) {
	backend = normalizedKnowledgeBackendConfig(backend)
	var store vectorstore.VectorStore
	switch backend.Driver {
	case "pgvector":
		installFrameworkPostgresClientAdapter()
		databaseURL := c.databaseURL
		if backend.ConnectionRef != "" {
			resolved, err := c.secrets.Resolve(ctx, backend.ConnectionRef)
			if err != nil {
				return nil, fmt.Errorf("resolve knowledge pgvector connection: %w", err)
			}
			databaseURL = strings.TrimSpace(resolved)
		}
		dsn, err := scopedKnowledgeDSN(databaseURL, tenantID, appCode, metadata)
		if err != nil {
			return nil, err
		}
		pgvectorStore, err := frameworkpgvector.New(
			frameworkpgvector.WithPGVectorClientDSN(dsn),
			frameworkpgvector.WithExtraOptions(
				frameworkSchemaManaged{table: knowledgeVectorTable},
				FrameworkPostgresScope("knowledge", tenantID),
			),
			frameworkpgvector.WithTable(knowledgeVectorTable),
			frameworkpgvector.WithIndexDimension(knowledgeEmbeddingDimensions),
			frameworkpgvector.WithLanguageExtension("simple"),
			frameworkpgvector.WithEnableTSVector(true),
			frameworkpgvector.WithHybridFusionMode(frameworkpgvector.HybridFusionRRF),
			frameworkpgvector.WithMaxResults(100),
		)
		if err != nil {
			return nil, fmt.Errorf("construct framework pgvector store: %w", err)
		}
		store = pgvectorStore
	case "qdrant":
		connection, err := c.resolveQdrantConfig(ctx, backend.ConnectionRef)
		if err != nil {
			return nil, err
		}
		options := []frameworkqdrant.Option{
			frameworkqdrant.WithHost(connection.Host),
			frameworkqdrant.WithPort(connection.Port),
			frameworkqdrant.WithTLS(connection.TLS),
			frameworkqdrant.WithCollectionName(knowledgeQdrantCollection(tenantID, appCode)),
			frameworkqdrant.WithDimension(c.knowledge.EmbeddingDimensions),
			frameworkqdrant.WithMaxResults(100),
		}
		if connection.APIKey != "" {
			options = append(options, frameworkqdrant.WithAPIKey(connection.APIKey))
		}
		qdrantStore, err := frameworkqdrant.New(ctx, options...)
		if err != nil {
			return nil, fmt.Errorf("construct framework Qdrant store: %w", err)
		}
		store = qdrantStore
	case "elasticsearch":
		connection, err := c.resolveElasticsearchConfig(ctx, backend.ConnectionRef)
		if err != nil {
			return nil, err
		}
		options := []frameworkelasticsearch.Option{
			frameworkelasticsearch.WithAddresses(connection.Addresses),
			frameworkelasticsearch.WithIndexName(knowledgeVectorNamespace(tenantID, appCode)),
			frameworkelasticsearch.WithVectorDimension(c.knowledge.EmbeddingDimensions),
			frameworkelasticsearch.WithMaxResults(100),
		}
		if connection.Username != "" {
			options = append(options, frameworkelasticsearch.WithUsername(connection.Username))
		}
		if connection.Password != "" {
			options = append(options, frameworkelasticsearch.WithPassword(connection.Password))
		}
		if connection.APIKey != "" {
			options = append(options, frameworkelasticsearch.WithAPIKey(connection.APIKey))
		}
		if connection.CertificateFingerprint != "" {
			options = append(options, frameworkelasticsearch.WithCertificateFingerprint(connection.CertificateFingerprint))
		}
		elasticsearchStore, err := frameworkelasticsearch.New(options...)
		if err != nil {
			return nil, fmt.Errorf("construct framework Elasticsearch store: %w", err)
		}
		store = elasticsearchStore
	default:
		return nil, fmt.Errorf("unsupported knowledge backend %q", backend.Driver)
	}
	scopedStore, err := newScopedVectorStore(store, tenantID, appCode, metadata)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	var scoped vectorstore.VectorStore = scopedStore
	profileID = strings.TrimSpace(profileID)
	if c.health != nil && profileID != "" && backendhealth.ShouldProtect(backend.Driver) {
		healthKey := backendhealth.Key{ProfileID: profileID, Domain: BackendDomainKnowledge, Driver: backend.Driver}
		if err := c.health.RegisterProbe(healthKey, func(probeCtx context.Context) error {
			_, probeErr := scoped.Count(probeCtx)
			return probeErr
		}); err != nil {
			_ = scoped.Close()
			return nil, err
		}
		if err := c.health.Check(ctx, healthKey); err != nil {
			_ = scoped.Close()
			return nil, fmt.Errorf("probe Knowledge backend %q: %w", profileID, err)
		}
		scoped = observeVectorStore(scoped, c.health, healthKey)
	}
	return scoped, nil
}

type qdrantKnowledgeConnection struct {
	Host   string `json:"host"`
	Port   int    `json:"port"`
	APIKey string `json:"api_key,omitempty"`
	TLS    bool   `json:"tls,omitempty"`
}

type elasticsearchKnowledgeConnection struct {
	Addresses              []string `json:"addresses"`
	Username               string   `json:"username,omitempty"`
	Password               string   `json:"password,omitempty"`
	APIKey                 string   `json:"api_key,omitempty"`
	CertificateFingerprint string   `json:"certificate_fingerprint,omitempty"`
}

func (c frameworkKnowledgeConfig) resolveQdrantConfig(ctx context.Context, reference string) (qdrantKnowledgeConnection, error) {
	if strings.TrimSpace(reference) == "" {
		return qdrantKnowledgeConnection{}, errors.New("Qdrant knowledge backend requires connection_ref")
	}
	encoded, err := c.secrets.Resolve(ctx, reference)
	if err != nil {
		return qdrantKnowledgeConnection{}, fmt.Errorf("resolve Qdrant knowledge connection: %w", err)
	}
	var connection qdrantKnowledgeConnection
	if err := json.Unmarshal([]byte(encoded), &connection); err != nil {
		return qdrantKnowledgeConnection{}, fmt.Errorf("decode Qdrant knowledge connection: %w", err)
	}
	connection.Host = strings.TrimSpace(connection.Host)
	if connection.Host == "" {
		return qdrantKnowledgeConnection{}, errors.New("Qdrant knowledge connection host is required")
	}
	if connection.Port == 0 {
		connection.Port = 6334
	}
	if connection.Port < 1 || connection.Port > 65535 {
		return qdrantKnowledgeConnection{}, errors.New("Qdrant knowledge connection port is invalid")
	}
	return connection, nil
}

func (c frameworkKnowledgeConfig) resolveElasticsearchConfig(ctx context.Context, reference string) (elasticsearchKnowledgeConnection, error) {
	if strings.TrimSpace(reference) == "" {
		return elasticsearchKnowledgeConnection{}, errors.New("Elasticsearch knowledge backend requires connection_ref")
	}
	encoded, err := c.secrets.Resolve(ctx, reference)
	if err != nil {
		return elasticsearchKnowledgeConnection{}, fmt.Errorf("resolve Elasticsearch knowledge connection: %w", err)
	}
	var connection elasticsearchKnowledgeConnection
	if err := json.Unmarshal([]byte(encoded), &connection); err != nil {
		return elasticsearchKnowledgeConnection{}, fmt.Errorf("decode Elasticsearch knowledge connection: %w", err)
	}
	addresses := make([]string, 0, len(connection.Addresses))
	for _, address := range connection.Addresses {
		if address = strings.TrimSpace(address); address != "" {
			addresses = append(addresses, address)
		}
	}
	if len(addresses) == 0 {
		return elasticsearchKnowledgeConnection{}, errors.New("Elasticsearch knowledge connection requires at least one address")
	}
	connection.Addresses = addresses
	connection.Username = strings.TrimSpace(connection.Username)
	connection.Password = strings.TrimSpace(connection.Password)
	connection.APIKey = strings.TrimSpace(connection.APIKey)
	connection.CertificateFingerprint = strings.TrimSpace(connection.CertificateFingerprint)
	return connection, nil
}

func knowledgeQdrantCollection(tenantID, appCode string) string {
	return knowledgeVectorNamespace(tenantID, appCode)
}

func knowledgeVectorNamespace(tenantID, appCode string) string {
	digest := sha256.Sum256([]byte(tenantID + "\x00" + appCode))
	return "trpc_knowledge_" + hex.EncodeToString(digest[:12])
}

func normalizedKnowledgeBackendConfig(backend config.BackendConfig) config.BackendConfig {
	backend.Driver = strings.ToLower(strings.TrimSpace(backend.Driver))
	if backend.Driver == "" {
		backend.Driver = "pgvector"
	}
	backend.ConnectionRef = strings.TrimSpace(backend.ConnectionRef)
	return backend
}

func (c frameworkKnowledgeConfig) newEmbedder(ctx context.Context) (*embedderopenai.Embedder, error) {
	provider, ok := c.providers[c.knowledge.EmbeddingProviderID]
	if !ok {
		return nil, fmt.Errorf("knowledge embedding provider %q is unavailable", c.knowledge.EmbeddingProviderID)
	}
	apiKey, err := c.secrets.Resolve(ctx, provider.APIKeyRef)
	if err != nil {
		return nil, fmt.Errorf("resolve knowledge embedding credentials: %w", err)
	}
	return embedderopenai.New(
		embedderopenai.WithAPIKey(apiKey),
		embedderopenai.WithBaseURL(provider.BaseURL),
		embedderopenai.WithModel(c.knowledge.EmbeddingModel),
		embedderopenai.WithDimensions(c.knowledge.EmbeddingDimensions),
	), nil
}

func (c frameworkKnowledgeConfig) newReranker(ctx context.Context) (reranker.Reranker, error) {
	configured := c.knowledge.Reranker
	topN := configured.TopN
	switch strings.ToLower(strings.TrimSpace(configured.Type)) {
	case "", "topk":
		return topk.New(topk.WithK(topN)), nil
	case "cohere":
		apiKey, err := c.resolveOptionalSecret(ctx, configured.APIKeyRef)
		if err != nil {
			return nil, err
		}
		options := []cohere.Option{cohere.WithTopN(topN), cohere.WithHTTPClient(c.httpClient)}
		if configured.Endpoint != "" {
			options = append(options, cohere.WithEndpoint(configured.Endpoint))
		}
		if configured.Model != "" {
			options = append(options, cohere.WithModel(configured.Model))
		}
		if apiKey != "" {
			options = append(options, cohere.WithAPIKey(apiKey))
		}
		return cohere.New(options...)
	case "infinity":
		apiKey, err := c.resolveOptionalSecret(ctx, configured.APIKeyRef)
		if err != nil {
			return nil, err
		}
		options := []infinity.Option{
			infinity.WithEndpoint(configured.Endpoint), infinity.WithTopN(topN), infinity.WithHTTPClient(c.httpClient),
		}
		if configured.Model != "" {
			options = append(options, infinity.WithModel(configured.Model))
		}
		if apiKey != "" {
			options = append(options, infinity.WithAPIKey(apiKey))
		}
		return infinity.New(options...)
	default:
		return nil, fmt.Errorf("unsupported framework reranker %q", configured.Type)
	}
}

func (c frameworkKnowledgeConfig) resolveOptionalSecret(ctx context.Context, reference string) (string, error) {
	if strings.TrimSpace(reference) == "" {
		return "", nil
	}
	value, err := c.secrets.Resolve(ctx, reference)
	if err != nil {
		return "", fmt.Errorf("resolve knowledge reranker credentials: %w", err)
	}
	return value, nil
}

func scopedKnowledgeDSN(databaseURL, tenantID, appCode string, metadata map[string]any) (string, error) {
	if strings.TrimSpace(databaseURL) == "" || strings.TrimSpace(tenantID) == "" || strings.TrimSpace(appCode) == "" {
		return "", errors.New("knowledge database URL, tenant, and application are required")
	}
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		return "", fmt.Errorf("parse knowledge database URL: %w", err)
	}
	if (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Host == "" {
		return "", errors.New("knowledge database URL must be a PostgreSQL URL with a host")
	}
	// The shared platform login owns the schema and has BYPASSRLS for control-
	// plane work. Framework vector-store connections must explicitly assume the
	// tenant role so PostgreSQL RLS is actually enforced on knowledge_vectors.
	query := parsed.Query()
	startupOptions := strings.TrimSpace(query.Get("options"))
	settings := []string{
		"role=trpc_tenant",
		"app.tenant_id=" + tenantID,
		"app.app_code=" + appCode,
	}
	if jobID, ok := metadata[knowledgeJobMetadataKey].(string); ok && jobID != "" {
		settings = append(settings, "app.knowledge_ingest_job="+jobID)
	}
	if owner, ok := metadata[knowledgeOwnerMetadataKey].(string); ok && owner != "" {
		settings = append(settings, "app.knowledge_ingest_owner="+owner)
	}
	for _, setting := range settings {
		if strings.ContainsAny(setting, " \t\r\n") {
			return "", errors.New("knowledge PostgreSQL runtime scope contains whitespace")
		}
		if startupOptions != "" {
			startupOptions += " "
		}
		startupOptions += "-c " + setting
	}
	query.Set("options", startupOptions)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}
