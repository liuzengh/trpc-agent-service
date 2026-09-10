// Package qdrant resolves the official Qdrant VectorStore for scoped knowledge.
package qdrant

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	platformegress "github.com/liuzengh/trpc-agent-service/trpcservice/egress"
	platformknowledge "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	openaiopt "github.com/openai/openai-go/option"
	frameworkknowledge "trpc.group/trpc-go/trpc-agent-go/knowledge"
	frameworkembedder "trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder/openai"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
	frameworkqdrant "trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/qdrant"
	qdrantstorage "trpc.group/trpc-go/trpc-agent-go/storage/qdrant"
)

const (
	providerName = "qdrant"

	minQdrantPort              = 1
	maxQdrantPort              = 65535
	maxCollectionSegmentLength = 96

	optionEmbeddingModel      = "embedding_model"
	optionEmbeddingDimensions = "embedding_dimensions"
	optionEmbeddingProfile    = "embedding_profile"
	optionIndexGeneration     = "index_generation"
)

// Endpoint is an operator-controlled Qdrant gRPC endpoint.
type Endpoint struct {
	Host string
	Port int
	TLS  bool
}

// Validate checks endpoint connection parameters.
func (e Endpoint) Validate() error {
	if e.Host == "" {
		return errors.New("qdrant host is required")
	}
	if e.Port < minQdrantPort || e.Port > maxQdrantPort {
		return errors.New("qdrant port is invalid")
	}
	return nil
}

// EndpointResolver resolves a Qdrant endpoint by logical backend name.
// It must not consume tenant-provided network addresses.
type EndpointResolver interface {
	ResolveQdrantEndpoint(context.Context, string) (Endpoint, error)
}

// EmbedderEndpointPolicy validates a configured embedding endpoint before the
// resolver sends model credentials to it.
type EmbedderEndpointPolicy interface {
	ResolveModelBaseURL(context.Context, worker.Execution, string) (string, error)
}

// Resolver owns Qdrant VectorStores selected by immutable app configs.
type Resolver struct {
	secrets   platformsecret.SecretProvider
	endpoints EndpointResolver
	catalog   platformknowledge.Catalog
	policy    EmbedderEndpointPolicy

	mu     sync.Mutex
	closed bool
	stores map[string]vectorstore.VectorStore
}

// NewResolver creates a scoped Qdrant knowledge resolver.
func NewResolver(
	secrets platformsecret.SecretProvider,
	endpoints EndpointResolver,
	catalog platformknowledge.Catalog,
	policy EmbedderEndpointPolicy,
) (*Resolver, error) {
	if secrets == nil {
		return nil, errors.New("secret provider is required")
	}
	if endpoints == nil {
		return nil, errors.New("qdrant endpoint resolver is required")
	}
	if catalog == nil {
		return nil, errors.New("knowledge catalog is required")
	}
	if policy == nil {
		return nil, errors.New("embedding endpoint policy is required")
	}
	return &Resolver{
		secrets:   secrets,
		endpoints: endpoints,
		catalog:   catalog,
		policy:    policy,
		stores:    make(map[string]vectorstore.VectorStore),
	}, nil
}

// ResolveKnowledge returns the SQL-guarded Knowledge selected by exec. A nil
// result means the app config has no Knowledge backend.
func (r *Resolver) ResolveKnowledge(
	ctx context.Context,
	exec worker.Execution,
) (frameworkknowledge.Knowledge, error) {
	if r == nil || r.secrets == nil || r.endpoints == nil || r.catalog == nil || r.policy == nil {
		return nil, errors.New("qdrant knowledge resolver is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ref := exec.Config.BackendConfig.Knowledge
	if ref.IsZero() {
		return nil, nil
	}
	settings, err := validateBackend(ref)
	if err != nil {
		return nil, err
	}
	endpoint, err := r.endpoints.ResolveQdrantEndpoint(ctx, ref.Name)
	if err != nil {
		return nil, fmt.Errorf("resolve qdrant endpoint: %w", err)
	}
	if err := endpoint.Validate(); err != nil {
		return nil, err
	}
	store, err := r.resolveStore(ctx, exec, ref, settings, endpoint)
	if err != nil {
		return nil, err
	}
	embedder, err := r.newEmbedder(ctx, exec, settings)
	if err != nil {
		return nil, err
	}
	inner := frameworkknowledge.New(
		frameworkknowledge.WithVectorStore(store),
		frameworkknowledge.WithEmbedder(embedder),
	)
	result, err := platformknowledge.NewScopedKnowledge(
		inner,
		r.catalog,
		exec.Tenant.Scope(),
		exec.Tenant.ConfigVersion,
		exec.Config.KnowledgeBaseIDs,
	)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Close releases all VectorStores created by this resolver.
func (r *Resolver) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	var result error
	for _, store := range r.stores {
		result = errors.Join(result, store.Close())
	}
	r.stores = nil
	return result
}

func (r *Resolver) resolveStore(
	ctx context.Context,
	exec worker.Execution,
	ref tenant.BackendRef,
	settings backendSettings,
	endpoint Endpoint,
) (vectorstore.VectorStore, error) {
	key, err := storeKey(exec.Tenant.Scope(), exec.Tenant.ConfigVersion, ref, settings, endpoint)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, errors.New("qdrant knowledge resolver is closed")
	}
	if store := r.stores[key]; store != nil {
		r.mu.Unlock()
		return store, nil
	}
	r.mu.Unlock()

	clientOptions := []qdrantstorage.ClientBuilderOpt{
		qdrantstorage.WithHost(endpoint.Host),
		qdrantstorage.WithPort(endpoint.Port),
		qdrantstorage.WithTLS(endpoint.TLS),
	}
	if ref.SecretRef != (tenant.SecretRef{}) {
		apiKey, err := r.secrets.ResolveSecret(ctx, exec.Tenant.Scope(), ref.SecretRef)
		if err != nil {
			return nil, fmt.Errorf("resolve qdrant api key: %w", err)
		}
		if apiKey == "" {
			return nil, errors.New("qdrant api key is required")
		}
		clientOptions = append(clientOptions, qdrantstorage.WithAPIKey(apiKey))
	}
	client, err := qdrantstorage.NewClient(ctx, clientOptions...)
	if err != nil {
		return nil, fmt.Errorf("create qdrant client: %w", err)
	}
	store, err := frameworkqdrant.New(ctx,
		frameworkqdrant.WithClient(client),
		frameworkqdrant.WithCollectionName(settings.collectionName()),
		frameworkqdrant.WithDimension(settings.embeddingDimensions),
	)
	if err != nil {
		result := fmt.Errorf("create qdrant vector store: %w", err)
		if closeErr := client.Close(); closeErr != nil {
			result = errors.Join(result, fmt.Errorf("close qdrant client: %w", closeErr))
		}
		return nil, result
	}
	managed := &managedStore{VectorStore: store, client: client}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		closedErr := errors.New("qdrant knowledge resolver is closed")
		if closeErr := managed.Close(); closeErr != nil {
			return nil, errors.Join(closedErr, fmt.Errorf("close qdrant store: %w", closeErr))
		}
		return nil, closedErr
	}
	if existing := r.stores[key]; existing != nil {
		if closeErr := managed.Close(); closeErr != nil {
			return nil, fmt.Errorf("close duplicate qdrant store: %w", closeErr)
		}
		return existing, nil
	}
	r.stores[key] = managed
	return managed, nil
}

type managedStore struct {
	vectorstore.VectorStore
	client qdrantstorage.Client
}

func (s *managedStore) Close() error {
	if s == nil {
		return nil
	}
	var result error
	if s.VectorStore != nil {
		result = errors.Join(result, s.VectorStore.Close())
	}
	if s.client != nil {
		result = errors.Join(result, s.client.Close())
	}
	return result
}

// ValidateBackend checks that ref selects the Qdrant knowledge adapter.
func ValidateBackend(ref tenant.BackendRef) error {
	_, err := validateBackend(ref)
	return err
}

func (r *Resolver) newEmbedder(
	ctx context.Context,
	exec worker.Execution,
	settings backendSettings,
) (frameworkembedder.Embedder, error) {
	embeddingKey, err := r.secrets.ResolveSecret(ctx, exec.Tenant.Scope(), exec.Config.Model.APIKeyRef)
	if err != nil {
		return nil, fmt.Errorf("resolve embedding api key: %w", err)
	}
	if embeddingKey == "" {
		return nil, errors.New("embedding api key is required")
	}
	embedderOptions := []openai.Option{
		openai.WithAPIKey(embeddingKey),
		openai.WithModel(settings.embeddingModel),
		openai.WithDimensions(settings.embeddingDimensions),
		openai.WithRequestOptions(openaiopt.WithHTTPClient(&http.Client{
			Transport: platformegress.NewHTTPTransport(),
		})),
	}
	if configuredURL := exec.Config.Model.Parameters["base_url"]; configuredURL != "" {
		baseURL, err := r.policy.ResolveModelBaseURL(ctx, exec, configuredURL)
		if err != nil {
			return nil, fmt.Errorf("resolve embedding base url: %w", err)
		}
		embedderOptions = append(embedderOptions, openai.WithBaseURL(baseURL))
	}
	return openai.New(embedderOptions...), nil
}

func metadataString(metadata map[string]any, key string) string {
	value, _ := metadata[key].(string)
	return value
}

type backendSettings struct {
	embeddingModel      string
	embeddingDimensions int
	embeddingProfile    string
	indexGeneration     string
}

func validateBackend(ref tenant.BackendRef) (backendSettings, error) {
	if ref.Kind != tenant.BackendVector {
		return backendSettings{}, errors.New("qdrant knowledge backend kind must be vector")
	}
	if ref.Provider != providerName {
		return backendSettings{}, fmt.Errorf("unsupported knowledge provider %q", ref.Provider)
	}
	settings := backendSettings{
		embeddingModel:   ref.Options[optionEmbeddingModel],
		embeddingProfile: ref.Options[optionEmbeddingProfile],
		indexGeneration:  ref.Options[optionIndexGeneration],
	}
	if settings.embeddingModel == "" {
		return backendSettings{}, errors.New("qdrant embedding_model option is required")
	}
	if !validCollectionSegment(settings.embeddingProfile) {
		return backendSettings{}, errors.New("qdrant embedding_profile option is invalid")
	}
	if !validCollectionSegment(settings.indexGeneration) {
		return backendSettings{}, errors.New("qdrant index_generation option is invalid")
	}
	dimensions, err := strconv.Atoi(ref.Options[optionEmbeddingDimensions])
	if err != nil || dimensions <= 0 {
		return backendSettings{}, errors.New("qdrant embedding_dimensions option is invalid")
	}
	settings.embeddingDimensions = dimensions
	return settings, nil
}

func (s backendSettings) collectionName() string {
	return "knowledge-" + s.embeddingProfile + "-" + s.indexGeneration
}

func validCollectionSegment(value string) bool {
	if value == "" || len(value) > maxCollectionSegmentLength {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			character == '-' || character == '_' {
			continue
		}
		return false
	}
	return !strings.HasPrefix(value, "-")
}

func storeKey(
	scope tenant.Scope,
	version string,
	ref tenant.BackendRef,
	settings backendSettings,
	endpoint Endpoint,
) (string, error) {
	parts := []string{
		version,
		ref.Provider,
		ref.Name,
		ref.SecretRef.Name,
		ref.SecretRef.Version,
		settings.embeddingModel,
		strconv.Itoa(settings.embeddingDimensions),
		settings.embeddingProfile,
		settings.indexGeneration,
		endpoint.Host,
		strconv.Itoa(endpoint.Port),
		strconv.FormatBool(endpoint.TLS),
	}
	return scope.Key("knowledge", parts...)
}
