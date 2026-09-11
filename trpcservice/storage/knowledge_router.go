package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	remoteembedding "github.com/liuzengh/trpc-agent-service/trpcservice/embedding"
	"github.com/liuzengh/trpc-agent-service/trpcservice/modelops"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"golang.org/x/sync/singleflight"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
	vectorinmemory "trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/inmemory"
	vectorqdrant "trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/qdrant"
)

const knowledgeResourceType = "knowledge"

type KnowledgeDocument struct {
	ID       string
	Name     string
	Content  string
	Metadata map[string]any
}

type KnowledgeRouter struct {
	repository controlplane.Repository
	secrets    secret.Store
	mu         sync.RWMutex
	closed     bool
	handles    map[string]*knowledgeHandle
	stores     map[string]vectorstore.VectorStore
	group      singleflight.Group
	budget     *tenant.Guard
}

type KnowledgeRouterOption func(*KnowledgeRouter)

func WithEmbeddingBudget(guard *tenant.Guard) KnowledgeRouterOption {
	return func(r *KnowledgeRouter) { r.budget = guard }
}

type knowledgeHandle struct {
	knowledge        *scopedKnowledge
	store            vectorstore.VectorStore
	embedder         embedder.Embedder
	config           revisionKnowledgeConfig
	secondary        *knowledgeHandle
	onSecondaryError func(context.Context, error)
}

func NewKnowledgeRouter(
	repository controlplane.Repository,
	secretStore secret.Store,
	opts ...KnowledgeRouterOption,
) (*KnowledgeRouter, error) {
	if repository == nil || secretStore == nil {
		return nil, fmt.Errorf("knowledge router repository and secret store are required")
	}
	router := &KnowledgeRouter{
		repository: repository,
		secrets:    secretStore,
		handles:    make(map[string]*knowledgeHandle),
		stores:     make(map[string]vectorstore.VectorStore),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(router)
		}
	}
	return router, nil
}

func (r *KnowledgeRouter) KnowledgeForRevision(
	ctx context.Context,
	scope runtimecontext.Scope,
	revision controlplane.AgentRevision,
) (knowledge.Knowledge, bool, error) {
	handle, enabled, err := r.handleFor(ctx, scope, revision)
	if err != nil || !enabled {
		return nil, enabled, err
	}
	return handle.knowledge, true, nil
}

func (r *KnowledgeRouter) UpsertDocument(
	ctx context.Context, scope runtimecontext.Scope, revision controlplane.AgentRevision, doc KnowledgeDocument,
) (int, error) {
	if err := scope.Validate(); err != nil {
		return 0, err
	}
	if _, _, err := runtimecontext.ValidateStorageScope(ctx, scope.StorageScope); err != nil {
		return 0, err
	}
	if revision.TenantID != scope.TenantID || revision.AppID != scope.AppID || revision.ID != scope.RevisionID {
		return 0, errors.New("knowledge revision scope mismatch")
	}
	doc.ID = strings.TrimSpace(doc.ID)
	doc.Content = strings.TrimSpace(doc.Content)
	if doc.ID == "" || len(doc.ID) > 128 || doc.Content == "" {
		return 0, errors.New("knowledge document identity and content required")
	}
	encoded, err := json.Marshal(doc)
	if err != nil || len(encoded) > 1<<20 {
		return 0, errors.New("knowledge document exceeds durable intent limit")
	}
	var chunks int
	err = r.withSync(ctx, scope, func(ctx context.Context, state *controlplane.KnowledgeSync, save func() error) error {
		state.Epoch++
		state.Intents[doc.ID] = controlplane.KnowledgeIntent{RevisionID: revision.ID, Document: encoded}
		if err := save(); err != nil {
			return err
		}
		var err error
		chunks, err = r.upsertDocument(ctx, scope, revision, doc)
		if err != nil {
			return err
		}
		delete(state.Intents, doc.ID)
		return save()
	})
	return chunks, err
}

func (r *KnowledgeRouter) upsertDocument(
	ctx context.Context,
	scope runtimecontext.Scope,
	revision controlplane.AgentRevision,
	doc KnowledgeDocument,
) (int, error) {
	ctx, span := startStorageSpan(ctx, "knowledge.upsert", scope.StorageScope)
	defer span.End()
	handle, enabled, err := r.handleFor(ctx, scope, revision)
	if err != nil {
		return 0, err
	}
	if !enabled {
		return 0, errors.New("knowledge is disabled for this revision")
	}
	doc.ID = strings.TrimSpace(doc.ID)
	doc.Name = strings.TrimSpace(doc.Name)
	doc.Content = strings.TrimSpace(doc.Content)
	if doc.ID == "" || doc.Content == "" {
		return 0, errors.New("knowledge document ID and content are required")
	}
	chunks, err := upsertKnowledgeHandle(ctx, scope, handle, doc)
	if err != nil {
		return 0, err
	}
	if handle.secondary != nil {
		if _, err := upsertKnowledgeHandle(ctx, scope, handle.secondary, doc); err != nil {
			if handle.onSecondaryError != nil {
				handle.onSecondaryError(ctx, err)
			}
			return 0, fmt.Errorf("secondary knowledge write: %w", err)
		}
	}
	return chunks, nil
}

func upsertKnowledgeHandle(
	ctx context.Context,
	scope runtimecontext.Scope,
	handle *knowledgeHandle,
	doc KnowledgeDocument,
) (int, error) {
	if err := handle.store.DeleteByFilter(ctx, vectorstore.WithDeleteFilter(map[string]any{
		"tenant_id": scope.TenantID, "app_id": scope.AppID, "source_document_id": doc.ID,
	})); err != nil {
		return 0, fmt.Errorf("replace knowledge document: %w", err)
	}
	chunks := splitKnowledgeText(doc.Content, handle.config.ChunkSize, handle.config.ChunkOverlap)
	for index, content := range chunks {
		metadata := cloneMetadata(doc.Metadata)
		metadata["tenant_id"] = scope.TenantID
		metadata["app_id"] = scope.AppID
		metadata["source_document_id"] = doc.ID
		metadata["chunk_index"] = index
		chunkID := knowledgeChunkID(scope.TenantID, scope.AppID, doc.ID, index)
		embedding, err := handle.embedder.GetEmbedding(ctx, content)
		if err != nil {
			return index, fmt.Errorf("embed knowledge chunk %d: %w", index, err)
		}
		if err := handle.store.Add(ctx, &document.Document{
			ID: chunkID, Name: doc.Name, Content: content, Metadata: metadata,
		}, embedding); err != nil {
			return index, fmt.Errorf("store knowledge chunk %d: %w", index, err)
		}
	}
	return len(chunks), nil
}

func (r *KnowledgeRouter) DeleteDocument(
	ctx context.Context, scope runtimecontext.Scope, revision controlplane.AgentRevision, documentID string,
) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if _, _, err := runtimecontext.ValidateStorageScope(ctx, scope.StorageScope); err != nil {
		return err
	}
	if revision.TenantID != scope.TenantID || revision.AppID != scope.AppID || revision.ID != scope.RevisionID {
		return errors.New("knowledge revision scope mismatch")
	}
	documentID = strings.TrimSpace(documentID)
	if documentID == "" || len(documentID) > 128 {
		return errors.New("knowledge document identity required")
	}
	return r.withSync(ctx, scope, func(ctx context.Context, state *controlplane.KnowledgeSync, save func() error) error {
		state.Epoch++
		state.Intents[documentID] = controlplane.KnowledgeIntent{RevisionID: revision.ID, Deleted: true}
		if err := save(); err != nil {
			return err
		}
		if err := r.deleteDocument(ctx, scope, revision, documentID); err != nil {
			return err
		}
		delete(state.Intents, documentID)
		return save()
	})
}

func (r *KnowledgeRouter) deleteDocument(
	ctx context.Context,
	scope runtimecontext.Scope,
	revision controlplane.AgentRevision,
	documentID string,
) error {
	ctx, span := startStorageSpan(ctx, "knowledge.delete", scope.StorageScope)
	defer span.End()
	handle, enabled, err := r.handleFor(ctx, scope, revision)
	if err != nil {
		return err
	}
	if !enabled {
		return errors.New("knowledge is disabled for this revision")
	}
	filter := map[string]any{
		"tenant_id": scope.TenantID, "app_id": scope.AppID,
		"source_document_id": strings.TrimSpace(documentID),
	}
	if err := handle.store.DeleteByFilter(ctx, vectorstore.WithDeleteFilter(filter)); err != nil {
		return err
	}
	if handle.secondary != nil {
		if err := handle.secondary.store.DeleteByFilter(
			ctx, vectorstore.WithDeleteFilter(filter),
		); err != nil {
			if handle.onSecondaryError != nil {
				handle.onSecondaryError(ctx, err)
			}
			return fmt.Errorf("secondary knowledge delete: %w", err)
		}
	}
	return nil
}

func (r *KnowledgeRouter) Ready(ctx context.Context) error {
	if r == nil || r.repository == nil {
		return errors.New("knowledge router is not initialized")
	}
	return r.repository.Ready(ctx)
}

func (r *KnowledgeRouter) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	stores := make([]vectorstore.VectorStore, 0, len(r.stores))
	var embedders []io.Closer
	for _, handle := range r.handles {
		if closer, ok := handle.embedder.(io.Closer); ok {
			embedders = append(embedders, closer)
		}
	}
	for _, store := range r.stores {
		stores = append(stores, store)
	}
	r.mu.Unlock()
	var closeErr error
	for _, closer := range embedders {
		closeErr = errors.Join(closeErr, closer.Close())
	}
	for _, store := range stores {
		closeErr = errors.Join(closeErr, store.Close())
	}
	return closeErr
}

type revisionKnowledgeConfig struct {
	Enabled      bool                     `json:"enabled"`
	MaxResults   int                      `json:"max_results"`
	MinScore     float64                  `json:"min_score"`
	ChunkSize    int                      `json:"chunk_size"`
	ChunkOverlap int                      `json:"chunk_overlap"`
	Embedding    knowledgeEmbeddingConfig `json:"embedding"`
}

type knowledgeEmbeddingConfig struct {
	PromptCostPerMillion float64 `json:"prompt_cost_per_million"`
	Provider             string  `json:"provider"`
	Model                string  `json:"model"`
	BaseURL              string  `json:"base_url"`
	Dimensions           int     `json:"dimensions"`
	SecretRef            string  `json:"secret_ref"`
}

type knowledgeBackendConfig struct {
	Host           string `json:"host"`
	Port           int    `json:"port"`
	TLS            bool   `json:"tls"`
	CollectionName string `json:"collection_name"`
	Dimensions     int    `json:"dimensions"`
	MaxResults     int    `json:"max_results"`
}

func ValidateRevisionKnowledgeConfig(raw json.RawMessage) error {
	_, err := parseRevisionKnowledgeConfig(raw)
	return err
}

func parseRevisionKnowledgeConfig(raw json.RawMessage) (revisionKnowledgeConfig, error) {
	var config revisionKnowledgeConfig
	if err := decodeStorageConfig(raw, &config); err != nil {
		return revisionKnowledgeConfig{}, fmt.Errorf("decode revision knowledge config: %w", err)
	}
	if !config.Enabled {
		return config, nil
	}
	if config.Embedding.PromptCostPerMillion < 0 || config.Embedding.PromptCostPerMillion > 1_000_000 {
		return revisionKnowledgeConfig{}, errors.New("invalid embedding token price")
	}
	if config.Embedding.Dimensions <= 0 || config.Embedding.Dimensions > 65536 {
		return revisionKnowledgeConfig{}, errors.New("knowledge embedding dimensions must be between 1 and 65536")
	}
	if config.Embedding.Provider != "hash" && config.Embedding.Provider != "openai" {
		return revisionKnowledgeConfig{}, fmt.Errorf(
			"unsupported knowledge embedding provider %q", config.Embedding.Provider,
		)
	}
	if config.Embedding.Provider == "openai" && config.Embedding.Model == "" {
		return revisionKnowledgeConfig{}, errors.New("OpenAI knowledge embedding model is required")
	}
	if config.Embedding.Provider == "openai" {
		baseURL := config.Embedding.BaseURL
		if baseURL == "" {
			baseURL = remoteembedding.DefaultBaseURL
		}
		if err := (remoteembedding.Config{Model: config.Embedding.Model, BaseURL: baseURL, APIKey: "validation-only", Dimensions: config.Embedding.Dimensions}).Validate(); err != nil {
			return revisionKnowledgeConfig{}, err
		}
	}
	if config.ChunkSize <= 0 {
		config.ChunkSize = 800
	}
	if config.ChunkOverlap < 0 || config.ChunkOverlap >= config.ChunkSize {
		return revisionKnowledgeConfig{}, errors.New(
			"knowledge chunk_overlap must be non-negative and smaller than chunk_size",
		)
	}
	return config, nil
}

func (r *KnowledgeRouter) handleFor(
	ctx context.Context,
	scope runtimecontext.Scope,
	revision controlplane.AgentRevision,
) (*knowledgeHandle, bool, error) {
	if err := scope.Validate(); err != nil {
		return nil, false, err
	}
	if _, _, err := runtimecontext.ValidateStorageScope(ctx, scope.StorageScope); err != nil {
		return nil, false, err
	}
	if revision.TenantID != scope.TenantID || revision.AppID != scope.AppID || revision.ID != scope.RevisionID {
		return nil, false, errors.New("knowledge revision scope mismatch")
	}
	revisionConfig, err := parseRevisionKnowledgeConfig(revision.KnowledgeConfig)
	if err != nil {
		return nil, false, err
	}
	if !revisionConfig.Enabled {
		return nil, false, nil
	}
	migration, migrationErr := r.repository.GetActiveBackendMigration(
		ctx, scope.TenantID, scope.AppID, knowledgeResourceType,
	)
	if migrationErr == nil {
		return r.migrationHandle(ctx, scope, revision, revisionConfig, migration)
	}
	if !errors.Is(migrationErr, controlplane.ErrNotFound) {
		return nil, false, migrationErr
	}
	binding, err := resolveBackendBinding(
		ctx, r.repository, scope.TenantID, scope.AppID, knowledgeResourceType,
	)
	if err != nil {
		return nil, false, err
	}
	handle, err := r.cachedHandle(ctx, scope, revision, revisionConfig, binding)
	return handle, true, err
}

func (r *KnowledgeRouter) migrationHandle(
	ctx context.Context,
	scope runtimecontext.Scope,
	revision controlplane.AgentRevision,
	config revisionKnowledgeConfig,
	migration controlplane.BackendMigration,
) (*knowledgeHandle, bool, error) {
	sourceBinding, err := r.repository.GetBackendBinding(
		ctx, scope.TenantID, migration.SourceBindingID,
	)
	if err != nil {
		return nil, false, err
	}
	source, err := r.cachedHandle(ctx, scope, revision, config, sourceBinding)
	if err != nil {
		return nil, false, err
	}
	if migration.State == controlplane.MigrationPlanned ||
		migration.State == controlplane.MigrationRollback {
		return source, true, nil
	}
	targetBinding, err := r.repository.GetBackendBinding(
		ctx, scope.TenantID, migration.TargetBindingID,
	)
	if err != nil {
		return nil, false, err
	}
	target, err := r.cachedHandle(ctx, scope, revision, config, targetBinding)
	if err != nil {
		return nil, false, err
	}
	switch migration.State {
	case controlplane.MigrationDualWrite, controlplane.MigrationBackfill,
		controlplane.MigrationVerify:
		combined := *source
		combined.secondary = target
		combined.onSecondaryError = func(ctx context.Context, _ error) {
			_ = r.repository.AdjustBackendMigrationRepair(
				ctx, migration.TenantID, migration.ID, 1,
			)
		}
		return &combined, true, nil
	case controlplane.MigrationCutover:
		combined := *target
		combined.secondary = source
		combined.onSecondaryError = func(ctx context.Context, _ error) {
			_ = r.repository.AdjustBackendMigrationRepair(
				ctx, migration.TenantID, migration.ID, 1,
			)
		}
		return &combined, true, nil
	default:
		return nil, false, fmt.Errorf("unsupported active knowledge migration state %q", migration.State)
	}
}

func (r *KnowledgeRouter) cachedHandle(
	ctx context.Context,
	scope runtimecontext.Scope,
	revision controlplane.AgentRevision,
	revisionConfig revisionKnowledgeConfig,
	binding controlplane.BackendBinding,
) (*knowledgeHandle, error) {
	// Publishing an unrelated Agent revision or changing migration state must
	// not silently replace an InMemory vector store with an empty instance.
	physicalConfig, _ := json.Marshal(struct {
		Backend   json.RawMessage
		Knowledge revisionKnowledgeConfig
	}{binding.Config, revisionConfig})
	digest := sha256.Sum256(physicalConfig)
	cacheKey := scope.StorageScope + "\x00" + binding.ID + "\x00" + hex.EncodeToString(digest[:])
	r.mu.RLock()
	handle := r.handles[cacheKey]
	closed := r.closed
	r.mu.RUnlock()
	if closed {
		return nil, errors.New("knowledge router is closed")
	}
	if handle != nil {
		return handle, nil
	}
	value, err, _ := r.group.Do(cacheKey, func() (any, error) {
		r.mu.RLock()
		cached := r.handles[cacheKey]
		r.mu.RUnlock()
		if cached != nil {
			return cached, nil
		}
		built, err := r.build(ctx, scope, binding, revisionConfig)
		if err != nil {
			return nil, err
		}
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			_ = built.store.Close()
			if closer, ok := built.embedder.(io.Closer); ok {
				_ = closer.Close()
			}
			return nil, errors.New("knowledge router is closed")
		}
		r.handles[cacheKey] = built
		r.mu.Unlock()
		return built, nil
	})
	if err != nil {
		return nil, err
	}
	return value.(*knowledgeHandle), nil
}

func (r *KnowledgeRouter) build(
	ctx context.Context,
	scope runtimecontext.Scope,
	binding controlplane.BackendBinding,
	revisionConfig revisionKnowledgeConfig,
) (*knowledgeHandle, error) {
	var backendConfig knowledgeBackendConfig
	if err := decodeStorageConfig(binding.Config, &backendConfig); err != nil {
		return nil, fmt.Errorf("decode knowledge binding %q: %w", binding.ID, err)
	}
	if backendConfig.Dimensions == 0 {
		backendConfig.Dimensions = revisionConfig.Embedding.Dimensions
	}
	if backendConfig.Dimensions != revisionConfig.Embedding.Dimensions {
		return nil, errors.New("knowledge vector and embedding dimensions differ")
	}
	selectedEmbedder, err := r.buildEmbedder(ctx, scope.TenantID, revisionConfig.Embedding)
	if err != nil {
		return nil, err
	}
	// Until cached in a handle, failures must not orphan an HTTP transport.
	owned := false
	defer func() {
		if !owned {
			if closer, ok := selectedEmbedder.(io.Closer); ok {
				_ = closer.Close()
			}
		}
	}()
	if r.budget != nil {
		wrapped, err := modelops.NewEmbedding(selectedEmbedder, r.budget, scope.TenantID, scope.AppID, revisionConfig.Embedding.PromptCostPerMillion)
		if err != nil {
			return nil, err
		}
		selectedEmbedder = wrapped
	}
	// Physical storage is independent of prompt/chunking/retrieval settings.
	// New revision views may change those settings without losing existing data.
	backendBytes, _ := json.Marshal(backendConfig)
	storeKey := scope.StorageScope + "\x00" + binding.ID + "\x00" + binding.BackendType + "\x00" + string(backendBytes)
	value, err, _ := r.group.Do("physical-store\x00"+storeKey, func() (any, error) {
		r.mu.RLock()
		existing := r.stores[storeKey]
		closed := r.closed
		r.mu.RUnlock()
		if closed {
			return nil, errors.New("knowledge router is closed")
		}
		if existing != nil {
			return existing, nil
		}
		var store vectorstore.VectorStore
		switch strings.ToLower(binding.BackendType) {
		case "inmemory":
			store = vectorinmemory.New(vectorinmemory.WithMaxResults(backendConfig.MaxResults))
		case "qdrant":
			if backendConfig.Host == "" || backendConfig.Port <= 0 ||
				backendConfig.CollectionName == "" {
				return nil, errors.New("qdrant host, port and collection_name are required")
			}
			options := []vectorqdrant.Option{
				vectorqdrant.WithHost(backendConfig.Host),
				vectorqdrant.WithPort(backendConfig.Port),
				vectorqdrant.WithTLS(backendConfig.TLS),
				vectorqdrant.WithDimension(backendConfig.Dimensions),
				vectorqdrant.WithCollectionName(backendConfig.CollectionName),
				vectorqdrant.WithMaxResults(backendConfig.MaxResults),
			}
			if binding.SecretRef != "" {
				apiKey, err := r.secrets.Resolve(ctx, binding.TenantID, secret.Knowledge, binding.SecretRef)
				if err != nil {
					return nil, err
				}
				options = append(options, vectorqdrant.WithAPIKey(apiKey))
			}
			store, err = vectorqdrant.New(ctx, options...)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unsupported knowledge backend %q", binding.BackendType)
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.closed {
			_ = store.Close()
			return nil, errors.New("knowledge router is closed")
		}
		r.stores[storeKey] = store
		return store, nil
	})
	if err != nil {
		return nil, err
	}
	store := value.(vectorstore.VectorStore)
	scoped := &scopedKnowledge{
		store: store, embedder: selectedEmbedder,
		tenantID: scope.TenantID, appID: scope.AppID,
		maxResults: revisionConfig.MaxResults, minScore: revisionConfig.MinScore,
	}
	owned = true
	return &knowledgeHandle{
		knowledge: scoped, store: store, embedder: selectedEmbedder, config: revisionConfig,
	}, nil
}

func (r *KnowledgeRouter) buildEmbedder(
	ctx context.Context,
	tenantID string,
	config knowledgeEmbeddingConfig,
) (embedder.Embedder, error) {
	switch strings.ToLower(config.Provider) {
	case "hash":
		return NewHashEmbedder(config.Dimensions), nil
	case "openai":
		if config.SecretRef == "" {
			return nil, errors.New("OpenAI embedding requires an explicit tenant credential reference")
		}
		apiKey, err := r.secrets.Resolve(ctx, tenantID, secret.Embedding, config.SecretRef)
		if err != nil {
			return nil, err
		}
		baseURL := config.BaseURL
		if baseURL == "" {
			baseURL = remoteembedding.DefaultBaseURL
		}
		return remoteembedding.NewRemote(remoteembedding.Config{Model: config.Model, BaseURL: baseURL, APIKey: apiKey, Dimensions: config.Dimensions})
	default:
		return nil, fmt.Errorf("unsupported knowledge embedding provider %q", config.Provider)
	}
}

type scopedKnowledge struct {
	store      vectorstore.VectorStore
	embedder   embedder.Embedder
	tenantID   string
	appID      string
	maxResults int
	minScore   float64
}

func (k *scopedKnowledge) Search(
	ctx context.Context,
	request *knowledge.SearchRequest,
) (*knowledge.SearchResult, error) {
	if _, _, err := runtimecontext.ValidateStorageScope(ctx, "t/"+k.tenantID+"/a/"+k.appID); err != nil {
		return nil, err
	}
	ctx, span := startStorageSpan(
		ctx, "knowledge.search", "t/"+k.tenantID+"/a/"+k.appID,
	)
	defer span.End()
	if request == nil || strings.TrimSpace(request.Query) == "" {
		return nil, errors.New("knowledge search query is required")
	}
	embedding, err := k.embedder.GetEmbedding(ctx, request.Query)
	if err != nil {
		return nil, err
	}
	metadata := map[string]any{"tenant_id": k.tenantID, "app_id": k.appID}
	if request.SearchFilter != nil {
		for key, value := range request.SearchFilter.Metadata {
			if key == "tenant_id" || key == "app_id" {
				continue
			}
			metadata[key] = value
		}
	}
	limit := request.MaxResults
	if limit <= 0 {
		limit = k.maxResults
	}
	if limit <= 0 {
		limit = 10
	}
	minScore := request.MinScore
	if minScore <= 0 {
		minScore = k.minScore
	}
	result, err := k.store.Search(ctx, &vectorstore.SearchQuery{
		Query: request.Query, Vector: embedding, Limit: limit, MinScore: minScore,
		Filter:     &vectorstore.SearchFilter{Metadata: metadata},
		SearchMode: request.SearchMode,
	})
	if err != nil {
		return nil, err
	}
	response := &knowledge.SearchResult{Documents: make([]*knowledge.Result, 0, len(result.Results))}
	for _, item := range result.Results {
		response.Documents = append(response.Documents, &knowledge.Result{
			Document: item.Document, Score: item.Score,
		})
	}
	if len(response.Documents) > 0 {
		response.Document = response.Documents[0].Document
		response.Score = response.Documents[0].Score
		response.Text = response.Document.Content
	}
	return response, nil
}

func splitKnowledgeText(text string, size int, overlap int) []string {
	runes := []rune(text)
	if len(runes) <= size {
		return []string{text}
	}
	result := make([]string, 0, (len(runes)+size-1)/size)
	step := size - overlap
	for start := 0; start < len(runes); start += step {
		end := start + size
		if end > len(runes) {
			end = len(runes)
		}
		result = append(result, string(runes[start:end]))
		if end == len(runes) {
			break
		}
	}
	return result
}

func knowledgeChunkID(tenantID string, appID string, documentID string, index int) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%s\x00%d", tenantID, appID, documentID, index)))
	return "k_" + hex.EncodeToString(digest[:])
}

func cloneMetadata(input map[string]any) map[string]any {
	result := make(map[string]any, len(input)+4)
	for key, value := range input {
		if key == "tenant_id" || key == "app_id" || key == "source_document_id" || key == "chunk_index" {
			continue
		}
		result[key] = value
	}
	return result
}

var _ knowledge.Knowledge = (*scopedKnowledge)(nil)
