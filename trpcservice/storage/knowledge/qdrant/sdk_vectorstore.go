package qdrant

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
	sdkqdrant "trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/qdrant"
)

// sdkStoreConfig is intentionally smaller than Adapter.Config: the service
// owns endpoint admission and short-lived secret resolution, while the SDK
// owns Qdrant collection validation, gRPC transport and vector search.
type sdkStoreConfig struct {
	host       string
	port       int
	tls        bool
	apiKey     string
	collection string
	dimension  int
}

type sdkStoreFactory func(context.Context, sdkStoreConfig) (vectorstore.VectorStore, error)

func newOfficialSDKStore(ctx context.Context, config sdkStoreConfig) (vectorstore.VectorStore, error) {
	return sdkqdrant.New(ctx,
		sdkqdrant.WithHost(config.host),
		sdkqdrant.WithPort(config.port),
		sdkqdrant.WithTLS(config.tls),
		sdkqdrant.WithAPIKey(config.apiKey),
		sdkqdrant.WithCollectionName(config.collection),
		sdkqdrant.WithDimension(config.dimension),
		sdkqdrant.WithDistance(sdkqdrant.DistanceDot),
		sdkqdrant.WithBM25(false),
		sdkqdrant.WithMaxResults(64),
	)
}

// sdkReadOnlyVectorStore is a governance membrane around the official SDK
// implementation. It deliberately has no Qdrant query translation: the SDK
// receives the scoped filter and performs the search. The service then loads
// every candidate through its migration authority before returning it, so an
// untrusted backend response can never broaden tenant or version scope.
type sdkReadOnlyVectorStore struct {
	adapter  *Adapter
	scope    RuntimeScope
	newStore sdkStoreFactory

	mu     sync.Mutex
	active *leasedSDKStore
	closed bool
}

// leasedSDKStore keeps an official SDK client alive while in-flight requests
// use it. A token rotation retires the old lease rather than closing its gRPC
// connection underneath a search that has already been admitted.
type leasedSDKStore struct {
	tokenFingerprint [sha256.Size]byte
	store            vectorstore.VectorStore
	uses             int
	retired          bool
}

func newSDKReadOnlyVectorStore(adapter *Adapter, scope RuntimeScope, factory sdkStoreFactory) (*sdkReadOnlyVectorStore, error) {
	if adapter == nil || adapter.RuntimeEngine() != "sdk" || !scope.valid() || factory == nil ||
		adapter.endpoint == nil || adapter.grpcPort < 1 {
		return nil, runtime.ErrInvariantViolation
	}
	return &sdkReadOnlyVectorStore{adapter: adapter, scope: scope, newStore: factory}, nil
}

func (s *sdkReadOnlyVectorStore) Add(context.Context, *document.Document, []float64) error {
	return runtime.ErrCapabilityUnsupported
}

func (s *sdkReadOnlyVectorStore) Update(context.Context, *document.Document, []float64) error {
	return runtime.ErrCapabilityUnsupported
}

func (s *sdkReadOnlyVectorStore) Delete(context.Context, string) error {
	return runtime.ErrCapabilityUnsupported
}

func (s *sdkReadOnlyVectorStore) DeleteByFilter(context.Context, ...vectorstore.DeleteOption) error {
	return runtime.ErrCapabilityUnsupported
}

func (s *sdkReadOnlyVectorStore) UpdateByFilter(context.Context, ...vectorstore.UpdateByFilterOption) (int64, error) {
	return 0, runtime.ErrCapabilityUnsupported
}

func (s *sdkReadOnlyVectorStore) Count(context.Context, ...vectorstore.CountOption) (int, error) {
	return 0, runtime.ErrCapabilityUnsupported
}

func (s *sdkReadOnlyVectorStore) GetMetadata(context.Context, ...vectorstore.GetMetadataOption) (map[string]vectorstore.DocumentMetadata, error) {
	return nil, runtime.ErrCapabilityUnsupported
}

func (s *sdkReadOnlyVectorStore) Get(ctx context.Context, id string) (*document.Document, []float64, error) {
	if s == nil || s.adapter == nil || !s.scope.valid() || id == "" {
		return nil, nil, runtime.ErrInvariantViolation
	}
	var image knowledgedriver.ChunkImage
	err := s.withStore(ctx, func(store vectorstore.VectorStore) error {
		doc, vector, err := store.Get(ctx, pointID(knowledgedriver.ChunkKey{TenantID: s.scope.TenantID, KnowledgeID: s.scope.KnowledgeID, KnowledgeVersion: s.scope.KnowledgeVersion, ChunkID: id}))
		if err != nil {
			return err
		}
		image, err = s.imageFromSDKDocument(doc, vector)
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	return documentFor(image), float64Vector(image.Vector), nil
}

func (s *sdkReadOnlyVectorStore) Search(ctx context.Context, query *vectorstore.SearchQuery) (*vectorstore.SearchResult, error) {
	if s == nil || s.adapter == nil || !s.scope.valid() || query == nil || query.Query == "" ||
		len(query.Vector) != s.adapter.VectorSize() || !finiteVector(query.Vector) || math.IsNaN(query.MinScore) || math.IsInf(query.MinScore, 0) {
		return nil, runtime.ErrInvariantViolation
	}
	if query.SearchMode != vectorstore.SearchModeHybrid && query.SearchMode != vectorstore.SearchModeVector {
		return nil, runtime.ErrCapabilityUnsupported
	}
	if err := s.validateFilter(query.Filter); err != nil {
		return nil, err
	}
	limit := query.Limit
	if limit <= 0 {
		limit = 10
	}
	if limit > 64 {
		return nil, runtime.ErrInvariantViolation
	}
	// The official implementation receives only its public filter contract.
	// Immutable scope facts live under metadata because that is the SDK's
	// documented Qdrant payload convention. Its Search result includes payload
	// but omits vectors, while ImageDigest covers vectors; the public interface
	// has no batch Get. Preserve the one Get per candidate fail-closed check and
	// expose its cost through SDKSearchObserver/Benchmark rather than bypassing
	// the official SDK with a private Qdrant client.
	started := time.Now()
	getCalls := 0
	var candidates []sdkCandidate
	err := s.withStore(ctx, func(store vectorstore.VectorStore) error {
		result, err := s.searchSDK(ctx, store, query, limit)
		if err != nil {
			return err
		}
		candidates = make([]sdkCandidate, 0, len(result.Results))
		for _, scored := range result.Results {
			if scored == nil || scored.Document == nil || strings.TrimSpace(scored.Document.ID) == "" {
				return runtime.ErrInvariantViolation
			}
			doc, vector, err := store.Get(ctx, scored.Document.ID)
			getCalls++
			if err != nil {
				return err
			}
			image, err := s.imageFromSDKDocument(doc, vector)
			if err != nil {
				return err
			}
			candidates = append(candidates, sdkCandidate{image: image, score: scored.Score})
		}
		return nil
	})
	s.adapter.observeSDKSearch(ctx, SDKSearchObservation{Duration: time.Since(started), CandidateCount: len(candidates), GetCalls: getCalls, Failed: err != nil})
	if err != nil {
		return nil, err
	}
	out := &vectorstore.SearchResult{Results: make([]*vectorstore.ScoredDocument, 0, len(candidates))}
	seen := make(map[knowledgedriver.ChunkKey]struct{}, len(candidates))
	for _, candidate := range candidates {
		image := candidate.image
		if image.Operation == knowledgedriver.OperationDelete || !s.matchesAdditionalFilter(image, query.Filter) {
			continue
		}
		if _, exists := seen[image.Key]; exists {
			return nil, runtime.ErrInvariantViolation
		}
		seen[image.Key] = struct{}{}
		out.Results = append(out.Results, &vectorstore.ScoredDocument{Document: documentFor(image), Score: candidate.score})
		if len(out.Results) == limit {
			break
		}
	}
	return out, nil
}

func (s *sdkReadOnlyVectorStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	s.closed = true
	entry := s.active
	s.active = nil
	if entry != nil {
		entry.retired = true
	}
	closeNow := entry != nil && entry.uses == 0
	s.mu.Unlock()
	if closeNow {
		return entry.store.Close()
	}
	return nil
}

type sdkCandidate struct {
	image knowledgedriver.ChunkImage
	score float64
}

func (s *sdkReadOnlyVectorStore) searchSDK(ctx context.Context, store vectorstore.VectorStore, query *vectorstore.SearchQuery, limit int) (*vectorstore.SearchResult, error) {
	filter := &vectorstore.SearchFilter{Metadata: map[string]any{
		sdkScopeMetadataKey + ".tenant_id":         s.scope.TenantID,
		sdkScopeMetadataKey + ".knowledge_id":      s.scope.KnowledgeID,
		sdkScopeMetadataKey + ".knowledge_version": s.scope.KnowledgeVersion,
	}}
	for key, value := range query.Filter.Metadata {
		if key == "tenant_id" || key == "knowledge_id" || key == "knowledge_version" {
			continue
		}
		filter.Metadata[key] = value
	}
	if len(query.Filter.IDs) != 0 {
		filter.IDs = append([]string(nil), query.Filter.IDs...)
	}
	sdkQuery := *query
	sdkQuery.Limit = limit
	sdkQuery.Filter = filter
	result, err := store.Search(ctx, &sdkQuery)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, runtime.ErrBackendUnavailable
	}
	return result, nil
}

func (s *sdkReadOnlyVectorStore) imageFromSDKDocument(doc *document.Document, vector []float64) (knowledgedriver.ChunkImage, error) {
	if doc == nil || doc.Metadata == nil || len(vector) != s.adapter.VectorSize() || !finiteVector(vector) {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	rawScope, ok := doc.Metadata[sdkScopeMetadataKey].(map[string]any)
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	tenant, ok := text(rawScope, "tenant_id")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	knowledgeID, ok := text(rawScope, "knowledge_id")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	version, ok := int64Value(rawScope, "knowledge_version")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	chunkID, ok := text(rawScope, "chunk_id")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	revision, ok := int64Value(rawScope, "revision")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	operation, ok := text(rawScope, "operation")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	sourceDigest, ok := text(rawScope, "source_digest")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	contentDigest, ok := text(rawScope, "content_digest")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	metadataDigest, ok := text(rawScope, "metadata_digest")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	profile, ok := text(rawScope, "embedding_profile_id")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	embeddingVersion, ok := int64Value(rawScope, "embedding_version")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	generation, ok := text(rawScope, "vector_generation")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	watermark, ok := text(rawScope, "snapshot_watermark")
	if !ok || watermark != s.adapter.snapshotWatermark {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	wantDigest, ok := text(rawScope, "image_digest")
	if !ok || doc.ID != pointID(knowledgedriver.ChunkKey{TenantID: tenant, KnowledgeID: knowledgeID, KnowledgeVersion: version, ChunkID: chunkID}) {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	metadata := make(map[string]string, len(doc.Metadata)-1)
	for key, value := range doc.Metadata {
		if key == sdkScopeMetadataKey {
			continue
		}
		stringValue, ok := value.(string)
		if !ok {
			return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
		}
		metadata[key] = stringValue
	}
	image := knowledgedriver.ChunkImage{Key: knowledgedriver.ChunkKey{TenantID: tenant, KnowledgeID: knowledgeID, KnowledgeVersion: version, ChunkID: chunkID}, Revision: revision, Operation: knowledgedriver.Operation(operation), SourceDigest: sourceDigest, ContentDigest: contentDigest, MetadataDigest: metadataDigest, EmbeddingProfileID: profile, EmbeddingVersion: embeddingVersion, VectorGeneration: generation, Content: doc.Content, Metadata: metadata, Vector: float32Vector(vector)}
	if image.Operation == knowledgedriver.OperationDelete {
		image.Content, image.Metadata, image.Vector = "", nil, nil
	}
	gotDigest, err := knowledgedriver.ImageDigest(image)
	if err != nil || gotDigest != wantDigest || !s.matches(image) {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	return image, nil
}

func (s *sdkReadOnlyVectorStore) matches(image knowledgedriver.ChunkImage) bool {
	return image.Key.TenantID == s.scope.TenantID && image.Key.KnowledgeID == s.scope.KnowledgeID &&
		image.Key.KnowledgeVersion == s.scope.KnowledgeVersion && image.EmbeddingProfileID == s.scope.EmbedderProfile &&
		image.EmbeddingVersion == s.scope.EmbedderVersion && image.VectorGeneration == s.scope.VectorGeneration
}

func (s *sdkReadOnlyVectorStore) validateFilter(filter *vectorstore.SearchFilter) error {
	return (&ReadOnlyVectorStore{scope: s.scope}).validateFilter(filter)
}

func (s *sdkReadOnlyVectorStore) matchesAdditionalFilter(image knowledgedriver.ChunkImage, filter *vectorstore.SearchFilter) bool {
	return (&ReadOnlyVectorStore{scope: s.scope}).matchesAdditionalFilter(image, filter)
}

func (s *sdkReadOnlyVectorStore) withStore(ctx context.Context, use func(vectorstore.VectorStore) error) error {
	if ctx == nil || use == nil || s.adapter.endpoint == nil {
		return runtime.ErrInvariantViolation
	}
	token := ""
	if s.adapter.tokens != nil {
		value, err := s.adapter.tokens.Token(ctx)
		if err != nil || strings.TrimSpace(value) == "" {
			return runtime.ErrBackendUnavailable
		}
		token = value
	}
	entry, err := s.acquireStore(ctx, token)
	if err != nil {
		return err
	}
	defer s.releaseStore(entry)
	return use(entry.store)
}

func (s *sdkReadOnlyVectorStore) acquireStore(ctx context.Context, token string) (*leasedSDKStore, error) {
	tokenFingerprint := sha256.Sum256([]byte(token))
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, runtime.ErrCapabilityUnsupported
	}
	if s.active != nil && s.active.tokenFingerprint == tokenFingerprint && !s.active.retired {
		s.active.uses++
		entry := s.active
		s.mu.Unlock()
		return entry, nil
	}
	s.mu.Unlock()

	store, err := s.newStore(ctx, sdkStoreConfig{host: s.adapter.endpoint.Hostname(), port: s.adapter.grpcPort,
		tls: s.adapter.endpoint.Scheme == "https", apiKey: token, collection: s.adapter.collection, dimension: s.adapter.vectorSize})
	if err != nil {
		return nil, fmt.Errorf("initialize official Qdrant vector store: %w", err)
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = store.Close()
		return nil, runtime.ErrCapabilityUnsupported
	}
	if s.active != nil && s.active.tokenFingerprint == tokenFingerprint && !s.active.retired {
		s.active.uses++
		entry := s.active
		s.mu.Unlock()
		_ = store.Close()
		return entry, nil
	}
	old := s.active
	if old != nil {
		old.retired = true
	}
	entry := &leasedSDKStore{tokenFingerprint: tokenFingerprint, store: store, uses: 1}
	s.active = entry
	closeOld := old != nil && old.uses == 0
	s.mu.Unlock()
	if closeOld {
		_ = old.store.Close()
	}
	return entry, nil
}

func (s *sdkReadOnlyVectorStore) releaseStore(entry *leasedSDKStore) {
	if entry == nil {
		return
	}
	s.mu.Lock()
	entry.uses--
	closeNow := entry.retired && entry.uses == 0
	s.mu.Unlock()
	if closeNow {
		_ = entry.store.Close()
	}
}

var _ vectorstore.VectorStore = (*sdkReadOnlyVectorStore)(nil)
