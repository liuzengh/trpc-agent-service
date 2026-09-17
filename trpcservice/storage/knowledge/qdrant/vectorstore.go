package qdrant

import (
	"context"
	"fmt"
	"math"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

// RuntimeScope fixes the immutable Knowledge and embedding facts used by one
// framework retrieval instance. It is deliberately narrower than Adapter: an
// Agent may search, but cannot mutate or enumerate the shared collection.
type RuntimeScope struct {
	TenantID         string
	KnowledgeID      string
	KnowledgeVersion int64
	EmbedderProfile  string
	EmbedderVersion  int64
	VectorGeneration string
}

func (s RuntimeScope) valid() bool {
	return s.TenantID != "" && s.KnowledgeID != "" && s.KnowledgeVersion > 0 &&
		s.EmbedderProfile != "" && s.EmbedderVersion > 0 && s.VectorGeneration != ""
}

// ReadOnlyVectorStore adapts the migration-governed Qdrant envelope to the
// public trpc-agent-go VectorStore contract. It never writes via the runtime
// path: durable ingestion and backend migration remain the sole mutation
// authorities for this collection.
type ReadOnlyVectorStore struct {
	adapter *Adapter
	scope   RuntimeScope
}

func NewReadOnlyVectorStore(adapter *Adapter, scope RuntimeScope) (vectorstore.VectorStore, error) {
	if adapter == nil || adapter.VectorSize() < 1 || !scope.valid() {
		return nil, runtime.ErrInvariantViolation
	}
	if adapter.RuntimeEngine() == "sdk" {
		return newSDKReadOnlyVectorStore(adapter, scope, newOfficialSDKStore)
	}
	if adapter.RuntimeEngine() != "native" {
		return nil, runtime.ErrCapabilityUnsupported
	}
	return &ReadOnlyVectorStore{adapter: adapter, scope: scope}, nil
}

func (s *ReadOnlyVectorStore) Add(context.Context, *document.Document, []float64) error {
	return runtime.ErrCapabilityUnsupported
}

func (s *ReadOnlyVectorStore) Update(context.Context, *document.Document, []float64) error {
	return runtime.ErrCapabilityUnsupported
}

func (s *ReadOnlyVectorStore) Delete(context.Context, string) error {
	return runtime.ErrCapabilityUnsupported
}

func (s *ReadOnlyVectorStore) DeleteByFilter(context.Context, ...vectorstore.DeleteOption) error {
	return runtime.ErrCapabilityUnsupported
}

func (s *ReadOnlyVectorStore) UpdateByFilter(context.Context, ...vectorstore.UpdateByFilterOption) (int64, error) {
	return 0, runtime.ErrCapabilityUnsupported
}

func (s *ReadOnlyVectorStore) Count(context.Context, ...vectorstore.CountOption) (int, error) {
	return 0, runtime.ErrCapabilityUnsupported
}

func (s *ReadOnlyVectorStore) GetMetadata(context.Context, ...vectorstore.GetMetadataOption) (map[string]vectorstore.DocumentMetadata, error) {
	return nil, runtime.ErrCapabilityUnsupported
}

func (s *ReadOnlyVectorStore) Get(ctx context.Context, id string) (*document.Document, []float64, error) {
	if s == nil || s.adapter == nil || !s.scope.valid() || id == "" {
		return nil, nil, runtime.ErrInvariantViolation
	}
	image, err := s.adapter.LoadChunk(ctx, knowledgedriver.ChunkKey{TenantID: s.scope.TenantID,
		KnowledgeID: s.scope.KnowledgeID, KnowledgeVersion: s.scope.KnowledgeVersion, ChunkID: id})
	if err != nil {
		return nil, nil, err
	}
	if !s.matches(image) || image.Operation == knowledgedriver.OperationDelete {
		return nil, nil, runtime.ErrNotFound
	}
	return documentFor(image), float64Vector(image.Vector), nil
}

func (s *ReadOnlyVectorStore) Search(ctx context.Context, query *vectorstore.SearchQuery) (*vectorstore.SearchResult, error) {
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
	// Qdrant always receives the mandatory immutable scope. When a caller adds
	// a supported narrowing filter, fetch the bounded candidate window and
	// apply that filter only after re-validating every adapter envelope. This
	// avoids translating an open-ended framework filter language into an
	// under-specified Qdrant query while keeping a caller unable to broaden the
	// collection scope.
	fetchLimit := limit
	if s.hasAdditionalFilter(query.Filter) {
		fetchLimit = 64
	}
	points, err := s.adapter.searchVector(ctx, s.request(), float32Vector(query.Vector), fetchLimit, query.MinScore)
	if err != nil {
		return nil, err
	}
	result := &vectorstore.SearchResult{Results: make([]*vectorstore.ScoredDocument, 0, len(points))}
	seen := make(map[knowledgedriver.ChunkKey]struct{}, len(points))
	for _, point := range points {
		image, decodeErr := s.adapter.checkedSearchImage(s.request(), point)
		if decodeErr != nil {
			return nil, decodeErr
		}
		if !s.matches(image) {
			return nil, runtime.ErrTenantScope
		}
		if image.Operation == knowledgedriver.OperationDelete {
			continue
		}
		if !s.matchesAdditionalFilter(image, query.Filter) {
			continue
		}
		if _, exists := seen[image.Key]; exists {
			return nil, runtime.ErrInvariantViolation
		}
		seen[image.Key] = struct{}{}
		result.Results = append(result.Results, &vectorstore.ScoredDocument{Document: documentFor(image), Score: point.Score})
		if len(result.Results) == limit {
			break
		}
	}
	return result, nil
}

func (s *ReadOnlyVectorStore) Close() error { return nil }

func (s *ReadOnlyVectorStore) request() knowledgedriver.SearchRequest {
	return knowledgedriver.SearchRequest{TenantID: s.scope.TenantID, KnowledgeID: s.scope.KnowledgeID, KnowledgeVersion: s.scope.KnowledgeVersion}
}

func (s *ReadOnlyVectorStore) matches(image knowledgedriver.ChunkImage) bool {
	return image.Key.TenantID == s.scope.TenantID && image.Key.KnowledgeID == s.scope.KnowledgeID &&
		image.Key.KnowledgeVersion == s.scope.KnowledgeVersion && image.EmbeddingProfileID == s.scope.EmbedderProfile &&
		image.EmbeddingVersion == s.scope.EmbedderVersion && image.VectorGeneration == s.scope.VectorGeneration
}

func (s *ReadOnlyVectorStore) validateFilter(filter *vectorstore.SearchFilter) error {
	if filter == nil || len(filter.Metadata) < 3 {
		return runtime.ErrTenantScope
	}
	if filter.FilterCondition != nil {
		return runtime.ErrCapabilityUnsupported
	}
	want := map[string]any{"tenant_id": s.scope.TenantID, "knowledge_id": s.scope.KnowledgeID, "knowledge_version": s.scope.KnowledgeVersion}
	for key, value := range want {
		if fmt.Sprint(filter.Metadata[key]) != fmt.Sprint(value) {
			return runtime.ErrTenantScope
		}
	}
	return nil
}

func (s *ReadOnlyVectorStore) hasAdditionalFilter(filter *vectorstore.SearchFilter) bool {
	return filter != nil && (len(filter.IDs) != 0 || len(filter.Metadata) > 3)
}

func (s *ReadOnlyVectorStore) matchesAdditionalFilter(image knowledgedriver.ChunkImage, filter *vectorstore.SearchFilter) bool {
	if filter == nil {
		return false
	}
	if len(filter.IDs) != 0 {
		matched := false
		for _, id := range filter.IDs {
			if id == image.Key.ChunkID {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	for key, want := range filter.Metadata {
		if key == "tenant_id" || key == "knowledge_id" || key == "knowledge_version" {
			continue
		}
		if got, exists := image.Metadata[key]; !exists || got != fmt.Sprint(want) {
			return false
		}
	}
	return true
}

func finiteVector(values []float64) bool {
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) || value > math.MaxFloat32 || value < -math.MaxFloat32 {
			return false
		}
	}
	return true
}

func float32Vector(values []float64) []float32 {
	result := make([]float32, len(values))
	for index, value := range values {
		result[index] = float32(value)
	}
	return result
}

func float64Vector(values []float32) []float64 {
	result := make([]float64, len(values))
	for index, value := range values {
		result[index] = float64(value)
	}
	return result
}

func documentFor(image knowledgedriver.ChunkImage) *document.Document {
	metadata := make(map[string]any, len(image.Metadata)+3)
	for key, value := range image.Metadata {
		metadata[key] = value
	}
	metadata["tenant_id"] = image.Key.TenantID
	metadata["knowledge_id"] = image.Key.KnowledgeID
	metadata["knowledge_version"] = image.Key.KnowledgeVersion
	name, _ := metadata["title"].(string)
	return &document.Document{ID: image.Key.ChunkID, Name: name, Content: image.Content, Metadata: metadata}
}

var _ vectorstore.VectorStore = (*ReadOnlyVectorStore)(nil)
