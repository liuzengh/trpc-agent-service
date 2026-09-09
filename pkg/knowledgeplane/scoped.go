// Package knowledgeplane provides tenant-scoped adapters for shared vector
// infrastructure. Tenant configuration never receives raw Qdrant credentials
// or a collection handle.
package knowledgeplane

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

const (
	metadataTenantID   = "_tsa_tenant_id"
	metadataAgentAppID = "_tsa_agent_app_id"
	metadataDocumentID = "_tsa_document_id"
)

var ErrScopeViolation = errors.New("knowledge vector operation violates tenant scope")

// ScopedStore implements the framework VectorStore interface while hiding the
// physical shared-collection identity and forcing scope on every operation.
type ScopedStore struct {
	tenantID   string
	agentAppID string
	backend    vectorstore.VectorStore
}

func NewScopedStore(tenantID, agentAppID string, backend vectorstore.VectorStore) (*ScopedStore, error) {
	if err := tenant.ValidateTenantID(tenantID); err != nil {
		return nil, fmt.Errorf("%w: tenant", ErrScopeViolation)
	}
	if err := tenant.ValidateAgentAppName(agentAppID); err != nil || backend == nil {
		return nil, fmt.Errorf("%w: agent app or backend", ErrScopeViolation)
	}
	return &ScopedStore{tenantID: tenantID, agentAppID: agentAppID, backend: backend}, nil
}

func (s *ScopedStore) Add(ctx context.Context, doc *document.Document, embedding []float64) error {
	physical, err := s.physicalDocument(doc)
	if err != nil {
		return err
	}
	return s.backend.Add(ctx, physical, append([]float64(nil), embedding...))
}

func (s *ScopedStore) Get(ctx context.Context, id string) (*document.Document, []float64, error) {
	if id == "" {
		return nil, nil, ErrScopeViolation
	}
	doc, embedding, err := s.backend.Get(ctx, s.physicalID(id))
	if err != nil {
		return nil, nil, err
	}
	logical, err := s.logicalDocument(doc)
	if err != nil {
		return nil, nil, err
	}
	return logical, append([]float64(nil), embedding...), nil
}

func (s *ScopedStore) Update(ctx context.Context, doc *document.Document, embedding []float64) error {
	physical, err := s.physicalDocument(doc)
	if err != nil {
		return err
	}
	return s.backend.Update(ctx, physical, append([]float64(nil), embedding...))
}

func (s *ScopedStore) Delete(ctx context.Context, id string) error {
	if id == "" {
		return ErrScopeViolation
	}
	return s.backend.Delete(ctx, s.physicalID(id))
}

func (s *ScopedStore) Search(ctx context.Context, query *vectorstore.SearchQuery) (*vectorstore.SearchResult, error) {
	if query == nil {
		return nil, ErrScopeViolation
	}
	cloned := *query
	cloned.Vector = append([]float64(nil), query.Vector...)
	filter, err := s.scopedFilter(query.Filter)
	if err != nil {
		return nil, err
	}
	cloned.Filter = filter
	result, err := s.backend.Search(ctx, &cloned)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, ErrScopeViolation
	}
	out := &vectorstore.SearchResult{Results: make([]*vectorstore.ScoredDocument, 0, len(result.Results))}
	for _, item := range result.Results {
		if item == nil {
			return nil, ErrScopeViolation
		}
		logical, err := s.logicalDocument(item.Document)
		if err != nil {
			return nil, err
		}
		out.Results = append(out.Results, &vectorstore.ScoredDocument{Document: logical, Score: item.Score})
	}
	return out, nil
}

func (s *ScopedStore) DeleteByFilter(ctx context.Context, opts ...vectorstore.DeleteOption) error {
	config := vectorstore.ApplyDeleteOptions(opts...)
	filter, err := s.mergeMetadata(config.Filter)
	if err != nil {
		return err
	}
	ids := make([]string, len(config.DocumentIDs))
	for i, id := range config.DocumentIDs {
		if id == "" {
			return ErrScopeViolation
		}
		ids[i] = s.physicalID(id)
	}
	return s.backend.DeleteByFilter(ctx,
		vectorstore.WithDeleteDocumentIDs(ids), vectorstore.WithDeleteFilter(filter),
		vectorstore.WithDeleteAll(config.DeleteAll))
}

func (s *ScopedStore) UpdateByFilter(ctx context.Context, opts ...vectorstore.UpdateByFilterOption) (int64, error) {
	config, err := vectorstore.ApplyUpdateByFilterOptions(opts...)
	if err != nil {
		return 0, err
	}
	// Universal conditions cannot be safely amended by the framework option
	// surface. Require explicit logical IDs so the physical hash itself is the
	// scope fence; callers can use repeated updates for arbitrary selections.
	if len(config.DocumentIDs) == 0 {
		return 0, ErrScopeViolation
	}
	ids := make([]string, len(config.DocumentIDs))
	for i, id := range config.DocumentIDs {
		if id == "" {
			return 0, ErrScopeViolation
		}
		ids[i] = s.physicalID(id)
	}
	return s.backend.UpdateByFilter(ctx,
		vectorstore.WithUpdateByFilterDocumentIDs(ids),
		vectorstore.WithUpdateByFilterCondition(config.FilterCondition),
		vectorstore.WithUpdateByFilterUpdates(config.Updates))
}

func (s *ScopedStore) Count(ctx context.Context, opts ...vectorstore.CountOption) (int, error) {
	config := vectorstore.ApplyCountOptions(opts...)
	filter, err := s.mergeMetadata(config.Filter)
	if err != nil {
		return 0, err
	}
	return s.backend.Count(ctx, vectorstore.WithCountFilter(filter))
}

func (s *ScopedStore) GetMetadata(ctx context.Context, opts ...vectorstore.GetMetadataOption) (map[string]vectorstore.DocumentMetadata, error) {
	config, err := vectorstore.ApplyGetMetadataOptions(opts...)
	if err != nil {
		return nil, err
	}
	filter, err := s.mergeMetadata(config.Filter)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(config.IDs))
	for i, id := range config.IDs {
		ids[i] = s.physicalID(id)
	}
	delegated := []vectorstore.GetMetadataOption{
		vectorstore.WithGetMetadataIDs(ids), vectorstore.WithGetMetadataFilter(filter),
	}
	if config.Limit > 0 {
		delegated = append(delegated, vectorstore.WithGetMetadataLimit(config.Limit))
	}
	if config.Offset >= 0 {
		delegated = append(delegated, vectorstore.WithGetMetadataOffset(config.Offset))
	}
	values, err := s.backend.GetMetadata(ctx, delegated...)
	if err != nil {
		return nil, err
	}
	out := make(map[string]vectorstore.DocumentMetadata, len(values))
	for _, value := range values {
		logicalID, ok := value.Metadata[metadataDocumentID].(string)
		if !ok || logicalID == "" || value.Metadata[metadataTenantID] != s.tenantID || value.Metadata[metadataAgentAppID] != s.agentAppID {
			return nil, ErrScopeViolation
		}
		clean := cloneMetadata(value.Metadata)
		deleteReserved(clean)
		out[logicalID] = vectorstore.DocumentMetadata{Metadata: clean}
	}
	return out, nil
}

func (s *ScopedStore) Close() error { return s.backend.Close() }

func (s *ScopedStore) physicalDocument(doc *document.Document) (*document.Document, error) {
	if doc == nil || doc.ID == "" {
		return nil, ErrScopeViolation
	}
	physical := doc.Clone()
	physical.ID = s.physicalID(doc.ID)
	metadata, err := s.mergeMetadata(doc.Metadata)
	if err != nil {
		return nil, err
	}
	metadata[metadataDocumentID] = doc.ID
	physical.Metadata = metadata
	return physical, nil
}

func (s *ScopedStore) logicalDocument(doc *document.Document) (*document.Document, error) {
	if doc == nil || doc.Metadata == nil || doc.Metadata[metadataTenantID] != s.tenantID || doc.Metadata[metadataAgentAppID] != s.agentAppID {
		return nil, ErrScopeViolation
	}
	logicalID, ok := doc.Metadata[metadataDocumentID].(string)
	if !ok || logicalID == "" || doc.ID != s.physicalID(logicalID) {
		return nil, ErrScopeViolation
	}
	logical := doc.Clone()
	logical.ID = logicalID
	deleteReserved(logical.Metadata)
	return logical, nil
}

func (s *ScopedStore) scopedFilter(filter *vectorstore.SearchFilter) (*vectorstore.SearchFilter, error) {
	result := &vectorstore.SearchFilter{}
	if filter != nil {
		result.IDs = make([]string, len(filter.IDs))
		for i, id := range filter.IDs {
			result.IDs[i] = s.physicalID(id)
		}
		result.FilterCondition = filter.FilterCondition
		result.Metadata = filter.Metadata
	}
	metadata, err := s.mergeMetadata(result.Metadata)
	if err != nil {
		return nil, err
	}
	result.Metadata = metadata
	return result, nil
}

func (s *ScopedStore) mergeMetadata(input map[string]any) (map[string]any, error) {
	result := cloneMetadata(input)
	for key := range result {
		if key == metadataTenantID || key == metadataAgentAppID || key == metadataDocumentID {
			return nil, ErrScopeViolation
		}
	}
	result[metadataTenantID] = s.tenantID
	result[metadataAgentAppID] = s.agentAppID
	return result, nil
}

func (s *ScopedStore) physicalID(logical string) string {
	digest := sha256.Sum256([]byte(s.tenantID + "\x00" + s.agentAppID + "\x00" + logical))
	return hex.EncodeToString(digest[:])
}

func cloneMetadata(input map[string]any) map[string]any {
	result := make(map[string]any, len(input)+3)
	for key, value := range input {
		result[key] = value
	}
	return result
}

func deleteReserved(metadata map[string]any) {
	delete(metadata, metadataTenantID)
	delete(metadata, metadataAgentAppID)
	delete(metadata, metadataDocumentID)
}

var _ vectorstore.VectorStore = (*ScopedStore)(nil)
