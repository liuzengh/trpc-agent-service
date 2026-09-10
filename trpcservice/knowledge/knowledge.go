// Package knowledge provides tenant-scoped knowledge retrieval contracts.
package knowledge

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	frameworkknowledge "trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/searchfilter"
)

const (
	// MetadataTenantID is the required Qdrant payload metadata field.
	MetadataTenantID = "tenant_id"
	// MetadataAppID is the required Qdrant payload metadata field.
	MetadataAppID = "app_id"
	// MetadataKnowledgeBaseID is the required Qdrant payload metadata field.
	MetadataKnowledgeBaseID = "knowledge_base_id"
	// MetadataDocumentID is the required Qdrant payload metadata field.
	MetadataDocumentID = "document_id"
	// MetadataDocumentVersion is the required Qdrant payload metadata field.
	MetadataDocumentVersion = "document_version"
	// MetadataChunkID is the required Qdrant payload metadata field.
	MetadataChunkID = "chunk_id"
	// MetadataIndexGeneration is the required Qdrant payload metadata field.
	MetadataIndexGeneration = "index_generation"

	// scopedSearchInitialOverfetch bounds the first candidate batch while
	// leaving room for SQL authorization to reject vector hits.
	scopedSearchInitialOverfetch = 4
	// scopedSearchMaxOverfetch bounds the extra candidates requested when a
	// provider does not expose a paging cursor through the framework API.
	scopedSearchMaxOverfetch = 256
)

// ChunkRef identifies one derived vector chunk under its authoritative scope.
type ChunkRef struct {
	Scope           tenant.Scope
	ConfigVersion   string
	KnowledgeBaseID string
	DocumentID      string
	DocumentVersion string
	ChunkID         string
	IndexGeneration string
}

// Validate checks that ChunkRef contains a complete authoritative identity.
func (r ChunkRef) Validate() error {
	if err := r.Scope.Validate(); err != nil {
		return err
	}
	if r.ConfigVersion == "" {
		return errors.New("config version is required")
	}
	if r.KnowledgeBaseID == "" {
		return errors.New("knowledge base id is required")
	}
	if r.DocumentID == "" {
		return errors.New("document id is required")
	}
	if r.DocumentVersion == "" {
		return errors.New("document version is required")
	}
	if r.ChunkID == "" {
		return errors.New("chunk id is required")
	}
	if r.IndexGeneration == "" {
		return errors.New("index generation is required")
	}
	return nil
}

// Catalog authorizes a derived chunk against platform SQL metadata.
// Implementations must return false for deleted, pending, unbound, or
// cross-scope chunks.
type Catalog interface {
	AvailableKnowledgeChunk(context.Context, ChunkRef) (bool, error)
}

// ScopedKnowledge adds mandatory platform filtering and SQL authorization to a
// framework Knowledge implementation. It never trusts a vector result alone.
type ScopedKnowledge struct {
	inner   frameworkknowledge.Knowledge
	catalog Catalog
	scope   tenant.Scope
	config  string
	baseIDs []string
}

// NewScopedKnowledge creates a Knowledge view limited to the supplied base IDs.
func NewScopedKnowledge(
	inner frameworkknowledge.Knowledge,
	catalog Catalog,
	scope tenant.Scope,
	configVersion string,
	knowledgeBaseIDs []string,
) (*ScopedKnowledge, error) {
	if inner == nil {
		return nil, errors.New("knowledge implementation is required")
	}
	if catalog == nil {
		return nil, errors.New("knowledge catalog is required")
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if configVersion == "" {
		return nil, errors.New("config version is required")
	}
	baseIDs := uniqueNonEmpty(knowledgeBaseIDs)
	return &ScopedKnowledge{
		inner:   inner,
		catalog: catalog,
		scope:   scope,
		config:  configVersion,
		baseIDs: baseIDs,
	}, nil
}

// Search retrieves candidates for all authorized knowledge bases, then
// filters every result through the SQL authority.
func (k *ScopedKnowledge) Search(
	ctx context.Context,
	req *frameworkknowledge.SearchRequest,
) (*frameworkknowledge.SearchResult, error) {
	if k == nil || k.inner == nil || k.catalog == nil {
		return nil, errors.New("scoped knowledge is not initialized")
	}
	if req == nil {
		return nil, errors.New("search request is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(k.baseIDs) == 0 {
		return &frameworkknowledge.SearchResult{}, nil
	}

	searchLimit := scopedSearchLimit(req.MaxResults)
	var all []*frameworkknowledge.Result
	for {
		searchRequest := scopedRequest(req, k.scope, k.baseIDs)
		searchRequest.MaxResults = searchLimit
		result, err := k.inner.Search(ctx, searchRequest)
		if err != nil {
			return nil, fmt.Errorf("search scoped knowledge: %w", err)
		}
		all, err = k.authorizeCandidates(ctx, result)
		if err != nil {
			return nil, err
		}
		if req.MaxResults <= 0 || len(all) >= req.MaxResults ||
			result == nil || len(result.Documents) < searchLimit {
			break
		}
		nextLimit := nextScopedSearchLimit(searchLimit, req.MaxResults)
		if nextLimit == searchLimit {
			break
		}
		searchLimit = nextLimit
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].Score > all[j].Score })
	if req.MaxResults > 0 && len(all) > req.MaxResults {
		all = all[:req.MaxResults]
	}
	return searchResult(all), nil
}

func (k *ScopedKnowledge) authorizeCandidates(
	ctx context.Context,
	result *frameworkknowledge.SearchResult,
) ([]*frameworkknowledge.Result, error) {
	all := make([]*frameworkknowledge.Result, 0)
	if result == nil {
		return all, nil
	}
	for _, candidate := range result.Documents {
		if candidate == nil || candidate.Document == nil {
			continue
		}
		baseID := metadataString(candidate.Document.Metadata, MetadataKnowledgeBaseID)
		if !slices.Contains(k.baseIDs, baseID) {
			continue
		}
		ref, ok := chunkRef(k.scope, k.config, baseID, candidate.Document)
		if !ok {
			continue
		}
		available, err := k.catalog.AvailableKnowledgeChunk(ctx, ref)
		if err != nil {
			return nil, fmt.Errorf("authorize knowledge chunk: %w", err)
		}
		if available {
			all = append(all, candidate)
		}
	}
	return all, nil
}

func scopedSearchLimit(maxResults int) int {
	if maxResults <= 0 {
		return maxResults
	}
	extra := maxResults * (scopedSearchInitialOverfetch - 1)
	if extra < 0 || extra > scopedSearchMaxOverfetch {
		extra = scopedSearchMaxOverfetch
	}
	maxInt := int(^uint(0) >> 1)
	if maxResults > maxInt-extra {
		return maxInt
	}
	return maxResults + extra
}

func nextScopedSearchLimit(current, maxResults int) int {
	maxLimit := scopedSearchMaxLimit(maxResults)
	if current <= 0 || current >= maxLimit {
		return current
	}
	next := current * 2
	if next < current || next > maxLimit {
		return maxLimit
	}
	return next
}

func scopedSearchMaxLimit(maxResults int) int {
	if maxResults <= 0 {
		return maxResults
	}
	maxInt := int(^uint(0) >> 1)
	if maxResults > maxInt-scopedSearchMaxOverfetch {
		return maxInt
	}
	return maxResults + scopedSearchMaxOverfetch
}

func scopedRequest(
	req *frameworkknowledge.SearchRequest,
	scope tenant.Scope,
	baseIDs []string,
) *frameworkknowledge.SearchRequest {
	cloned := *req
	filter := &frameworkknowledge.SearchFilter{}
	if req.SearchFilter != nil {
		filter.DocumentIDs = slices.Clone(req.SearchFilter.DocumentIDs)
		filter.FilterCondition = req.SearchFilter.FilterCondition
		filter.Metadata = cloneMetadata(req.SearchFilter.Metadata)
	}
	if filter.Metadata == nil {
		filter.Metadata = make(map[string]any)
	}
	filter.Metadata[MetadataTenantID] = scope.TenantID
	filter.Metadata[MetadataAppID] = scope.AppID
	delete(filter.Metadata, MetadataKnowledgeBaseID)
	baseValues := make([]any, len(baseIDs))
	for index, baseID := range baseIDs {
		baseValues[index] = baseID
	}
	baseCondition := searchfilter.In("metadata."+MetadataKnowledgeBaseID, baseValues...)
	if filter.FilterCondition == nil {
		filter.FilterCondition = baseCondition
	} else {
		filter.FilterCondition = searchfilter.And(filter.FilterCondition, baseCondition)
	}
	cloned.SearchFilter = filter
	return &cloned
}

func chunkRef(scope tenant.Scope, configVersion, baseID string, doc *document.Document) (ChunkRef, bool) {
	if doc == nil || doc.Metadata == nil {
		return ChunkRef{}, false
	}
	metadata := doc.Metadata
	if metadataString(metadata, MetadataTenantID) != scope.TenantID ||
		metadataString(metadata, MetadataAppID) != scope.AppID ||
		metadataString(metadata, MetadataKnowledgeBaseID) != baseID {
		return ChunkRef{}, false
	}
	ref := ChunkRef{
		Scope:           scope,
		ConfigVersion:   configVersion,
		KnowledgeBaseID: baseID,
		DocumentID:      metadataString(metadata, MetadataDocumentID),
		DocumentVersion: metadataString(metadata, MetadataDocumentVersion),
		ChunkID:         metadataString(metadata, MetadataChunkID),
		IndexGeneration: metadataString(metadata, MetadataIndexGeneration),
	}
	if err := ref.Validate(); err != nil {
		return ChunkRef{}, false
	}
	return ref, true
}

func searchResult(documents []*frameworkknowledge.Result) *frameworkknowledge.SearchResult {
	result := &frameworkknowledge.SearchResult{Documents: documents}
	if len(documents) == 0 {
		return result
	}
	result.Document = documents[0].Document
	result.Score = documents[0].Score
	result.Text = documents[0].Document.Content
	return result
}

func metadataString(metadata map[string]any, key string) string {
	value, ok := metadata[key]
	if !ok {
		return ""
	}
	text, _ := value.(string)
	return text
}

func cloneMetadata(input map[string]any) map[string]any {
	if len(input) == 0 {
		return nil
	}
	cloned := make(map[string]any, len(input))
	for key, value := range input {
		cloned[key] = value
	}
	return cloned
}

func uniqueNonEmpty(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
