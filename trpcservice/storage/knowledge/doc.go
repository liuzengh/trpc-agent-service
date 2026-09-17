// Package knowledge hosts tenant-filtered adapters over the public
// tRPC-Agent-Go Knowledge contract.
package knowledge

import (
	"context"
	"fmt"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	upstream "trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
)

const (
	metadataTenantID         = "tenant_id"
	metadataKnowledgeID      = "knowledge_id"
	metadataKnowledgeVersion = "knowledge_version"
)

type RetrievalLimits struct {
	MaxQueryBytes   int
	MaxResults      int
	MaxResultBytes  int
	AllowedMetadata map[string]bool
}

// TenantKnowledge enforces the trusted tenant and fixed knowledge version on
// both sides of an upstream search. A backend that cannot honor metadata
// filters is caught by the result-scope validation and fails closed.
type TenantKnowledge struct {
	TenantID      string
	AgentAppID    string
	KnowledgeRef  profile.VersionedRef
	ConfigVersion int64
	Backend       upstream.Knowledge
	Limits        RetrievalLimits
}

func (k TenantKnowledge) Search(ctx context.Context, request *upstream.SearchRequest) (*upstream.SearchResult, error) {
	if k.Backend == nil || k.TenantID == "" || k.AgentAppID == "" ||
		k.KnowledgeRef.ID == "" || k.KnowledgeRef.Version < 1 || k.ConfigVersion < 1 || request == nil {
		return nil, runtime.ErrInvariantViolation
	}
	if k.Limits.MaxQueryBytes > 0 && len(request.Query) > k.Limits.MaxQueryBytes {
		return nil, fmt.Errorf("%w: knowledge query exceeds limit", runtime.ErrInvariantViolation)
	}
	cloned := *request
	cloned.History = append([]upstream.ConversationMessage(nil), request.History...)
	cloned.SearchFilter = cloneFilter(request.SearchFilter)
	if cloned.SearchFilter == nil {
		cloned.SearchFilter = &upstream.SearchFilter{}
	}
	if cloned.SearchFilter.Metadata == nil {
		cloned.SearchFilter.Metadata = make(map[string]any)
	}
	required := map[string]any{
		metadataTenantID:         k.TenantID,
		metadataKnowledgeID:      k.KnowledgeRef.ID,
		metadataKnowledgeVersion: k.KnowledgeRef.Version,
	}
	for key, value := range required {
		if existing, ok := cloned.SearchFilter.Metadata[key]; ok && fmt.Sprint(existing) != fmt.Sprint(value) {
			return nil, runtime.ErrTenantScope
		}
		cloned.SearchFilter.Metadata[key] = value
	}
	if k.Limits.MaxResults > 0 && (cloned.MaxResults <= 0 || cloned.MaxResults > k.Limits.MaxResults) {
		cloned.MaxResults = k.Limits.MaxResults
	}
	result, err := k.Backend.Search(ctx, &cloned)
	if err != nil || result == nil {
		return result, err
	}
	if err := k.validateResult(result); err != nil {
		return nil, err
	}
	return k.sanitizeResult(result)
}

func cloneFilter(filter *upstream.SearchFilter) *upstream.SearchFilter {
	if filter == nil {
		return nil
	}
	cloned := *filter
	cloned.DocumentIDs = append([]string(nil), filter.DocumentIDs...)
	cloned.Metadata = make(map[string]any, len(filter.Metadata))
	for key, value := range filter.Metadata {
		cloned.Metadata[key] = value
	}
	return &cloned
}

func (k TenantKnowledge) validateResult(result *upstream.SearchResult) error {
	if result.Document != nil {
		if err := k.validateDocument(result.Document); err != nil {
			return err
		}
	}
	for _, item := range result.Documents {
		if item == nil || item.Document == nil {
			return runtime.ErrInvariantViolation
		}
		if err := k.validateDocument(item.Document); err != nil {
			return err
		}
	}
	return nil
}

func (k TenantKnowledge) validateDocument(value *document.Document) error {
	if value.Metadata == nil || fmt.Sprint(value.Metadata[metadataTenantID]) != k.TenantID ||
		fmt.Sprint(value.Metadata[metadataKnowledgeID]) != k.KnowledgeRef.ID ||
		fmt.Sprint(value.Metadata[metadataKnowledgeVersion]) != fmt.Sprint(k.KnowledgeRef.Version) {
		return runtime.ErrTenantScope
	}
	return nil
}

func (k TenantKnowledge) sanitizeResult(result *upstream.SearchResult) (*upstream.SearchResult, error) {
	cloned := *result
	cloned.Document = k.sanitizeDocument(result.Document)
	cloned.Documents = make([]*upstream.Result, 0, len(result.Documents))
	total := len(cloned.Text)
	if cloned.Document != nil {
		total += len(cloned.Document.Content)
	}
	for _, item := range result.Documents {
		copyItem := *item
		copyItem.Document = k.sanitizeDocument(item.Document)
		total += len(copyItem.Document.Content)
		cloned.Documents = append(cloned.Documents, &copyItem)
	}
	if k.Limits.MaxResultBytes > 0 && total > k.Limits.MaxResultBytes {
		return nil, fmt.Errorf("%w: knowledge result exceeds limit", runtime.ErrInvariantViolation)
	}
	return &cloned, nil
}

func (k TenantKnowledge) sanitizeDocument(value *document.Document) *document.Document {
	if value == nil {
		return nil
	}
	cloned := value.Clone()
	cloned.EmbeddingText = ""
	metadata := make(map[string]any)
	for key, item := range cloned.Metadata {
		if strings.HasPrefix(key, "_") || key == "acl" || key == "internal_path" {
			continue
		}
		if key == metadataTenantID || key == metadataKnowledgeID || key == metadataKnowledgeVersion || k.Limits.AllowedMetadata[key] {
			metadata[key] = item
		}
	}
	cloned.Metadata = metadata
	return cloned
}

var _ upstream.Knowledge = TenantKnowledge{}
