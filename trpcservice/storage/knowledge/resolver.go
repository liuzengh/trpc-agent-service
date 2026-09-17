package knowledge

import (
	"context"
	"fmt"
	"sort"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	upstream "trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
)

// Resolver materializes framework Knowledge instances for an Agent. Each ref
// is wrapped with a fixed tenant/knowledge/version scope; production Factory
// additionally pins the manifest, embedding profile and collection generation.
type Resolver struct {
	// Backend is retained as a compatibility seam for deterministic tests and
	// non-Qdrant adapters. Production uses Factory so each Knowledge ref gets
	// its own manifest-pinned framework retrieval chain.
	Backend upstream.Knowledge
	Factory ScopedKnowledgeFactory
	Limits  RetrievalLimits
}

// ResolveKnowledge returns the framework Knowledge for an Agent's knowledge
// refs. It returns (nil, nil) for an Agent with no knowledge, a single
// TenantKnowledge for one ref, and a Composite that fans out across multiple
// refs otherwise.
func (r Resolver) ResolveKnowledge(ctx context.Context, tenantID, agentAppID string, refs []profile.VersionedRef, configVersion int64) (upstream.Knowledge, error) {
	if r.Backend == nil && r.Factory == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	if len(refs) == 0 {
		return nil, nil
	}
	if tenantID == "" || agentAppID == "" || configVersion < 1 {
		return nil, runtime.ErrInvariantViolation
	}
	children := make([]upstream.Knowledge, 0, len(refs))
	for _, ref := range refs {
		backend := r.Backend
		if r.Factory != nil {
			var err error
			backend, err = r.Factory.KnowledgeFor(ctx, tenantID, ref, configVersion)
			if err != nil {
				return nil, err
			}
		}
		if backend == nil {
			return nil, runtime.ErrCapabilityUnsupported
		}
		children = append(children, TenantKnowledge{TenantID: tenantID, AgentAppID: agentAppID, KnowledgeRef: ref,
			ConfigVersion: configVersion, Backend: backend, Limits: r.Limits})
	}
	if len(children) == 1 {
		return children[0], nil
	}
	return Composite(children), nil
}

// Composite fans one search out across multiple tenant-scoped knowledge bases
// and merges the results by descending score. It preserves the upstream
// Knowledge contract so the framework's knowledge search tool needs no change.
type Composite []upstream.Knowledge

func (c Composite) Search(ctx context.Context, request *upstream.SearchRequest) (*upstream.SearchResult, error) {
	if len(c) == 0 || request == nil {
		return nil, runtime.ErrInvariantViolation
	}
	best := make(map[string]*upstream.Result)
	for _, child := range c {
		if child == nil {
			continue
		}
		result, err := child.Search(ctx, request)
		if err != nil {
			return nil, err
		}
		if result == nil {
			continue
		}
		candidates := result.Documents
		if len(candidates) == 0 && result.Document != nil {
			candidates = []*upstream.Result{{Document: result.Document, Score: result.Score}}
		}
		for _, item := range candidates {
			if item == nil || item.Document == nil {
				continue
			}
			id := compositeDocumentKey(item.Document)
			if existing, ok := best[id]; !ok || item.Score > existing.Score {
				best[id] = item
			}
		}
	}
	merged := make([]*upstream.Result, 0, len(best))
	for _, item := range best {
		merged = append(merged, item)
	}
	if len(merged) == 0 {
		return &upstream.SearchResult{}, nil
	}
	sort.SliceStable(merged, func(i, j int) bool { return merged[i].Score > merged[j].Score })
	limit := request.MaxResults
	if limit <= 0 || limit > len(merged) {
		limit = len(merged)
	}
	merged = merged[:limit]
	result := &upstream.SearchResult{Documents: merged, Document: merged[0].Document, Score: merged[0].Score}
	if merged[0].Document != nil {
		result.Text = merged[0].Document.Content
	}
	return result, nil
}

func compositeDocumentKey(value *document.Document) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value.Metadata[metadataTenantID]) + "\x00" + fmt.Sprint(value.Metadata[metadataKnowledgeID]) +
		"\x00" + fmt.Sprint(value.Metadata[metadataKnowledgeVersion]) + "\x00" + value.ID
}

var _ upstream.Knowledge = Composite{}
