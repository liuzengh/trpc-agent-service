// Package knowledgebase provides a tenant-scoped RAG service on top of the
// public tRPC-Agent-Go embedder and vector-store contracts.
package knowledgebase

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	servicelog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

const (
	maxDocumentBytes = 1 << 20
	chunkRunes       = 1600
	chunkOverlap     = 200
)

// Service owns one immutable tenant/app vector index and embedder.
type Service struct {
	tenantID, appID string
	store           vectorstore.VectorStore
	embedder        embedder.Embedder
	redactor        *servicelog.Redactor
	catalog         *sql.DB
	mirror          *Service
}

// IngestRequest is intentionally tenant-free. Scope is fixed when the service
// is built from a trusted published snapshot.
type IngestRequest struct {
	DocumentID string
	Name       string
	Content    string
	Metadata   map[string]any
}

// New creates a scoped knowledge service.
func New(tenantID, appID string, store vectorstore.VectorStore, embeddings embedder.Embedder, secrets ...string) (*Service, error) {
	if tenantID == "" || appID == "" || store == nil || embeddings == nil {
		return nil, errors.New("knowledge: tenant, app, vector store, and embedder are required")
	}
	return &Service{tenantID: tenantID, appID: appID, store: store, embedder: embeddings, redactor: servicelog.NewRedactor(nil, secrets)}, nil
}

// WithMigration configures the shared document catalog and an optional
// synchronous shadow index. It must be called before the service is published.
func (service *Service) WithMigration(catalog *sql.DB, mirror *Service) *Service {
	service.catalog, service.mirror = catalog, mirror
	return service
}

// Ingest chunks, embeds, and upserts one text document. Reserved isolation
// metadata is always overwritten with trusted service scope.
func (service *Service) Ingest(ctx context.Context, request IngestRequest) ([]string, error) {
	if ctx == nil || strings.TrimSpace(request.DocumentID) == "" || strings.TrimSpace(request.Content) == "" {
		return nil, errors.New("knowledge: document_id and content are required")
	}
	ctx = servicelog.WithRedactor(ctx, service.redactor)
	ids, err := service.ingestIndex(ctx, request)
	if err != nil {
		return nil, err
	}
	if service.mirror != nil {
		if _, err := service.mirror.ingestIndex(ctx, request); err != nil {
			return nil, errors.Join(errors.New("knowledge: mirror write failed"), err)
		}
	}
	if service.catalog != nil {
		metadata, err := json.Marshal(cloneMetadata(request.Metadata))
		if err != nil {
			return nil, errors.New("knowledge: metadata is invalid")
		}
		_, err = service.catalog.ExecContext(ctx, `INSERT INTO runtime_knowledge_documents (tenant_id,app_id,document_id,name,content,metadata_json,version,status) VALUES ($1,$2,$3,$4,$5,$6,1,'active') ON CONFLICT (tenant_id,app_id,document_id) DO UPDATE SET name=EXCLUDED.name,content=EXCLUDED.content,metadata_json=EXCLUDED.metadata_json,version=runtime_knowledge_documents.version+1,status='active',updated_at=NOW()`, service.tenantID, service.appID, request.DocumentID, request.Name, request.Content, metadata)
		if err != nil {
			return nil, errors.New("knowledge: document catalog write failed")
		}
	}
	return ids, nil
}

func (service *Service) ingestIndex(ctx context.Context, request IngestRequest) ([]string, error) {
	if len(request.DocumentID) > 256 || len(request.Content) > maxDocumentBytes {
		return nil, errors.New("knowledge: document exceeds size limits")
	}
	chunks := splitText(request.Content)
	ids := make([]string, 0, len(chunks))
	for index, content := range chunks {
		digest := sha256.Sum256([]byte(service.tenantID + "\x00" + service.appID + "\x00" + request.DocumentID + "\x00" + fmt.Sprint(index)))
		id := hex.EncodeToString(digest[:])
		metadata := cloneMetadata(request.Metadata)
		metadata["tenant_id"] = service.tenantID
		metadata["app_id"] = service.appID
		metadata["document_id"] = request.DocumentID
		metadata["chunk_index"] = index
		vector, err := service.embedder.GetEmbedding(ctx, content)
		if err != nil || len(vector) == 0 {
			return nil, errors.New("knowledge: embedding generation failed")
		}
		now := time.Now().UTC()
		doc := &document.Document{ID: id, Name: request.Name, Content: content, Metadata: metadata, CreatedAt: now, UpdatedAt: now}
		if err := service.store.Add(ctx, doc, vector); err != nil {
			return nil, errors.New("knowledge: vector index write failed")
		}
		ids = append(ids, id)
	}
	// Remove chunks left by an older, longer version only after every new
	// chunk has been durably upserted. A transient embedding/write error thus
	// never deletes the last complete version.
	indexed, err := service.store.GetMetadata(ctx, vectorstore.WithGetMetadataFilter(map[string]any{"tenant_id": service.tenantID, "app_id": service.appID, "document_id": request.DocumentID}))
	if err != nil {
		return nil, errors.New("knowledge: stale chunk reconciliation failed")
	}
	active := make(map[string]bool, len(ids))
	for _, id := range ids {
		active[id] = true
	}
	for id := range indexed {
		if !active[id] {
			if err := service.store.Delete(ctx, id); err != nil {
				return nil, errors.New("knowledge: stale chunk reconciliation failed")
			}
		}
	}
	return ids, nil
}

// Search implements knowledge.Knowledge and enforces tenant/app filters even
// if a caller supplies its own metadata criteria.
func (service *Service) Search(ctx context.Context, request *knowledge.SearchRequest) (*knowledge.SearchResult, error) {
	if ctx == nil || request == nil || strings.TrimSpace(request.Query) == "" {
		return nil, errors.New("knowledge: query is required")
	}
	ctx = servicelog.WithRedactor(ctx, service.redactor)
	vector, err := service.embedder.GetEmbedding(ctx, request.Query)
	if err != nil || len(vector) == 0 {
		return nil, errors.New("knowledge: query embedding failed")
	}
	limit := request.MaxResults
	if limit <= 0 || limit > 100 {
		limit = 10
	}
	metadata := map[string]any{"tenant_id": service.tenantID, "app_id": service.appID}
	filter := &vectorstore.SearchFilter{Metadata: metadata}
	if request.SearchFilter != nil {
		filter.IDs = append([]string(nil), request.SearchFilter.DocumentIDs...)
		filter.FilterCondition = request.SearchFilter.FilterCondition
		for key, value := range request.SearchFilter.Metadata {
			if key != "tenant_id" && key != "app_id" {
				metadata[key] = value
			}
		}
	}
	result, err := service.store.Search(ctx, &vectorstore.SearchQuery{Query: request.Query, Vector: vector, Limit: limit, MinScore: request.MinScore, Filter: filter, SearchMode: vectorstore.SearchModeVector})
	if err != nil {
		return nil, errors.New("knowledge: vector search failed")
	}
	response := &knowledge.SearchResult{Documents: make([]*knowledge.Result, 0, len(result.Results))}
	for _, item := range result.Results {
		if item == nil || item.Document == nil {
			continue
		}
		response.Documents = append(response.Documents, &knowledge.Result{Document: item.Document, Score: item.Score})
	}
	if len(response.Documents) > 0 {
		response.Document = response.Documents[0].Document
		response.Score = response.Documents[0].Score
		response.Text = response.Documents[0].Document.Content
	}
	return response, nil
}

// Close releases the vector-store client.
func (service *Service) Close() error {
	var mirrorErr error
	if service.mirror != nil {
		mirrorErr = service.mirror.Close()
	}
	return errors.Join(service.store.Close(), mirrorErr)
}

func cloneMetadata(source map[string]any) map[string]any {
	result := make(map[string]any, len(source)+4)
	for key, value := range source {
		if key != "tenant_id" && key != "app_id" {
			result[key] = value
		}
	}
	return result
}

func splitText(text string) []string {
	runes := []rune(strings.TrimSpace(text))
	if len(runes) <= chunkRunes {
		return []string{string(runes)}
	}
	var result []string
	for start := 0; start < len(runes); start += chunkRunes - chunkOverlap {
		end := start + chunkRunes
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

var _ knowledge.Knowledge = (*Service)(nil)
