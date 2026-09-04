// Package knowledge manages tenant-scoped knowledge bases on top of the
// tRPC-Agent-Go knowledge pipeline: metadata lives in MySQL (or memory), the
// embedding endpoint is a platform ModelEndpoint, and vectors live in a
// VectorStore (Milvus in production, in-memory in dev). Each KB maps to one
// vectorstore collection named {tenant_id}_{kb_id}, which is the tenant
// isolation boundary.
package knowledge

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	fwk "trpc.group/trpc-go/trpc-agent-go/knowledge"
	embedderpkg "trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder/openai"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/source"
	filesrc "trpc.group/trpc-go/trpc-agent-go/knowledge/source/file"
	urlsrc "trpc.group/trpc-go/trpc-agent-go/knowledge/source/url"
	kbtool "trpc.group/trpc-go/trpc-agent-go/knowledge/tool"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
	inmemoryvs "trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/inmemory"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/llm"

	fwtool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// Document ingestion states.
const (
	StatusIngesting = "ingesting"
	StatusReady     = "ready"
	StatusFailed    = "failed"
)

// DefaultDimension is the embedding dimension assumed when a KB does not
// specify one (matches text-embedding-3-small).
const DefaultDimension = 1536

// Sentinel errors.
var (
	ErrKBNotFound      = errors.New("knowledge: kb not found")
	ErrDocNotFound     = errors.New("knowledge: document not found")
	ErrEmptySource     = errors.New("knowledge: document needs a source_uri or text")
	ErrEndpointMissing = errors.New("knowledge: embedding endpoint not configured")
)

// KnowledgeBase is a tenant-level shared asset: multiple agents may reference
// it via RuntimeProfile.KnowledgeIDs. Vectors live in CollectionName.
type KnowledgeBase struct {
	ID                  string `json:"id"`
	TenantID            string `json:"tenant_id"`
	Name                string `json:"name"`
	EmbeddingEndpointID string `json:"embedding_endpoint_id"` // ModelEndpoint ref
	CollectionName      string `json:"collection_name"`       // {tenant_id}_{kb_id}
	Dimension           int    `json:"dimension,omitempty"`   // 0 = DefaultDimension
}

// Document tracks one ingestion into a KB. Text is inline content accepted at
// creation; raw file storage (MinIO) lands in a later phase.
type Document struct {
	ID         string `json:"id"`
	KBID       string `json:"kb_id"`
	Title      string `json:"title,omitempty"`
	SourceURI  string `json:"source_uri,omitempty"` // http(s) URL, or "inline" for text docs
	Text       string `json:"text,omitempty"`       // inline content; not persisted
	ChunkCount int    `json:"chunk_count"`
	Status     string `json:"status"`
	Error      string `json:"error,omitempty"`
}

// Hit is one search result exposed by the platform API.
type Hit struct {
	DocumentID string  `json:"document_id"`
	SourceName string  `json:"source_name"`
	Content    string  `json:"content"`
	Score      float64 `json:"score"`
}

// EmbedderFactory builds the embedder for a KB from its ModelEndpoint.
type EmbedderFactory func(ctx context.Context, kb *KnowledgeBase) (embedderpkg.Embedder, error)

// VectorStoreFactory builds the vectorstore for one KB collection.
type VectorStoreFactory func(ctx context.Context, kb *KnowledgeBase) (vectorstore.VectorStore, error)

// Store is the metadata persistence contract (memory or MySQL). The
// in-memory store keeps the service runnable without MySQL; MySQL and other
// backend stores are implemented in the infra/storage package against this
// interface.
type Store interface {
	CreateKB(ctx context.Context, kb *KnowledgeBase) error
	GetKB(ctx context.Context, id string) (*KnowledgeBase, error)
	ListKBs(ctx context.Context, tenantID string) ([]*KnowledgeBase, error)
	DeleteKB(ctx context.Context, id string) error
	CreateDocument(ctx context.Context, doc *Document) error
	UpdateDocument(ctx context.Context, doc *Document) error
	GetDocument(ctx context.Context, id string) (*Document, error)
	ListDocuments(ctx context.Context, kbID string) ([]*Document, error)
}

// Manager owns KB metadata and a cached search instance per KB.
type Manager struct {
	store Store
	vsf   VectorStoreFactory
	embf  EmbedderFactory

	mu   sync.Mutex
	inst map[string]*fwk.BuiltinKnowledge // kbID -> search instance
}

// NewManager returns an in-memory-metadata manager.
func NewManager(vsf VectorStoreFactory, embf EmbedderFactory) *Manager {
	return &Manager{store: newMemStore(), vsf: vsf, embf: embf, inst: make(map[string]*fwk.BuiltinKnowledge)}
}

// NewManagerWithStore returns a manager over the given store implementation,
// used by infra/storage to back a Manager with MySQL.
func NewManagerWithStore(s Store, vsf VectorStoreFactory, embf EmbedderFactory) *Manager {
	return &Manager{store: s, vsf: vsf, embf: embf, inst: make(map[string]*fwk.BuiltinKnowledge)}
}

// ---------------------------------------------------------------- metadata --

// Create registers a KB, computing its isolation collection name.
func (m *Manager) Create(ctx context.Context, kb *KnowledgeBase) error {
	if kb.ID == "" || kb.TenantID == "" || kb.Name == "" {
		return errors.New("knowledge: id, tenant and name are required")
	}
	if kb.EmbeddingEndpointID == "" {
		return ErrEndpointMissing
	}
	if kb.Dimension <= 0 {
		kb.Dimension = DefaultDimension
	}
	// Milvus collection names allow only letters, digits and underscores, so
	// UUID-style tenant/kb ids (with hyphens) are normalized to underscores.
	kb.CollectionName = sanitizeName(kb.TenantID) + "_" + sanitizeName(kb.ID)
	// Milvus also requires the first character to be an underscore or a
	// letter; a numeric-leading tenant id (e.g. "1") would otherwise produce
	// an invalid name like "1_xxx".
	if len(kb.CollectionName) > 0 && kb.CollectionName[0] >= '0' && kb.CollectionName[0] <= '9' {
		kb.CollectionName = "kb_" + kb.CollectionName
	}
	return m.store.CreateKB(ctx, kb)
}

// sanitizeName maps any character outside [A-Za-z0-9_] to an underscore.
func sanitizeName(s string) string {
	out := []byte(s)
	for i, c := range out {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_':
		default:
			out[i] = '_'
		}
	}
	return string(out)
}

// Get returns a KB by id.
func (m *Manager) Get(ctx context.Context, id string) (*KnowledgeBase, error) {
	return m.store.GetKB(ctx, id)
}

// List returns the KBs of a tenant (empty tenantID returns all).
func (m *Manager) List(ctx context.Context, tenantID string) ([]*KnowledgeBase, error) {
	return m.store.ListKBs(ctx, tenantID)
}

// Delete removes KB metadata. Vectors in the collection are left behind
// (the framework VectorStore has no drop); a later phase wires collection GC.
func (m *Manager) Delete(ctx context.Context, id string) error {
	m.mu.Lock()
	delete(m.inst, id)
	m.mu.Unlock()
	return m.store.DeleteKB(ctx, id)
}

// ------------------------------------------------------------- ingestion --

// AddDocument ingests one document now: URL sources are fetched, inline text
// goes through a temp file, both are chunked and embedded into the KB
// collection. Status transitions ingesting -> ready | failed.
func (m *Manager) AddDocument(ctx context.Context, doc *Document) error {
	if doc.ID == "" || doc.KBID == "" {
		return errors.New("knowledge: document id and kb_id are required")
	}
	if doc.SourceURI == "" && doc.Text == "" {
		return ErrEmptySource
	}
	kb, err := m.store.GetKB(ctx, doc.KBID)
	if err != nil {
		return err
	}

	if doc.SourceURI == "" {
		doc.SourceURI = "inline"
	}
	doc.Status = StatusIngesting
	doc.Error = ""
	if err := m.store.CreateDocument(ctx, doc); err != nil {
		return err
	}

	if err := m.ingest(ctx, kb, doc); err != nil {
		doc.Status = StatusFailed
		doc.Error = truncate(err.Error(), 512)
		if uerr := m.store.UpdateDocument(ctx, doc); uerr != nil {
			return errors.Join(err, uerr)
		}
		return err
	}
	return nil
}

// ingest builds an ephemeral pipeline instance (own VectorStore client for the
// KB collection) and loads the single source, then records the chunk count.
//
// The instance is NOT closed afterwards: BuiltinKnowledge.Close also closes
// the underlying VectorStore, and in dev mode that store is the shared
// per-collection instance — closing it would wipe the KB. VectorStores are
// owned by the factory cache and live for the process lifetime.
func (m *Manager) ingest(ctx context.Context, kb *KnowledgeBase, doc *Document) error {
	vs, err := m.vsf(ctx, kb)
	if err != nil {
		return fmt.Errorf("knowledge: vector store: %w", err)
	}
	emb, err := m.embf(ctx, kb)
	if err != nil {
		return fmt.Errorf("knowledge: embedder: %w", err)
	}

	src, cleanup, err := m.buildSource(doc)
	if err != nil {
		return err
	}
	if cleanup != nil {
		defer cleanup()
	}

	inst := fwk.New(fwk.WithVectorStore(vs), fwk.WithEmbedder(emb))
	if err := inst.AddSource(ctx, src); err != nil {
		return fmt.Errorf("knowledge: load source: %w", err)
	}

	infos, err := inst.ShowDocumentInfo(ctx, fwk.WithShowDocumentInfoSourceName(src.Name()))
	if err != nil {
		return fmt.Errorf("knowledge: document info: %w", err)
	}
	doc.Status = StatusReady
	doc.ChunkCount = len(infos)
	return m.store.UpdateDocument(ctx, doc)
}

// buildSource maps a document to a framework source. Inline text is written
// to a temp file (the framework's text reader consumes files); URL documents
// use the URL source.
func (m *Manager) buildSource(doc *Document) (src source.Source, cleanup func(), err error) {
	if doc.Text != "" {
		f, err := os.CreateTemp("", "kbdoc-*.txt")
		if err != nil {
			return nil, nil, fmt.Errorf("knowledge: temp file: %w", err)
		}
		if _, err := f.WriteString(doc.Text); err != nil {
			_ = f.Close()
			_ = os.Remove(f.Name())
			return nil, nil, fmt.Errorf("knowledge: write temp file: %w", err)
		}
		if err := f.Close(); err != nil {
			_ = os.Remove(f.Name())
			return nil, nil, err
		}
		s := filesrc.New([]string{f.Name()},
			filesrc.WithName(doc.ID),
			filesrc.WithMetadataValue("doc_id", doc.ID),
		)
		return s, func() { _ = os.Remove(f.Name()) }, nil
	}
	s := urlsrc.New([]string{doc.SourceURI})
	return s, nil, nil
}

// GetDocument returns one document's ingestion state.
func (m *Manager) GetDocument(ctx context.Context, id string) (*Document, error) {
	return m.store.GetDocument(ctx, id)
}

// ListDocuments returns the documents of a KB.
func (m *Manager) ListDocuments(ctx context.Context, kbID string) ([]*Document, error) {
	return m.store.ListDocuments(ctx, kbID)
}

// ---------------------------------------------------------------- search --

// Search queries the KB collection. An empty KB or no matches return empty
// hits, not an error (the framework signals "no relevant documents" as an
// error; that is a normal miss here).
func (m *Manager) Search(ctx context.Context, kbID, query string, limit int) ([]*Hit, error) {
	if limit <= 0 {
		limit = 5
	}
	inst, err := m.instance(ctx, kbID)
	if err != nil {
		return nil, err
	}
	res, err := inst.Search(ctx, &fwk.SearchRequest{Query: query, MaxResults: limit})
	if err != nil {
		if strings.Contains(err.Error(), "no relevant documents") {
			return nil, nil
		}
		return nil, fmt.Errorf("knowledge: search: %w", err)
	}
	hits := make([]*Hit, 0, len(res.Documents))
	for _, r := range res.Documents {
		if r == nil || r.Document == nil {
			continue
		}
		hits = append(hits, &Hit{
			DocumentID: r.Document.ID,
			SourceName: r.Document.Name,
			Content:    r.Document.Content,
			Score:      r.Score,
		})
	}
	return hits, nil
}

// instance returns the cached search instance for a KB (no sources attached:
// search only reads the vectorstore, sources matter at ingestion time).
func (m *Manager) instance(ctx context.Context, kbID string) (*fwk.BuiltinKnowledge, error) {
	m.mu.Lock()
	if inst, ok := m.inst[kbID]; ok {
		m.mu.Unlock()
		return inst, nil
	}
	m.mu.Unlock()

	kb, err := m.store.GetKB(ctx, kbID)
	if err != nil {
		return nil, err
	}
	vs, err := m.vsf(ctx, kb)
	if err != nil {
		return nil, fmt.Errorf("knowledge: vector store: %w", err)
	}
	emb, err := m.embf(ctx, kb)
	if err != nil {
		return nil, fmt.Errorf("knowledge: embedder: %w", err)
	}
	inst := fwk.New(fwk.WithVectorStore(vs), fwk.WithEmbedder(emb))

	m.mu.Lock()
	// Another goroutine may have won the race; keep one instance per KB.
	// (The loser is dropped without Close: Close would tear down the shared
	// VectorStore, see ingest.)
	if existing, ok := m.inst[kbID]; ok {
		m.mu.Unlock()
		return existing, nil
	}
	m.inst[kbID] = inst
	m.mu.Unlock()
	return inst, nil
}

// SearchTool returns the framework knowledge-search tool bound to the KB,
// registered under the given tool name.
func (m *Manager) SearchTool(ctx context.Context, kbID, name string) (fwtool.Tool, error) {
	inst, err := m.instance(ctx, kbID)
	if err != nil {
		return nil, err
	}
	return kbtool.NewKnowledgeSearchTool(inst, kbtool.WithToolName(name)), nil
}

// ------------------------------------------------------------- factories --

// InMemoryVectorStoreFactory keeps one in-memory store per collection so the
// ephemeral ingest instance and the cached search instance observe the same
// data. This is the dev/test path when no Milvus address is configured.
func InMemoryVectorStoreFactory() VectorStoreFactory {
	var mu sync.Mutex
	stores := make(map[string]vectorstore.VectorStore)
	return func(_ context.Context, kb *KnowledgeBase) (vectorstore.VectorStore, error) {
		mu.Lock()
		defer mu.Unlock()
		if vs, ok := stores[kb.CollectionName]; ok {
			return vs, nil
		}
		vs := inmemoryvs.New()
		stores[kb.CollectionName] = vs
		return vs, nil
	}
}

// RegistryEmbedderFactory resolves the KB's ModelEndpoint (OpenAI-compatible)
// and builds the framework embedder from it.
func RegistryEmbedderFactory(reg *llm.Registry) EmbedderFactory {
	return func(ctx context.Context, kb *KnowledgeBase) (embedderpkg.Embedder, error) {
		ep, err := reg.Get(ctx, kb.EmbeddingEndpointID)
		if err != nil {
			return nil, fmt.Errorf("knowledge: endpoint %q: %w", kb.EmbeddingEndpointID, err)
		}
		// Only embedding endpoints may back a KB: a chat endpoint (or an empty
		// type on an old endpoint) must not be silently used for embeddings.
		if ep.Type != "" && ep.Type != llm.EndpointTypeEmbedding {
			return nil, fmt.Errorf("knowledge: endpoint %q is type %q, not an embedding endpoint", kb.EmbeddingEndpointID, ep.Type)
		}
		key, err := reg.ResolveAPIKey(ctx, ep)
		if err != nil {
			return nil, fmt.Errorf("knowledge: endpoint %q: %w", kb.EmbeddingEndpointID, err)
		}
		opts := []openai.Option{
			openai.WithBaseURL(ep.BaseURL),
			openai.WithModel(ep.ModelName),
			openai.WithAPIKey(key),
		}
		if kb.Dimension > 0 {
			opts = append(opts, openai.WithDimensions(kb.Dimension))
		}
		return openai.New(opts...), nil
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// memStore is the zero-dependency metadata store.
type memStore struct {
	mu   sync.RWMutex
	kbs  map[string]*KnowledgeBase
	docs map[string]*Document
}

func newMemStore() *memStore {
	return &memStore{kbs: make(map[string]*KnowledgeBase), docs: make(map[string]*Document)}
}

func (s *memStore) CreateKB(_ context.Context, kb *KnowledgeBase) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.kbs[kb.ID]; ok {
		return fmt.Errorf("knowledge: kb %q already exists", kb.ID)
	}
	cp := *kb
	s.kbs[kb.ID] = &cp
	return nil
}

func (s *memStore) GetKB(_ context.Context, id string) (*KnowledgeBase, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	kb, ok := s.kbs[id]
	if !ok {
		return nil, ErrKBNotFound
	}
	cp := *kb
	return &cp, nil
}

func (s *memStore) ListKBs(_ context.Context, tenantID string) ([]*KnowledgeBase, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*KnowledgeBase, 0)
	for _, kb := range s.kbs {
		if tenantID == "" || kb.TenantID == tenantID {
			cp := *kb
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *memStore) DeleteKB(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.kbs[id]; !ok {
		return ErrKBNotFound
	}
	delete(s.kbs, id)
	return nil
}

func (s *memStore) CreateDocument(_ context.Context, doc *Document) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.docs[doc.ID]; ok {
		return fmt.Errorf("knowledge: document %q already exists", doc.ID)
	}
	cp := *doc
	s.docs[doc.ID] = &cp
	return nil
}

func (s *memStore) UpdateDocument(_ context.Context, doc *Document) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.docs[doc.ID]; !ok {
		return ErrDocNotFound
	}
	cp := *doc
	s.docs[doc.ID] = &cp
	return nil
}

func (s *memStore) GetDocument(_ context.Context, id string) (*Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	doc, ok := s.docs[id]
	if !ok {
		return nil, ErrDocNotFound
	}
	cp := *doc
	return &cp, nil
}

func (s *memStore) ListDocuments(_ context.Context, kbID string) ([]*Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Document, 0)
	for _, doc := range s.docs {
		if doc.KBID == kbID {
			cp := *doc
			out = append(out, &cp)
		}
	}
	return out, nil
}
