package platform

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"time"

	upstreamartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	artifacts3 "trpc.group/trpc-go/trpc-agent-go/artifact/s3"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
	embeddingopenai "trpc.group/trpc-go/trpc-agent-go/knowledge/embedder/openai"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
	vectorlocal "trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/inmemory"
	vectorqdrant "trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/qdrant"
)

type ObjectBackendConfig struct {
	Bucket       string `json:"bucket,omitempty"`
	Endpoint     string `json:"endpoint,omitempty"`
	Region       string `json:"region,omitempty"`
	AccessKeyEnv string `json:"access_key_env,omitempty"`
	SecretKeyEnv string `json:"secret_key_env,omitempty"`
}
type VectorBackendConfig struct {
	Backend         string `json:"backend,omitempty"`
	Host            string `json:"host,omitempty"`
	Port            int    `json:"port,omitempty"`
	TLS             bool   `json:"tls,omitempty"`
	APIKeyEnv       string `json:"api_key_env,omitempty"`
	Generation      string `json:"generation,omitempty"`
	Dimensions      int    `json:"dimensions,omitempty"`
	EmbeddingModel  string `json:"embedding_model,omitempty"`
	EmbeddingURL    string `json:"embedding_url,omitempty"`
	EmbeddingKeyEnv string `json:"embedding_key_env,omitempty"`
}

type objectReference struct {
	Tenant   string `json:"tenant"`
	Session  string `json:"session"`
	Name     string `json:"name"`
	Version  int    `json:"version"`
	Checksum string `json:"checksum"`
}

func (s *compositeStore) configureServices(selection backendSelection) error {
	if selection.Object.Bucket != "" {
		config := selection.Object
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		options := []artifacts3.Option{artifacts3.WithEndpoint(config.Endpoint), artifacts3.WithRegion(config.Region), artifacts3.WithPathStyle(true)}
		if config.AccessKeyEnv != "" {
			access, secret := os.Getenv(config.AccessKeyEnv), os.Getenv(config.SecretKeyEnv)
			if access == "" || secret == "" {
				return errors.New("object credentials unavailable")
			}
			options = append(options, artifacts3.WithCredentials(access, secret))
		}
		service, err := artifacts3.NewService(ctx, config.Bucket, options...)
		if err != nil {
			return err
		}
		s.objects = service
	}
	s.vectorConfig = selection.Vector
	if selection.Vector.Backend != "" {
		c := selection.Vector
		if c.Generation == "" || c.Dimensions <= 0 || c.EmbeddingModel == "" {
			return errors.New("vector generation, dimensions and embedding model required")
		}
		if c.Backend != "inmemory" && c.Backend != "qdrant" {
			return errors.New("unsupported vector backend")
		}
		s.embedder = embeddingopenai.New(embeddingopenai.WithModel(c.EmbeddingModel), embeddingopenai.WithDimensions(c.Dimensions), embeddingopenai.WithBaseURL(c.EmbeddingURL), embeddingopenai.WithAPIKey(os.Getenv(c.EmbeddingKeyEnv)))
		s.vectors = map[string]vectorstore.VectorStore{}
	}
	return nil
}

func objectSession(ref objectReference) upstreamartifact.SessionInfo {
	return upstreamartifact.SessionInfo{AppName: "tenant-" + stableID(ref.Tenant), UserID: "tenant-" + stableID(ref.Tenant), SessionID: "session-" + stableID(ref.Session)}
}
func (s *compositeStore) publishObject(ctx context.Context, item Artifact) (Artifact, error) {
	if s.objects == nil || !strings.HasPrefix(item.ContentRef, "session-event://") {
		return item, nil
	}
	// Only a committed event belonging to this tenant/session may be published.
	prefix := "session-event://" + item.TenantID + "/" + item.SessionID + "/"
	if !strings.HasPrefix(item.ContentRef, prefix) {
		return Artifact{}, errors.New("invalid artifact source scope")
	}
	key := strings.TrimPrefix(item.ContentRef, prefix)
	events, err := s.ListSessionEvents(ctx, item.TenantID, item.SessionID, 0)
	if err != nil {
		return Artifact{}, err
	}
	var content []byte
	for _, event := range events {
		if event.IdempotencyKey == key && event.Type == "message.completed" {
			var payload map[string]string
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return Artifact{}, err
			}
			content = []byte(payload["output"])
			break
		}
	}
	if content == nil {
		return Artifact{}, errors.New("artifact source event missing")
	}
	ref := objectReference{Tenant: item.TenantID, Session: item.SessionID, Name: "response-" + stableID(key+string(content)), Checksum: contentChecksum(content)}
	versions, err := s.objects.ListVersions(ctx, objectSession(ref), ref.Name)
	if err != nil {
		return Artifact{}, err
	}
	if len(versions) > 0 {
		ref.Version = versions[len(versions)-1]
	} else {
		ref.Version, err = s.objects.SaveArtifact(ctx, objectSession(ref), ref.Name, &upstreamartifact.Artifact{Data: content, MimeType: "text/plain; charset=utf-8"})
		if err != nil {
			return Artifact{}, err
		}
	}
	loaded, err := s.objects.LoadArtifact(ctx, objectSession(ref), ref.Name, &ref.Version)
	if err != nil {
		return Artifact{}, err
	}
	if loaded == nil || contentChecksum(loaded.Data) != ref.Checksum {
		return Artifact{}, errors.New("artifact checksum mismatch")
	}
	encoded, _ := json.Marshal(ref)
	item.ContentRef = "artifact://" + base64.RawURLEncoding.EncodeToString(encoded)
	return item, nil
}
func (s *compositeStore) LoadArtifactContent(ctx context.Context, tenant, id string) ([]byte, error) {
	items, err := s.ListArtifacts(ctx, tenant, "")
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		if item.ID != id || item.Status != "published" {
			continue
		}
		if !strings.HasPrefix(item.ContentRef, "artifact://") || s.objects == nil {
			return nil, ErrNotFound
		}
		data, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(item.ContentRef, "artifact://"))
		if err != nil {
			return nil, err
		}
		var ref objectReference
		if err = json.Unmarshal(data, &ref); err != nil {
			return nil, err
		}
		if ref.Tenant != tenant || ref.Session != item.SessionID {
			return nil, ErrNotFound
		}
		content, err := s.objects.LoadArtifact(ctx, objectSession(ref), ref.Name, &ref.Version)
		if err != nil {
			return nil, err
		}
		if content == nil || contentChecksum(content.Data) != ref.Checksum {
			return nil, errors.New("artifact recovery required")
		}
		return content.Data, nil
	}
	return nil, ErrNotFound
}

func (s *compositeStore) vectorStore(ctx context.Context, tenant string) (vectorstore.VectorStore, error) {
	s.vectorMu.Lock()
	defer s.vectorMu.Unlock()
	if store := s.vectors[tenant]; store != nil {
		return store, nil
	}
	c := s.vectorConfig
	var store vectorstore.VectorStore
	if c.Backend == "inmemory" {
		store = vectorlocal.New()
	} else {
		remote, err := vectorqdrant.New(ctx, vectorqdrant.WithHost(c.Host), vectorqdrant.WithPort(c.Port), vectorqdrant.WithTLS(c.TLS), vectorqdrant.WithAPIKey(os.Getenv(c.APIKeyEnv)), vectorqdrant.WithDimension(c.Dimensions), vectorqdrant.WithCollectionName("tenant_"+stableID(tenant)+"_"+stableID(c.Generation)))
		if err != nil {
			return nil, err
		}
		store = remote
	}
	s.vectors[tenant] = store
	return store, nil
}
func (s *compositeStore) indexKnowledge(ctx context.Context, item KnowledgeRecord) error {
	store, err := s.vectorStore(ctx, item.TenantID)
	if err != nil {
		return err
	}
	// A content-addressed ID prevents an old concurrent update overwriting a newer
	// vector. Retrieval always resolves the current authoritative record.
	id := stableID(item.TenantID + "\x00" + item.AgentAppID + "\x00" + item.ID + "\x00" + item.Content + "\x00" + s.vectorConfig.Generation)
	if doc, _, err := store.Get(ctx, id); err == nil && doc != nil && doc.Content == item.Content {
		return nil
	}
	vector, err := s.embedder.GetEmbedding(ctx, item.Content)
	if err != nil {
		return err
	}
	if len(vector) != s.vectorConfig.Dimensions {
		return errors.New("embedding dimension mismatch")
	}
	return store.Add(ctx, &document.Document{ID: id, Name: item.Source, Content: item.Content, Metadata: map[string]any{"tenant_id": item.TenantID, "app_id": item.AgentAppID, "knowledge_id": item.ID, "content_hash": stableID(item.Content)}, CreatedAt: item.CreatedAt, UpdatedAt: item.CreatedAt}, vector)
}
func (s *compositeStore) SearchKnowledge(ctx context.Context, tenant, app, query string) ([]KnowledgeRecord, error) {
	records, err := s.ListKnowledge(ctx, tenant, app)
	if err != nil || s.embedder == nil {
		return records, err
	}
	// Repair from the durable authoritative records, including after a local index
	// restart. An index outage retains usable authoritative context.
	for _, item := range records {
		if err := s.indexKnowledge(ctx, item); err != nil {
			return records, nil
		}
	}
	vector, err := s.embedder.GetEmbedding(ctx, query)
	if err != nil || len(vector) != s.vectorConfig.Dimensions {
		return records, nil
	}
	store, err := s.vectorStore(ctx, tenant)
	if err != nil {
		return records, nil
	}
	result, err := store.Search(ctx, &vectorstore.SearchQuery{Vector: vector, Limit: 5, SearchMode: vectorstore.SearchModeVector, Filter: &vectorstore.SearchFilter{Metadata: map[string]any{"tenant_id": tenant, "app_id": app}}})
	if err != nil {
		return records, nil
	}
	current := map[string]KnowledgeRecord{}
	for _, item := range records {
		current[item.ID] = item
	}
	items := []KnowledgeRecord{}
	seen := map[string]bool{}
	for _, hit := range result.Results {
		if hit == nil || hit.Document == nil {
			continue
		}
		meta := hit.Document.Metadata
		if meta["tenant_id"] != tenant || meta["app_id"] != app {
			continue
		}
		id, _ := meta["knowledge_id"].(string)
		item, ok := current[id]
		if ok && !seen[id] && meta["content_hash"] == stableID(item.Content) {
			items = append(items, item)
			seen[id] = true
		}
	}
	if len(items) == 0 {
		return records, nil
	}
	return items, nil
}

// ReindexKnowledge rebuilds a configured generation from authoritative content.
// Keep writers stopped and select the new profile only after this succeeds.
func ReindexKnowledge(ctx context.Context, store DataStore, tenant string) error {
	s, ok := store.(*compositeStore)
	if !ok || s.embedder == nil {
		return errors.New("vector profile required")
	}
	records, err := s.ListKnowledge(ctx, tenant, "")
	if err != nil {
		return err
	}
	for _, item := range records {
		if err := s.indexKnowledge(ctx, item); err != nil {
			return err
		}
	}
	after, err := s.ListKnowledge(ctx, tenant, "")
	if err != nil {
		return err
	}
	beforeJSON, _ := json.Marshal(records)
	afterJSON, _ := json.Marshal(after)
	if string(beforeJSON) != string(afterJSON) {
		return errors.New("knowledge changed during reindex")
	}
	return nil
}

var _ embedder.Embedder = (*embeddingopenai.Embedder)(nil)

func contentChecksum(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
