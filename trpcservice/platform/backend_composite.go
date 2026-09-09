package platform

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sort"
	"sync"
	upstreamartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
	"trpc.group/trpc-go/trpc-agent-go/memory/mem0"
)

// BackendEndpoint is server-owned configuration. Public requests only reference
// catalog IDs; connection strings never cross the management API boundary.
type BackendEndpoint struct {
	Backend string `json:"backend"`
	Address string `json:"address,omitempty"`
}

type compositeStore struct {
	externalMemory *mem0.Service
	objects        upstreamartifact.Service
	vectorConfig   VectorBackendConfig
	embedder       embedder.Embedder
	vectorMu       sync.Mutex
	vectors        map[string]vectorstore.VectorStore
	DataStore
	memory    DataStore
	artifacts DataStore
	knowledge DataStore
	owned     []DataStore
}

func (s *compositeStore) ListMemory(ctx context.Context, tenant, session string) ([]MemoryRecord, error) {
	return s.memory.ListMemory(ctx, tenant, session)
}
func (s *compositeStore) PutMemory(ctx context.Context, item MemoryRecord) error {
	if err := s.memory.PutMemory(ctx, item); err != nil {
		return err
	}
	if s.externalMemory != nil {
		// Read back generated identity/timestamp before ingestion.
		records, err := s.memory.ListMemory(ctx, item.TenantID, item.SessionID)
		if err == nil {
			for _, record := range records {
				if record.Key == item.Key {
					return s.syncExternalMemory(ctx, record)
				}
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *compositeStore) PutArtifact(ctx context.Context, item Artifact) (Artifact, error) {
	// Use a stable metadata identity independent of storage location.
	if item.ID == "" {
		item.ID = "artifact-" + stableID(item.TenantID+"\x00"+item.SessionID+"\x00"+item.Name+"\x00"+item.ContentRef)
	}
	published, err := s.publishObject(ctx, item)
	if err != nil {
		return Artifact{}, err
	}
	return s.artifacts.(ArtifactStore).PutArtifact(ctx, published)
}
func (s *compositeStore) ListArtifacts(ctx context.Context, tenant, session string) ([]Artifact, error) {
	return s.artifacts.(ArtifactStore).ListArtifacts(ctx, tenant, session)
}
func (s *compositeStore) PutKnowledge(ctx context.Context, item KnowledgeRecord) (KnowledgeRecord, error) {
	saved, err := s.knowledge.(KnowledgeStore).PutKnowledge(ctx, item)
	if err == nil && s.embedder != nil {
		_ = s.indexKnowledge(ctx, saved)
	}
	return saved, err
}
func (s *compositeStore) ListKnowledge(ctx context.Context, tenant, app string) ([]KnowledgeRecord, error) {
	return s.knowledge.(KnowledgeStore).ListKnowledge(ctx, tenant, app)
}
func (s *compositeStore) ListSessionIDs(ctx context.Context, tenant string) ([]string, error) {
	set := map[string]bool{}
	for _, store := range []DataStore{s.DataStore, s.memory} {
		ids, err := store.(sessionLister).ListSessionIDs(ctx, tenant)
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			set[id] = true
		}
	}
	ids := []string{}
	for id := range set {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}
func (s *compositeStore) AppendSessionEvent(ctx context.Context, event SessionEvent) error {
	// Publish lease acquisition to independently routed stores before execution.
	if event.Type == "session.lease.acquired" {
		for _, store := range s.owned {
			if store != s.DataStore {
				if err := store.AppendSessionEvent(ctx, event); err != nil {
					return err
				}
			}
		}
	}
	return s.DataStore.AppendSessionEvent(ctx, event)
}
func (s *compositeStore) Health(ctx context.Context) BackendHealth {
	health := s.DataStore.Health(ctx)
	for _, store := range s.owned {
		if store.Health(ctx).Status != "healthy" {
			health.Status = "unavailable"
		}
	}
	return health
}
func (s *compositeStore) Close() error {
	var errs []error
	if s.externalMemory != nil {
		errs = append(errs, s.externalMemory.Close())
	}
	if closer, ok := s.objects.(interface{ Close() error }); ok {
		errs = append(errs, closer.Close())
	}
	for _, store := range s.vectors {
		errs = append(errs, store.Close())
	}
	for _, store := range s.owned {
		if err := closeDataStoreWithError(store); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
func newBackendStore(selection backendSelection) (DataStore, error) {
	base, err := newBasicBackendStore(BackendEndpoint{selection.Backend, selection.Address})
	if err != nil {
		return nil, err
	}
	if selection.Memory.Backend == "" && selection.Artifact.Backend == "" && selection.Knowledge.Backend == "" && selection.Object.Bucket == "" && selection.Vector.Backend == "" && selection.ExternalMemory.Host == "" {
		return base, nil
	}
	s := &compositeStore{DataStore: base, memory: base, artifacts: base, knowledge: base, owned: []DataStore{base}}
	stores := map[BackendEndpoint]DataStore{{selection.Backend, selection.Address}: base}
	for _, entry := range []struct {
		endpoint BackendEndpoint
		target   *DataStore
	}{{selection.Memory, &s.memory}, {selection.Artifact, &s.artifacts}, {selection.Knowledge, &s.knowledge}} {
		if entry.endpoint.Backend == "" {
			continue
		}
		store := stores[entry.endpoint]
		if store == nil {
			store, err = newBasicBackendStore(entry.endpoint)
			if err != nil {
				_ = s.Close()
				return nil, err
			}
			stores[entry.endpoint] = store
			s.owned = append(s.owned, store)
		}
		*entry.target = store
	}
	if _, ok := s.artifacts.(ArtifactStore); !ok {
		_ = s.Close()
		return nil, errors.New("artifact backend unsupported")
	}
	if _, ok := s.knowledge.(KnowledgeStore); !ok {
		_ = s.Close()
		return nil, errors.New("knowledge backend unsupported")
	}
	if err := s.configureServices(selection); err != nil {
		_ = s.Close()
		return nil, err
	}
	if err := s.configureExternalMemory(selection.ExternalMemory); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

// ConfigureBackendProfiles loads named combinations without exposing addresses.
// Each profile uses backend/address for Session/Summary and optional overrides.
func (h *AdminHandler) ConfigureBackendProfiles(path string) error {
	if path == "" {
		return nil
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	var profiles map[string]backendSelection
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&profiles); err != nil {
		return err
	}
	for id, selection := range profiles {
		if id == "" {
			return errors.New("empty backend profile ID")
		}
		for _, backend := range []string{selection.Backend, selection.Memory.Backend, selection.Artifact.Backend, selection.Knowledge.Backend} {
			switch backend {
			case "", "inmemory", "redis", "sqlite", "postgres":
			default:
				return errors.New("unsupported backend profile")
			}
		}
		if selection.Backend == "" {
			return errors.New("session backend required")
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, selection := range profiles {
		selection.ProfileID = id
		h.backendCatalog[id] = selection
	}
	return nil
}

// OpenBackendProfile constructs a server-owned profile for maintenance tools.
func OpenBackendProfile(path, id string) (DataStore, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var profiles map[string]backendSelection
	if err = json.Unmarshal(data, &profiles); err != nil {
		return nil, err
	}
	selection, ok := profiles[id]
	if !ok {
		return nil, errors.New("backend profile not found")
	}
	return newBackendStore(selection)
}
