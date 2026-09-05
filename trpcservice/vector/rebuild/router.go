package rebuild

import (
	"context"
	"sort"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
)

// ProjectionEntry binds one server-owned projection configuration to its
// pre-provisioned derived store. Entries are registered by composition or
// integration fixtures; callers can never select a projection per request.
type ProjectionEntry struct {
	Fingerprint string
	Config      vector.ProjectionConfig
	Store       vector.VectorStore
}

// Router is the server-owned projection registry. It routes vector writes and
// identity reads by the projection fingerprint recomputed from the
// server-owned document reference, so a rebuild run targeting a new
// projection writes only into its registered pre-provisioned store. Search
// and Close fan out; the retrieval cutover between projections stays outside
// this boundary (no atomic alias protocol exists yet).
type Router struct {
	mu      sync.RWMutex
	entries map[string]ProjectionEntry
	order   []string
}

// NewRouter fails closed without entries.
func NewRouter(entries []ProjectionEntry) (*Router, error) {
	if len(entries) == 0 {
		return nil, ErrInvalidConfig
	}
	router := &Router{entries: make(map[string]ProjectionEntry, len(entries))}
	for _, entry := range entries {
		if entry.Store == nil || !validHex64(entry.Fingerprint) {
			return nil, ErrInvalidConfig
		}
		fingerprint, err := ProjectionFingerprint(entry.Config)
		if err != nil || fingerprint != entry.Fingerprint {
			return nil, ErrInvalidConfig
		}
		if _, duplicate := router.entries[fingerprint]; duplicate {
			return nil, ErrInvalidConfig
		}
		router.entries[fingerprint] = entry
		router.order = append(router.order, fingerprint)
	}
	return router, nil
}

func (r *Router) route(ref vector.VectorDocumentRef) (vector.VectorStore, error) {
	fingerprint, err := ProjectionFingerprint(vector.ProjectionConfig{
		Model: ref.Model, ModelVersion: ref.ModelVersion,
		SchemaVersion: ref.SchemaVersion, Dimension: ref.Dimension,
	})
	if err != nil {
		return nil, vector.ErrInvalidDocument
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.entries[fingerprint]
	if !ok {
		return nil, vector.ErrInvalidConfig
	}
	return entry.Store, nil
}

// Ready reports readiness of every registered store.
func (r *Router) Ready(ctx context.Context) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, fingerprint := range r.order {
		if err := r.entries[fingerprint].Store.Ready(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Upsert routes by the request's recomputed projection fingerprint.
func (r *Router) Upsert(ctx context.Context, request vector.UpsertRequest) error {
	store, err := r.route(request.Ref)
	if err != nil {
		return err
	}
	return store.Upsert(ctx, request)
}

// Delete routes by the request's recomputed projection fingerprint.
func (r *Router) Delete(ctx context.Context, request vector.DeleteRequest) error {
	store, err := r.route(request.Ref)
	if err != nil {
		return err
	}
	return store.Delete(ctx, request)
}

// Search fans out across registered projections and merges results in stable
// fingerprint order. Bounded by each store's own TopK validation.
func (r *Router) Search(ctx context.Context, request vector.SearchRequest) ([]vector.SearchResult, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var merged []vector.SearchResult
	for _, fingerprint := range r.order {
		results, err := r.entries[fingerprint].Store.Search(ctx, request)
		if err != nil {
			return nil, err
		}
		merged = append(merged, results...)
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Score != merged[j].Score {
			return merged[i].Score > merged[j].Score
		}
		return merged[i].Ref.DocumentID < merged[j].Ref.DocumentID
	})
	return merged, nil
}

// Close closes every registered store once.
func (r *Router) Close(ctx context.Context) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var firstErr error
	for _, fingerprint := range r.order {
		if err := r.entries[fingerprint].Store.Close(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// InspectIdentities queries every registered projection and merges identities
// by server-owned document ID. Wrong-tenant backend rows fail closed.
func (r *Router) InspectIdentities(ctx context.Context, documentIDs []string) (map[string]vector.DocumentIdentity, error) {
	merged := make(map[string]vector.DocumentIdentity, len(documentIDs))
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, fingerprint := range r.order {
		reader, ok := r.entries[fingerprint].Store.(vector.IdentityReader)
		if !ok {
			continue
		}
		identities, err := reader.InspectIdentities(ctx, documentIDs)
		if err != nil {
			return nil, err
		}
		for _, identity := range identities {
			if existing, clash := merged[identity.DocumentID]; clash && existing != identity {
				return nil, vector.ErrConflict
			}
			merged[identity.DocumentID] = identity
		}
	}
	return merged, nil
}

// ListIdentities merges the bounded observation windows of every registered
// projection in stable document-ID order.
func (r *Router) ListIdentities(ctx context.Context, limit int) ([]vector.DocumentIdentity, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var merged []vector.DocumentIdentity
	for _, fingerprint := range r.order {
		reader, ok := r.entries[fingerprint].Store.(vector.IdentityReader)
		if !ok {
			continue
		}
		identities, err := reader.ListIdentities(ctx, limit)
		if err != nil {
			return nil, err
		}
		merged = append(merged, identities...)
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].DocumentID < merged[j].DocumentID })
	if len(merged) > limit {
		merged = merged[:limit]
	}
	return merged, nil
}

var _ vector.VectorStore = (*Router)(nil)
var _ vector.IdentityReader = (*Router)(nil)

// tenant unused guard for future tenant-scoped routing.
var _ = tenant.TenantContext{}
