package storage

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sort"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

// The pinned Qdrant adapter ignores GetMetadata.Offset. Enumerate bounded
// metadata, sort IDs locally, then checkpoint the last completed ID. Refuse
// larger scopes explicitly rather than silently truncating a migration.
const maxMigrationChunks = 100000
const migrationBatch = 50

func (r *KnowledgeRouter) withSync(ctx context.Context, scope runtimecontext.Scope, fn func(context.Context, *controlplane.KnowledgeSync, func() error) error) error {
	if _, _, err := runtimecontext.ValidateStorageScope(ctx, scope.StorageScope); err != nil {
		return err
	}
	repo, ok := r.repository.(controlplane.KnowledgeSyncRepository)
	if !ok {
		return errors.New("durable knowledge coordination unavailable")
	}
	return repo.WithKnowledgeSync(ctx, scope.TenantID, scope.AppID, fn)
}

func (r *KnowledgeRouter) repairKnowledgeIntents(ctx context.Context, scope runtimecontext.Scope, state *controlplane.KnowledgeSync, save func() error) error {
	ids := make([]string, 0, len(state.Intents))
	for id := range state.Intents {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		intent := state.Intents[id]
		revision, err := r.repository.GetRevision(ctx, scope.TenantID, intent.RevisionID)
		if err != nil {
			return err
		}
		if revision.TenantID != scope.TenantID || revision.AppID != scope.AppID {
			return errors.New("knowledge repair scope mismatch")
		}
		repairScope := scope
		repairScope.RevisionID = revision.ID
		if intent.Deleted {
			err = r.deleteDocument(ctx, repairScope, revision, id)
		} else {
			var doc KnowledgeDocument
			if err = json.Unmarshal(intent.Document, &doc); err != nil {
				return err
			}
			if doc.ID != id {
				return errors.New("invalid knowledge intent identity")
			}
			_, err = r.upsertDocument(ctx, repairScope, revision, doc)
		}
		if err != nil {
			return err
		}
		delete(state.Intents, id)
		if err = save(); err != nil {
			return err
		}
	}
	return nil
}

func (r *KnowledgeRouter) migrationPair(ctx context.Context, scope runtimecontext.Scope, revision controlplane.AgentRevision, id string) (controlplane.BackendMigration, *knowledgeHandle, *knowledgeHandle, error) {
	if revision.TenantID != scope.TenantID || revision.AppID != scope.AppID || revision.ID != scope.RevisionID {
		return controlplane.BackendMigration{}, nil, nil, errors.New("knowledge migration revision scope mismatch")
	}
	m, err := r.repository.GetBackendMigration(ctx, scope.TenantID, id)
	if err != nil {
		return m, nil, nil, err
	}
	if m.AppID != scope.AppID || m.ResourceType != "knowledge" || (m.State != controlplane.MigrationBackfill && m.State != controlplane.MigrationVerify && m.State != controlplane.MigrationCutover) {
		return m, nil, nil, errors.New("knowledge migration is not in a repair/verification state")
	}
	cfg, err := parseRevisionKnowledgeConfig(revision.KnowledgeConfig)
	if err != nil || !cfg.Enabled {
		return m, nil, nil, errors.New("knowledge migration revision must enable knowledge")
	}
	get := func(id string) (*knowledgeHandle, error) {
		b, err := r.repository.GetBackendBinding(ctx, scope.TenantID, id)
		if err != nil {
			return nil, err
		}
		if b.TenantID != scope.TenantID || b.AppID != scope.AppID || b.ResourceType != "knowledge" {
			return nil, errors.New("knowledge migration binding scope mismatch")
		}
		return r.cachedHandle(ctx, scope, revision, cfg, b)
	}
	s, err := get(m.SourceBindingID)
	if err != nil {
		return m, nil, nil, err
	}
	t, err := get(m.TargetBindingID)
	return m, s, t, err
}
func migrationIDs(ctx context.Context, scope runtimecontext.Scope, store vectorstore.VectorStore) ([]string, error) {
	filter := map[string]any{"tenant_id": scope.TenantID, "app_id": scope.AppID}
	count, err := store.Count(ctx, vectorstore.WithCountFilter(filter))
	if err != nil {
		return nil, err
	}
	if count > maxMigrationChunks {
		return nil, errors.New("knowledge migration exceeds 100000-chunk safety bound")
	}
	items, err := store.GetMetadata(ctx, vectorstore.WithGetMetadataFilter(filter), vectorstore.WithGetMetadataLimit(maxMigrationChunks+1))
	if err != nil {
		return nil, err
	}
	if len(items) != count {
		return nil, errors.New("knowledge enumeration incomplete or changed")
	}
	ids := make([]string, 0, len(items))
	for id := range items {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}
func scopedChunk(scope runtimecontext.Scope, doc *document.Document) bool {
	return doc != nil && doc.Metadata["tenant_id"] == scope.TenantID && doc.Metadata["app_id"] == scope.AppID
}

// BackfillKnowledgeBatch copies stored chunks and vectors, including pre-journal
// history, without re-embedding or trying to reconstruct lost original files.
// Each successful chunk advances durable progress. A concurrent write changes
// epoch and restarts the scan; repeated copies use stable vector IDs.
func (r *KnowledgeRouter) BackfillKnowledgeBatch(ctx context.Context, scope runtimecontext.Scope, revision controlplane.AgentRevision, id string) (bool, error) {
	if err := scope.Validate(); err != nil {
		return false, err
	}
	if revision.TenantID != scope.TenantID || revision.AppID != scope.AppID || revision.ID != scope.RevisionID {
		return false, errors.New("knowledge migration revision scope mismatch")
	}
	var done bool
	err := r.withSync(ctx, scope, func(ctx context.Context, state *controlplane.KnowledgeSync, save func() error) error {
		if err := r.repairKnowledgeIntents(ctx, scope, state, save); err != nil {
			return err
		}
		_, source, target, err := r.migrationPair(ctx, scope, revision, id)
		if err != nil {
			return err
		}
		p := state.Migrations[id]
		if p.Epoch != state.Epoch || p.RevisionChecksum != revision.Checksum {
			p = controlplane.KnowledgeProgress{Epoch: state.Epoch, RevisionChecksum: revision.Checksum}
		}
		if p.Backfilled {
			p.Cursor = ""
			p.Chunks = 0
			p.Backfilled = false
		}
		p.Passed = false
		p.VerifyCursor = ""
		p.VerifiedChunks = 0
		state.Migrations[id] = p
		if err := save(); err != nil {
			return err
		}
		ids, err := migrationIDs(ctx, scope, source.store)
		if err != nil {
			return err
		}
		targetIDs, err := migrationIDs(ctx, scope, target.store)
		if err != nil {
			return err
		}
		present := map[string]bool{}
		for _, chunkID := range ids {
			present[chunkID] = true
		}
		// Target-only chunks are stale copies of deleted/shortened documents.
		for _, chunkID := range targetIDs {
			if !present[chunkID] {
				if err := target.store.Delete(ctx, chunkID); err != nil {
					return err
				}
			}
		}
		copied := 0
		for _, chunkID := range ids {
			if chunkID <= p.Cursor {
				continue
			}
			if copied == migrationBatch {
				return nil
			}
			doc, vector, err := source.store.Get(ctx, chunkID)
			if err != nil {
				return err
			}
			if !scopedChunk(scope, doc) || doc.ID != chunkID || len(vector) != source.config.Embedding.Dimensions {
				return errors.New("invalid source knowledge chunk")
			}
			if err := target.store.Add(ctx, doc, vector); err != nil {
				return err
			}
			p.Cursor = chunkID
			p.Chunks++
			state.Migrations[id] = p
			if err := save(); err != nil {
				return err
			}
			copied++
		}
		p.Backfilled = true
		state.Migrations[id] = p
		done = true
		return save()
	})
	return done, err
}

// Verification compares every chunk's text, metadata and vector, plus a top-1
// retrieval probe for each non-zero vector. No request-supplied "passed" flag
// can create this proof. It is invalidated by subsequent platform writes.
func (r *KnowledgeRouter) VerifyKnowledgeBatch(ctx context.Context, scope runtimecontext.Scope, revision controlplane.AgentRevision, id string) (bool, error) {
	if err := scope.Validate(); err != nil {
		return false, err
	}
	var done bool
	err := r.withSync(ctx, scope, func(ctx context.Context, state *controlplane.KnowledgeSync, save func() error) error {
		m, source, target, err := r.migrationPair(ctx, scope, revision, id)
		if err != nil {
			return err
		}
		p := state.Migrations[id]
		if len(state.Intents) > 0 || !p.Backfilled || p.Epoch != state.Epoch || p.RevisionChecksum != revision.Checksum {
			return errors.New("knowledge changed; run backfill before verification")
		}
		if p.Passed {
			done = true
			return nil
		}
		ids, err := migrationIDs(ctx, scope, source.store)
		if err != nil {
			return err
		}
		targets, err := migrationIDs(ctx, scope, target.store)
		if err != nil {
			return err
		}
		if len(ids) != len(targets) {
			return errors.New("knowledge chunk count mismatch")
		}
		for i := range ids {
			if ids[i] != targets[i] {
				return errors.New("knowledge chunk identities mismatch")
			}
		}
		checked := 0
		for _, chunkID := range ids {
			if chunkID <= p.VerifyCursor {
				continue
			}
			if checked == migrationBatch {
				state.Migrations[id] = p
				return save()
			}
			s, sv, err := source.store.Get(ctx, chunkID)
			if err != nil {
				return err
			}
			t, tv, err := target.store.Get(ctx, chunkID)
			if err != nil {
				return err
			}
			if !scopedChunk(scope, s) || !scopedChunk(scope, t) || s.ID != t.ID || s.Content != t.Content || s.Name != t.Name || !sameJSON(s.Metadata, t.Metadata) || !equalVector(sv, tv) || len(sv) != source.config.Embedding.Dimensions {
				return errors.New("knowledge content metadata or embedding mismatch")
			}
			if err := verifyRetrieval(ctx, scope, source, target, sv); err != nil {
				return err
			}
			p.VerifyCursor = chunkID
			p.VerifiedChunks++
			state.Migrations[id] = p
			if err := save(); err != nil {
				return err
			}
			checked++
		}
		p.Passed = true
		state.Migrations[id] = p
		// A full equality scan proves previous secondary failures repaired.
		if m.RepairBacklog > 0 {
			if err := r.repository.AdjustBackendMigrationRepair(ctx, m.TenantID, m.ID, -m.RepairBacklog); err != nil {
				return err
			}
		}
		done = true
		return save()
	})
	return done, err
}
func sameJSON(a, b any) bool {
	aa, e := json.Marshal(a)
	bb, f := json.Marshal(b)
	return e == nil && f == nil && string(aa) == string(bb)
}
func equalVector(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if math.IsNaN(a[i]) || math.IsInf(a[i], 0) || math.IsNaN(b[i]) || math.IsInf(b[i], 0) || math.Abs(a[i]-b[i]) > 1e-5 {
			return false
		}
	}
	return true
}
func verifyRetrieval(ctx context.Context, scope runtimecontext.Scope, s, t *knowledgeHandle, vector []float64) error {
	var norm float64
	for _, v := range vector {
		norm += v * v
	}
	if norm == 0 {
		return errors.New("knowledge retrieval cannot verify a zero vector")
	}
	query := &vectorstore.SearchQuery{Vector: vector, Limit: 1, SearchMode: vectorstore.SearchModeVector, Filter: &vectorstore.SearchFilter{Metadata: map[string]any{"tenant_id": scope.TenantID, "app_id": scope.AppID}}}
	a, err := s.store.Search(ctx, query)
	if err != nil {
		return err
	}
	b, err := t.store.Search(ctx, query)
	if err != nil {
		return err
	}
	// Equal scores allow ties from identical chunks; compare score, not an
	// unstable top-1 ID. Full content/ID equality is checked separately.
	if a == nil || b == nil || len(a.Results) != 1 || len(b.Results) != 1 || !scopedChunk(scope, a.Results[0].Document) || !scopedChunk(scope, b.Results[0].Document) || math.Abs(a.Results[0].Score-b.Results[0].Score) > 1e-4 {
		return errors.New("knowledge retrieval verification failed")
	}
	return nil
}
