package knowledgeingest

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/source"
)

type memoryKnowledgeMigrationStore struct {
	status storage.KnowledgeMigrationStatus
}

func (s *memoryKnowledgeMigrationStore) CreateKnowledgeMigration(_ context.Context, status storage.KnowledgeMigrationStatus) (storage.KnowledgeMigrationStatus, error) {
	if s.status.ID != "" && s.status.Phase != storage.KnowledgeMigrationDone && s.status.Phase != storage.KnowledgeMigrationRolledBack {
		return storage.KnowledgeMigrationStatus{}, storage.ErrKnowledgeMigrationConflict
	}
	status.ID = "knowledge-migration-1"
	status.Generation = 1
	status.Phase = storage.KnowledgeMigrationPrepared
	s.status = status
	return status, nil
}

func (s *memoryKnowledgeMigrationStore) GetKnowledgeMigration(_ context.Context, tenantID, appCode, id string) (storage.KnowledgeMigrationStatus, error) {
	if s.status.ID != id || s.status.TenantID != tenantID || s.status.AppCode != appCode {
		return storage.KnowledgeMigrationStatus{}, errors.New("not found")
	}
	return s.status, nil
}

func (s *memoryKnowledgeMigrationStore) UpdateKnowledgeMigration(_ context.Context, status storage.KnowledgeMigrationStatus, expected uint64) (storage.KnowledgeMigrationStatus, error) {
	if expected == 0 || s.status.Generation != expected || status.Generation != expected {
		return storage.KnowledgeMigrationStatus{}, storage.ErrKnowledgeMigrationConflict
	}
	status.Generation++
	s.status = status
	return status, nil
}

func (s *memoryKnowledgeMigrationStore) ActiveKnowledgeMigration(context.Context, string, string) (storage.KnowledgeMigrationStatus, bool, error) {
	if s.status.ID == "" || s.status.Phase == storage.KnowledgeMigrationDone || s.status.Phase == storage.KnowledgeMigrationRolledBack {
		return storage.KnowledgeMigrationStatus{}, false, nil
	}
	return s.status, true, nil
}

func (s *memoryKnowledgeMigrationStore) ListKnowledgeMigrations(context.Context, string, string, int) ([]storage.KnowledgeMigrationStatus, error) {
	if s.status.ID == "" {
		return nil, nil
	}
	return []storage.KnowledgeMigrationStatus{s.status}, nil
}

func (s *memoryKnowledgeMigrationStore) WithKnowledgeMigrationLock(_ context.Context, _, _ string, operation func() error) error {
	return operation()
}

type staticKnowledgeSources struct {
	sources []storage.KnowledgeDocumentSource
}

func (s staticKnowledgeSources) ListKnowledgeDocumentSources(context.Context, string, string) ([]storage.KnowledgeDocumentSource, error) {
	return append([]storage.KnowledgeDocumentSource(nil), s.sources...), nil
}
func (staticKnowledgeSources) SaveKnowledgeDocumentSnapshot(context.Context, string, string, string, []byte) error {
	return nil
}

type recordingMigrationPipeline struct {
	documents   []storage.KnowledgeDocument
	loads       []storage.KnowledgeLoadRequest
	targetCount map[string]int
	probeErr    error
}

func (p *recordingMigrationPipeline) LoadKnowledgeSource(_ context.Context, request storage.KnowledgeLoadRequest, src source.Source) (int, error) {
	documents, err := src.ReadDocuments(context.Background())
	if err != nil {
		return 0, err
	}
	p.loads = append(p.loads, request)
	return len(documents), nil
}

func (p *recordingMigrationPipeline) CountKnowledgeDocument(_ context.Context, _, _, documentID string, _ config.BackendConfig) (int, error) {
	if documentID == "__migration_probe__" && p.probeErr != nil {
		return 0, p.probeErr
	}
	return p.targetCount[documentID], nil
}

func (p *recordingMigrationPipeline) ListKnowledgeDocuments(context.Context, string, string) ([]storage.KnowledgeDocument, error) {
	return append([]storage.KnowledgeDocument(nil), p.documents...), nil
}

func testKnowledgeBackendProfiles(t *testing.T) *storage.MemoryBackendProfileStore {
	t.Helper()
	profiles := storage.NewMemoryBackendProfileStore()
	if _, err := profiles.CreateBackendProfile(context.Background(), storage.BackendProfile{
		ProfileID: "knowledge-qdrant", DisplayName: "Knowledge Qdrant", Driver: "qdrant", Domains: []string{storage.BackendDomainKnowledge},
		ConnectionRef: "env:QDRANT_URL", Status: storage.BackendProfileActive,
	}); err != nil {
		t.Fatal(err)
	}
	if err := profiles.ReplaceTenantBackendProfiles(context.Background(), "tenant-a", []string{"platform-pgvector", "knowledge-qdrant"}); err != nil {
		t.Fatal(err)
	}
	return profiles
}

func TestKnowledgeMigratorReindexesCanonicalSnapshotThenPublishes(t *testing.T) {
	ctx := context.Background()
	repository := tenant.NewMemoryRepository()
	active := config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Storage: config.StoragePolicy{Knowledge: config.BackendProfileRef{ProfileID: "platform-pgvector"}},
	}
	if _, err := repository.Publish(ctx, active); err != nil {
		t.Fatal(err)
	}
	canonical, err := json.Marshal([]*document.Document{{ID: "chunk-1", Content: "stable snapshot"}})
	if err != nil {
		t.Fatal(err)
	}
	sources := staticKnowledgeSources{sources: []storage.KnowledgeDocumentSource{{
		TenantID: "tenant-a", AppCode: "support", DocumentID: "doc-1", Name: "Guide", CanonicalDocuments: canonical,
	}}}
	pipeline := &recordingMigrationPipeline{
		documents:   []storage.KnowledgeDocument{{TenantID: "tenant-a", AppCode: "support", DocumentID: "doc-1", Status: "ready", TotalChunks: 1}},
		targetCount: map[string]int{"doc-1": 1},
	}
	store := &memoryKnowledgeMigrationStore{}
	migrator, err := NewMigrator(store, repository, sources, pipeline, testKnowledgeBackendProfiles(t))
	if err != nil {
		t.Fatal(err)
	}
	status, err := migrator.StartKnowledgeMigration(ctx, "tenant-a", "support", "knowledge-qdrant")
	if err != nil {
		t.Fatal(err)
	}
	status, err = migrator.AdvanceKnowledgeMigration(ctx, "tenant-a", "support", status.ID, status.Generation)
	if err != nil || status.Phase != storage.KnowledgeMigrationReindexed || status.ReindexedDocuments != 1 {
		t.Fatalf("reindex = %+v, %v", status, err)
	}
	if len(pipeline.loads) != 1 || pipeline.loads[0].Backend.Driver != "qdrant" || pipeline.loads[0].DocumentID != "doc-1" {
		t.Fatalf("reindex calls = %#v", pipeline.loads)
	}
	status, err = migrator.AdvanceKnowledgeMigration(ctx, "tenant-a", "support", status.ID, status.Generation)
	if err != nil || status.Phase != storage.KnowledgeMigrationVerified || status.VerifiedDocuments != 1 {
		t.Fatalf("verify = %+v, %v", status, err)
	}
	status, err = migrator.AdvanceKnowledgeMigration(ctx, "tenant-a", "support", status.ID, status.Generation)
	if err != nil || status.Phase != storage.KnowledgeMigrationDone {
		t.Fatalf("publish = %+v, %v", status, err)
	}
	published, err := repository.GetActive(ctx, "tenant-a", "support")
	if err != nil {
		t.Fatal(err)
	}
	if published.Config.ConfigVersion != 2 || published.Config.Storage.Knowledge.ProfileID != "knowledge-qdrant" {
		t.Fatalf("published knowledge config = %#v", published.Config.Storage.Knowledge)
	}
}

func TestKnowledgeMigratorDoesNotPublishWhenVerificationFails(t *testing.T) {
	ctx := context.Background()
	repository := tenant.NewMemoryRepository()
	active := config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Storage: config.StoragePolicy{Knowledge: config.BackendProfileRef{ProfileID: "platform-pgvector"}},
	}
	if _, err := repository.Publish(ctx, active); err != nil {
		t.Fatal(err)
	}
	canonical, _ := json.Marshal([]*document.Document{{ID: "chunk-1", Content: "snapshot"}})
	store := &memoryKnowledgeMigrationStore{status: storage.KnowledgeMigrationStatus{
		ID: "knowledge-migration-1", TenantID: "tenant-a", AppCode: "support", Generation: 2,
		SourceProfileID: "platform-pgvector", TargetProfileID: "knowledge-qdrant",
		Source: config.BackendConfig{Driver: "pgvector"}, Target: config.BackendConfig{Driver: "qdrant", ConnectionRef: "env:QDRANT_URL"},
		Phase: storage.KnowledgeMigrationReindexed, ReindexedDocuments: 1,
	}}
	pipeline := &recordingMigrationPipeline{
		documents:   []storage.KnowledgeDocument{{TenantID: "tenant-a", AppCode: "support", DocumentID: "doc-1", Status: "ready", TotalChunks: 2}},
		targetCount: map[string]int{"doc-1": 1},
	}
	migrator, err := NewMigrator(store, repository, staticKnowledgeSources{sources: []storage.KnowledgeDocumentSource{{DocumentID: "doc-1", CanonicalDocuments: canonical}}}, pipeline, testKnowledgeBackendProfiles(t))
	if err != nil {
		t.Fatal(err)
	}
	status, err := migrator.AdvanceKnowledgeMigration(ctx, "tenant-a", "support", store.status.ID, store.status.Generation)
	if err == nil || status.Phase != storage.KnowledgeMigrationReindexed || status.LastError == "" {
		t.Fatalf("verification failure = %+v, %v", status, err)
	}
	published, err := repository.GetActive(ctx, "tenant-a", "support")
	if err != nil {
		t.Fatal(err)
	}
	if published.Config.ConfigVersion != 1 || published.Config.Storage.Knowledge.ProfileID != "platform-pgvector" {
		t.Fatalf("verification failure published config = %#v", published.Config)
	}
}

func TestKnowledgeMigratorRollbackBoundaries(t *testing.T) {
	t.Parallel()
	base := storage.KnowledgeMigrationStatus{
		ID: "migration-1", TenantID: "tenant-a", AppCode: "support", Generation: 3,
		Phase: storage.KnowledgeMigrationReindexed, LastError: "previous error",
	}
	store := &memoryKnowledgeMigrationStore{status: base}
	migrator := &Migrator{store: store}

	if _, err := migrator.RollbackKnowledgeMigration(context.Background(), "tenant-a", "support", base.ID, 0); !errors.Is(err, storage.ErrKnowledgeMigrationConflict) {
		t.Fatalf("Rollback(expected=0) error = %v", err)
	}
	if _, err := migrator.RollbackKnowledgeMigration(context.Background(), "tenant-a", "support", base.ID, 2); !errors.Is(err, storage.ErrKnowledgeMigrationConflict) {
		t.Fatalf("Rollback(stale generation) error = %v", err)
	}
	rolled, err := migrator.RollbackKnowledgeMigration(context.Background(), "tenant-a", "support", base.ID, 3)
	if err != nil || rolled.Phase != storage.KnowledgeMigrationRolledBack || rolled.Generation != 4 || rolled.LastError != "" {
		t.Fatalf("Rollback() = %#v, %v", rolled, err)
	}
	// Already rolled back is idempotent and does not bump generation again.
	again, err := migrator.RollbackKnowledgeMigration(context.Background(), "tenant-a", "support", base.ID, rolled.Generation)
	if err != nil || again.Generation != rolled.Generation || again.Phase != storage.KnowledgeMigrationRolledBack {
		t.Fatalf("Rollback(idempotent) = %#v, %v", again, err)
	}

	store.status = base
	store.status.Phase = storage.KnowledgeMigrationDone
	if _, err := migrator.RollbackKnowledgeMigration(context.Background(), "tenant-a", "support", base.ID, base.Generation); err == nil || !strings.Contains(err.Error(), "cannot be rolled back") {
		t.Fatalf("Rollback(done) error = %v", err)
	}
	if _, err := migrator.RollbackKnowledgeMigration(context.Background(), "tenant-a", "support", "missing", base.Generation); err == nil {
		t.Fatal("Rollback(missing) error = nil")
	}
}

var _ storage.KnowledgeMigrationStore = (*memoryKnowledgeMigrationStore)(nil)
var _ storage.KnowledgeSourceStore = staticKnowledgeSources{}
