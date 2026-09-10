package web

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

type consoleSessionMigrationStore struct {
	status storage.SessionMigrationStatus
}

func (s *consoleSessionMigrationStore) CreateSessionMigration(context.Context, storage.SessionMigrationStatus) (storage.SessionMigrationStatus, error) {
	return storage.SessionMigrationStatus{}, errors.New("unused")
}
func (s *consoleSessionMigrationStore) GetSessionMigration(context.Context, string, string, string) (storage.SessionMigrationStatus, error) {
	return s.status, nil
}
func (s *consoleSessionMigrationStore) UpdateSessionMigration(context.Context, storage.SessionMigrationStatus, uint64) (storage.SessionMigrationStatus, error) {
	return storage.SessionMigrationStatus{}, errors.New("unused")
}
func (s *consoleSessionMigrationStore) ActiveSessionMigration(context.Context, string, string) (storage.SessionMigrationStatus, bool, error) {
	return s.status, s.status.ID != "", nil
}
func (s *consoleSessionMigrationStore) ListSessionMigrations(context.Context, string, string, int) ([]storage.SessionMigrationStatus, error) {
	if s.status.ID == "" {
		return nil, nil
	}
	return []storage.SessionMigrationStatus{s.status}, nil
}

type consoleSessionMigrator struct {
	status storage.SessionMigrationStatus
}

func (m *consoleSessionMigrator) StartSessionMigration(context.Context, string, string, string) (storage.SessionMigrationStatus, error) {
	return m.status, nil
}
func (m *consoleSessionMigrator) AdvanceSessionMigration(_ context.Context, _, _, _ string, generation uint64) (storage.SessionMigrationStatus, error) {
	if generation != m.status.Generation {
		return storage.SessionMigrationStatus{}, storage.ErrSessionMigrationConflict
	}
	return m.status, nil
}
func (m *consoleSessionMigrator) RollbackSessionMigration(ctx context.Context, tenantID, appCode, id string, generation uint64) (storage.SessionMigrationStatus, error) {
	return m.AdvanceSessionMigration(ctx, tenantID, appCode, id, generation)
}

type consoleKnowledgeMigrationStore struct {
	status storage.KnowledgeMigrationStatus
	active bool
}

func (s *consoleKnowledgeMigrationStore) CreateKnowledgeMigration(context.Context, storage.KnowledgeMigrationStatus) (storage.KnowledgeMigrationStatus, error) {
	return storage.KnowledgeMigrationStatus{}, errors.New("unused")
}
func (s *consoleKnowledgeMigrationStore) GetKnowledgeMigration(context.Context, string, string, string) (storage.KnowledgeMigrationStatus, error) {
	return s.status, nil
}
func (s *consoleKnowledgeMigrationStore) UpdateKnowledgeMigration(context.Context, storage.KnowledgeMigrationStatus, uint64) (storage.KnowledgeMigrationStatus, error) {
	return storage.KnowledgeMigrationStatus{}, errors.New("unused")
}
func (s *consoleKnowledgeMigrationStore) ActiveKnowledgeMigration(context.Context, string, string) (storage.KnowledgeMigrationStatus, bool, error) {
	return s.status, s.active, nil
}
func (s *consoleKnowledgeMigrationStore) ListKnowledgeMigrations(context.Context, string, string, int) ([]storage.KnowledgeMigrationStatus, error) {
	if s.status.ID == "" {
		return nil, nil
	}
	return []storage.KnowledgeMigrationStatus{s.status}, nil
}
func (s *consoleKnowledgeMigrationStore) WithKnowledgeMigrationLock(_ context.Context, _, _ string, operation func() error) error {
	return operation()
}

type consoleKnowledgeMigrator struct {
	status storage.KnowledgeMigrationStatus
}

func (m *consoleKnowledgeMigrator) StartKnowledgeMigration(context.Context, string, string, string) (storage.KnowledgeMigrationStatus, error) {
	return m.status, nil
}
func (m *consoleKnowledgeMigrator) AdvanceKnowledgeMigration(context.Context, string, string, string, uint64) (storage.KnowledgeMigrationStatus, error) {
	return m.status, nil
}
func (m *consoleKnowledgeMigrator) RollbackKnowledgeMigration(context.Context, string, string, string, uint64) (storage.KnowledgeMigrationStatus, error) {
	return m.status, nil
}

func TestConsoleStorageMigrationAPIsPreserveGenerationFenceAndSnakeCase(t *testing.T) {
	sessionStatus := storage.SessionMigrationStatus{
		ID: "session-migration-1", TenantID: "example", AppCode: "support", Generation: 2,
		SourceProfileID: "platform-postgres", TargetProfileID: "session-redis",
		Source: storage.SessionBackendRef{Driver: "postgres"}, Target: storage.SessionBackendRef{Driver: "redis", ConnectionRef: "env:SECRET_REDIS"},
		Phase: storage.SessionMigrationDualWrite,
	}
	knowledgeStatus := storage.KnowledgeMigrationStatus{
		ID: "knowledge-migration-1", TenantID: "example", AppCode: "support", Generation: 3,
		SourceProfileID: "platform-pgvector", TargetProfileID: "knowledge-qdrant",
		Phase: storage.KnowledgeMigrationReindexed,
	}
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.SessionMigrationStore = &consoleSessionMigrationStore{status: sessionStatus}
		dependencies.SessionMigrator = &consoleSessionMigrator{status: sessionStatus}
		dependencies.KnowledgeMigrationStore = &consoleKnowledgeMigrationStore{status: knowledgeStatus}
		dependencies.KnowledgeMigrator = &consoleKnowledgeMigrator{status: knowledgeStatus}
	})

	stale := handler.request(t, http.MethodPatch, "/api/v1/storage/session-migrations",
		`{"tenant_id":"example","app_code":"support","migration_id":"session-migration-1","generation":1,"action":"advance"}`)
	if stale.Code != http.StatusConflict {
		t.Fatalf("stale Session migration status = %d: %s", stale.Code, stale.Body.String())
	}

	listed := handler.request(t, http.MethodGet, "/api/v1/storage/knowledge-migrations?tenant=example&app=support", "")
	if listed.Code != http.StatusOK {
		t.Fatalf("knowledge migrations status = %d: %s", listed.Code, listed.Body.String())
	}
	body := listed.Body.String()
	for _, expected := range []string{`"migration_id":"knowledge-migration-1"`, `"generation":3`, `"phase":"reindexed"`} {
		if !strings.Contains(body, expected) {
			t.Fatalf("knowledge migration response missing %s: %s", expected, body)
		}
	}
	if strings.Contains(body, "connection_ref") || strings.Contains(body, "source_driver") || strings.Contains(body, "target_driver") {
		t.Fatalf("knowledge migration response leaked physical backend details: %s", body)
	}
	if strings.Contains(body, `"ID"`) || strings.Contains(body, `"Generation"`) {
		t.Fatalf("knowledge migration response leaked Go field names: %s", body)
	}
}

func TestConsolePausesKnowledgeWritesDuringBackendMigration(t *testing.T) {
	queue := &recordingKnowledgeIngestQueue{}
	store := &consoleKnowledgeMigrationStore{
		active: true,
		status: storage.KnowledgeMigrationStatus{
			ID: "knowledge-migration-1", TenantID: "example", AppCode: "support", Generation: 1,
			Phase: storage.KnowledgeMigrationPrepared,
		},
	}
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		dependencies.KnowledgeIngest = queue
		dependencies.KnowledgeMigrationStore = store
		dependencies.KnowledgeMigrator = &consoleKnowledgeMigrator{status: store.status}
	})

	create := handler.request(t, http.MethodPost, "/api/v1/knowledge",
		`{"tenant_id":"example","app_code":"support","document_id":"blocked","content":"must not be queued"}`)
	if create.Code != http.StatusConflict || !strings.Contains(create.Body.String(), "knowledge-migration-1") {
		t.Fatalf("knowledge create during migration = %d: %s", create.Code, create.Body.String())
	}
	if queue.request.DocumentID != "" {
		t.Fatalf("knowledge write reached ingest queue during migration: %#v", queue.request)
	}

	remove := handler.request(t, http.MethodDelete, "/api/v1/knowledge?tenant=example&app=support&document_id=blocked", "")
	if remove.Code != http.StatusConflict || queue.cancelled != "" {
		t.Fatalf("knowledge delete during migration = %d cancelled=%q body=%s", remove.Code, queue.cancelled, remove.Body.String())
	}
}

var _ storage.SessionMigrationStore = (*consoleSessionMigrationStore)(nil)
var _ storage.KnowledgeMigrationStore = (*consoleKnowledgeMigrationStore)(nil)
