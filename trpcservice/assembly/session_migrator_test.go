package assembly

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credential"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type noopSessionSummarizer struct{}

func (noopSessionSummarizer) ShouldSummarize(*session.Session) bool { return false }
func (noopSessionSummarizer) Summarize(context.Context, *session.Session) (string, error) {
	return "", nil
}
func (noopSessionSummarizer) SetPrompt(string)         {}
func (noopSessionSummarizer) SetModel(model.Model)     {}
func (noopSessionSummarizer) Metadata() map[string]any { return nil }

type memorySessionMigrationStore struct {
	status platformstorage.SessionMigrationStatus
}

func (s *memorySessionMigrationStore) CreateSessionMigration(_ context.Context, status platformstorage.SessionMigrationStatus) (platformstorage.SessionMigrationStatus, error) {
	if s.status.ID != "" && s.status.Phase != platformstorage.SessionMigrationDone && s.status.Phase != platformstorage.SessionMigrationRolledBack {
		return platformstorage.SessionMigrationStatus{}, platformstorage.ErrSessionMigrationConflict
	}
	status.ID = "migration-1"
	status.Generation = 1
	status.Phase = platformstorage.SessionMigrationPrepared
	s.status = status
	return status, nil
}

func (s *memorySessionMigrationStore) GetSessionMigration(_ context.Context, tenantID, appCode, id string) (platformstorage.SessionMigrationStatus, error) {
	if s.status.ID != id || s.status.TenantID != tenantID || s.status.AppCode != appCode {
		return platformstorage.SessionMigrationStatus{}, platformstorage.ErrSessionMigrationNotFound
	}
	return s.status, nil
}

func (s *memorySessionMigrationStore) UpdateSessionMigration(_ context.Context, status platformstorage.SessionMigrationStatus, expected uint64) (platformstorage.SessionMigrationStatus, error) {
	if s.status.Generation != expected || status.Generation != expected {
		return platformstorage.SessionMigrationStatus{}, platformstorage.ErrSessionMigrationConflict
	}
	status.Generation++
	s.status = status
	return status, nil
}

func (s *memorySessionMigrationStore) ActiveSessionMigration(context.Context, string, string) (platformstorage.SessionMigrationStatus, bool, error) {
	if s.status.ID == "" || s.status.Phase == platformstorage.SessionMigrationDone || s.status.Phase == platformstorage.SessionMigrationRolledBack {
		return platformstorage.SessionMigrationStatus{}, false, nil
	}
	return s.status, true, nil
}

func (s *memorySessionMigrationStore) ListSessionMigrations(context.Context, string, string, int) ([]platformstorage.SessionMigrationStatus, error) {
	if s.status.ID == "" {
		return nil, nil
	}
	return []platformstorage.SessionMigrationStatus{s.status}, nil
}

type staticApplicationSessionCatalog struct {
	sessions []platformstorage.Session
}

func (c staticApplicationSessionCatalog) ListApplicationSessions(context.Context, string, string) ([]platformstorage.Session, error) {
	return append([]platformstorage.Session(nil), c.sessions...), nil
}

func TestSessionMigratorBackfillsVerifiesAndPublishesTarget(t *testing.T) {
	ctx := context.Background()
	redisServer := miniredis.RunT(t)
	resolver, err := credential.NewEnvironmentSecretResolver(func(name string) string {
		if name == "SESSION_REDIS_URL" {
			return "redis://" + redisServer.Addr()
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	profiles := testBackendProfileResolver{
		"session-source": {Driver: SessionDriverInMemory},
		"session-target": {Driver: SessionDriverRedis, ConnectionRef: "env:SESSION_REDIS_URL"},
	}
	provider, err := NewManagedSessionProvider("postgres://unused", resolver, profiles, noopSessionSummarizer{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	repository := tenant.NewMemoryRepository()
	active := config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Storage: config.StoragePolicy{Session: config.BackendProfileRef{ProfileID: "session-source"}},
	}
	if _, err := repository.Publish(ctx, active); err != nil {
		t.Fatal(err)
	}

	entry := platformstorage.Session{
		TenantID: "tenant-a", AppCode: "support", SessionKey: "tenant-a/support/session/session-1",
		SubjectID: "user-1", Status: "active",
	}
	source, err := provider.SessionServiceFor(ctx, active, platformstorage.SessionBackendRef{Driver: SessionDriverInMemory})
	if err != nil {
		t.Fatal(err)
	}
	key := frameworkSessionKey(entry)
	created, err := source.CreateSession(ctx, key, session.StateMap{"preference": []byte("concise")})
	if err != nil {
		t.Fatal(err)
	}
	response := &model.Response{Choices: []model.Choice{{Message: model.NewUserMessage("remember this")}}}
	if err := source.AppendEvent(ctx, created, event.NewResponseEvent("inv-1", "user", response)); err != nil {
		t.Fatal(err)
	}

	store := &memorySessionMigrationStore{}
	leaser := platformstorage.NewMemoryStateStore()
	migrator, err := NewSessionMigrator(store, repository, provider, staticApplicationSessionCatalog{sessions: []platformstorage.Session{entry}}, leaser)
	if err != nil {
		t.Fatal(err)
	}
	status, err := migrator.StartSessionMigration(ctx, "tenant-a", "support", "session-target")
	if err != nil {
		t.Fatal(err)
	}

	wantPhases := []platformstorage.SessionMigrationPhase{
		platformstorage.SessionMigrationDualWrite,
		platformstorage.SessionMigrationBackfill,
		platformstorage.SessionMigrationVerify,
		platformstorage.SessionMigrationCutRead,
		platformstorage.SessionMigrationStopOldWrite,
		platformstorage.SessionMigrationDone,
	}
	for _, want := range wantPhases {
		status, err = migrator.AdvanceSessionMigration(ctx, "tenant-a", "support", status.ID, status.Generation)
		if err != nil {
			t.Fatalf("advance to %s: %v", want, err)
		}
		if status.Phase != want {
			t.Fatalf("phase = %s, want %s", status.Phase, want)
		}
	}
	if status.BackfilledSessions != 1 || status.VerifiedSessions != 1 {
		t.Fatalf("migration counts = backfilled:%d verified:%d", status.BackfilledSessions, status.VerifiedSessions)
	}
	target, err := provider.SessionServiceFor(ctx, active, platformstorage.SessionBackendRef{Driver: SessionDriverRedis, ConnectionRef: "env:SESSION_REDIS_URL"})
	if err != nil {
		t.Fatal(err)
	}
	copied, err := target.GetSession(ctx, key, session.WithEventNum(10))
	if err != nil || copied == nil || len(copied.Events) != 1 || string(copied.SnapshotState()["preference"]) != "concise" {
		t.Fatalf("target session = %#v, %v", copied, err)
	}
	published, err := repository.GetActive(ctx, "tenant-a", "support")
	if err != nil {
		t.Fatal(err)
	}
	if published.Config.ConfigVersion != 2 || published.Config.Storage.Session.ProfileID != "session-target" {
		t.Fatalf("published target config = %#v", published.Config.Storage.Session)
	}
}

func TestSessionMigratorRejectsStaleGenerationAndLateRollback(t *testing.T) {
	store := &memorySessionMigrationStore{status: platformstorage.SessionMigrationStatus{
		ID: "migration-1", TenantID: "tenant-a", AppCode: "support", Generation: 3,
		Phase: platformstorage.SessionMigrationStopOldWrite,
	}}
	migrator := &SessionMigrator{store: store}
	if _, err := migrator.RollbackSessionMigration(context.Background(), "tenant-a", "support", "migration-1", 2); !errors.Is(err, platformstorage.ErrSessionMigrationConflict) {
		t.Fatalf("stale rollback error = %v", err)
	}
	if _, err := migrator.RollbackSessionMigration(context.Background(), "tenant-a", "support", "migration-1", 3); err == nil {
		t.Fatal("late rollback unexpectedly succeeded")
	}
}

var _ platformstorage.SessionMigrationStore = (*memorySessionMigrationStore)(nil)
var _ platformstorage.ApplicationSessionLister = staticApplicationSessionCatalog{}
