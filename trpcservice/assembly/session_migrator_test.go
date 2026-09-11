package assembly

import (
	"context"
	"errors"
	"testing"
	"time"

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
	status  platformstorage.SessionMigrationStatus
	repairs memorySessionRepairStore
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

func (s *memorySessionMigrationStore) ActiveSessionMigrationRoute(ctx context.Context, tenantID, appCode string) (platformstorage.SessionMigrationRoute, bool, error) {
	status, ok, err := s.ActiveSessionMigration(ctx, tenantID, appCode)
	if err != nil || !ok {
		return platformstorage.SessionMigrationRoute{}, ok, err
	}
	route, err := status.Route()
	return route, true, err
}

func (s *memorySessionMigrationStore) UpsertSessionMigrationRepair(ctx context.Context, repair platformstorage.SessionMigrationRepair) (platformstorage.SessionMigrationRepair, error) {
	return s.repairs.UpsertSessionMigrationRepair(ctx, repair)
}

func (s *memorySessionMigrationStore) CountPendingSessionMigrationRepairs(ctx context.Context, tenantID, migrationID string) (int, error) {
	return s.repairs.CountPendingSessionMigrationRepairs(ctx, tenantID, migrationID)
}

func (s *memorySessionMigrationStore) ClaimSessionMigrationRepairs(ctx context.Context, tenantID, migrationID, owner string, limit int, ttl time.Duration) ([]platformstorage.SessionMigrationRepair, error) {
	return s.repairs.ClaimSessionMigrationRepairs(ctx, tenantID, migrationID, owner, limit, ttl)
}

func (s *memorySessionMigrationStore) CompleteSessionMigrationRepair(ctx context.Context, repair platformstorage.SessionMigrationRepair, owner string) (bool, error) {
	return s.repairs.CompleteSessionMigrationRepair(ctx, repair, owner)
}

func (s *memorySessionMigrationStore) FailSessionMigrationRepair(ctx context.Context, repair platformstorage.SessionMigrationRepair, owner string, retryAt time.Time, failure string) error {
	return s.repairs.FailSessionMigrationRepair(ctx, repair, owner, retryAt, failure)
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
	trackSource, ok := source.(session.TrackService)
	if !ok {
		t.Fatal("source Session backend does not expose TrackService")
	}
	if err := trackSource.AppendTrackEvent(ctx, created, &session.TrackEvent{Track: "trace", Payload: []byte(`{"step":"source"}`)}); err != nil {
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
	if copied.Tracks["trace"] == nil || len(copied.Tracks["trace"].Events) != 1 {
		t.Fatalf("target Session track events = %#v", copied.Tracks)
	}
	published, err := repository.GetActive(ctx, "tenant-a", "support")
	if err != nil {
		t.Fatal(err)
	}
	if published.Config.ConfigVersion != 2 || published.Config.Storage.Session.ProfileID != "session-target" {
		t.Fatalf("published target config = %#v", published.Config.Storage.Session)
	}
}

func TestSessionMigratorRollbackRestoresSourceOnlyWrites(t *testing.T) {
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
	store := &memorySessionMigrationStore{}
	provider, err := NewManagedSessionProvider("postgres://unused", resolver, profiles, noopSessionSummarizer{}, store)
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
	migrator, err := NewSessionMigrator(store, repository, provider, staticApplicationSessionCatalog{}, platformstorage.NewMemoryStateStore())
	if err != nil {
		t.Fatal(err)
	}
	status, err := migrator.StartSessionMigration(ctx, "tenant-a", "support", "session-target")
	if err != nil {
		t.Fatal(err)
	}
	status, err = migrator.AdvanceSessionMigration(ctx, "tenant-a", "support", status.ID, status.Generation)
	if err != nil || status.Phase != platformstorage.SessionMigrationDualWrite {
		t.Fatalf("advance to dual write = %+v, %v", status, err)
	}
	status, err = migrator.RollbackSessionMigration(ctx, "tenant-a", "support", status.ID, status.Generation)
	if err != nil || status.Phase != platformstorage.SessionMigrationRolledBack {
		t.Fatalf("rollback = %+v, %v", status, err)
	}
	route, err := status.Route()
	if err != nil || !route.Reader.Equal(status.Source) || !route.PrimaryWriter.Equal(status.Source) || len(route.ReplicaWriters) != 0 {
		t.Fatalf("rollback route = %+v, %v", route, err)
	}

	service, err := provider.Session(ctx, active)
	if err != nil {
		t.Fatal(err)
	}
	key := session.Key{AppName: active.AppName(), UserID: "customer-1", SessionID: "after-rollback"}
	if _, err := service.CreateSession(ctx, key, session.StateMap{"mode": []byte("source-only")}); err != nil {
		t.Fatalf("create session after rollback: %v", err)
	}
	source, err := provider.SessionServiceFor(ctx, active, status.Source)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := source.GetSession(ctx, key); err != nil || got == nil {
		t.Fatalf("source session after rollback = %#v, %v", got, err)
	}
	target, err := provider.SessionServiceFor(ctx, active, status.Target)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := target.GetSession(ctx, key); err == nil && got != nil {
		t.Fatalf("target received post-rollback write: %#v", got)
	}
}

func TestSessionRepairUsesIntentDirectionAfterRouteFlips(t *testing.T) {
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

	configuration := config.TenantConfig{TenantID: "tenant-a", AppCode: "support"}
	sourceRef := platformstorage.SessionBackendRef{ProfileID: "session-source", Driver: SessionDriverInMemory}
	targetRef := platformstorage.SessionBackendRef{ProfileID: "session-target", Driver: SessionDriverRedis, ConnectionRef: "env:SESSION_REDIS_URL"}
	source, err := provider.SessionServiceFor(ctx, configuration, sourceRef)
	if err != nil {
		t.Fatal(err)
	}
	target, err := provider.SessionServiceFor(ctx, configuration, targetRef)
	if err != nil {
		t.Fatal(err)
	}
	key := session.Key{AppName: configuration.AppName(), UserID: "user-1", SessionID: "tenant-a/support/session/session-1"}
	if _, err := source.CreateSession(ctx, key, session.StateMap{"version": []byte("source-newer")}); err != nil {
		t.Fatal(err)
	}
	if _, err := target.CreateSession(ctx, key, session.StateMap{"version": []byte("target-older")}); err != nil {
		t.Fatal(err)
	}

	status := platformstorage.SessionMigrationStatus{
		ID: "migration-1", TenantID: configuration.TenantID, AppCode: configuration.AppCode,
		Generation: 8, SourceProfileID: sourceRef.ProfileID, TargetProfileID: targetRef.ProfileID,
		Source: sourceRef, Target: targetRef, Phase: platformstorage.SessionMigrationCutRead,
	}
	migrator := &SessionMigrator{
		provider: provider, leaser: platformstorage.NewMemoryStateStore(),
		leaseTTL: sessionMigrationLeaseTTL, maximumEvents: sessionMigrationEventCap,
	}
	repair := platformstorage.SessionMigrationRepair{
		MigrationID: status.ID, TenantID: status.TenantID, AppCode: status.AppCode,
		Scope: platformstorage.SessionRepairScopeSession, ScopeKey: key.SessionID, SubjectID: key.UserID,
		RouteGeneration: 7, PrimaryProfileID: sourceRef.ProfileID, ReplicaProfileID: targetRef.ProfileID,
	}
	if err := migrator.repairOne(ctx, configuration, status, repair); err != nil {
		t.Fatalf("repairOne() error = %v", err)
	}
	got, err := target.GetSession(ctx, key)
	if err != nil || got == nil || string(got.SnapshotState()["version"]) != "source-newer" {
		t.Fatalf("target after intent-directed repair = %#v, %v", got, err)
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
