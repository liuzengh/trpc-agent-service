package storage

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestSessionMigrationRouteByPhase(t *testing.T) {
	t.Parallel()
	source := SessionBackendRef{Driver: "postgres"}
	target := SessionBackendRef{Driver: "redis", ConnectionRef: "env:SESSION_REDIS"}
	tests := []struct {
		phase       SessionMigrationPhase
		reader      SessionBackendRef
		writerCount int
	}{
		{phase: SessionMigrationPrepared, reader: source, writerCount: 1},
		{phase: SessionMigrationRolledBack, reader: source, writerCount: 1},
		{phase: SessionMigrationDualWrite, reader: source, writerCount: 2},
		{phase: SessionMigrationBackfill, reader: source, writerCount: 2},
		{phase: SessionMigrationVerify, reader: source, writerCount: 2},
		{phase: SessionMigrationCutRead, reader: target, writerCount: 2},
		{phase: SessionMigrationStopOldWrite, reader: target, writerCount: 1},
		{phase: SessionMigrationDone, reader: target, writerCount: 1},
	}
	for _, test := range tests {
		t.Run(string(test.phase), func(t *testing.T) {
			status := SessionMigrationStatus{Source: source, Target: target, Phase: test.phase}
			route, err := status.Route()
			if err != nil {
				t.Fatal(err)
			}
			if !route.Reader.Equal(test.reader) || 1+len(route.ReplicaWriters) != test.writerCount {
				t.Fatalf("Route() = %+v", route)
			}
		})
	}
	if _, err := (SessionMigrationStatus{Phase: "invalid"}).Route(); err == nil {
		t.Fatal("Route() accepted invalid phase")
	}
}

func TestSessionBackendRefNormalizesAndCompares(t *testing.T) {
	t.Parallel()
	backend := (SessionBackendRef{Driver: " REDIS ", ConnectionRef: " env:SESSION_REDIS "}).Normalize("postgres")
	if backend.Driver != "redis" || backend.ConnectionRef != "env:SESSION_REDIS" {
		t.Fatalf("Normalize() = %+v", backend)
	}
	if !backend.Equal(SessionBackendRef{Driver: "redis", ConnectionRef: "env:SESSION_REDIS"}) {
		t.Fatal("Equal() rejected identical backend")
	}
	if got := (SessionBackendRef{}).Normalize("postgres"); got.Driver != "postgres" {
		t.Fatalf("default driver = %q", got.Driver)
	}
}

func TestNewPostgresSessionMigrationStoreRequiresDatabase(t *testing.T) {
	t.Parallel()
	if _, err := NewPostgresSessionMigrationStore(nil); err == nil {
		t.Fatal("NewPostgresSessionMigrationStore(nil) error = nil")
	}
}

func TestPostgresSessionMigrationLifecycle(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	ctx := context.Background()
	const tenantID = "support-session-migration"
	const appCode = "support"
	if _, err := database.ExecContext(ctx, "INSERT INTO tenants (id) VALUES ($1) ON CONFLICT (id) DO NOTHING", tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, "INSERT INTO applications (tenant_id, app_code, status) VALUES ($1,$2,'active') ON CONFLICT (tenant_id, app_code) DO NOTHING", tenantID, appCode); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, "DELETE FROM session_backend_migrations WHERE tenant_id=$1 AND app_code=$2", tenantID, appCode); err != nil {
		t.Fatal(err)
	}

	store, err := NewPostgresSessionMigrationStore(database)
	if err != nil {
		t.Fatal(err)
	}
	base := SessionMigrationStatus{
		TenantID:        tenantID,
		AppCode:         appCode,
		SourceProfileID: "session-postgres",
		TargetProfileID: "session-redis",
		Source:          SessionBackendRef{Driver: "postgres"},
		Target:          SessionBackendRef{Driver: "redis", ConnectionRef: "env:SESSION_REDIS"},
	}

	if _, err := store.CreateSessionMigration(ctx, SessionMigrationStatus{}); err == nil {
		t.Fatal("CreateSessionMigration() accepted incomplete status")
	}
	same := base
	same.Target = same.Source
	if _, err := store.CreateSessionMigration(ctx, same); err == nil {
		t.Fatal("CreateSessionMigration() accepted identical source and target")
	}

	created, err := store.CreateSessionMigration(ctx, base)
	if err != nil {
		t.Fatalf("CreateSessionMigration() error = %v", err)
	}
	if created.ID == "" || created.Generation != 1 || created.Phase != SessionMigrationPrepared || created.CreatedAt.IsZero() {
		t.Fatalf("created migration = %+v", created)
	}
	if _, err := store.CreateSessionMigration(ctx, base); !errors.Is(err, ErrSessionMigrationConflict) {
		t.Fatalf("second CreateSessionMigration() error = %v, want conflict", err)
	}

	loaded, err := store.GetSessionMigration(ctx, tenantID, appCode, created.ID)
	if err != nil || loaded.ID != created.ID || loaded.Generation != 1 {
		t.Fatalf("GetSessionMigration() = %+v, %v", loaded, err)
	}
	if _, err := store.GetSessionMigration(ctx, tenantID, appCode, "missing"); !errors.Is(err, ErrSessionMigrationNotFound) {
		t.Fatalf("GetSessionMigration(missing) error = %v", err)
	}

	active, found, err := store.ActiveSessionMigration(ctx, tenantID, appCode)
	if err != nil || !found || active.ID != created.ID {
		t.Fatalf("ActiveSessionMigration() = %+v, %v, %v", active, found, err)
	}
	route, found, err := store.ActiveSessionMigrationRoute(ctx, tenantID, appCode)
	if err != nil || !found || !route.Reader.Equal(base.Source) {
		t.Fatalf("ActiveSessionMigrationRoute() = %+v, %v, %v", route, found, err)
	}

	if _, err := store.UpdateSessionMigration(ctx, created, 0); !errors.Is(err, ErrSessionMigrationConflict) {
		t.Fatalf("UpdateSessionMigration(generation 0) error = %v", err)
	}
	for _, phase := range []SessionMigrationPhase{
		SessionMigrationDualWrite,
		SessionMigrationBackfill,
		SessionMigrationVerify,
	} {
		created.Phase = phase
		created.BackfilledSessions++
		created.VerifiedSessions++
		created.LastError = ""
		updated, updateErr := store.UpdateSessionMigration(ctx, created, created.Generation)
		if updateErr != nil {
			t.Fatalf("UpdateSessionMigration(%s) error = %v", phase, updateErr)
		}
		if updated.Generation != created.Generation+1 || updated.Phase != phase {
			t.Fatalf("updated migration = %+v", updated)
		}
		created = updated
	}

	repair, err := store.UpsertSessionMigrationRepair(ctx, SessionMigrationRepair{
		MigrationID: created.ID, TenantID: tenantID, AppCode: appCode,
		Scope: SessionRepairScopeSession, ScopeKey: "session-1", SubjectID: "user-1",
		RouteGeneration: created.Generation, PrimaryProfileID: base.SourceProfileID, ReplicaProfileID: base.TargetProfileID,
	})
	if err != nil {
		t.Fatalf("UpsertSessionMigrationRepair() error = %v", err)
	}
	if _, err := store.UpsertSessionMigrationRepair(ctx, SessionMigrationRepair{
		MigrationID: created.ID, TenantID: tenantID, AppCode: appCode,
		Scope: SessionRepairScopeSession, ScopeKey: "wrong-direction", SubjectID: "user-2",
		RouteGeneration: created.Generation, PrimaryProfileID: base.TargetProfileID, ReplicaProfileID: base.SourceProfileID,
	}); !errors.Is(err, ErrSessionMigrationConflict) {
		t.Fatalf("wrong-direction repair intent error = %v, want conflict", err)
	}
	blocked := created
	blocked.Phase = SessionMigrationCutRead
	if _, err := store.UpdateSessionMigration(ctx, blocked, blocked.Generation); !errors.Is(err, ErrSessionMigrationRepairsPending) {
		t.Fatalf("UpdateSessionMigration(cut_read with repair) error = %v, want pending repair", err)
	}
	if completed, err := store.CompleteSessionMigrationRepair(ctx, repair, ""); err != nil || !completed {
		t.Fatalf("CompleteSessionMigrationRepair() = %v, %v", completed, err)
	}
	previousGeneration := created.Generation
	for _, phase := range []SessionMigrationPhase{
		SessionMigrationCutRead,
		SessionMigrationStopOldWrite,
		SessionMigrationDone,
	} {
		created.Phase = phase
		created.BackfilledSessions++
		created.VerifiedSessions++
		updated, updateErr := store.UpdateSessionMigration(ctx, created, created.Generation)
		if updateErr != nil {
			t.Fatalf("UpdateSessionMigration(%s) error = %v", phase, updateErr)
		}
		created = updated
		if phase == SessionMigrationCutRead {
			if _, err := store.UpsertSessionMigrationRepair(ctx, SessionMigrationRepair{
				MigrationID: created.ID, TenantID: tenantID, AppCode: appCode,
				Scope: SessionRepairScopeSession, ScopeKey: "stale-session", SubjectID: "user-2",
				RouteGeneration: previousGeneration, PrimaryProfileID: base.SourceProfileID, ReplicaProfileID: base.TargetProfileID,
			}); !errors.Is(err, ErrSessionMigrationConflict) {
				t.Fatalf("stale repair intent error = %v, want conflict", err)
			}
		}
	}

	stale := created
	stale.Generation--
	if _, err := store.UpdateSessionMigration(ctx, stale, stale.Generation); !errors.Is(err, ErrSessionMigrationConflict) {
		t.Fatalf("stale UpdateSessionMigration() error = %v, want conflict", err)
	}

	if _, found, err := store.ActiveSessionMigration(ctx, tenantID, appCode); err != nil || found {
		t.Fatalf("ActiveSessionMigration(done) found=%v err=%v", found, err)
	}
	if _, found, err := store.ActiveSessionMigrationRoute(ctx, tenantID, appCode); err != nil || found {
		t.Fatalf("ActiveSessionMigrationRoute(done) found=%v err=%v", found, err)
	}
	listed, err := store.ListSessionMigrations(ctx, tenantID, appCode, 0)
	if err != nil || len(listed) != 1 || listed[0].ID != created.ID {
		t.Fatalf("ListSessionMigrations() = %+v, %v", listed, err)
	}
	if _, err := store.ListSessionMigrations(ctx, "", appCode, 10); err == nil {
		t.Fatal("ListSessionMigrations() accepted empty tenant")
	}

	rolledBack := base
	rolledBack.ID = "rollback-migration"
	rolledBack, err = store.CreateSessionMigration(ctx, rolledBack)
	if err != nil {
		t.Fatal(err)
	}
	rolledBack.Phase = SessionMigrationRolledBack
	if _, err := store.UpdateSessionMigration(ctx, rolledBack, rolledBack.Generation); err != nil {
		t.Fatal(err)
	}
}
