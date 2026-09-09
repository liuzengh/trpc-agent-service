//go:build integration

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/datamigration"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/migrationruntime"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type onlineSessionFixture struct {
	db      *sql.DB
	gateDB  *sql.DB
	tenant  *tenant.Tenant
	runtime *migrationruntime.Runtime
	source  session.Service
	target  session.Service
	wrapped session.Service
	key     session.Key
	jobID   string
}

func newOnlineSessionFixture(t *testing.T) onlineSessionFixture {
	t.Helper()
	db := openDatabase(t)
	gateDB := openMigrationGateDatabase(t)
	ctx := context.Background()
	tenantID := "online-session-" + uuid.NewString()
	profiles, err := storage.LoadBackendProfiles(`[
		{"id":"online-redis","backend":"redis","connectionEnv":"SOURCE_REDIS","allowInsecure":true},
		{"id":"online-postgres","backend":"postgres","connectionEnv":"TARGET_POSTGRES","allowInsecure":true}
	]`, func(name string) (string, bool) {
		switch name {
		case "SOURCE_REDIS":
			return os.LookupEnv("TEST_REDIS_URL")
		case "TARGET_POSTGRES":
			return os.LookupEnv("TEST_DATABASE_URL")
		default:
			return "", false
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	tenantValue := &tenant.Tenant{ID: tenantID, ConfigVersion: 1, Storage: tenant.StorageConfig{
		SessionBackend: "redis", SessionProfile: "online-redis", MemoryBackend: "redis", MemoryProfile: "online-redis",
	}}
	config, err := json.Marshal(map[string]any{"storage": tenantValue.Storage})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO tenants(id,name,status,config,config_version) VALUES($1,'online migration','active',$2,1)`, tenantID, config); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM data_migration_live_intents WHERE migration_id IN (SELECT id FROM data_migrations WHERE tenant_id=$1)`, tenantID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM data_migration_live_journal WHERE migration_id IN (SELECT id FROM data_migrations WHERE tenant_id=$1)`, tenantID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM data_migration_live_routes WHERE tenant_id=$1`, tenantID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM data_migrations WHERE tenant_id=$1`, tenantID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM control_plane_audit WHERE tenant_id=$1`, tenantID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM audit_logs WHERE tenant_id=$1`, tenantID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM tenants WHERE id=$1`, tenantID)
	})
	runtime, err := migrationruntime.New(migrationruntime.Options{DB: db, GateDB: gateDB, StorageProfiles: profiles, BatchSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	factory := storage.NewBackendFactoryWithProfiles(profiles)
	source, err := factory.CreateSessionServiceForTenant(tenantID, &tenantValue.Storage)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	targetConfig := tenantValue.Storage
	targetConfig.SessionBackend, targetConfig.SessionProfile = "postgres", "online-postgres"
	target, err := factory.CreateSessionServiceForTenant(tenantID, &targetConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = target.Close() })
	appName, err := storage.TenantScopedAppName(tenantValue, "support")
	if err != nil {
		t.Fatal(err)
	}
	key := session.Key{AppName: appName, UserID: "owner-1", SessionID: "session-1"}
	// This row predates the migration decorator, exercising real backend discovery.
	if _, err := source.CreateSession(ctx, key, session.StateMap{"phase": []byte("original")}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = source.DeleteSession(context.Background(), key)
		_ = target.DeleteSession(context.Background(), key)
	})
	wrapped, err := runtime.SessionDecorator()(tenantValue, source)
	if err != nil {
		t.Fatal(err)
	}
	fixture := onlineSessionFixture{db: db, gateDB: gateDB, tenant: tenantValue, runtime: runtime, source: source, target: target, wrapped: wrapped, key: key, jobID: "live-" + uuid.NewString()}
	fixture.append(t, source, "before-migration")
	return fixture
}

func TestOnlineSessionConcurrentReadsUseIndependentGatePool(t *testing.T) {
	f := newOnlineSessionFixture(t)
	f.create(t)
	f.advance(t, datamigration.PhaseRollbackWindow)
	if _, err := f.runtime.Coordinator.Complete(context.Background(), f.jobID, "operator", "verified target"); err != nil {
		t.Fatal(err)
	}
	// Keep metadata capacity deliberately smaller than both the gate pool and
	// offered load. Lock holders must finish through the independent SQL pool.
	f.db.SetMaxOpenConns(3)
	f.gateDB.SetMaxOpenConns(8)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const concurrency = 25
	start := make(chan struct{})
	results := make(chan error, concurrency)
	for range concurrency {
		go func() {
			<-start
			for range 3 {
				value, err := f.wrapped.GetSession(ctx, f.key)
				if err != nil {
					results <- err
					return
				}
				if value == nil || len(value.Events) != 1 {
					results <- errors.New("completed migration lost session history")
					return
				}
			}
			results <- nil
		}()
	}
	close(start)
	for range concurrency {
		if err := <-results; err != nil {
			t.Error(err)
		}
	}
	if f.db.Stats().InUse != 0 || f.gateDB.Stats().InUse != 0 {
		t.Fatal("completed Session operations retained control or gate connections")
	}
	// Terminating the owning runtime must close its Session handles while the
	// caller-owned gate and metadata pools remain usable for orderly cleanup.
	if err := f.runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.gateDB.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
}

type failingLiveSessionTarget struct {
	datamigration.LiveBackend
	metadata datamigration.LiveBackendMetadata
	fail     *atomic.Bool
}

func (b failingLiveSessionTarget) MigrationInfo() datamigration.LiveBackendInfo {
	return b.metadata.MigrationInfo()
}

func (b failingLiveSessionTarget) Apply(ctx context.Context, record datamigration.Record) error {
	if b.fail.Swap(false) {
		return errors.New("injected target write interruption")
	}
	return b.LiveBackend.Apply(ctx, record)
}

func TestOnlineSessionRecoversFailedDeleteBeforeRecreation(t *testing.T) {
	f := newOnlineSessionFixture(t)
	f.create(t)
	f.advance(t, datamigration.PhaseValidate)
	var fail atomic.Bool
	coordinator, err := datamigration.NewLiveCoordinator(datamigration.LiveOptions{
		DB: f.db, GateDB: f.gateDB, Owner: "delete-recreate", BatchSize: 1,
		Resolve: func(ctx context.Context, tenantID string, domain datamigration.Domain, profile string) (datamigration.LiveBackend, func(), error) {
			backend, release, err := f.runtime.Sessions.Resolve(ctx, tenantID, domain, profile)
			if err == nil && profile == "online-postgres" {
				return failingLiveSessionTarget{LiveBackend: backend, metadata: backend.(datamigration.LiveBackendMetadata), fail: &fail}, release, nil
			}
			return backend, release, err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	wrapped, err := f.runtime.Sessions.Decorator(coordinator)(f.tenant, f.source)
	if err != nil {
		t.Fatal(err)
	}
	fail.Store(true)
	if err := wrapped.DeleteSession(context.Background(), f.key); err == nil {
		t.Fatal("injected target failure was hidden")
	}
	var pending int
	if err := f.db.QueryRow(`SELECT count(*) FROM data_migration_live_intents WHERE migration_id=$1`, f.jobID).Scan(&pending); err != nil || pending != 1 {
		t.Fatalf("delete intent not retained: count=%d err=%v", pending, err)
	}
	if _, err := wrapped.CreateSession(context.Background(), f.key, session.StateMap{"phase": []byte("recreated")}); err != nil {
		t.Fatal(err)
	}
	f.append(t, wrapped, "new-history")
	f.assertEvents(t, f.source, "new-history")
	f.assertEvents(t, f.target, "new-history")
	f.advance(t, datamigration.PhaseRollbackWindow)
}

func TestOnlineSessionRollbackSurvivesUnavailableTarget(t *testing.T) {
	f := newOnlineSessionFixture(t)
	f.create(t)
	f.advance(t, datamigration.PhaseRollbackWindow)
	f.append(t, f.wrapped, "latest-before-target-failure")
	coordinator, err := datamigration.NewLiveCoordinator(datamigration.LiveOptions{
		DB: f.db, GateDB: f.gateDB, Owner: "target-failure-recovery", BatchSize: 1,
		Resolve: func(ctx context.Context, tenantID string, domain datamigration.Domain, profile string) (datamigration.LiveBackend, func(), error) {
			if profile == "online-postgres" {
				return nil, nil, errors.New("injected target unavailable")
			}
			return f.runtime.Sessions.Resolve(ctx, tenantID, domain, profile)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Rollback(context.Background(), f.jobID, "operator", "target unavailable"); err != nil {
		t.Fatal(err)
	}
	f.assertEvents(t, f.wrapped, "before-migration", "latest-before-target-failure")
	f.append(t, f.wrapped, "after-target-failure-rollback")
	f.assertEvents(t, f.source, "before-migration", "latest-before-target-failure", "after-target-failure-rollback")
}

func TestOnlineSessionConfigConflictCanAbortWithoutLosingSource(t *testing.T) {
	f := newOnlineSessionFixture(t)
	f.create(t)
	f.advance(t, datamigration.PhaseCutover)
	if _, err := f.db.Exec(`UPDATE tenants SET config=jsonb_set(config,'{operator_note}','"preserve-me"'),config_version=config_version+1 WHERE id=$1`, f.tenant.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.runtime.Coordinator.RunOnce(context.Background(), f.jobID); !errors.Is(err, datamigration.ErrMigrationConflict) {
		t.Fatalf("stale configuration cutover accepted: %v", err)
	}
	if _, err := f.runtime.Coordinator.Abort(context.Background(), f.jobID, "operator", "configuration changed"); err != nil {
		t.Fatal(err)
	}
	f.append(t, f.wrapped, "after-abort")
	f.assertEvents(t, f.source, "before-migration", "after-abort")
	var note string
	if err := f.db.QueryRow(`SELECT config->>'operator_note' FROM tenants WHERE id=$1`, f.tenant.ID).Scan(&note); err != nil || note != "preserve-me" {
		t.Fatalf("unrelated config was lost: %q, %v", note, err)
	}
}

func (f onlineSessionFixture) append(t *testing.T, service session.Service, id string) {
	t.Helper()
	ctx := context.Background()
	value, err := service.GetSession(ctx, f.key, session.WithEventNum(100))
	if err != nil || value == nil {
		t.Fatalf("read session for append: value=%v err=%v", value, err)
	}
	if err := service.AppendEvent(ctx, value, &event.Event{
		ID: id, Timestamp: time.Date(2026, 9, 6, 0, 0, len(value.Events), 0, time.UTC),
		Response: &model.Response{Choices: []model.Choice{{Message: model.Message{Role: model.RoleUser, Content: id}}}},
	}); err != nil {
		t.Fatal(err)
	}
}

func (f onlineSessionFixture) create(t *testing.T) {
	t.Helper()
	_, err := f.runtime.Coordinator.Create(context.Background(), datamigration.LiveRequest{
		ID: f.jobID, TenantID: f.tenant.ID, Domain: datamigration.DomainSession,
		SourceProfile: "online-redis", TargetProfile: "online-postgres", SourceBackend: "redis", TargetBackend: "postgres",
		ExpectedConfigVersion: 1, Actor: "integration-operator", Reason: "verify live copy and rollback",
	})
	if err != nil {
		t.Fatal(err)
	}
}

func (f onlineSessionFixture) advance(t *testing.T, target datamigration.Phase) {
	t.Helper()
	for step := 0; step < 40; step++ {
		job, err := f.runtime.Coordinator.Get(context.Background(), f.jobID)
		if err != nil {
			t.Fatal(err)
		}
		if job.Phase == target {
			return
		}
		if _, err := f.runtime.Coordinator.RunOnce(context.Background(), f.jobID); err != nil {
			t.Fatalf("step %d from %s: %v", step, job.Phase, err)
		}
	}
	t.Fatalf("migration did not reach %s", target)
}

func (f onlineSessionFixture) assertEvents(t *testing.T, service session.Service, ids ...string) {
	t.Helper()
	value, err := service.GetSession(context.Background(), f.key, session.WithEventNum(100))
	if err != nil || value == nil {
		t.Fatalf("missing migrated session: %v", err)
	}
	if len(value.Events) != len(ids) {
		t.Fatalf("event count=%d, want %d", len(value.Events), len(ids))
	}
	for index, id := range ids {
		if value.Events[index].ID != id {
			t.Fatalf("event[%d]=%s, want %s", index, value.Events[index].ID, id)
		}
	}
}

func TestOnlineSessionMigrationCapturesWritesAndRollsBackWithoutLosingNewEvents(t *testing.T) {
	f := newOnlineSessionFixture(t)
	f.create(t)
	f.advance(t, datamigration.PhaseValidate)
	// The cached wrapper was created before the migration. Late writes must be mirrored.
	f.append(t, f.wrapped, "during-validation")
	f.advance(t, datamigration.PhaseRollbackWindow)
	f.assertEvents(t, f.target, "before-migration", "during-validation")
	f.append(t, f.wrapped, "after-cutover")
	f.assertEvents(t, f.target, "before-migration", "during-validation", "after-cutover")
	if _, err := f.runtime.Coordinator.Rollback(context.Background(), f.jobID, "integration-operator", "rollback exercise"); err != nil {
		t.Fatal(err)
	}
	f.assertEvents(t, f.source, "before-migration", "during-validation", "after-cutover")
	f.append(t, f.wrapped, "after-rollback")
	f.assertEvents(t, f.source, "before-migration", "during-validation", "after-cutover", "after-rollback")
	var profile string
	if err := f.db.QueryRow(`SELECT config->'storage'->>'sessionProfile' FROM tenants WHERE id=$1`, f.tenant.ID).Scan(&profile); err != nil || profile != "online-redis" {
		t.Fatalf("rollback profile=%s err=%v", profile, err)
	}
}

func TestOnlineSessionCompleteRoutesPreviouslyCachedWriterToTarget(t *testing.T) {
	f := newOnlineSessionFixture(t)
	f.create(t)
	f.advance(t, datamigration.PhaseRollbackWindow)
	if _, err := f.runtime.Coordinator.Complete(context.Background(), f.jobID, "integration-operator", "accept new backend"); err != nil {
		t.Fatal(err)
	}
	f.append(t, f.wrapped, "after-complete")
	f.assertEvents(t, f.target, "before-migration", "after-complete")
	f.assertEvents(t, f.source, "before-migration")
	if err := f.wrapped.DeleteSession(context.Background(), f.key); err != nil {
		t.Fatal(err)
	}
	value, err := f.target.GetSession(context.Background(), f.key)
	if err != nil || value != nil {
		t.Fatalf("completed route deletion did not reach target: value=%v err=%v", value, err)
	}
}
