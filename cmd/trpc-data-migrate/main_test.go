package main

import (
	"context"
	"testing"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/datamigration"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
)

func migrationTestTenant(tenantID string) config.TenantConfig {
	return config.TenantConfig{
		TenantID: tenantID,
		App:      config.AppConfig{Name: "assistant"},
		Data: config.DataConfig{Session: config.BackendConfig{
			Type: "redis", DSNEnv: "SOURCE_REDIS", Namespace: "source-prefix",
		}},
	}
}

func migrationTestJob(tenant config.TenantConfig) datamigration.Job {
	return datamigration.Job{
		MigrationID:  "migration-1",
		TenantID:     tenant.TenantID,
		AppName:      domain.AppNamespace(tenant.TenantID, tenant.App.Name),
		ResourceKind: "session",
		Source:       sourceDescriptor(tenant.Data.Session.DSNEnv, configuredSourcePrefix(tenant, "")),
		Target:       targetDescriptor("TARGET_POSTGRES"),
		Phase:        datamigration.PhaseComplete,
		Status:       datamigration.StatusComplete,
	}
}

func TestSourcePrefixForJobRejectsWrongDatabase(t *testing.T) {
	tenant := migrationTestTenant("tenant-a")
	job := migrationTestJob(tenant)
	_, err := sourcePrefixForJob(job, options{
		sourceRedisEnv: "SOURCE_REDIS",
		targetDSNEnv:   "OTHER_POSTGRES",
	})
	if err == nil {
		t.Fatal("migration accepted a target database different from the durable job")
	}
}

func TestCheckJobTenantRejectsWrongTenant(t *testing.T) {
	job := migrationTestJob(migrationTestTenant("tenant-a"))
	if err := checkJobTenant(job, migrationTestTenant("tenant-b")); err == nil {
		t.Fatal("migration job was accepted for another tenant")
	}
	if err := checkJobTenant(job, migrationTestTenant("tenant-a")); err != nil {
		t.Fatalf("matching tenant was rejected: %v", err)
	}
}

type migrationTestStore struct {
	job      datamigration.Job
	getCalls int
}

func (s *migrationTestStore) Create(context.Context, datamigration.Job) error { return nil }

func (s *migrationTestStore) Get(context.Context, string) (datamigration.Job, error) {
	s.getCalls++
	return s.job, nil
}

func (s *migrationTestStore) CompareAndSwap(context.Context, int64, datamigration.Job) error {
	return nil
}

func TestTenantJobRejectsWrongTenantOrAppNamespace(t *testing.T) {
	job := migrationTestJob(migrationTestTenant("tenant-a"))
	store := &migrationTestStore{job: job}

	if _, err := tenantJob(context.Background(), store, job.MigrationID, migrationTestTenant("tenant-b")); err == nil {
		t.Fatal("tenantJob exposed a migration to another tenant")
	}
	wrongApp := migrationTestTenant("tenant-a")
	wrongApp.App.Name = "other-app"
	if _, err := tenantJob(context.Background(), store, job.MigrationID, wrongApp); err == nil {
		t.Fatal("tenantJob exposed a migration to another app namespace")
	}
	if store.getCalls != 2 {
		t.Fatalf("store Get calls = %d, want 2", store.getCalls)
	}
}

type recordingUnfreezer struct {
	calls []struct {
		tenantID    string
		appName     string
		migrationID string
	}
}

func (r *recordingUnfreezer) UnfreezeTenant(_ context.Context, tenantID, appName, migrationID string) error {
	r.calls = append(r.calls, struct {
		tenantID    string
		appName     string
		migrationID string
	}{tenantID: tenantID, appName: appName, migrationID: migrationID})
	return nil
}

func TestUnfreezeTerminalReleasesOnlyValidTerminalRoutes(t *testing.T) {
	tenant := migrationTestTenant("tenant-a")
	job := migrationTestJob(tenant)

	t.Run("complete uses target database", func(t *testing.T) {
		control := &recordingUnfreezer{}
		err := unfreezeTerminal(context.Background(), control, job, tenant, func() string {
			return job.Target
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(control.calls) != 1 || control.calls[0].migrationID != job.MigrationID {
			t.Fatalf("unfreeze calls = %+v", control.calls)
		}
	})

	t.Run("rollback uses source database", func(t *testing.T) {
		control := &recordingUnfreezer{}
		rolledBack := job
		rolledBack.Status = datamigration.StatusRolledBack
		err := unfreezeTerminal(context.Background(), control, rolledBack, tenant, func() string {
			return job.Source
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(control.calls) != 1 || control.calls[0].migrationID != job.MigrationID {
			t.Fatalf("unfreeze calls = %+v", control.calls)
		}
	})

	t.Run("wrong route does not release", func(t *testing.T) {
		control := &recordingUnfreezer{}
		err := unfreezeTerminal(context.Background(), control, job, tenant, func() string {
			return targetDescriptor("OTHER_POSTGRES")
		})
		if err == nil {
			t.Fatal("terminal migration was unfrozen on the wrong database")
		}
		if len(control.calls) != 0 {
			t.Fatalf("unfreeze calls = %+v", control.calls)
		}
	})

	t.Run("wrong tenant does not release", func(t *testing.T) {
		control := &recordingUnfreezer{}
		err := unfreezeTerminal(context.Background(), control, job, migrationTestTenant("tenant-b"), func() string {
			return job.Target
		})
		if err == nil {
			t.Fatal("terminal migration was unfrozen for another tenant")
		}
		if len(control.calls) != 0 {
			t.Fatalf("unfreeze calls = %+v", control.calls)
		}
	})

	t.Run("nonterminal does not release", func(t *testing.T) {
		control := &recordingUnfreezer{}
		nonterminal := job
		nonterminal.Phase = datamigration.PhaseFinalize
		err := unfreezeTerminal(context.Background(), control, nonterminal, tenant, func() string {
			return job.Target
		})
		if err == nil {
			t.Fatal("nonterminal migration was unfrozen")
		}
		if len(control.calls) != 0 {
			t.Fatalf("unfreeze calls = %+v", control.calls)
		}
	})
}
