package admin

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestMemoryMigrationControlPlanePinsCandidateBinding(t *testing.T) {
	ctx := context.Background()
	service, configs, _ := sessionMigrationService(t, ctx)
	principal := Principal{Authenticated: true, TenantID: "tenant-a", SubjectID: "operator", CanManage: true}
	metadata := tenant.ChangeMetadata{ActorType: "admin", ActorID: "operator", ReasonCode: "memory_move", CorrelationID: "request-1", TraceID: "trace-1"}
	published, err := configs.Publish(ctx, config.PublishInput{TenantID: "tenant-a", ExpectedTenantVersion: 1, Payload: memoryMigrationConfig("redis-memory", 1), Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := configs.Stage(ctx, config.StageInput{TenantID: "tenant-a", ExpectedTenantVersion: published.Tenant.Version, Payload: memoryMigrationConfig("postgres-memory", 2), Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.CreateMemoryMigration(ctx, principal, "tenant-a", MemoryMigrationCreateInput{MigrationID: "memory-move", TargetConfigVersion: candidate.ConfigVersion}, metadata)
	if err != nil || created.Domain != "memory" || created.Source.ConfigVersion != published.Snapshot.ConfigVersion || created.Target.ConfigVersion != candidate.ConfigVersion {
		t.Fatalf("created=%#v err=%v", created, err)
	}
	paused, err := service.PauseMemoryMigration(ctx, principal, "tenant-a", created.MigrationID, MemoryMigrationControlInput{ExpectedMigrationVersion: created.Version}, metadata)
	if err != nil || paused.State != migration.StatePaused {
		t.Fatalf("paused=%#v err=%v", paused, err)
	}
	if _, err := service.ResumeMemoryMigration(ctx, principal, "tenant-a", created.MigrationID, MemoryMigrationControlInput{ExpectedMigrationVersion: paused.Version}, metadata); err != nil {
		t.Fatal(err)
	}
}

func memoryMigrationConfig(profileID string, version int64) config.ConfigV1 {
	return config.ConfigV1{SchemaVersion: config.CurrentSchemaVersion, DefaultAgentAppID: "app", PolicyVersion: 1,
		BackendBindings: []config.BackendBinding{{Domain: "memory", BackendProfileID: profileID, BackendVersion: version, Required: []string{"strong_ryw"}}}}
}
