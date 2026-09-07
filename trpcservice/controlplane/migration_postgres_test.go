package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

func TestPostgresBackendMigrationIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_URL is not set")
	}
	repository, err := New(context.Background(), config.ControlPlaneConfig{
		Backend: config.ControlPlaneBackendPostgres, PostgresURL: dsn,
		AutoMigrate: true, MaxOpenConns: 10, MaxIdleConns: 2,
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	t.Cleanup(func() { _ = repository.Close() })
	mutable := repository.(MutableRepository)
	suffix := fmt.Sprintf("%x", time.Now().UnixNano())
	tenantID, appID, revisionID := "tm-"+suffix, "am-"+suffix, "rm-"+suffix
	now := time.Now().UTC()
	if err := mutable.CreateTenant(context.Background(), Tenant{
		ID: tenantID, DisplayName: tenantID, Status: StatusActive, Region: "test",
		QuotaConfig: json.RawMessage(`{}`), AuditPolicy: json.RawMessage(`{}`),
		SecretNamespace: "test", Version: 1, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("tenant: %v", err)
	}
	if err := mutable.CreateAgentApp(context.Background(), AgentApp{
		ID: appID, TenantID: tenantID, Name: appID, Status: StatusActive,
		RolloutPolicy: json.RawMessage(`{}`), Version: 1, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("app: %v", err)
	}
	revision := AgentRevision{
		ID: revisionID, TenantID: tenantID, AppID: appID, RevisionNo: 1,
		AgentType: "llm", AgentConfig: json.RawMessage(`{"name":"a","instruction":"i"}`),
		ModelConfig: json.RawMessage(`{"source":"startup_env"}`), ToolPolicy: json.RawMessage(`{}`),
		KnowledgeConfig: json.RawMessage(`{}`), MemoryConfig: json.RawMessage(`{}`),
		GuardrailConfig: json.RawMessage(`{}`), CreatedBy: "test", CreatedAt: now,
	}
	revision.Checksum = RevisionChecksum(revision)
	if err := mutable.CreateRevision(context.Background(), revision); err != nil {
		t.Fatalf("revision: %v", err)
	}
	sourceID, targetID := "source-"+suffix, "target-"+suffix
	for _, binding := range []BackendBinding{
		{ID: sourceID, TenantID: tenantID, AppID: appID, ResourceType: "memory", BackendType: "inmemory", Config: json.RawMessage(`{}`), IsolationLevel: "shared", MigrationState: "active", Version: 1, CreatedAt: now, UpdatedAt: now},
		{ID: targetID, TenantID: tenantID, AppID: appID, ResourceType: "memory", BackendType: "inmemory", Config: json.RawMessage(`{}`), IsolationLevel: "shared", MigrationState: "migration_target", Version: 1, CreatedAt: now, UpdatedAt: now},
	} {
		if err := mutable.CreateBackendBinding(context.Background(), binding); err != nil {
			t.Fatalf("binding: %v", err)
		}
	}
	migration := BackendMigration{
		ID: "migration-" + suffix, TenantID: tenantID, AppID: appID,
		ResourceType: "memory", SourceBindingID: sourceID, TargetBindingID: targetID,
		State: MigrationPlanned, Checkpoint: json.RawMessage(`{}`),
		Verification: json.RawMessage(`{}`), Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	if err := mutable.CreateBackendMigration(context.Background(), migration); err != nil {
		t.Fatalf("migration: %v", err)
	}
	active, err := repository.GetActiveBackendMigration(context.Background(), tenantID, appID, "memory")
	if err != nil || active.ID != migration.ID {
		t.Fatalf("active=%+v err=%v", active, err)
	}
	if _, err := mutable.TransitionBackendMigration(context.Background(), tenantID, migration.ID, MigrationCompleted, 1, nil, []byte(`{"passed":true}`)); err == nil {
		t.Fatal("unverified migration completed")
	}
	if err := repository.(ResourceSyncRepository).WithResourceSync(context.Background(), tenantID, appID, "memory", func(_ context.Context, s *ResourceSync, save func() error) error {
		s.Subjects["fixture"] = true
		RecordResourceProof(s, migration, "fixture", "fixture-verified")
		return save()
	}); err != nil {
		t.Fatal(err)
	}
	completed, err := mutable.TransitionBackendMigration(
		context.Background(), tenantID, migration.ID, MigrationCompleted, 1, nil,
		json.RawMessage(`{"passed":true}`),
	)
	if err != nil || completed.State != MigrationCompleted {
		t.Fatalf("completed=%+v err=%v", completed, err)
	}
	source, _ := repository.GetBackendBinding(context.Background(), tenantID, sourceID)
	target, _ := repository.GetBackendBinding(context.Background(), tenantID, targetID)
	if source.MigrationState != "retired" || target.MigrationState != "active" {
		t.Fatalf("source=%+v target=%+v", source, target)
	}
}

func TestPostgresChannelBindingUpdateIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_URL is not set")
	}
	repository, err := New(context.Background(), config.ControlPlaneConfig{
		Backend: config.ControlPlaneBackendPostgres, PostgresURL: dsn,
		AutoMigrate: true, MaxOpenConns: 10, MaxIdleConns: 2,
	})
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	t.Cleanup(func() { _ = repository.Close() })
	mutable := repository.(MutableRepository)
	suffix := fmt.Sprintf("%x", time.Now().UnixNano())
	tenantID, appID := "tc-"+suffix, "ac-"+suffix
	now := time.Now().UTC()
	if err := mutable.CreateTenant(context.Background(), Tenant{
		ID: tenantID, DisplayName: tenantID, Status: StatusActive, Region: "test",
		QuotaConfig: json.RawMessage(`{}`), AuditPolicy: json.RawMessage(`{}`),
		SecretNamespace: "test", Version: 1, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("tenant: %v", err)
	}
	if err := mutable.CreateAgentApp(context.Background(), AgentApp{
		ID: appID, TenantID: tenantID, Name: appID, Status: StatusActive,
		RolloutPolicy: json.RawMessage(`{}`), Version: 1, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("app: %v", err)
	}
	bindingID := "channel-" + suffix
	if err := mutable.CreateChannelBinding(context.Background(), ChannelBinding{
		ID: bindingID, TenantID: tenantID, AppID: appID, ChannelType: "telegram",
		AccountID: "bot-" + suffix, CallbackKey: "callback-" + suffix,
		Config: json.RawMessage(`{"require_mention":false}`), SecretRef: "env://BOT_TOKEN",
		Status: StatusActive, Version: 1, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("create channel binding: %v", err)
	}
	updated, err := mutable.UpdateChannelBinding(
		context.Background(), tenantID, bindingID,
		json.RawMessage(`{"require_mention":true}`), StatusActive, 1,
	)
	var updatedConfig map[string]any
	if decodeErr := json.Unmarshal(updated.Config, &updatedConfig); decodeErr != nil {
		t.Fatalf("decode updated config: %v", decodeErr)
	}
	if err != nil || updated.Version != 2 || updatedConfig["require_mention"] != true {
		t.Fatalf("updated=%+v err=%v", updated, err)
	}
	if _, err := mutable.UpdateChannelBinding(
		context.Background(), tenantID, bindingID,
		json.RawMessage(`{}`), StatusActive, 1,
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale update error=%v", err)
	}
}
