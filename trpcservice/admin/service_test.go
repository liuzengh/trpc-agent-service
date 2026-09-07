package admin

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/background"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
)

func TestAdminCreatesAndPublishesRevision(t *testing.T) {
	repository := controlplane.NewMemoryRepository(controlplane.BootstrapData{})
	service, err := New(repository)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	tenant, err := service.CreateTenant(context.Background(), controlplane.Tenant{
		ID: "tenant-a", DisplayName: "Tenant A", Region: "local", SecretNamespace: "tenant/a",
	})
	if err != nil || tenant.Version != 1 {
		t.Fatalf("tenant=%+v err=%v", tenant, err)
	}
	app, err := service.CreateAgentApp(context.Background(), controlplane.AgentApp{
		ID: "app-a", TenantID: tenant.ID, Name: "Agent A",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	revision, err := service.CreateRevision(context.Background(), controlplane.AgentRevision{
		ID: "revision-1", TenantID: tenant.ID, AppID: app.ID, RevisionNo: 1,
		AgentType: "llm", AgentConfig: json.RawMessage(`{"name":"agent","instruction":"hello"}`),
		ModelConfig: json.RawMessage(`{"source":"startup_env"}`), CreatedBy: "admin",
	})
	if err != nil || revision.Checksum == "" {
		t.Fatalf("revision=%+v err=%v", revision, err)
	}
	published, err := service.PublishRevision(
		context.Background(), tenant.ID, app.ID, revision.ID, app.Version,
	)
	if err != nil || published.StableRevisionID != revision.ID || published.Version != 2 {
		t.Fatalf("published=%+v err=%v", published, err)
	}
	if _, err := service.PublishRevision(
		context.Background(), tenant.ID, app.ID, revision.ID, app.Version,
	); !errors.Is(err, controlplane.ErrConflict) {
		t.Fatalf("stale publish error=%v", err)
	}
	rolled, err := service.UpdateRolloutPolicy(
		context.Background(), tenant.ID, app.ID,
		json.RawMessage(`{"canary_revision_id":"revision-1","canary_percent":10,"salt":"v1"}`),
		published.Version,
	)
	if err != nil || rolled.Version != 3 {
		t.Fatalf("rollout=%+v err=%v", rolled, err)
	}
}

func TestAdminRejectsUnknownTool(t *testing.T) {
	repository := controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData())
	service, _ := New(repository)
	_, err := service.CreateRevision(context.Background(), controlplane.AgentRevision{
		ID: "revision-unknown-tool", TenantID: "tutorial-tenant", AppID: "tutorial-app",
		RevisionNo: 2, AgentType: "llm", CreatedBy: "admin",
		AgentConfig: json.RawMessage(`{"name":"agent","instruction":"hello"}`),
		ModelConfig: json.RawMessage(`{"source":"startup_env"}`),
		ToolPolicy:  json.RawMessage(`{"allowed_tools":["not_registered"]}`),
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("error=%v", err)
	}
}

func TestAdminAuditsControlPlaneMutation(t *testing.T) {
	repository := controlplane.NewMemoryRepository(controlplane.BootstrapData{})
	auditWriter := audit.NewMemoryWriter()
	service, err := New(repository)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	service.WithAuditWriter(auditWriter)
	_, err = service.CreateTenant(context.Background(), controlplane.Tenant{
		ID: "tenant-a", DisplayName: "Tenant A", Region: "local", SecretNamespace: "tenant/a",
	})
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	events := auditWriter.Events()
	if len(events) != 1 || events[0].Decision != "admin_tenant_created" ||
		events[0].TenantID != "tenant-a" || events[0].UserID != "admin" {
		t.Fatalf("events=%+v", events)
	}
}

func TestAdminUpdatesChannelBinding(t *testing.T) {
	repository := controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData())
	auditWriter := audit.NewMemoryWriter()
	service, _ := New(repository)
	service.WithAuditWriter(auditWriter)
	updated, err := service.UpdateChannelBinding(
		context.Background(), "tutorial-tenant", "tutorial-http-binding",
		json.RawMessage(`{"allowed_chat_ids":[-100]}`), controlplane.StatusActive, 1,
	)
	if err != nil || updated.Version != 2 ||
		string(updated.Config) != `{"allowed_chat_ids":[-100]}` {
		t.Fatalf("updated=%+v err=%v", updated, err)
	}
	events := auditWriter.Events()
	if len(events) != 1 || events[0].Decision != "admin_channel_binding_updated" {
		t.Fatalf("events=%+v", events)
	}
	if _, err := service.UpdateChannelBinding(
		context.Background(), "tutorial-tenant", "tutorial-http-binding",
		json.RawMessage(`{}`), controlplane.StatusActive, 1,
	); !errors.Is(err, controlplane.ErrConflict) {
		t.Fatalf("stale update error=%v", err)
	}
}

func TestAdminUpsertsKnowledgeDocument(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	data.BackendBindings = append(data.BackendBindings, controlplane.BackendBinding{
		ID: "knowledge", TenantID: "tutorial-tenant", AppID: "tutorial-app",
		ResourceType: "knowledge", BackendType: "inmemory", MigrationState: "active", Version: 1,
		Config: json.RawMessage(`{"dimensions":32}`),
	})
	data.Revisions[0].KnowledgeConfig = json.RawMessage(`{
        "enabled":true,"chunk_size":100,
        "embedding":{"provider":"hash","dimensions":32}
    }`)
	data.Revisions[0].Checksum = controlplane.RevisionChecksum(data.Revisions[0])
	repository := controlplane.NewMemoryRepository(data)
	router, err := platformstorage.NewKnowledgeRouter(repository, secret.StaticStore{})
	if err != nil {
		t.Fatalf("new knowledge router: %v", err)
	}
	t.Cleanup(func() {
		_ = router.Close()
		_ = repository.Close()
	})
	service, _ := New(repository)
	service.WithKnowledgeRouter(router)
	chunks, err := service.UpsertKnowledgeDocument(context.Background(), KnowledgeDocumentInput{
		TenantID: "tutorial-tenant", AppID: "tutorial-app",
		RevisionID: "tutorial-revision-1", DocumentID: "policy",
		Name: "Policy", Content: "refunds are available for thirty days",
	})
	if err != nil || chunks != 1 {
		t.Fatalf("chunks=%d err=%v", chunks, err)
	}
	kb, enabled, err := router.KnowledgeForRevision(
		context.Background(), runtimecontext.TutorialScope(), data.Revisions[0],
	)
	if err != nil || !enabled {
		t.Fatalf("enabled=%t err=%v", enabled, err)
	}
	result, err := kb.Search(context.Background(), &knowledge.SearchRequest{Query: "refunds"})
	if err != nil || result.Document == nil {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestAdminQueuesKnowledgeDocumentWhenJobsConfigured(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	data.BackendBindings = append(data.BackendBindings, controlplane.BackendBinding{
		ID: "knowledge", TenantID: "tutorial-tenant", AppID: "tutorial-app",
		ResourceType: "knowledge", BackendType: "inmemory", MigrationState: "active", Version: 1,
		Config: json.RawMessage(`{"dimensions":32}`),
	})
	data.Revisions[0].KnowledgeConfig = json.RawMessage(`{
        "enabled":true,"embedding":{"provider":"hash","dimensions":32}
    }`)
	data.Revisions[0].Checksum = controlplane.RevisionChecksum(data.Revisions[0])
	repository := controlplane.NewMemoryRepository(data)
	router, _ := platformstorage.NewKnowledgeRouter(repository, secret.StaticStore{})
	jobs := background.NewMemoryRepository()
	t.Cleanup(func() {
		_ = jobs.Close()
		_ = router.Close()
		_ = repository.Close()
	})
	service, _ := New(repository)
	service.WithKnowledgeRouter(router).WithBackgroundJobs(jobs)
	result, err := service.SubmitKnowledgeDocument(context.Background(), KnowledgeDocumentInput{
		TenantID: "tutorial-tenant", AppID: "tutorial-app", RevisionID: "tutorial-revision-1",
		DocumentID: "policy", OperationID: "upload-v1", Content: "refund policy",
	})
	if err != nil || !result.Queued || result.JobID == "" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	job, err := jobs.Claim(context.Background(), "jobs", time.Second)
	if err != nil || job.Type != background.JobKnowledgeUpsert {
		t.Fatalf("job=%+v err=%v", job, err)
	}
}

func TestAdminBackendMigrationStateMachine(t *testing.T) {
	repository := controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData())
	service, _ := New(repository)
	target, err := service.CreateBackendBinding(context.Background(), controlplane.BackendBinding{
		ID: "memory-target", TenantID: "tutorial-tenant", AppID: "tutorial-app",
		ResourceType: "memory", BackendType: "inmemory",
		MigrationState: "migration_target", Config: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	migration, err := service.CreateBackendMigration(context.Background(), controlplane.BackendMigration{
		ID: "memory-migration", TenantID: "tutorial-tenant", AppID: "tutorial-app",
		ResourceType: "memory", SourceBindingID: "tutorial-memory-backend",
		TargetBindingID: target.ID,
	})
	if err != nil || migration.State != controlplane.MigrationPlanned {
		t.Fatalf("migration=%+v err=%v", migration, err)
	}
	for _, next := range []string{
		controlplane.MigrationDualWrite,
		controlplane.MigrationBackfill,
		controlplane.MigrationVerify,
	} {
		migration, err = service.TransitionBackendMigration(
			context.Background(), migration.TenantID, migration.ID,
			next, migration.Version, nil, nil,
		)
		if err != nil {
			t.Fatalf("transition to %s: %v", next, err)
		}
	}
	if _, err := service.TransitionBackendMigration(
		context.Background(), migration.TenantID, migration.ID,
		controlplane.MigrationCutover, migration.Version, nil, nil,
	); !errors.Is(err, ErrInvalid) {
		t.Fatalf("cutover without verification error=%v", err)
	}
	if _, err := service.TransitionBackendMigration(context.Background(), migration.TenantID, migration.ID, controlplane.MigrationCutover, migration.Version, nil, json.RawMessage(`{"passed":true}`)); err == nil {
		t.Fatal("caller-supplied proof accepted")
	}
	if err := repository.WithResourceSync(context.Background(), migration.TenantID, migration.AppID, "memory", func(_ context.Context, s *controlplane.ResourceSync, save func() error) error {
		s.Subjects["fixture"] = true
		controlplane.RecordResourceProof(s, migration, "fixture", "fixture-verified")
		return save()
	}); err != nil {
		t.Fatal(err)
	}
	migration, err = repository.TransitionBackendMigration(context.Background(), migration.TenantID, migration.ID, migration.State, migration.Version, nil, []byte(`{"passed":true}`))
	if err != nil {
		t.Fatal(err)
	}
	migration, err = service.TransitionBackendMigration(
		context.Background(), migration.TenantID, migration.ID,
		controlplane.MigrationCutover, migration.Version, nil,
		nil,
	)
	if err != nil || migration.State != controlplane.MigrationCutover {
		t.Fatalf("cutover=%+v err=%v", migration, err)
	}
	migration, err = service.TransitionBackendMigration(
		context.Background(), migration.TenantID, migration.ID,
		controlplane.MigrationCompleted, migration.Version, nil, nil,
	)
	if err != nil || migration.State != controlplane.MigrationCompleted {
		t.Fatalf("complete=%+v err=%v", migration, err)
	}
	source, _ := repository.GetBackendBinding(
		context.Background(), "tutorial-tenant", "tutorial-memory-backend",
	)
	activeTarget, _ := repository.GetBackendBinding(
		context.Background(), "tutorial-tenant", target.ID,
	)
	if source.MigrationState != "retired" || activeTarget.MigrationState != "active" {
		t.Fatalf("source=%+v target=%+v", source, activeTarget)
	}
}
