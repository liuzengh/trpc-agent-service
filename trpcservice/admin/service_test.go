package admin

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
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
