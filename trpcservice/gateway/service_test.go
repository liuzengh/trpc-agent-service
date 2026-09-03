package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/routing"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestIntakeResolvesAndAcceptsBinding(t *testing.T) {
	repository := controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData())
	t.Cleanup(func() { _ = repository.Close() })
	resolver, err := routing.NewControlPlaneResolver(repository)
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	journal := NewMemoryJournal()
	auditWriter := audit.NewMemoryWriter()
	intake, err := NewIntake(resolver, journal, WithAuditWriter(auditWriter))
	if err != nil {
		t.Fatalf("new intake: %v", err)
	}
	t.Cleanup(func() { _ = intake.Close() })
	result, err := intake.Accept(context.Background(), IntakeRequest{
		BindingKey:        "tutorial-http",
		ExternalMessageID: "message-1",
		UserID:            "alice",
		SessionID:         "session",
		ChatType:          "direct",
		Text:              "hello",
	})
	if err != nil {
		t.Fatalf("accept message: %v", err)
	}
	if result.RequestID == "" || result.RevisionID != "tutorial-revision-1" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if len(journal.Tasks()) != 1 || journal.Tasks()[0].Scope.TenantID != "tutorial-tenant" {
		t.Fatalf("unexpected tasks: %+v", journal.Tasks())
	}
	events := auditWriter.Events()
	if len(events) != 1 || events[0].Decision != "inbound_accepted" ||
		events[0].TenantID != "tutorial-tenant" {
		t.Fatalf("audit events: %+v", events)
	}
}

func TestIntakeEnforcesTenantRateLimit(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	data.Tenants[0].QuotaConfig = json.RawMessage(`{"requests_per_minute":1}`)
	repository := controlplane.NewMemoryRepository(data)
	resolver, _ := routing.NewControlPlaneResolver(repository)
	journal := NewMemoryJournal()
	guard, _ := tenant.NewGuard(context.Background(), repository, config.QuotaConfig{
		Backend: config.QuotaBackendLocal,
	})
	intake, _ := NewIntake(resolver, journal, WithQuotaGuard(guard))
	t.Cleanup(func() {
		_ = guard.Close()
		_ = intake.Close()
		_ = repository.Close()
	})
	request := IntakeRequest{
		BindingKey: "tutorial-http", ExternalMessageID: "rate-1",
		UserID: "alice", SessionID: "session", ChatType: "direct", Text: "hello",
	}
	if _, err := intake.Accept(context.Background(), request); err != nil {
		t.Fatalf("first: %v", err)
	}
	request.ExternalMessageID = "rate-2"
	if _, err := intake.Accept(context.Background(), request); !errors.Is(err, tenant.ErrRateLimited) {
		t.Fatalf("rate error=%v", err)
	}
}

func TestIntakePinsCanaryRevisionPerConversation(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	canary := data.Revisions[0]
	canary.ID = "tutorial-canary"
	canary.RevisionNo = 2
	canary.Checksum = controlplane.RevisionChecksum(canary)
	data.Revisions = append(data.Revisions, canary)
	data.Apps[0].RolloutPolicy = json.RawMessage(`{
        "canary_revision_id":"tutorial-canary","canary_percent":100
    }`)
	repository := controlplane.NewMemoryRepository(data)
	resolver, _ := routing.NewControlPlaneResolver(repository)
	journal := NewMemoryJournal()
	intake, _ := NewIntake(resolver, journal)
	t.Cleanup(func() {
		_ = intake.Close()
		_ = repository.Close()
	})
	first, err := intake.Accept(context.Background(), IntakeRequest{
		BindingKey: "tutorial-http", ExternalMessageID: "canary-1",
		UserID: "alice", SessionID: "pinned", ChatType: "direct", Text: "first",
	})
	if err != nil || first.RevisionID != canary.ID {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	_, err = repository.UpdateRolloutPolicy(
		context.Background(), "tutorial-tenant", "tutorial-app",
		json.RawMessage(`{"canary_percent":0}`), 1,
	)
	if err != nil {
		t.Fatalf("disable canary: %v", err)
	}
	second, err := intake.Accept(context.Background(), IntakeRequest{
		BindingKey: "tutorial-http", ExternalMessageID: "canary-2",
		UserID: "alice", SessionID: "pinned", ChatType: "direct", Text: "second",
	})
	if err != nil || second.RevisionID != canary.ID {
		t.Fatalf("second=%+v err=%v", second, err)
	}
}
