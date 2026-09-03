package gateway

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/routing"
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
