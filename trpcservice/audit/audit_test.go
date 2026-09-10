package audit_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
)

func TestControlPlaneActorContextCarriesOnlyStableIdentity(t *testing.T) {
	ctx := audit.WithControlPlaneActor(context.Background(), "admin:operator", "operator")
	actorID, actorRole, ok := audit.ControlPlaneActorFromContext(ctx)
	if !ok || actorID != "admin:operator" || actorRole != "operator" {
		t.Fatalf("control-plane actor = %q/%q ok=%t", actorID, actorRole, ok)
	}
	if _, _, ok := audit.ControlPlaneActorFromContext(context.Background()); ok {
		t.Fatal("missing control-plane actor context was accepted")
	}
}

func TestEventValidateRequiresScopedCorrelationMetadata(t *testing.T) {
	event := audit.Event{
		TenantID:      "tenant-a",
		AppID:         "app-a",
		Decision:      "completed",
		EventType:     audit.ExecutionCompleted,
		TraceID:       "trace-a",
		RequestID:     "request-a",
		ConfigVersion: "v1",
	}
	if err := event.Validate(); err != nil {
		t.Fatalf("validate event: %v", err)
	}

	event.ToolName = "delete"
	event.Latency = time.Second
	event.InputTokens = 10
	event.OutputTokens = 20
	event.TotalTokens = 30
	if err := event.Validate(); err != nil {
		t.Fatalf("validate metadata-only tool event: %v", err)
	}
}

func TestEventValidateRejectsInvalidMeasurements(t *testing.T) {
	base := audit.Event{
		TenantID:      "tenant-a",
		AppID:         "app-a",
		Decision:      "completed",
		EventType:     audit.ExecutionCompleted,
		TraceID:       "trace-a",
		RequestID:     "request-a",
		ConfigVersion: "v1",
	}
	tests := []audit.Event{
		func() audit.Event { event := base; event.Latency = -time.Millisecond; return event }(),
		func() audit.Event { event := base; event.TotalTokens = -1; return event }(),
	}
	for _, event := range tests {
		if err := event.Validate(); err == nil {
			t.Fatal("invalid audit measurement was accepted")
		}
	}
	cost := -0.1
	base.Cost = &cost
	if err := base.Validate(); err == nil {
		t.Fatal("negative audit cost was accepted")
	}
}

func TestRedactEventRemovesPIIAndCredentials(t *testing.T) {
	event := audit.Event{
		UserID:    "alice@example.com",
		SessionID: "13800138000",
		AgentName: "Bearer token123",
		ToolName:  "password=secret123",
	}
	redacted := audit.RedactEvent(event)
	for _, value := range []string{redacted.UserID, redacted.SessionID, redacted.AgentName, redacted.ToolName} {
		for _, secret := range []string{"alice@example.com", "13800138000", "Bearer token123", "secret123"} {
			if strings.Contains(value, secret) {
				t.Fatalf("redacted value %q still contains %q", value, secret)
			}
		}
	}
}
