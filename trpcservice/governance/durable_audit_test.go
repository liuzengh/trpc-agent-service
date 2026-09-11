package governance

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

func TestDurableToolAuditSinkWritesRedactedTenantEvidence(t *testing.T) {
	store := storage.NewMemoryStateStore()
	sink, err := NewDurableToolAuditSink(store)
	if err != nil {
		t.Fatalf("NewDurableToolAuditSink() error = %v", err)
	}
	if err := sink.RecordToolAudit(context.Background(), ToolAuditEvent{
		TenantID: "tenant-a", TraceID: "trace-1", RequestID: "req-1",
		Channel: "web", UserID: "user-1", SessionID: "tenant-a/support/web/conv",
		AgentName: "assistant", PolicyVersion: "3",
		ToolName: "knowledge_search", Outcome: ToolOutcomeAllowed, LatencyMS: 12,
		ArgumentsDigest: "digest-only", OccurredAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("RecordToolAudit() error = %v", err)
	}
	events, err := store.ListAudit(context.Background(), "tenant-a", "trace-1")
	if err != nil {
		t.Fatalf("ListAudit() error = %v", err)
	}
	if len(events) != 1 || events[0].Action != "tool.knowledge_search" || events[0].Result != string(ToolOutcomeAllowed) ||
		events[0].ToolName != "knowledge_search" || events[0].Decision != string(ToolOutcomeAllowed) ||
		events[0].Channel != "web" || events[0].UserID != "user-1" ||
		!regexp.MustCompile(`^hmac-sha256:[0-9a-f]{64}$`).MatchString(events[0].Detail) {
		t.Fatalf("durable audit events = %+v", events)
	}
}
