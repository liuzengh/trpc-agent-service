package audit

import (
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestEventDigestAndStableID(t *testing.T) {
	event := Event{SchemaVersion: 1, AuditID: "audit", TenantID: "tenant", Action: "tool_confirmation",
		Decision: "approved", ResourceRefs: []string{"confirmation://tenant/value"}, OccurredAt: time.Unix(10, 20).UTC()}
	first, err := Digest(event)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Digest(event)
	if err != nil || first != second {
		t.Fatalf("digests=%q/%q err=%v", first, second, err)
	}
	id, err := StableID("tenant", "outbox")
	if err != nil || id == "" {
		t.Fatalf("id=%q err=%v", id, err)
	}
	event.Action = "bad\nvalue"
	if !errors.Is(Validate(event), runtime.ErrInvalidEnvelope) {
		t.Fatal("control character accepted")
	}
}
