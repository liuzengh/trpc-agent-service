package metrics

import (
	"strings"
	"testing"
)

func TestQueueBacklogQueriesUseDomainTerminalStates(t *testing.T) {
	inbox, err := queueBacklogQuery("inbox")
	if err != nil || !strings.Contains(inbox, "'completed','canceled','rejected'") {
		t.Fatalf("inbox query=%q err=%v", inbox, err)
	}
	outbox, err := queueBacklogQuery("outbox")
	if err != nil || !strings.Contains(outbox, "status<>'sent'") {
		t.Fatalf("outbox query=%q err=%v", outbox, err)
	}
	if _, err := queueBacklogQuery("unknown"); err == nil {
		t.Fatal("unknown queue must fail closed")
	}
}
