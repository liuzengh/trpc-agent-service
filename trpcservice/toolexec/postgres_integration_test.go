package toolexec

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestPostgresToolJournalIntegration(t *testing.T) {
	db := isolatedToolDB(t)
	journal, _ := NewPostgresJournal(db)
	requestID := fmt.Sprintf("tool-integration-%d", time.Now().UnixNano())
	execution := Execution{
		TenantID: "tutorial-tenant", RequestID: requestID,
		RevisionID: "tutorial-revision-1", ToolCallID: "call-1",
		ToolName: "dangerous_demo", ArgumentsHash: Hash([]byte(`{"action":"test"}`)),
	}
	started, err := journal.Start(context.Background(), execution)
	if err != nil || started.Existing {
		t.Fatalf("started=%+v err=%v", started, err)
	}
	if err := journal.Complete(
		context.Background(), started.Execution.ID, StatusSucceeded, Hash([]byte("ok")), "",
	); err != nil {
		t.Fatalf("complete: %v", err)
	}
	replayed, err := journal.Start(context.Background(), execution)
	if err != nil || !replayed.Existing || replayed.Execution.Status != StatusSucceeded {
		t.Fatalf("replayed=%+v err=%v", replayed, err)
	}
}
