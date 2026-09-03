package toolexec

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

func TestPostgresToolJournalIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_URL is not set")
	}
	control, err := controlplane.New(context.Background(), config.ControlPlaneConfig{
		Backend: config.ControlPlaneBackendPostgres, PostgresURL: dsn,
		AutoMigrate: true, BootstrapTutorial: true, MaxOpenConns: 5, MaxIdleConns: 1,
	})
	if err != nil {
		t.Fatalf("control plane: %v", err)
	}
	t.Cleanup(func() { _ = control.Close() })
	journal, _ := NewForControlPlane(control)
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
