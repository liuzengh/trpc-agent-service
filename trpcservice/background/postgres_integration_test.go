package background

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

func TestPostgresRepositoryIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_URL is not set")
	}
	control, err := controlplane.New(context.Background(), config.ControlPlaneConfig{
		Backend: config.ControlPlaneBackendPostgres, PostgresURL: dsn,
		AutoMigrate: true, BootstrapTutorial: true,
		MaxOpenConns: 10, MaxIdleConns: 2,
	})
	if err != nil {
		t.Fatalf("control plane: %v", err)
	}
	t.Cleanup(func() { _ = control.Close() })
	repository, err := NewForControlPlane(control)
	if err != nil {
		t.Fatalf("new jobs: %v", err)
	}
	dedupe := "integration:" + time.Now().UTC().Format(time.RFC3339Nano)
	enqueued, err := repository.Enqueue(context.Background(), EnqueueRequest{
		TenantID: "tutorial-tenant", AppID: "tutorial-app", RevisionID: "tutorial-revision-1",
		Type: JobSummary, DedupeKey: dedupe, Payload: json.RawMessage(`{"turn_seq":1}`),
	})
	if err != nil || enqueued.Job.ID == "" {
		t.Fatalf("enqueued=%+v err=%v", enqueued, err)
	}
	job, err := repository.Claim(context.Background(), "integration-worker", time.Second)
	if err != nil || job.ID != enqueued.Job.ID {
		t.Fatalf("job=%+v err=%v", job, err)
	}
	if err := repository.Complete(context.Background(), job.ID, "integration-worker"); err != nil {
		t.Fatalf("complete: %v", err)
	}
}
