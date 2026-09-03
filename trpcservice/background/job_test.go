package background

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestMemoryRepositoryLifecycleAndDedupe(t *testing.T) {
	repository := NewMemoryRepository()
	request := EnqueueRequest{
		TenantID: "tenant-a", AppID: "app-a", RevisionID: "revision-a",
		Type: JobSummary, DedupeKey: "session-a:1", Payload: json.RawMessage(`{"turn":1}`),
	}
	first, err := repository.Enqueue(context.Background(), request)
	if err != nil || first.Duplicate {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	second, err := repository.Enqueue(context.Background(), request)
	if err != nil || !second.Duplicate || second.Job.ID != first.Job.ID {
		t.Fatalf("second=%+v err=%v", second, err)
	}
	job, err := repository.Claim(context.Background(), "worker", time.Second)
	if err != nil || job.AttemptCount != 1 {
		t.Fatalf("job=%+v err=%v", job, err)
	}
	if err := repository.Complete(context.Background(), job.ID, "worker"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if _, err := repository.Claim(context.Background(), "worker", time.Second); !errors.Is(err, ErrNoJob) {
		t.Fatalf("claim after complete error=%v", err)
	}
}

func TestMemoryRepositoryDeadJobCanBeRetried(t *testing.T) {
	repository := NewMemoryRepository()
	enqueued, err := repository.Enqueue(context.Background(), EnqueueRequest{
		TenantID: "tenant-a", AppID: "app-a", RevisionID: "revision-a",
		Type: JobKnowledgeUpsert, DedupeKey: "doc:v1", MaxAttempts: 1,
		Payload: json.RawMessage(`{"document":{}}`),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	job, _ := repository.Claim(context.Background(), "worker", time.Second)
	if err := repository.Fail(
		context.Background(), job, "worker", time.Now(), errors.New("failed"),
	); err != nil {
		t.Fatalf("fail: %v", err)
	}
	dead, err := repository.Get(context.Background(), "tenant-a", enqueued.Job.ID)
	if err != nil || dead.Status != "dead" || dead.LastError == "" {
		t.Fatalf("dead=%+v err=%v", dead, err)
	}
	if err := repository.Retry(context.Background(), "tenant-a", dead.ID); err != nil {
		t.Fatalf("retry: %v", err)
	}
	retried, _ := repository.Get(context.Background(), "tenant-a", dead.ID)
	if retried.Status != "pending" || retried.AttemptCount != 0 {
		t.Fatalf("retried=%+v", retried)
	}
}
