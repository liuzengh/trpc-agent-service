package storagemigration

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type stagedCopier struct {
	mu        sync.Mutex
	calls     int
	failFirst bool
}

func (copier *stagedCopier) Step(_ context.Context, job Job, _ int) (Progress, error) {
	copier.mu.Lock()
	defer copier.mu.Unlock()
	copier.calls++
	if copier.failFirst {
		copier.failFirst = false
		return Progress{}, errors.New("temporary")
	}
	done := copier.calls >= 4
	return Progress{Checkpoint: []byte(`{"table":1}`), SourceRows: 3, CopiedRows: int64(min(copier.calls, 3)), Done: done}, nil
}

func TestTwoWorkersResumeRetryAndCompleteOneJob(t *testing.T) {
	store := NewMemoryStore()
	source := tenant.BackendConfig{Type: tenant.BackendPostgres}
	target := tenant.BackendConfig{Type: tenant.BackendPostgres, Endpoint: "postgres://target/runtime"}
	job, err := NewJob("tenant-a", "assistant", 1, DomainMemory, source, target, "ops")
	if err != nil {
		t.Fatal(err)
	}
	job, err = store.Create(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	copier := &stagedCopier{failFirst: true}
	first, _ := NewWorker(store, copier, WorkerConfig{Owner: "node-a", PollInterval: time.Millisecond, RetryBase: time.Millisecond})
	second, _ := NewWorker(store, copier, WorkerConfig{Owner: "node-b", PollInterval: time.Millisecond, RetryBase: time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := first.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := second.Start(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		current, err := store.Get(context.Background(), job.TenantID, job.JobID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Status == StatusCompleted {
			if current.SourceRows != 3 || current.CopiedRows != 3 || current.Attempts < 2 {
				t.Fatalf("job=%+v", current)
			}
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	current, _ := store.Get(context.Background(), job.TenantID, job.JobID)
	if current.Status != StatusCompleted {
		t.Fatalf("status=%s", current.Status)
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
	defer closeCancel()
	if err := first.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
}
