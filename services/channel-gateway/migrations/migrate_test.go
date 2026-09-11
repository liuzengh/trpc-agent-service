package migrations

import (
	"context"
	"errors"
	"testing"
	"time"
)

type rollbackFunc func(context.Context) error

func (f rollbackFunc) Rollback(ctx context.Context) error { return f(ctx) }

func TestMigrationRollbackUsesIndependentBoundedContext(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if canceled.Err() == nil {
		t.Fatal("fixture cancellation missing")
	}
	var cleanup context.Context
	calls := 0
	before := time.Now()
	rollbackBounded(rollbackFunc(func(ctx context.Context) error {
		calls++
		cleanup = ctx
		if ctx.Err() != nil {
			t.Fatalf("cleanup inherited canceled startup context: %v", ctx.Err())
		}
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("unbounded migration cleanup")
		}
		remaining := deadline.Sub(before)
		if remaining <= 0 || remaining > 5*time.Second+time.Second {
			t.Fatalf("unexpected cleanup timeout: %v", remaining)
		}
		return errors.New("rollback transport failure")
	}))
	if calls != 1 {
		t.Fatalf("rollback calls=%d", calls)
	}
	if cleanup == nil || !errors.Is(cleanup.Err(), context.Canceled) {
		t.Fatal("cleanup timer/context not released after rollback")
	}
}
