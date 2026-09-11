package postgresadapter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type rollbackProbe struct {
	pgx.Tx
	cleanup     context.Context
	deadline    time.Time
	contextErr  error
	err         error
	hasDeadline bool
}

func (p *rollbackProbe) Rollback(ctx context.Context) error {
	p.cleanup = ctx
	p.deadline, p.hasDeadline = ctx.Deadline()
	p.contextErr = ctx.Err()
	return p.err
}
func TestRollbackHasFreshBoundedContextAndReleasesTimer(t *testing.T) {
	probe := &rollbackProbe{}
	before := time.Now()
	if err := rollbackTransaction(probe); err != nil {
		t.Fatal(err)
	}
	if !probe.hasDeadline {
		t.Fatal("rollback cleanup has no deadline")
	}
	if probe.deadline.Before(before) || probe.deadline.After(time.Now().Add(rollbackTimeout)) {
		t.Fatalf("unexpected cleanup deadline: %s", probe.deadline)
	}
	if probe.contextErr != nil {
		t.Fatalf("cleanup received cancelled context: %v", probe.contextErr)
	}
	if !errors.Is(probe.cleanup.Err(), context.Canceled) {
		t.Fatal("cleanup timer was not released")
	}
}
func TestRollbackReturnsDatabaseFailure(t *testing.T) {
	want := errors.New("rollback failed")
	probe := &rollbackProbe{err: want}
	if err := rollbackTransaction(probe); !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
}
