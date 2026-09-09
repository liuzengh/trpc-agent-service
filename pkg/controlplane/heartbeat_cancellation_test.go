package controlplane

import (
	"context"
	"database/sql/driver"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

type heartbeatEnteredArgument struct {
	entered chan struct{}
	once    sync.Once
}

func (a *heartbeatEnteredArgument) Match(value driver.Value) bool {
	if value != int64(7) {
		return false
	}
	a.once.Do(func() { close(a.entered) })
	return true
}

func TestRunHeartbeatCancellationDuringRenewalIsNormalShutdown(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	entered := &heartbeatEnteredArgument{entered: make(chan struct{})}
	mock.ExpectExec("UPDATE execution_records").
		WithArgs(entered, "attempt-7", int64(DefaultExecutionLeaseTTL/time.Millisecond)).
		WillDelayFor(time.Second).
		WillReturnResult(sqlmock.NewResult(0, 1))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- NewExecutionRecorder(db).RunHeartbeat(ctx, ExecutionHandle{ID: 7, Token: "attempt-7"}, time.Millisecond)
	}()
	select {
	case <-entered.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("heartbeat did not enter renewal")
	}
	// The HTTP handler stops its heartbeat after Runner completes, which can
	// cancel an in-flight renewal without implying execution ownership loss.
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("normal heartbeat shutdown reported lease failure: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("heartbeat did not stop after cancellation")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
