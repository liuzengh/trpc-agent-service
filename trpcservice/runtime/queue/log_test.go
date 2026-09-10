package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	servicelog "github.com/XnLemon/trpc-agent-service/trpcservice/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestStartLogsUnexpectedWorkerFailure(t *testing.T) {
	core, logs := observer.New(zapcore.ErrorLevel)
	restore := servicelog.SetDefault(zap.New(core))
	t.Cleanup(restore)

	worker, err := New(Config{
		Store:         &stubStore{claimErr: errors.New("database password=secret")},
		Handler:       func(context.Context, Task) error { return nil },
		TenantID:      "tenant-a",
		Owner:         "worker-a",
		LeaseDuration: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForObservedMessage(t, logs, "[runtime/queue] worker stopped")
	entry := logs.FilterMessage("[runtime/queue] worker stopped").All()[0]
	if got := entry.ContextMap()["error_type"]; got != "worker_stopped" {
		t.Fatalf("error_type = %v, want worker_stopped", got)
	}
	if got := entry.ContextMap()["error_class"]; got != "handler_error" {
		t.Fatalf("error_class = %v, want handler_error", got)
	}
	if err := worker.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLogWorkerStoppedSkipsIncompleteInputs(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	restore := servicelog.SetDefault(zap.New(core))
	t.Cleanup(restore)

	logWorkerStopped(nil, errors.New("worker failed"))
	logWorkerStopped(&Worker{}, nil)
	if logs.Len() != 0 {
		t.Fatalf("incomplete worker failure emitted logs: %v", logs.All())
	}
}

func waitForObservedMessage(t *testing.T, logs *observer.ObservedLogs, message string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if logs.FilterMessage(message).Len() > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("log message %q was not emitted", message)
}
