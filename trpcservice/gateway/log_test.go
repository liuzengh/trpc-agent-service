package gateway

import (
	"context"
	"errors"
	"testing"

	servicelog "github.com/XnLemon/trpc-agent-service/trpcservice/log"
	runtimerunner "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/runner"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestLogDispatchFailureUsesStableFields(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	restore := servicelog.SetDefault(zap.New(core))
	t.Cleanup(restore)

	logDispatchFailure(Principal{}, "request-1", "trace-1", context.Canceled)
	if logs.Len() != 0 {
		t.Fatalf("cancellation emitted logs: %v", logs.All())
	}

	logDispatchFailure(Principal{}, "request-1", "trace-1", errors.New("provider password=secret"))
	entry := logs.FilterMessage("[gateway] dispatch failed").All()[0]
	fields := entry.ContextMap()
	if fields["request_id"] != "request-1" || fields["trace_id"] != "trace-1" {
		t.Fatalf("correlation fields = %v", fields)
	}
	if fields["error_type"] != "internal_error" || fields["error_class"] != "error" {
		t.Fatalf("stable error fields = %v", fields)
	}
	if fields["error"] != nil {
		t.Fatalf("raw error was logged: %v", fields["error"])
	}

	logDispatchFailure(Principal{}, "request-2", "trace-2", context.DeadlineExceeded)
	if logs.FilterMessage("[gateway] dispatch timed out").Len() != 1 {
		t.Fatalf("timeout log = %v", logs.All())
	}
}

func TestLogDispatchFailureClassifiesRunnerCapacity(t *testing.T) {
	core, logs := observer.New(zapcore.ErrorLevel)
	restore := servicelog.SetDefault(zap.New(core))
	t.Cleanup(restore)

	logDispatchFailure(Principal{}, "request-1", "trace-1", runtimerunner.ErrRunnerCapacity)
	entries := logs.FilterMessage("[gateway] dispatch failed").All()
	if len(entries) != 1 {
		t.Fatalf("dispatch failure logs = %v, want one entry", entries)
	}
	if got := entries[0].ContextMap()["error_type"]; got != runtimerunner.ErrRunnerCapacity.Error() {
		t.Fatalf("error_type = %v, want %q", got, runtimerunner.ErrRunnerCapacity.Error())
	}
}
