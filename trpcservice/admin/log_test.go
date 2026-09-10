package admin

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	servicelog "github.com/XnLemon/trpc-agent-service/trpcservice/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestWriteMappedErrorLogsServerFailure(t *testing.T) {
	core, logs := observer.New(zapcore.ErrorLevel)
	restore := servicelog.SetDefault(zap.New(core))
	t.Cleanup(restore)

	response := httptest.NewRecorder()
	writeMappedError(response, "request-1", errors.New("database password=secret"))
	if response.Code != 500 {
		t.Fatalf("response status = %d, want 500", response.Code)
	}
	entry := logs.FilterMessage("[admin] request failed").All()[0]
	fields := entry.ContextMap()
	if fields["request_id"] != "request-1" || fields["status"] != int64(500) || fields["error_type"] != "internal_error" || fields["error_class"] != "error" {
		t.Fatalf("failure fields = %v", fields)
	}
	if fields["error"] != nil {
		t.Fatalf("raw error was logged: %v", fields["error"])
	}
}

func TestWriteMappedErrorSkipsClientFailure(t *testing.T) {
	core, logs := observer.New(zapcore.ErrorLevel)
	restore := servicelog.SetDefault(zap.New(core))
	t.Cleanup(restore)

	response := httptest.NewRecorder()
	writeMappedError(response, "request-1", errInvalidRequest)
	if response.Code != 400 {
		t.Fatalf("response status = %d, want 400", response.Code)
	}
	if logs.Len() != 0 {
		t.Fatalf("client failure emitted logs: %v", logs.All())
	}
}

func TestLogRequestFailureWarnsOnTimeoutAndSkipsCancellation(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	restore := servicelog.SetDefault(zap.New(core))
	t.Cleanup(restore)

	logRequestFailure("request-1", 500, "internal_error", context.Canceled)
	if logs.Len() != 0 {
		t.Fatalf("cancellation emitted logs: %v", logs.All())
	}

	logRequestFailure("request-1", 500, "internal_error", context.DeadlineExceeded)
	entries := logs.FilterMessage("[admin] request failed").All()
	if len(entries) != 1 || entries[0].Level != zapcore.WarnLevel {
		t.Fatalf("timeout log = %v, want one warn entry", entries)
	}
}
