package wecom_aibot

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	servicelog "github.com/XnLemon/trpc-agent-service/trpcservice/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestHandleCallbackLogsDispatchFailure(t *testing.T) {
	target, _ := testRoutingTarget(t)
	core, logs := observer.New(zapcore.ErrorLevel)
	restore := servicelog.SetDefault(zap.New(core))
	t.Cleanup(restore)

	manager := &Manager{
		botID:  "bot-1",
		target: target,
		dispatcher: dispatchFunc(func(context.Context, gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
			return nil, errors.New("provider password=secret")
		}),
		executionTimeout: time.Second,
	}
	frame, err := decodeFrame(callbackFrame(t, "request-1", "message-1", "", "user-1"))
	if err != nil {
		t.Fatal(err)
	}
	manager.handleCallback(context.Background(), frame)
	waitForObservedMessage(t, logs, "[wecom-aibot] callback dispatch failed")
	entry := logs.FilterMessage("[wecom-aibot] callback dispatch failed").All()[0]
	if got := entry.ContextMap()["request_id"]; got != "request-1" {
		t.Fatalf("request_id = %v, want request-1", got)
	}
	if got := entry.ContextMap()["error_type"]; got != "dispatch_failed" {
		t.Fatalf("error_type = %v, want dispatch_failed", got)
	}
	if got := entry.ContextMap()["error"]; got != nil {
		t.Fatalf("raw error was logged: %v", got)
	}
}

func TestLogCallbackFailureWarnsOnTimeoutAndSkipsExpectedErrors(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	restore := servicelog.SetDefault(zap.New(core))
	t.Cleanup(restore)

	logCallbackFailure("callback failed", "request-1", "dispatch_failed", nil)
	logCallbackFailure("callback failed", "request-1", "dispatch_failed", context.Canceled)
	if logs.Len() != 0 {
		t.Fatalf("expected errors emitted logs: %v", logs.All())
	}

	logCallbackFailure("callback timed out", "request-1", "dispatch_failed", context.DeadlineExceeded)
	entries := logs.FilterMessage("[wecom-aibot] callback timed out").All()
	if len(entries) != 1 || entries[0].Level != zapcore.WarnLevel {
		t.Fatalf("timeout log = %v, want one warn entry", entries)
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
