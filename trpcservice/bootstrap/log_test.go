package bootstrap

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	servicelog "github.com/XnLemon/trpc-agent-service/trpcservice/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

type failingPollingAdapter struct{}

func (failingPollingAdapter) Channel() channels.Channel { return channels.ChannelWeComAIBot }
func (failingPollingAdapter) Close() error              { return nil }
func (failingPollingAdapter) Run(context.Context) error { return errors.New("connection failed") }

func TestStartAIBotsLogsUnexpectedStop(t *testing.T) {
	core, logs := observer.New(zapcore.ErrorLevel)
	restore := servicelog.SetDefault(zap.New(core))
	t.Cleanup(restore)

	runtimeGraph := &Runtime{wecomAIBots: []channels.PollingAdapter{failingPollingAdapter{}}}
	if err := startAIBots(runtimeGraph); err != nil {
		t.Fatal(err)
	}
	waitForObservedMessage(t, logs, "[bootstrap] polling adapter stopped")
	entry := logs.FilterMessage("[bootstrap] polling adapter stopped").All()[0]
	if got := entry.ContextMap()["error_type"]; got != "polling_adapter_stopped" {
		t.Fatalf("error_type = %v, want polling_adapter_stopped", got)
	}
	if len(runtimeGraph.aiBotDone) != 1 {
		t.Fatalf("aiBotDone = %d, want 1", len(runtimeGraph.aiBotDone))
	}
	select {
	case <-runtimeGraph.aiBotDone[0]:
	case <-time.After(time.Second):
		t.Fatal("polling adapter did not stop")
	}
	runtimeGraph.aiBotCancel()
}

func TestLogPollingAdapterStoppedSkipsIncompleteInputs(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	restore := servicelog.SetDefault(zap.New(core))
	t.Cleanup(restore)

	logPollingAdapterStopped(nil, errors.New("connection failed"))
	logPollingAdapterStopped(failingPollingAdapter{}, nil)
	if logs.Len() != 0 {
		t.Fatalf("incomplete adapter failure emitted logs: %v", logs.All())
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
