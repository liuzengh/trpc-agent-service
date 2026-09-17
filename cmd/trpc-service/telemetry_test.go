package main

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

func TestNewRoleTelemetryDefaultsToNoopAndRejectsUnsafeConfiguration(t *testing.T) {
	logger := testLogger()
	provider, err := newRoleTelemetry(context.Background(), mapEnvironment(nil), "gateway", logger)
	if err != nil || provider != telemetry.Noop() {
		t.Fatalf("provider=%T err=%v", provider, err)
	}
	for name, value := range map[string]string{
		"TRPC_OTEL_ALLOW_INSECURE":        "sometimes",
		"TRPC_OTEL_MAX_QUEUE_SIZE":        "0",
		"TRPC_OTEL_MAX_EXPORT_BATCH_SIZE": "4096",
		"TRPC_OTEL_METRIC_INTERVAL":       "1ms",
		"TRPC_SERVICE_VERSION":            "bad\nversion",
	} {
		values := map[string]string{"TRPC_OTEL_ENDPOINT": "https://collector.example.test", name: value}
		if _, err := newRoleTelemetry(context.Background(), mapEnvironment(values), "gateway", logger); err == nil {
			t.Fatalf("%s=%q accepted", name, value)
		}
	}
	if _, err := newRoleTelemetry(context.Background(), mapEnvironment(map[string]string{
		"TRPC_OTEL_ENDPOINT": "http://collector:4318"}), "gateway", logger); err == nil {
		t.Fatal("insecure collector accepted without opt-in")
	}
}
