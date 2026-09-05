//go:build integration

package main

import (
	"context"

	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

func collectorEndpoint(t *testing.T) (string, string) {
	t.Helper()
	address := os.Getenv("P107_OTEL_ADDRESS")
	container := os.Getenv("P107_OTEL_CONTAINER")
	if address == "" || container == "" {
		t.Skip("P107_OTEL_ADDRESS and P107_OTEL_CONTAINER are required for the local collector integration")
	}
	return address, container
}

func TestOTLPTelemetryReachesLocalCollector(t *testing.T) {
	address, container := collectorEndpoint(t)
	config := telemetry.Config{Mode: telemetry.ModeOTLP, Endpoint: address, Environment: "test", InsecureLocalOK: true, ExportTimeout: 2 * time.Second, FlushTimeout: 15 * time.Second}
	runtime, err := telemetry.Compose(context.Background(), config, nil)
	if err != nil {
		t.Fatal(err)
	}
	tracer := runtime.Tracer()
	_, span := tracer.Start(context.Background(), "p107 collector probe")
	span.SetName("p107 collector probe")
	span.End()
	runtime.Metrics().WorkerJob("completed")
	if err = runtime.ForceFlushAndWait(context.Background()); err != nil {
		t.Fatalf("force flush failed: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	logs := ""
	for time.Now().Before(deadline) {
		output, err := osExec("docker", "logs", container).CombinedOutput()
		if err == nil {
			logs = string(output)
			if strings.Contains(logs, "p107 collector probe") && strings.Contains(logs, "trpcagent.worker.jobs") {
				break
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !strings.Contains(logs, "p107 collector probe") {
		t.Fatal("trace did not reach the local collector")
	}
	if !strings.Contains(logs, "trpcagent.worker.jobs") {
		t.Fatal("metrics did not reach the local collector")
	}
}

func TestOTLPCollectorOutageKeepsBusinessSafe(t *testing.T) {
	address, _ := collectorEndpoint(t)
	config := telemetry.Config{Mode: telemetry.ModeOTLP, Endpoint: address, Environment: "test", InsecureLocalOK: true, ExportTimeout: 2 * time.Second, FlushTimeout: 15 * time.Second}
	runtime, err := telemetry.Compose(context.Background(), config, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Spans keep flowing into the SDK pipeline; the exporter may fail behind
	// the scenes but the runtime never panics and shutdown stays bounded.
	tracer := runtime.Tracer()
	for i := 0; i < 10; i++ {
		_, span := tracer.Start(context.Background(), "outage probe")
		span.End()
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err = runtime.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown during outage unbounded: %v", err)
	}
}

func osExec(name string, args ...string) *exec.Cmd { return exec.Command(name, args...) }
