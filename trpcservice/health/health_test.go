package health

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/metrics"
)

func TestRegistryRequiredAndOptionalReadiness(t *testing.T) {
	registry := NewRegistry(time.Second, time.Second, metrics.NewMetrics())
	failed := true
	if err := registry.Add(Probe{Name: "postgres", Backend: "postgres", Required: true, Check: func(context.Context) error {
		if failed {
			return errors.New("connection refused postgres://secret")
		}
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Add(Probe{Name: "tracing", Backend: "otel", Required: false, Check: func(context.Context) error { return errors.New("down") }}); err != nil {
		t.Fatal(err)
	}
	registry.Check(context.Background())
	if registry.Ready() {
		t.Fatal("required dependency failure was ready")
	}
	failed = false
	registry.Check(context.Background())
	if !registry.Ready() {
		t.Fatal("healthy required dependency was not ready")
	}
	snapshot := registry.Snapshot()
	if len(snapshot) != 2 {
		t.Fatalf("unexpected sanitized dependency snapshot: %+v", snapshot)
	}
	for _, item := range snapshot {
		if item.Backend == "otel" && item.ErrorCode != "unavailable" {
			t.Fatalf("optional dependency error was not categorized: %+v", item)
		}
		if item.ErrorCode == "unavailable" && item.Name == "postgres" {
			t.Fatalf("raw dependency error leaked: %+v", item)
		}
	}
}

func TestRegistryGateBlocksReadiness(t *testing.T) {
	registry := NewRegistry(time.Second, time.Second, nil)
	registry.SetGate("control-plane", false)
	if registry.Ready() {
		t.Fatal("unready gate was ignored")
	}
	registry.SetGate("control-plane", true)
	if !registry.Ready() {
		t.Fatal("ready gate remained blocked")
	}
}
