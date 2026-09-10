package qdrant

import (
	"context"
	"testing"

	migrationmemory "github.com/liuzengh/trpc-agent-service/trpcservice/migration/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	ledgermemory "github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestNewMigrationDrillWiresBothDirections(t *testing.T) {
	source := newAdapterForDrill(t, "snapshot-a")
	target := newAdapterForDrill(t, "snapshot-a")
	driver, err := NewMigrationDrill(migrationmemory.New(), ledgermemory.New(), drillProbes{}, source, target)
	if err != nil {
		t.Fatal(err)
	}
	if driver.Source != source || driver.Backfill != source || driver.Target != target || driver.ReverseSource != target || driver.ReverseReplica != source || driver.SourceInventory != source || driver.TargetInventory != target || driver.SearchTarget != target {
		t.Fatalf("unexpected drill wiring: %#v", driver)
	}
	if _, err := NewMigrationDrill(migrationmemory.New(), ledgermemory.New(), drillProbes{}, source, newAdapterForDrill(t, "snapshot-b")); err != runtime.ErrInvariantViolation {
		t.Fatalf("mismatched snapshot: %v", err)
	}
}

func newAdapterForDrill(t *testing.T, watermark string) *Adapter {
	t.Helper()
	backend := newFakeQdrant()
	server := newTestServer(t, backend)
	adapter, err := New(Config{Endpoint: server.URL, Collection: "knowledge", VectorSize: 2, SnapshotWatermark: watermark, AllowInsecureHTTP: true}, fixedEmbedder{})
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

type drillProbes struct{}

func (drillProbes) Probes(context.Context, string, string) ([]knowledgedriver.Probe, error) {
	return nil, nil
}
