package node_test

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/migrations"
	"github.com/liuzengh/trpc-agent-service/trpcservice/node"
)

func TestPostgresStoreRegistersHeartbeatsAndListsNodes(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	if !strings.Contains(strings.ToLower(dsn), "test") {
		t.Skip("TEST_POSTGRES_DSN must target a test database")
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := migrations.Apply(context.Background(), database); err != nil {
		t.Fatalf("migrations.Apply() error = %v", err)
	}
	store, err := node.NewPostgresStore(database)
	if err != nil {
		t.Fatalf("NewPostgresStore() error = %v", err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	record := node.Record{
		NodeID: "node-integration", BootID: "boot-1", Role: "worker", State: node.StateReady,
		BuildVersion: "test", Inflight: 2, StartedAt: now, LastHeartbeat: now, LeaseUntil: now.Add(time.Minute),
	}
	if err := store.Register(context.Background(), record); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := store.Heartbeat(context.Background(), record.NodeID, record.BootID, node.StateDraining, 1, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("Heartbeat() error = %v", err)
	}
	items, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	var found *node.Record
	for index := range items {
		if items[index].NodeID == record.NodeID {
			found = &items[index]
			break
		}
	}
	if found == nil || found.State != node.StateDraining || found.Inflight != 1 || found.BootID != record.BootID {
		t.Fatalf("listed node = %+v", found)
	}
}
