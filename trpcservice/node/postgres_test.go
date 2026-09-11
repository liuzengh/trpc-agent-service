package node

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresStoreValidatesBeforeDatabaseAccess(t *testing.T) {
	t.Parallel()
	if _, err := NewPostgresStore(nil); err == nil {
		t.Fatal("NewPostgresStore(nil) error = nil")
	}
	database, err := sql.Open("pgx", "postgres://unused:unused@127.0.0.1:1/unused")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresStore(database)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Register(context.Background(), Record{}); err == nil {
		t.Fatal("Register() accepted invalid record")
	}
	lease := time.Now().UTC().Add(time.Minute)
	for _, test := range []struct {
		name     string
		nodeID   string
		bootID   string
		state    State
		inflight int
		lease    time.Time
	}{
		{name: "node", bootID: "boot", state: StateReady, lease: lease},
		{name: "boot", nodeID: "node", state: StateReady, lease: lease},
		{name: "inflight", nodeID: "node", bootID: "boot", state: StateReady, inflight: -1, lease: lease},
		{name: "lease", nodeID: "node", bootID: "boot", state: StateReady},
		{name: "state", nodeID: "node", bootID: "boot", state: StateOffline, lease: lease},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := store.Heartbeat(context.Background(), test.nodeID, test.bootID, test.state, test.inflight, test.lease); err == nil {
				t.Fatal("Heartbeat() error = nil")
			}
		})
	}
}

func TestPostgresStoreSurfacesClosedDatabaseErrors(t *testing.T) {
	t.Parallel()
	database, err := sql.Open("pgx", "postgres://unused:unused@127.0.0.1:1/unused")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresStore(database)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	record := Record{
		NodeID: "node-1", BootID: "boot-1", Role: "worker", State: StateReady,
		BuildVersion: "test", StartedAt: now, LastHeartbeat: now, LeaseUntil: now.Add(time.Minute),
	}
	if err := store.Register(context.Background(), record); err == nil || !strings.Contains(err.Error(), "register node") {
		t.Fatalf("Register(closed DB) error = %v", err)
	}
	if err := store.Heartbeat(context.Background(), record.NodeID, record.BootID, StateDraining, 0, now.Add(time.Minute)); err == nil || !strings.Contains(err.Error(), "heartbeat node") {
		t.Fatalf("Heartbeat(closed DB) error = %v", err)
	}
	if _, err := store.List(context.Background()); err == nil || !strings.Contains(err.Error(), "list nodes") {
		t.Fatalf("List(closed DB) error = %v", err)
	}
}
