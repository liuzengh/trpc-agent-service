package repository

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestMemoryStoreDefensiveCopiesAndTenantScope(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	payload := []byte("tenant-a")
	created, err := store.PublishConfig(ctx, ConfigRecord{TenantID: "a", Payload: payload}, 0)
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	created.Payload[0] = 'Y'
	found, err := store.GetConfigVersion(ctx, "a", 1)
	if err != nil || string(found.Payload) != "tenant-a" {
		t.Fatalf("found=%q err=%v", found.Payload, err)
	}
	if _, err := store.GetConfigVersion(ctx, "b", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant get error=%v", err)
	}
}

func TestMigrationContracts(t *testing.T) {
	up, err := MigrationSQL(DirectionUp)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"tenants", "agent_apps", "config_versions", "channel_bindings", "identity_mappings", "session_heads", "last_event_seq", "last_fence", "state_version", "message_events", "event_seq", "session_summaries", "cutoff_event_seq", "memory_entries", "memory_id", "source_event_seq", "inbox_messages", "inbox_seq", "claim_owner", "claim_token", "lease_until", "outbox_messages", "source_inbox_id", "fence", "derived_jobs", "audit_logs", "migration_jobs", "storage_migration_items", "source_route_hash", "run_statuses", "cancel_requested", "worker_nodes", "policy_budget_usage", "policy_budget_reservations", "tool_approvals", "tool_executions", "tool_call_id", "arguments_hash", "idempotency_key"} {
		if !strings.Contains(up, required) {
			t.Errorf("up migration missing %q", required)
		}
	}
	down, err := MigrationSQL(DirectionDown)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(down, "DROP TABLE IF EXISTS tenants") {
		t.Fatal("down migration does not remove tenants")
	}

	var calls []string
	exec := func(_ context.Context, sql string) error { calls = append(calls, sql); return nil }
	for _, direction := range []Direction{DirectionUp, DirectionUp, DirectionDown, DirectionDown, DirectionUp} {
		if err := Migrate(context.Background(), exec, direction); err != nil {
			t.Fatal(err)
		}
	}
	if len(calls) != 5 {
		t.Fatalf("migration calls=%d", len(calls))
	}
}
