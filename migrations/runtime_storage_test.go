package migrations

import (
	"os"
	"strings"
	"testing"
)

func TestRuntimeStorageMigrationDefinesTenantScopedInvariants(t *testing.T) {
	contents, err := os.ReadFile("0003_runtime_storage.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, fragment := range []string{
		"PRIMARY KEY (tenant_id, session_id)",
		"UNIQUE (tenant_id, session_id, event_seq)",
		"UNIQUE (tenant_id, binding_id, external_message_id)",
		"FOREIGN KEY (tenant_id, session_id)",
		"CHECK (status IN ('pending', 'sending', 'sent', 'retryable', 'dead_letter'))",
		"fencing_token",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("migration missing %q", fragment)
		}
	}
}

func TestRuntimeSessionDeletionMigrationCascadesDependentFacts(t *testing.T) {
	contents, err := os.ReadFile("0004_runtime_session_delete_cascade.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, fragment := range []string{
		"DROP CONSTRAINT message_event_tenant_id_session_id_fkey",
		"REFERENCES public.runtime_session(tenant_id, session_id) ON DELETE CASCADE",
		"DROP CONSTRAINT reply_outbox_tenant_id_event_id_fkey",
		"REFERENCES public.message_event(tenant_id, event_id) ON DELETE CASCADE",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("migration missing %q", fragment)
		}
	}
}

func TestRuntimeEventHistoryMigrationIsTenantScopedAndCascades(t *testing.T) {
	contents, err := os.ReadFile("0005_runtime_event_history.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, fragment := range []string{
		"PRIMARY KEY (tenant_id, session_id, event_id)",
		"UNIQUE (tenant_id, session_id, history_seq)",
		"REFERENCES public.runtime_session(tenant_id, session_id) ON DELETE CASCADE",
		"payload     JSONB NOT NULL",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("migration missing %q", fragment)
		}
	}
	if strings.Contains(sql, "GRANT SELECT, INSERT, UPDATE, DELETE ON public.runtime_event_history TO tenant_app_writer") {
		t.Fatal("runtime event history grants mutation to the runtime role")
	}
	if !strings.Contains(sql, "GRANT UPDATE (event_id) ON public.runtime_event_history TO tenant_app_writer") {
		t.Fatal("runtime event history is missing the narrow idempotent update grant")
	}
}

func TestRuntimeCapabilityMigrationNamespacesVectors(t *testing.T) {
	contents, err := os.ReadFile("0012_runtime_capabilities.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, fragment := range []string{
		"source      TEXT NOT NULL DEFAULT 'generic'",
		"PRIMARY KEY (tenant_id, source, document_id)",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("migration missing %q", fragment)
		}
	}
}
