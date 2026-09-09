package messaging

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/persistence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestEnsureBackendFingerprintCreatesReusesAndRejectsChange(t *testing.T) {
	store, server := newTestStore(t)
	route := persistence.Route{TenantID: "tenant", AgentAppID: "agent", Fingerprint: persistence.BackendFingerprint{
		SchemaVersion: persistence.FingerprintSchemaVersion, Kind: tenant.StorageKindPostgres,
		StorageProfileID: "pg", DatabaseIdentity: "db.example:5432/app", Namespace: "public.tenant_",
	}}
	if err := store.EnsureBackendFingerprint(context.Background(), route); err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureBackendFingerprint(context.Background(), route); err != nil {
		t.Fatalf("reuse failed: %v", err)
	}
	changed := route
	changed.Fingerprint.Namespace = "public.other_"
	if err := store.EnsureBackendFingerprint(context.Background(), changed); !errors.Is(err, persistence.ErrFingerprintConflict) {
		t.Fatalf("changed fingerprint error = %v", err)
	}
	isolated := changed
	isolated.TenantID = "other-tenant"
	if err := store.EnsureBackendFingerprint(context.Background(), isolated); err != nil {
		t.Fatalf("target tenant conflict affected another tenant: %v", err)
	}
	server.Close()
	if err := store.EnsureBackendFingerprint(context.Background(), route); err != nil {
		t.Fatalf("cached matching fingerprint required Redis: %v", err)
	}
}
