package compliancepostgres

import (
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestQuarantineCoordinateRequiresTenantScopedExactReference(t *testing.T) {
	event := audit.Event{SchemaVersion: 1, AuditID: "audit", TenantID: "tenant-a", RequestID: "request",
		Action: "artifact.quarantine", Decision: "alert", ErrorType: "version_mismatch",
		ResourceRefs: []string{"artifact-quarantine://tenant-a/upload/artifact-1/7"}, OccurredAt: time.Now().UTC()}
	kind, artifactID, version, _, err := quarantineCoordinate(event)
	if err != nil || kind != "upload" || artifactID != "artifact-1" || version != 7 {
		t.Fatalf("kind=%q artifact=%q version=%d err=%v", kind, artifactID, version, err)
	}
	event.ResourceRefs[0] = "artifact-quarantine://tenant-b/upload/artifact-1/7"
	if _, _, _, _, err := quarantineCoordinate(event); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("cross-tenant coordinate error=%v", err)
	}
	event.ResourceRefs[0] = "artifact-quarantine://tenant-a/unknown/artifact-1/7"
	if _, _, _, _, err := quarantineCoordinate(event); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("unknown kind error=%v", err)
	}
}
