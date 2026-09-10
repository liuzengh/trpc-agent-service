package migration_test

import (
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
)

func TestPermanentErrorPreservesCauseAndClassification(t *testing.T) {
	cause := errors.New("invalid backend dimension")
	err := migration.NewPermanentError(cause)
	if !migration.IsPermanentError(err) {
		t.Fatal("permanent error was not classified as permanent")
	}
	if !errors.Is(err, cause) {
		t.Fatalf("permanent error %v does not preserve cause", err)
	}
}

func TestRecordTransitionsFollowMigrationLifecycle(t *testing.T) {
	record := migration.Record{
		ID:                  "migration-1",
		TenantID:            "tenant-1",
		AppID:               "app-1",
		SourceConfigVersion: "v1",
		TargetConfigVersion: "v2",
		Status:              migration.StatusPending,
	}
	if err := record.Validate(); err != nil {
		t.Fatalf("validate pending migration: %v", err)
	}
	for _, next := range []migration.Status{
		migration.StatusDraining,
		migration.StatusCopying,
		migration.StatusVerifying,
		migration.StatusSucceeded,
	} {
		if !record.CanTransition(next) {
			t.Fatalf("%s cannot transition to %s", record.Status, next)
		}
		record.Status = next
	}
	if !record.IsTerminal() {
		t.Fatal("succeeded migration is not terminal")
	}
	if record.CanTransition(migration.StatusFailed) {
		t.Fatal("terminal migration accepts another transition")
	}
}

func TestRecordRejectsIncompleteLease(t *testing.T) {
	record := migration.Record{
		ID:                  "migration-1",
		TenantID:            "tenant-1",
		AppID:               "app-1",
		SourceConfigVersion: "v1",
		TargetConfigVersion: "v2",
		Status:              migration.StatusDraining,
		LeaseOwner:          "worker-1",
		LeaseUntil:          time.Now().Add(time.Minute),
	}
	if err := record.Validate(); err == nil {
		t.Fatal("validate migration succeeded without run token")
	}
}
