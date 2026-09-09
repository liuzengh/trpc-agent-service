package postgres

import (
	"context"
	"errors"
	"testing"
)

type readinessMigrator struct{ err error }

func (m readinessMigrator) Up(context.Context) error      { return m.err }
func (readinessMigrator) Down(context.Context, int) error { return nil }
func (readinessMigrator) Current(context.Context) (MigrationVersion, error) {
	return MigrationVersion{Version: 2}, nil
}
func (readinessMigrator) Close() {}

func TestMigrationReadinessFailsClosedUntilInitialization(t *testing.T) {
	gate := NewMigrationReadiness(readinessMigrator{})
	if !errors.Is(gate.Ready(context.Background()), ErrMigrationNotReady) {
		t.Fatal("gate became ready before migration initialization")
	}
	if err := gate.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := gate.Ready(context.Background()); err != nil {
		t.Fatalf("expected ready gate: %v", err)
	}
}

func TestMigrationReadinessBlocksAfterApplyFailure(t *testing.T) {
	gate := NewMigrationReadiness(readinessMigrator{err: ErrMigrationChecksum})
	if err := gate.Initialize(context.Background()); !errors.Is(err, ErrMigrationNotReady) || !errors.Is(err, ErrMigrationChecksum) {
		t.Fatalf("unexpected initialization error: %v", err)
	}
	if err := gate.Ready(context.Background()); !errors.Is(err, ErrMigrationChecksum) {
		t.Fatalf("expected checksum failure to remain observable: %v", err)
	}
}
