package platform

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

type flakyDestination struct {
	DataStore
	failures int
}
type corruptingDestination struct{ DataStore }

type identityCorruptingDestination struct{ DataStore }

func (s *corruptingDestination) PutMemory(ctx context.Context, item MemoryRecord) error {
	item.Value += "-corrupt"
	return s.DataStore.PutMemory(ctx, item)
}

func (s *identityCorruptingDestination) ListSessionEvents(ctx context.Context, tenant, session string, after uint64) ([]SessionEvent, error) {
	events, err := s.DataStore.ListSessionEvents(ctx, tenant, session, after)
	if err == nil && len(events) > 0 {
		events[0].ID = "corrupt-event-id"
		events[0].IdempotencyKey = "corrupt-idempotency-key"
	}
	return events, err
}

func (s *flakyDestination) AppendSessionEvent(ctx context.Context, event SessionEvent) error {
	if s.failures > 0 {
		s.failures--
		return errors.New("temporary storage failure")
	}
	return s.DataStore.AppendSessionEvent(ctx, event)
}

func TestRedisToSQLMigrationDryRunAndRepeat(t *testing.T) {
	server := miniredis.RunT(t)
	source := NewRedisStore(server.Addr())
	defer source.Close()
	destination, err := NewSQLiteStore(filepath.Join(t.TempDir(), "destination.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	ctx := context.Background()
	if err := source.AppendSessionEvent(ctx, SessionEvent{TenantID: "tenant-a", SessionID: "session-a", IdempotencyKey: "one", Type: "message", Payload: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	if err := source.PutMemory(ctx, MemoryRecord{TenantID: "tenant-a", SessionID: "session-a", Key: "name", Value: "Ada"}); err != nil {
		t.Fatal(err)
	}
	if err := source.PutMemory(ctx, MemoryRecord{TenantID: "tenant-a", SessionID: "memory-only", Key: "language", Value: "Go"}); err != nil {
		t.Fatal(err)
	}
	dry, err := MigrateRedisToSQL(ctx, source, destination, MigrationOptions{TenantID: "tenant-a", DryRun: true})
	if err != nil || dry.SourceCount != 3 || dry.DestinationCount != 0 || dry.Matched {
		t.Fatalf("dry=%#v err=%v", dry, err)
	}
	checkpoint := filepath.Join(t.TempDir(), "checkpoint.json")
	report, err := MigrateRedisToSQL(ctx, source, destination, MigrationOptions{TenantID: "tenant-a", CheckpointPath: checkpoint})
	if err != nil || report.Status != "completed" || report.SourceCount != report.DestinationCount {
		t.Fatalf("report=%#v err=%v", report, err)
	}
	repeated, err := MigrateRedisToSQL(ctx, source, destination, MigrationOptions{TenantID: "tenant-a", CheckpointPath: checkpoint})
	if err != nil || repeated.DestinationCount != 3 {
		t.Fatalf("repeated=%#v err=%v", repeated, err)
	}
}

func TestRedisToSQLMigrationResumesFromCheckpoint(t *testing.T) {
	server := miniredis.RunT(t)
	source := NewRedisStore(server.Addr())
	defer source.Close()
	destination, err := NewSQLiteStore(filepath.Join(t.TempDir(), "resume.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	ctx := context.Background()
	first := SessionEvent{TenantID: "tenant-a", SessionID: "a", IdempotencyKey: "one", Type: "message", Payload: []byte("one")}
	second := SessionEvent{TenantID: "tenant-a", SessionID: "b", IdempotencyKey: "two", Type: "message", Payload: []byte("two")}
	for _, event := range []SessionEvent{first, second} {
		if err := source.AppendSessionEvent(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	sourceEvents, err := source.ListSessionEvents(ctx, "tenant-a", "a", 0)
	if err != nil || len(sourceEvents) != 1 {
		t.Fatalf("source events=%#v err=%v", sourceEvents, err)
	}
	if err := destination.AppendSessionEvent(ctx, sourceEvents[0]); err != nil {
		t.Fatal(err)
	}
	checkpoint := filepath.Join(t.TempDir(), "resume.json")
	if err := writeCheckpoint(checkpoint, migrationCheckpoint{TenantID: "tenant-a", Next: 1}); err != nil {
		t.Fatal(err)
	}
	report, err := MigrateRedisToSQL(ctx, source, destination, MigrationOptions{TenantID: "tenant-a", CheckpointPath: checkpoint, BatchSize: 1})
	if err != nil || !report.Resumed || report.SourceCount != 2 || report.DestinationCount != 2 {
		t.Fatalf("report=%#v err=%v", report, err)
	}
}

func TestMigrationRetriesAndHonorsCancellation(t *testing.T) {
	source := NewInMemoryStore()
	destination := NewInMemoryStore()
	event := SessionEvent{TenantID: "tenant-a", SessionID: "session-a", IdempotencyKey: "one", Type: "message", Payload: []byte("one")}
	if err := source.AppendSessionEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	report, err := MigrateRedisToSQL(context.Background(), source, &flakyDestination{DataStore: destination, failures: 2}, MigrationOptions{TenantID: "tenant-a", MaxRetries: 3})
	if err != nil || report.Status != "completed" {
		t.Fatalf("report=%#v err=%v", report, err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := MigrateRedisToSQL(cancelled, source, NewInMemoryStore(), MigrationOptions{TenantID: "tenant-a"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
}

func TestMigrationDetectsMemoryContentMismatch(t *testing.T) {
	source := NewInMemoryStore()
	ctx := context.Background()
	if err := source.AppendSessionEvent(ctx, SessionEvent{TenantID: "tenant-a", SessionID: "session-a", IdempotencyKey: "one", Type: "message", Payload: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	if err := source.PutMemory(ctx, MemoryRecord{TenantID: "tenant-a", SessionID: "session-a", Key: "name", Value: "Ada"}); err != nil {
		t.Fatal(err)
	}
	report, err := MigrateRedisToSQL(ctx, source, &corruptingDestination{DataStore: NewInMemoryStore()}, MigrationOptions{TenantID: "tenant-a"})
	if err == nil || report.Status != "failed" {
		t.Fatalf("report=%#v err=%v", report, err)
	}
}

func TestMigrationDetectsEventIdentityMismatch(t *testing.T) {
	source := NewInMemoryStore()
	ctx := context.Background()
	if err := source.AppendSessionEvent(ctx, SessionEvent{TenantID: "tenant-a", SessionID: "session-a", IdempotencyKey: "one", Type: "message", Payload: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	report, err := MigrateRedisToSQL(ctx, source, &identityCorruptingDestination{DataStore: NewInMemoryStore()}, MigrationOptions{TenantID: "tenant-a"})
	if err == nil || report.Status != "failed" {
		t.Fatalf("report=%#v err=%v", report, err)
	}
}

func (s *flakyDestination) ListSessionIDs(ctx context.Context, tenant string) ([]string, error) {
	return s.DataStore.(sessionLister).ListSessionIDs(ctx, tenant)
}

func (s *corruptingDestination) ListSessionIDs(ctx context.Context, tenant string) ([]string, error) {
	return s.DataStore.(sessionLister).ListSessionIDs(ctx, tenant)
}

func (s *identityCorruptingDestination) ListSessionIDs(ctx context.Context, tenant string) ([]string, error) {
	return s.DataStore.(sessionLister).ListSessionIDs(ctx, tenant)
}
