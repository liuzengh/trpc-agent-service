package dataprojection

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/datamigration"
)

func TestTargetProjectsFreshRecordOnlyUnderCurrentLeaseFence(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	projector := &recordingProjector{domain: datamigration.DomainKnowledge}
	target, err := NewTarget(db, projector)
	if err != nil {
		t.Fatal(err)
	}
	record, err := NewKnowledgeTombstone("support", "faq-1", 18)
	if err != nil {
		t.Fatal(err)
	}
	fence := datamigration.LeaseFence{MigrationID: "migration-1", Owner: "worker-a", Version: 7}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT 1 FROM data_migrations").WithArgs(
		"migration-1", "worker-a", int64(7), "tenant-a", datamigration.DomainKnowledge,
	).WillReturnRows(sqlmock.NewRows([]string{"marker"}).AddRow(1))
	mock.ExpectExec("INSERT INTO data_migration_records").WithArgs(
		"tenant-a", datamigration.DomainKnowledge, record.Key, "migration-1", record.Payload, record.Version, record.Hash, true,
	).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE data_migration_records SET projected_at=clock_timestamp\(\)`).WithArgs(
		"tenant-a", datamigration.DomainKnowledge, record.Key, "migration-1", record.Version, record.Hash, true,
	).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE data_migrations SET updated_at=clock_timestamp\(\)`).WithArgs(
		"migration-1", "worker-a", int64(7), "tenant-a", datamigration.DomainKnowledge,
	).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := target.Upsert(context.Background(), "tenant-a", datamigration.DomainKnowledge, fence, []datamigration.Record{record}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if len(projector.records) != 1 || projector.tenantID != "tenant-a" || projector.records[0].Key != record.Key {
		t.Fatalf("projection = tenant %q records %#v", projector.tenantID, projector.records)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTargetKeepsProjectionMarkersSeparateAcrossMigrationTargets(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	projector := &recordingProjector{domain: datamigration.DomainKnowledge}
	target, err := NewTarget(db, projector)
	if err != nil {
		t.Fatal(err)
	}
	record, err := NewKnowledgeTombstone("support", "faq-1", 18)
	if err != nil {
		t.Fatal(err)
	}
	for _, migrationID := range []string{"migration-a-to-b", "migration-a-to-c"} {
		fence := datamigration.LeaseFence{MigrationID: migrationID, Owner: "worker-a", Version: 1}
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT 1 FROM data_migrations").WithArgs(
			migrationID, "worker-a", int64(1), "tenant-a", datamigration.DomainKnowledge,
		).WillReturnRows(sqlmock.NewRows([]string{"marker"}).AddRow(1))
		mock.ExpectExec("INSERT INTO data_migration_records").WithArgs(
			"tenant-a", datamigration.DomainKnowledge, record.Key, migrationID, record.Payload, record.Version, record.Hash, true,
		).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(`UPDATE data_migration_records SET projected_at=clock_timestamp\(\)`).WithArgs(
			"tenant-a", datamigration.DomainKnowledge, record.Key, migrationID, record.Version, record.Hash, true,
		).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(`UPDATE data_migrations SET updated_at=clock_timestamp\(\)`).WithArgs(
			migrationID, "worker-a", int64(1), "tenant-a", datamigration.DomainKnowledge,
		).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
		if err := target.Upsert(context.Background(), "tenant-a", datamigration.DomainKnowledge, fence, []datamigration.Record{record}); err != nil {
			t.Fatalf("migration %s upsert: %v", migrationID, err)
		}
	}
	if len(projector.records) != 2 {
		t.Fatalf("target migration projections=%d, want 2", len(projector.records))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTargetDoesNotReuseProjectionMarkerAcrossMigrationTargets(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	projector := &recordingProjector{domain: datamigration.DomainKnowledge}
	target, err := NewTarget(db, projector)
	if err != nil {
		t.Fatal(err)
	}
	record, err := NewKnowledgeTombstone("support", "faq-1", 18)
	if err != nil {
		t.Fatal(err)
	}
	for _, migrationID := range []string{"migration-a", "migration-b"} {
		fence := datamigration.LeaseFence{MigrationID: migrationID, Owner: "worker-a", Version: 1}
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT 1 FROM data_migrations").WithArgs(
			migrationID, "worker-a", int64(1), "tenant-a", datamigration.DomainKnowledge,
		).WillReturnRows(sqlmock.NewRows([]string{"marker"}).AddRow(1))
		mock.ExpectExec("INSERT INTO data_migration_records").WithArgs(
			"tenant-a", datamigration.DomainKnowledge, record.Key, migrationID, record.Payload, record.Version, record.Hash, true,
		).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(`UPDATE data_migration_records SET projected_at=clock_timestamp\(\)`).WithArgs(
			"tenant-a", datamigration.DomainKnowledge, record.Key, migrationID, record.Version, record.Hash, true,
		).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(`UPDATE data_migrations SET updated_at=clock_timestamp\(\)`).WithArgs(
			migrationID, "worker-a", int64(1), "tenant-a", datamigration.DomainKnowledge,
		).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
		if err := target.Upsert(context.Background(), "tenant-a", datamigration.DomainKnowledge, fence, []datamigration.Record{record}); err != nil {
			t.Fatalf("migration %s upsert: %v", migrationID, err)
		}
	}
	if len(projector.records) != 2 {
		t.Fatalf("fresh target projection count=%d, want 2", len(projector.records))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTargetRejectsDuplicateRecordKeysBeforeSideEffects(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	projector := &recordingProjector{domain: datamigration.DomainKnowledge}
	target, err := NewTarget(db, projector)
	if err != nil {
		t.Fatal(err)
	}
	record, err := NewKnowledgeTombstone("support", "faq-1", 18)
	if err != nil {
		t.Fatal(err)
	}
	fence := datamigration.LeaseFence{MigrationID: "migration-1", Owner: "worker-a", Version: 7}
	err = target.Upsert(context.Background(), "tenant-a", datamigration.DomainKnowledge, fence, []datamigration.Record{record, record})
	if !errors.Is(err, datamigration.ErrInvalidRecord) {
		t.Fatalf("error = %v, want ErrInvalidRecord", err)
	}
	if len(projector.records) != 0 {
		t.Fatalf("duplicate batch produced side effects: %#v", projector.records)
	}
}

func TestTargetSkipsAlreadyProjectedIdenticalRecord(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	projector := &recordingProjector{domain: datamigration.DomainKnowledge}
	target, err := NewTarget(db, projector)
	if err != nil {
		t.Fatal(err)
	}
	record, err := NewKnowledgeTombstone("support", "faq-1", 18)
	if err != nil {
		t.Fatal(err)
	}
	fence := datamigration.LeaseFence{MigrationID: "migration-1", Owner: "worker-a", Version: 7}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT 1 FROM data_migrations").WithArgs(
		"migration-1", "worker-a", int64(7), "tenant-a", datamigration.DomainKnowledge,
	).WillReturnRows(sqlmock.NewRows([]string{"marker"}).AddRow(1))
	mock.ExpectExec("INSERT INTO data_migration_records").WithArgs(
		"tenant-a", datamigration.DomainKnowledge, record.Key, "migration-1", record.Payload, record.Version, record.Hash, true,
	).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT version,content_hash,deleted,projected_at FROM data_migration_records").WithArgs(
		"tenant-a", datamigration.DomainKnowledge, record.Key, "migration-1",
	).WillReturnRows(sqlmock.NewRows([]string{"version", "content_hash", "deleted", "projected_at"}).AddRow(
		record.Version, record.Hash, true, time.Now(),
	))
	mock.ExpectExec(`UPDATE data_migrations SET updated_at=clock_timestamp\(\)`).WithArgs(
		"migration-1", "worker-a", int64(7), "tenant-a", datamigration.DomainKnowledge,
	).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := target.Upsert(context.Background(), "tenant-a", datamigration.DomainKnowledge, fence, []datamigration.Record{record}); err != nil {
		t.Fatal(err)
	}
	if len(projector.records) != 0 {
		t.Fatalf("identical record was projected again: %#v", projector.records)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTargetRejectsStaleFenceBeforeExternalSideEffect(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	projector := &recordingProjector{domain: datamigration.DomainKnowledge}
	target, err := NewTarget(db, projector)
	if err != nil {
		t.Fatal(err)
	}
	record, err := NewKnowledgeTombstone("support", "faq-1", 18)
	if err != nil {
		t.Fatal(err)
	}
	fence := datamigration.LeaseFence{MigrationID: "migration-1", Owner: "stale-worker", Version: 6}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT 1 FROM data_migrations").WithArgs(
		"migration-1", "stale-worker", int64(6), "tenant-a", datamigration.DomainKnowledge,
	).WillReturnRows(sqlmock.NewRows([]string{"marker"}))
	mock.ExpectRollback()

	err = target.Upsert(context.Background(), "tenant-a", datamigration.DomainKnowledge, fence, []datamigration.Record{record})
	if !errors.Is(err, datamigration.ErrMigrationFence) {
		t.Fatalf("error = %v, want ErrMigrationFence", err)
	}
	if len(projector.records) != 0 {
		t.Fatalf("stale fence produced external side effects: %#v", projector.records)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTargetRejectsSameVersionConflictBeforeExternalSideEffect(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	projector := &recordingProjector{domain: datamigration.DomainArtifact}
	target, err := NewTarget(db, projector)
	if err != nil {
		t.Fatal(err)
	}
	record, err := NewArtifactTombstone(
		artifact.SessionInfo{AppName: "support", UserID: "owner-1", SessionID: "session-1"},
		"report.txt", 7, "text/plain", 18,
	)
	if err != nil {
		t.Fatal(err)
	}
	fence := datamigration.LeaseFence{MigrationID: "migration-1", Owner: "worker-a", Version: 7}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT 1 FROM data_migrations").WithArgs(
		"migration-1", "worker-a", int64(7), "tenant-a", datamigration.DomainArtifact,
	).WillReturnRows(sqlmock.NewRows([]string{"marker"}).AddRow(1))
	mock.ExpectExec("INSERT INTO data_migration_records").WithArgs(
		"tenant-a", datamigration.DomainArtifact, record.Key, "migration-1", record.Payload, record.Version, record.Hash, true,
	).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT version,content_hash,deleted,projected_at FROM data_migration_records").WithArgs(
		"tenant-a", datamigration.DomainArtifact, record.Key, "migration-1",
	).WillReturnRows(sqlmock.NewRows([]string{"version", "content_hash", "deleted", "projected_at"}).AddRow(
		record.Version, "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", true, nil,
	))
	mock.ExpectRollback()

	err = target.Upsert(context.Background(), "tenant-a", datamigration.DomainArtifact, fence, []datamigration.Record{record})
	if !errors.Is(err, datamigration.ErrInvalidRecord) {
		t.Fatalf("error = %v, want ErrInvalidRecord", err)
	}
	if len(projector.records) != 0 {
		t.Fatalf("conflicting version produced external side effects: %#v", projector.records)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTargetRollsBackLedgerWhenProjectorFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	projectionFailure := errors.New("object store unavailable")
	projector := &recordingProjector{domain: datamigration.DomainArtifact, err: projectionFailure}
	target, err := NewTarget(db, projector)
	if err != nil {
		t.Fatal(err)
	}
	record, err := NewArtifactTombstone(
		artifact.SessionInfo{AppName: "support", UserID: "owner-1", SessionID: "session-1"},
		"report.txt", 7, "text/plain", 18,
	)
	if err != nil {
		t.Fatal(err)
	}
	fence := datamigration.LeaseFence{MigrationID: "migration-1", Owner: "worker-a", Version: 7}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT 1 FROM data_migrations").WithArgs(
		"migration-1", "worker-a", int64(7), "tenant-a", datamigration.DomainArtifact,
	).WillReturnRows(sqlmock.NewRows([]string{"marker"}).AddRow(1))
	mock.ExpectExec("INSERT INTO data_migration_records").WithArgs(
		"tenant-a", datamigration.DomainArtifact, record.Key, "migration-1", record.Payload, record.Version, record.Hash, true,
	).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectRollback()

	err = target.Upsert(context.Background(), "tenant-a", datamigration.DomainArtifact, fence, []datamigration.Record{record})
	if !errors.Is(err, projectionFailure) {
		t.Fatalf("error = %v, want projector failure", err)
	}
	if len(projector.records) != 1 {
		t.Fatalf("projector call count = %d, want 1", len(projector.records))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTargetRollsBackMarkerWhenFinalFenceIsLost(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	projector := &recordingProjector{domain: datamigration.DomainKnowledge}
	target, err := NewTarget(db, projector)
	if err != nil {
		t.Fatal(err)
	}
	record, err := NewKnowledgeTombstone("support", "faq-1", 18)
	if err != nil {
		t.Fatal(err)
	}
	fence := datamigration.LeaseFence{MigrationID: "migration-1", Owner: "worker-a", Version: 7}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT 1 FROM data_migrations").WithArgs(
		"migration-1", "worker-a", int64(7), "tenant-a", datamigration.DomainKnowledge,
	).WillReturnRows(sqlmock.NewRows([]string{"marker"}).AddRow(1))
	mock.ExpectExec("INSERT INTO data_migration_records").WithArgs(
		"tenant-a", datamigration.DomainKnowledge, record.Key, "migration-1", record.Payload, record.Version, record.Hash, true,
	).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE data_migration_records SET projected_at=clock_timestamp\(\)`).WithArgs(
		"tenant-a", datamigration.DomainKnowledge, record.Key, "migration-1", record.Version, record.Hash, true,
	).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE data_migrations SET updated_at=clock_timestamp\(\)`).WithArgs(
		"migration-1", "worker-a", int64(7), "tenant-a", datamigration.DomainKnowledge,
	).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	err = target.Upsert(context.Background(), "tenant-a", datamigration.DomainKnowledge, fence, []datamigration.Record{record})
	if !errors.Is(err, datamigration.ErrMigrationFence) {
		t.Fatalf("error = %v, want ErrMigrationFence", err)
	}
	if len(projector.records) != 1 {
		t.Fatalf("external side effect count = %d, want one idempotent replay candidate", len(projector.records))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTargetReplaysSameVersionWhenProjectionMarkerIsMissing(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	projector := &recordingProjector{domain: datamigration.DomainKnowledge}
	target, err := NewTarget(db, projector)
	if err != nil {
		t.Fatal(err)
	}
	record, err := NewKnowledgeTombstone("support", "faq-1", 18)
	if err != nil {
		t.Fatal(err)
	}
	fence := datamigration.LeaseFence{MigrationID: "migration-1", Owner: "worker-b", Version: 8}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT 1 FROM data_migrations").WithArgs(
		"migration-1", "worker-b", int64(8), "tenant-a", datamigration.DomainKnowledge,
	).WillReturnRows(sqlmock.NewRows([]string{"marker"}).AddRow(1))
	mock.ExpectExec("INSERT INTO data_migration_records").WithArgs(
		"tenant-a", datamigration.DomainKnowledge, record.Key, "migration-1", record.Payload, record.Version, record.Hash, true,
	).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT version,content_hash,deleted,projected_at FROM data_migration_records").WithArgs(
		"tenant-a", datamigration.DomainKnowledge, record.Key, "migration-1",
	).WillReturnRows(sqlmock.NewRows([]string{"version", "content_hash", "deleted", "projected_at"}).AddRow(
		record.Version, record.Hash, true, nil,
	))
	mock.ExpectExec(`UPDATE data_migration_records SET projected_at=clock_timestamp\(\)`).WithArgs(
		"tenant-a", datamigration.DomainKnowledge, record.Key, "migration-1", record.Version, record.Hash, true,
	).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE data_migrations SET updated_at=clock_timestamp\(\)`).WithArgs(
		"migration-1", "worker-b", int64(8), "tenant-a", datamigration.DomainKnowledge,
	).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := target.Upsert(context.Background(), "tenant-a", datamigration.DomainKnowledge, fence, []datamigration.Record{record}); err != nil {
		t.Fatal(err)
	}
	if len(projector.records) != 1 {
		t.Fatalf("unmarked same version projection count = %d, want 1", len(projector.records))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

type recordingProjector struct {
	domain   datamigration.Domain
	tenantID string
	records  []datamigration.Record
	err      error
}

func (p *recordingProjector) Domain() datamigration.Domain { return p.domain }
func (p *recordingProjector) Apply(_ context.Context, tenantID string, record datamigration.Record) error {
	p.tenantID = tenantID
	p.records = append(p.records, record)
	return p.err
}
