package datamigration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func postgresJobRow(now time.Time) *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id", "tenant_id", "domain", "source_profile", "target_profile", "phase",
		"paused", "cursor", "snapshot_watermark", "applied_watermark",
		"lease_owner", "lease_version", "lease_until", "last_error", "created_at", "updated_at",
	}).AddRow("migration-1", "tenant-a", "memory", "redis-a", "postgres-a", "PREPARE",
		false, "", int64(0), int64(0), "", int64(0), nil, "", now, now)
}

func TestPostgresStoreNormalizesNilContext(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC()
	mock.ExpectQuery("SELECT id, tenant_id, domain, source_profile").
		WithArgs("migration-1").WillReturnRows(postgresJobRow(now))

	job, err := NewPostgresStore(db).Get(nil, "migration-1")
	if err != nil {
		t.Fatalf("Get with nil context: %v", err)
	}
	if job.ID != "migration-1" {
		t.Fatalf("job=%#v", job)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresStoreRejectsReclaimBySameOwnerWhileLeaseIsValid(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, tenant_id, domain, source_profile").
		WithArgs("migration-1").WillReturnRows(sqlmock.NewRows([]string{
		"id", "tenant_id", "domain", "source_profile", "target_profile", "phase",
		"paused", "cursor", "snapshot_watermark", "applied_watermark",
		"lease_owner", "lease_version", "lease_until", "last_error", "created_at", "updated_at",
	}).AddRow("migration-1", "tenant-a", "memory", "redis-a", "postgres-a", "PREPARE",
		false, "", int64(0), int64(0), "worker-a", int64(7), now.Add(time.Minute), "", now, now))
	mock.ExpectQuery("SELECT clock_timestamp\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"clock_timestamp"}).AddRow(now))
	mock.ExpectRollback()
	_, err = NewPostgresStore(db).Claim(context.Background(), "migration-1", "worker-a", time.Minute)
	if !errors.Is(err, ErrMigrationLeaseHeld) {
		t.Fatalf("same-owner reclaim error=%v, want ErrMigrationLeaseHeld", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresStoreRenewChecksOwnerAndVersion(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, tenant_id, domain, source_profile").
		WithArgs("migration-1").WillReturnRows(sqlmock.NewRows([]string{
		"id", "tenant_id", "domain", "source_profile", "target_profile", "phase",
		"paused", "cursor", "snapshot_watermark", "applied_watermark",
		"lease_owner", "lease_version", "lease_until", "last_error", "created_at", "updated_at",
	}).AddRow("migration-1", "tenant-a", "memory", "redis-a", "postgres-a", "PREPARE",
		false, "", int64(0), int64(0), "worker-a", int64(7), now.Add(time.Minute), "", now, now))
	mock.ExpectQuery("SELECT clock_timestamp\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"clock_timestamp"}).AddRow(now))
	mock.ExpectExec("UPDATE data_migrations").
		WithArgs("migration-1", sqlmock.AnyArg(), "worker-a", int64(7)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	job, err := NewPostgresStore(db).Renew(context.Background(), "migration-1", "worker-a", 7, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if job.LeaseOwner != "worker-a" || job.LeaseVersion != 7 {
		t.Fatalf("renewed job=%#v", job)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresStoreRejectsSubMillisecondLeaseTTL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewPostgresStore(db)
	if _, err := store.Claim(context.Background(), "migration-1", "worker-a", 999*time.Microsecond); !errors.Is(err, ErrInvalidMigration) {
		t.Fatalf("claim error=%v, want ErrInvalidMigration", err)
	}
	if _, err := store.Renew(context.Background(), "migration-1", "worker-a", 1, 999*time.Microsecond); !errors.Is(err, ErrInvalidMigration) {
		t.Fatalf("renew error=%v, want ErrInvalidMigration", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresStoreClaimAndRelease(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, tenant_id, domain, source_profile").
		WithArgs("migration-1").WillReturnRows(postgresJobRow(now))
	mock.ExpectQuery("SELECT clock_timestamp\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"clock_timestamp"}).AddRow(now))
	mock.ExpectExec("UPDATE data_migrations").
		WithArgs("migration-1", "worker-a", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	store := NewPostgresStore(db)
	job, err := store.Claim(context.Background(), "migration-1", "worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if job.LeaseOwner != "worker-a" || job.LeaseVersion != 1 || job.Phase != PhasePrepare {
		t.Fatalf("claimed job=%#v", job)
	}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, tenant_id, domain, source_profile").
		WithArgs("migration-1").WillReturnRows(sqlmock.NewRows([]string{
		"id", "tenant_id", "domain", "source_profile", "target_profile", "phase",
		"paused", "cursor", "snapshot_watermark", "applied_watermark",
		"lease_owner", "lease_version", "lease_until", "last_error", "created_at", "updated_at",
	}).AddRow("migration-1", "tenant-a", "memory", "redis-a", "postgres-a", "PREPARE",
		false, "", int64(0), int64(0), "worker-a", int64(1), now.Add(time.Minute), "", now, now))
	mock.ExpectQuery("SELECT clock_timestamp\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"clock_timestamp"}).AddRow(now))
	mock.ExpectExec("UPDATE data_migrations").
		WithArgs("migration-1", "worker-a", int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := store.Release(context.Background(), "migration-1", "worker-a", 1); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresStoreClaimUsesDatabaseClockForLeaseDecision(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	localNow := time.Now().UTC()
	databaseNow := localNow.Add(-24 * time.Hour)
	leaseUntil := databaseNow.Add(-time.Minute)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, tenant_id, domain, source_profile").
		WithArgs("migration-1").WillReturnRows(sqlmock.NewRows([]string{
		"id", "tenant_id", "domain", "source_profile", "target_profile", "phase",
		"paused", "cursor", "snapshot_watermark", "applied_watermark",
		"lease_owner", "lease_version", "lease_until", "last_error", "created_at", "updated_at",
	}).AddRow("migration-1", "tenant-a", "memory", "redis-a", "postgres-a", "PREPARE",
		false, "", int64(0), int64(0), "worker-old", int64(7), leaseUntil, "", databaseNow, databaseNow))
	mock.ExpectQuery("SELECT clock_timestamp\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"clock_timestamp"}).AddRow(databaseNow))
	mock.ExpectExec("UPDATE data_migrations").
		WithArgs("migration-1", "worker-new", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	job, err := NewPostgresStore(db).Claim(context.Background(), "migration-1", "worker-new", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if job.LeaseOwner != "worker-new" || job.LeaseVersion != 8 {
		t.Fatalf("claimed job=%#v", job)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresStoreAdvanceRequiresFinalDatabaseFence(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC()
	next := PhaseSnapshotCopy
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, tenant_id, domain, source_profile").
		WithArgs("migration-1").WillReturnRows(sqlmock.NewRows([]string{
		"id", "tenant_id", "domain", "source_profile", "target_profile", "phase",
		"paused", "cursor", "snapshot_watermark", "applied_watermark",
		"lease_owner", "lease_version", "lease_until", "last_error", "created_at", "updated_at",
	}).AddRow("migration-1", "tenant-a", "memory", "redis-a", "postgres-a", "PREPARE",
		false, "", int64(0), int64(0), "worker-a", int64(7), now.Add(time.Minute), "", now, now))
	mock.ExpectQuery("SELECT clock_timestamp\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"clock_timestamp"}).AddRow(now))
	mock.ExpectExec("UPDATE data_migrations").
		WithArgs("migration-1", next, false, "", int64(0), int64(0), "", "worker-a", int64(7)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	_, err = NewPostgresStore(db).Advance(context.Background(), "migration-1", "worker-a", 7, JobPatch{Phase: &next})
	if !errors.Is(err, ErrMigrationFence) {
		t.Fatalf("advance error=%v, want ErrMigrationFence", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresStoreRejectsMigrationCheckpointRegression(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC()
	backward := int64(3)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, tenant_id, domain, source_profile").
		WithArgs("migration-1").WillReturnRows(sqlmock.NewRows([]string{
		"id", "tenant_id", "domain", "source_profile", "target_profile", "phase",
		"paused", "cursor", "snapshot_watermark", "applied_watermark",
		"lease_owner", "lease_version", "lease_until", "last_error", "created_at", "updated_at",
	}).AddRow("migration-1", "tenant-a", "memory", "redis-a", "postgres-a", "SNAPSHOT_COPY",
		false, "snapshot-4", int64(4), int64(4), "worker-a", int64(7), now.Add(time.Minute), "", now, now))
	mock.ExpectQuery("SELECT clock_timestamp\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"clock_timestamp"}).AddRow(now))
	mock.ExpectRollback()

	_, err = NewPostgresStore(db).Advance(context.Background(), "migration-1", "worker-a", 7,
		JobPatch{SnapshotWatermark: &backward})
	if !errors.Is(err, ErrInvalidMigration) {
		t.Fatalf("checkpoint regression error=%v, want ErrInvalidMigration", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
