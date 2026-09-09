package datamigration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresTargetPropagatesRecordInsertFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	target := NewPostgresTarget(db)
	record := migrationRecord("k1", 2, "payload")
	fence := LeaseFence{MigrationID: "migration-1", Owner: "worker-a", Version: 7}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT 1 FROM data_migrations").WithArgs("migration-1", "worker-a", int64(7), "tenant-a", DomainMemory).WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))
	backendErr := errors.New("record lookup failed")
	mock.ExpectExec("INSERT INTO data_migration_records").WithArgs("tenant-a", DomainMemory, "k1", "migration-1", record.Payload, record.Version, record.Hash, false).WillReturnError(backendErr)
	mock.ExpectRollback()
	// A backend error must abort before the transaction can be committed.
	if err := target.Upsert(context.Background(), "tenant-a", DomainMemory, fence, []Record{record}); err == nil {
		t.Fatal("unexpected successful upsert after backend read error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresTargetInsertsRecordWithLeaseFence(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	target := NewPostgresTarget(db)
	record := migrationRecord("k1", 2, "payload")
	fence := LeaseFence{MigrationID: "migration-1", Owner: "worker-a", Version: 7}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT 1 FROM data_migrations").WithArgs("migration-1", "worker-a", int64(7), "tenant-a", DomainMemory).WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))
	mock.ExpectExec("INSERT INTO data_migration_records").WithArgs("tenant-a", DomainMemory, "k1", "migration-1", record.Payload, record.Version, record.Hash, false).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE data_migrations SET updated_at=clock_timestamp()").WithArgs("migration-1", "worker-a", int64(7), "tenant-a", DomainMemory).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := target.Upsert(context.Background(), "tenant-a", DomainMemory, fence, []Record{record}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresTargetHandlesVersionedConflict(t *testing.T) {
	for _, test := range []struct {
		name            string
		existingVersion int64
		existingHash    string
		wantErr         error
		wantUpdate      bool
	}{
		{name: "newer target wins", existingVersion: 3, existingHash: "other"},
		{name: "equal idempotent", existingVersion: 2, existingHash: migrationRecord("k1", 2, "payload").Hash},
		{name: "equal conflict", existingVersion: 2, existingHash: migrationRecord("k1", 2, "different").Hash, wantErr: ErrInvalidRecord},
		{name: "source advances", existingVersion: 1, existingHash: migrationRecord("k1", 1, "old").Hash, wantUpdate: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			record := migrationRecord("k1", 2, "payload")
			fence := LeaseFence{MigrationID: "migration-1", Owner: "worker-a", Version: 7}
			mock.ExpectBegin()
			mock.ExpectQuery("SELECT 1 FROM data_migrations").WithArgs("migration-1", "worker-a", int64(7), "tenant-a", DomainMemory).WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))
			mock.ExpectExec("INSERT INTO data_migration_records").WithArgs("tenant-a", DomainMemory, "k1", "migration-1", record.Payload, record.Version, record.Hash, false).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery("SELECT version, content_hash, deleted FROM data_migration_records").WithArgs("tenant-a", DomainMemory, "k1", "migration-1").WillReturnRows(sqlmock.NewRows([]string{"version", "content_hash", "deleted"}).AddRow(test.existingVersion, test.existingHash, false))
			if test.wantUpdate {
				mock.ExpectExec(`SET payload=\$5, version=\$6, content_hash=\$7, deleted=\$8, projected_at=NULL`).WithArgs("tenant-a", DomainMemory, "k1", "migration-1", record.Payload, record.Version, record.Hash, false).WillReturnResult(sqlmock.NewResult(0, 1))
			}
			if test.wantErr == nil {
				mock.ExpectExec("UPDATE data_migrations SET updated_at=clock_timestamp()").WithArgs("migration-1", "worker-a", int64(7), "tenant-a", DomainMemory).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()
			} else {
				mock.ExpectRollback()
			}
			err = NewPostgresTarget(db).Upsert(context.Background(), "tenant-a", DomainMemory, fence, []Record{record})
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("Upsert error=%v, want %v", err, test.wantErr)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPostgresTargetRejectsStaleFenceBeforeRecordReads(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	target := NewPostgresTarget(db)
	fence := LeaseFence{MigrationID: "migration-1", Owner: "worker-a", Version: 7}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT 1 FROM data_migrations").WithArgs("migration-1", "worker-a", int64(7), "tenant-a", DomainMemory).WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()
	if err := target.Upsert(context.Background(), "tenant-a", DomainMemory, fence, nil); !errors.Is(err, ErrMigrationFence) {
		t.Fatalf("fence error=%v, want ErrMigrationFence", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresTargetBindsFenceToTenantAndDomain(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fence := LeaseFence{MigrationID: "migration-1", Owner: "worker-a", Version: 7}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT 1 FROM data_migrations").WithArgs("migration-1", "worker-a", int64(7), "tenant-b", DomainSession).WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()
	err = NewPostgresTarget(db).Upsert(context.Background(), "tenant-b", DomainSession, fence, nil)
	if !errors.Is(err, ErrMigrationFence) {
		t.Fatalf("scope mismatch error=%v, want ErrMigrationFence", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresTargetCanonicalizesTombstonePayload(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	target := NewPostgresTarget(db)
	digest := sha256.Sum256(nil)
	record := Record{Key: "deleted", Version: 3, Hash: hex.EncodeToString(digest[:]), Deleted: true}
	fence := LeaseFence{MigrationID: "migration-1", Owner: "worker-a", Version: 7}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT 1 FROM data_migrations").WithArgs("migration-1", "worker-a", int64(7), "tenant-a", DomainMemory).WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))
	mock.ExpectExec("INSERT INTO data_migration_records").WithArgs("tenant-a", DomainMemory, "deleted", "migration-1", []byte{}, record.Version, record.Hash, true).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE data_migrations SET updated_at=clock_timestamp()").WithArgs("migration-1", "worker-a", int64(7), "tenant-a", DomainMemory).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := target.Upsert(context.Background(), "tenant-a", DomainMemory, fence, []Record{record}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
