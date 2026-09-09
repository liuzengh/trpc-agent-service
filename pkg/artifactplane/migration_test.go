package artifactplane

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/minio/minio-go/v7"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
)

type migrationObjectProbe struct {
	objects   map[string][]byte
	prefix    string
	deleteErr error
	deletes   int
}

func (s *migrationObjectProbe) Put(_ context.Context, key, _ string, body []byte) error {
	s.objects[key] = append([]byte(nil), body...)
	return nil
}
func (s *migrationObjectProbe) Get(_ context.Context, key string) ([]byte, error) {
	body, ok := s.objects[key]
	if !ok {
		return nil, minio.ErrorResponse{Code: "NoSuchKey", StatusCode: 404}
	}
	return append([]byte(nil), body...), nil
}
func (s *migrationObjectProbe) Delete(_ context.Context, key string) error {
	s.deletes++
	if s.deleteErr != nil {
		return s.deleteErr
	}
	delete(s.objects, key)
	return nil
}

func TestArtifactMigrationRecoversCommittedSourceDeletion(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	objects := &migrationObjectProbe{objects: map[string][]byte{}, deleteErr: errors.New("object store temporarily unavailable")}
	service, err := NewService("tenant-a", db, objects, 1024)
	if err != nil {
		t.Fatal(err)
	}
	info := artifact.SessionInfo{AppName: "support", UserID: "user", SessionID: "session"}
	key := newObjectKey("tenant-a", info, "history.txt", 3)
	objects.objects[key] = []byte("old")
	stateRows := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"object_key", "mime_type", "deleted"}).AddRow(key, "text/plain", true)
	}
	stateQuery := `SELECT object_key,mime_type,deleted_at IS NOT NULL FROM artifact_versions`
	mock.ExpectQuery(stateQuery).WithArgs("tenant-a", "support", "user", "session", "history.txt", 3).WillReturnRows(stateRows())
	if err := service.ReconcileMigrationVersion(context.Background(), info, "history.txt", 3, "text/plain"); !errors.Is(err, objects.deleteErr) {
		t.Fatalf("cleanup failure lost: %v", err)
	}
	objects.deleteErr = nil
	mock.ExpectQuery(stateQuery).WithArgs("tenant-a", "support", "user", "session", "history.txt", 3).WillReturnRows(stateRows())
	if err := service.ReconcileMigrationVersion(context.Background(), info, "history.txt", 3, "text/plain"); err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery(`SELECT object_key,mime_type,size_bytes,content_sha256,deleted_at IS NOT NULL FROM artifact_versions`).WithArgs("tenant-a", "support", "user", "session", "history.txt", 3).WillReturnRows(
		sqlmock.NewRows([]string{"object_key", "mime_type", "size_bytes", "content_sha256", "deleted"}).AddRow(key, "text/plain", 3, hashBytes([]byte("old")), true))
	entry, value, err := service.ReadMigrationVersion(context.Background(), info, "history.txt", 3)
	if err != nil || !entry.Deleted || value != nil || objects.deletes != 2 {
		t.Fatalf("recovered tombstone=%+v value=%v deletes=%d err=%v", entry, value, objects.deletes, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactMigrationDeletionProtectsActiveAndDifferentMIMEVersions(t *testing.T) {
	for _, operation := range []string{"source", "target"} {
		for _, state := range []string{"active", "different_mime", "deleted", "foreign_key"} {
			t.Run(operation+"/"+state, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				objects := &migrationObjectProbe{objects: map[string][]byte{}}
				service, err := NewService("tenant-a", db, objects, 1024)
				if err != nil {
					t.Fatal(err)
				}
				info := artifact.SessionInfo{AppName: "support", UserID: "user", SessionID: "session"}
				key := newObjectKey("tenant-a", info, "report", 0)
				actualMIME := "text/plain"
				if state == "different_mime" {
					actualMIME = "application/json"
				}
				if state == "foreign_key" {
					key = newObjectKey("tenant-b", info, "report", 0)
				}
				objects.objects[key] = []byte("body")
				mock.ExpectQuery(`SELECT object_key,mime_type,deleted_at IS NOT NULL FROM artifact_versions`).WithArgs("tenant-a", "support", "user", "session", "report", 0).WillReturnRows(
					sqlmock.NewRows([]string{"object_key", "mime_type", "deleted"}).AddRow(key, actualMIME, state != "active"))
				if operation == "source" {
					err = service.ReconcileMigrationVersion(context.Background(), info, "report", 0, "text/plain")
				} else {
					err = service.DeleteMigrationVersion(context.Background(), info, "report", 0, "text/plain")
				}
				if state == "foreign_key" {
					if !errors.Is(err, ErrArtifactCorrupt) {
						t.Fatalf("foreign key allowed: %v", err)
					}
				} else if operation == "target" && state == "active" {
					if !errors.Is(err, ErrArtifactVersionConflict) {
						t.Fatalf("active version allowed: %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				if objects.deletes != 0 && state != "deleted" {
					t.Fatalf("protected version deleted: %d", objects.deletes)
				}
				if state == "deleted" && objects.deletes != 1 {
					t.Fatalf("tombstone not cleaned: %d", objects.deletes)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
func (s *migrationObjectProbe) HasObjectPrefix(_ context.Context, prefix string) (bool, error) {
	s.prefix = prefix
	return len(s.objects) > 0, nil
}

func TestArtifactMigrationVerifiesBucketDeletionAgainstSharedMetadata(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	objects := &migrationObjectProbe{objects: map[string][]byte{}}
	service, err := NewService("tenant-a", db, objects, 1024)
	if err != nil {
		t.Fatal(err)
	}
	info := artifact.SessionInfo{AppName: "support", UserID: "user", SessionID: "session"}
	key := newObjectKey("tenant-a", info, "history.txt", 3)
	query := `SELECT object_key,mime_type,size_bytes,content_sha256,deleted_at IS NOT NULL FROM artifact_versions`
	rows := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"object_key", "mime_type", "size_bytes", "content_sha256", "deleted"}).AddRow(key, "text/plain", 3, hashBytes([]byte("old")), true)
	}
	mock.ExpectQuery(query).WithArgs("tenant-a", "support", "user", "session", "history.txt", 3).WillReturnRows(rows())
	entry, value, err := service.ReadMigrationVersion(context.Background(), info, "history.txt", 3)
	if err != nil || !entry.Deleted || value != nil {
		t.Fatalf("absent tombstone=%+v value=%v err=%v", entry, value, err)
	}
	objects.objects[key] = []byte("old")
	mock.ExpectQuery(query).WithArgs("tenant-a", "support", "user", "session", "history.txt", 3).WillReturnRows(rows())
	if _, _, err := service.ReadMigrationVersion(context.Background(), info, "history.txt", 3); !errors.Is(err, ErrArtifactCorrupt) {
		t.Fatalf("shared SQL tombstone falsely verified undeleted target body: %v", err)
	}
	if empty, err := service.MigrationTargetEmpty(context.Background()); err != nil || empty || objects.prefix != "tsa-artifacts/v1/dGVuYW50LWE/" {
		t.Fatalf("physical namespace empty=%v prefix=%s err=%v", empty, objects.prefix, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactMigrationPaginationIncludesTombstonedHistory(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service, err := NewService("tenant-a", db, &migrationObjectProbe{objects: map[string][]byte{}}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	columns := []string{"app_name", "user_id", "session_id", "filename", "version", "mime_type", "deleted"}
	mock.ExpectQuery(`SELECT app_name,user_id,session_id,filename,version,mime_type,deleted_at IS NOT NULL`).WithArgs("tenant-a", 2).WillReturnRows(sqlmock.NewRows(columns).AddRow("support", "user", "session", "history.txt", 0, "text/plain", true).AddRow("support", "user", "session", "history.txt", 1, "text/plain", false))
	entries, cursor, done, err := service.MigrationEntries(context.Background(), "", 1)
	if err != nil || done || cursor == "" || len(entries) != 1 || !entries[0].Deleted {
		t.Fatalf("first page=%+v cursor=%s done=%v err=%v", entries, cursor, done, err)
	}
	mock.ExpectQuery(`SELECT app_name,user_id,session_id,filename,version,mime_type,deleted_at IS NOT NULL`).WithArgs("tenant-a", "support", "user", "session", "history.txt", 0, 2).WillReturnRows(sqlmock.NewRows(columns).AddRow("support", "user", "session", "history.txt", 1, "text/plain", false))
	entries, _, done, err = service.MigrationEntries(context.Background(), cursor, 1)
	if err != nil || !done || len(entries) != 1 || entries[0].Version != 1 {
		t.Fatalf("second page=%+v done=%v err=%v", entries, done, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
