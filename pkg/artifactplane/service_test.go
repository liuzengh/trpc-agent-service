package artifactplane

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
)

type recordingObjectStore struct {
	key, mime string
	data      []byte
	deleted   bool
}

func TestServiceDeletesOneProjectedVersionAndHidesItFromLoads(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	objects := &recordingObjectStore{key: "tsa-artifacts/object-7", data: []byte("hello")}
	service, err := NewService("tenant-a", db, objects, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	info := artifact.SessionInfo{AppName: "support", UserID: "owner-1", SessionID: "session-1"}
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")).WithArgs(sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("UPDATE artifact_versions SET deleted_at=COALESCE").WithArgs(
		"tenant-a", "support", "owner-1", "session-1", "report.txt", 7,
	).WillReturnRows(sqlmock.NewRows([]string{"object_key"}).AddRow(objects.key))
	mock.ExpectCommit()
	if err := service.DeleteProjectedVersion(context.Background(), info, "report.txt", 7); err != nil {
		t.Fatalf("delete projected version: %v", err)
	}
	if !objects.deleted {
		t.Fatal("tombstoned object body was not deleted")
	}
	mock.ExpectQuery("SELECT object_key,mime_type,size_bytes,content_sha256 FROM artifact_versions").WithArgs(
		"tenant-a", "support", "owner-1", "session-1", "report.txt", 7,
	).WillReturnError(sql.ErrNoRows)
	version := 7
	loaded, err := service.LoadArtifact(context.Background(), info, "report.txt", &version)
	if err != nil || loaded != nil {
		t.Fatalf("loaded=%#v error=%v", loaded, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestServiceProjectsExactVersionIdempotentlyAndMakesItLoadable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	objects := &recordingObjectStore{}
	service, err := NewService("tenant-a", db, objects, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	info := artifact.SessionInfo{AppName: "support", UserID: "owner-1", SessionID: "session-1"}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")).WithArgs(sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT mime_type,size_bytes,content_sha256,deleted_at,object_key FROM artifact_versions").WithArgs(
		"tenant-a", "support", "owner-1", "session-1", "report.txt", 7,
	).WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO artifact_versions").WithArgs(
		"tenant-a", "support", "owner-1", "session-1", "report.txt", 7,
		sqlmock.AnyArg(), "text/plain", int64(5), sha256Matcher("hello"),
	).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := service.ProjectVersion(context.Background(), info, "report.txt", 7, &artifact.Artifact{Data: []byte("hello"), MimeType: "text/plain"}); err != nil {
		t.Fatalf("project: %v", err)
	}
	if objects.key == "" || string(objects.data) != "hello" {
		t.Fatalf("stored object = %q/%q", objects.key, objects.data)
	}

	mock.ExpectQuery("SELECT object_key,mime_type,size_bytes,content_sha256 FROM artifact_versions").WithArgs(
		"tenant-a", "support", "owner-1", "session-1", "report.txt", 7,
	).WillReturnRows(sqlmock.NewRows([]string{"object_key", "mime_type", "size_bytes", "content_sha256"}).AddRow(
		objects.key, "text/plain", 5, hashBytes([]byte("hello")),
	))
	version := 7
	loaded, err := service.LoadArtifact(context.Background(), info, "report.txt", &version)
	if err != nil {
		t.Fatal(err)
	}
	if loaded == nil || string(loaded.Data) != "hello" || loaded.MimeType != "text/plain" {
		t.Fatalf("loaded = %#v", loaded)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func (s *recordingObjectStore) Put(_ context.Context, key, mime string, data []byte) error {
	s.key = key
	s.mime = mime
	s.data = append([]byte(nil), data...)
	return nil
}
func (s *recordingObjectStore) Get(context.Context, string) ([]byte, error) {
	return append([]byte(nil), s.data...), nil
}
func (s *recordingObjectStore) Delete(_ context.Context, key string) error {
	if key == s.key {
		s.deleted = true
	}
	return nil
}

func TestServiceAllocatesVersionUnderDatabaseFenceAndStoresObject(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	objects := &recordingObjectStore{}
	service, err := NewService("tenant-a", db, objects, 1<<20)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	info := artifact.SessionInfo{AppName: "support", UserID: "owner-1", SessionID: "session-1"}
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")).WithArgs(sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT COALESCE\\(MAX\\(version\\), -1\\) \\+ 1").WithArgs("tenant-a", "support", "owner-1", "session-1", "report.txt").WillReturnRows(sqlmock.NewRows([]string{"next"}).AddRow(0))
	mock.ExpectExec("INSERT INTO artifact_versions").WithArgs(
		"tenant-a", "support", "owner-1", "session-1", "report.txt", 0,
		sqlmock.AnyArg(), "text/plain", int64(5), sha256Matcher("hello"),
	).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	version, err := service.SaveArtifact(context.Background(), info, "report.txt", &artifact.Artifact{Data: []byte("hello"), MimeType: "text/plain"})
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	if version != 0 || string(objects.data) != "hello" || objects.mime != "text/plain" || objects.key == "" {
		t.Fatalf("version=%d object=%q/%q/%q", version, objects.key, objects.mime, objects.data)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

type sha256Argument string

func sha256Matcher(value string) sqlmock.Argument      { return sha256Argument(value) }
func (a sha256Argument) Match(value driver.Value) bool { return value == hashBytes([]byte(a)) }
