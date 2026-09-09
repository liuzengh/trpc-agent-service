package dataprojection

import (
	"context"
	"database/sql"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/artifactplane"
)

func TestArtifactProjectorPreservesSourceVersionAndMakesBodyLoadable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	objects := &projectionObjectStore{values: map[string][]byte{}}
	service, err := artifactplane.NewService("tenant-a", db, objects, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	info := artifact.SessionInfo{AppName: "support", UserID: "owner-1", SessionID: "session-1"}
	record, err := NewArtifactRecord(info, "report.txt", 7, &artifact.Artifact{Data: []byte("hello"), MimeType: "text/plain"}, 23)
	if err != nil {
		t.Fatal(err)
	}
	projector, err := NewArtifactProjector(func(_ context.Context, tenantID string) (ArtifactVersionStore, error) {
		if tenantID != "tenant-a" {
			t.Fatalf("resolver tenant = %q", tenantID)
		}
		return service, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")).WithArgs(sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT mime_type,size_bytes,content_sha256,deleted_at,object_key FROM artifact_versions").WithArgs(
		"tenant-a", "support", "owner-1", "session-1", "report.txt", 7,
	).WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO artifact_versions").WithArgs(
		"tenant-a", "support", "owner-1", "session-1", "report.txt", 7,
		sqlmock.AnyArg(), "text/plain", int64(5), sqlmock.AnyArg(),
	).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := projector.Apply(context.Background(), "tenant-a", record); err != nil {
		t.Fatalf("apply: %v", err)
	}

	key := objects.lastKey
	mock.ExpectQuery("SELECT object_key,mime_type,size_bytes,content_sha256 FROM artifact_versions").WithArgs(
		"tenant-a", "support", "owner-1", "session-1", "report.txt", 7,
	).WillReturnRows(sqlmock.NewRows([]string{"object_key", "mime_type", "size_bytes", "content_sha256"}).AddRow(
		key, "text/plain", 5, projectionHash([]byte("hello")),
	))
	version := 7
	loaded, err := service.LoadArtifact(context.Background(), info, "report.txt", &version)
	if err != nil {
		t.Fatal(err)
	}
	if loaded == nil || string(loaded.Data) != "hello" {
		t.Fatalf("loaded = %#v", loaded)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactProjectionKeySupportsFullValidatedScopeLengths(t *testing.T) {
	info := artifact.SessionInfo{
		AppName: "app-" + strings.Repeat("a", 124),
		UserID:  strings.Repeat("u", 255), SessionID: strings.Repeat("s", 512),
	}
	record, err := NewArtifactRecord(info, strings.Repeat("f", 508)+".txt", 7,
		&artifact.Artifact{Data: []byte("hello"), MimeType: "text/plain"}, 23)
	if err != nil {
		t.Fatalf("full valid artifact scope was rejected: %v", err)
	}
	if len(record.Key) <= 1024 || len(record.Key) > 4096 {
		t.Fatalf("encoded key length = %d", len(record.Key))
	}
}

func TestArtifactProjectorReplaysExactVersionTombstone(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	objects := &projectionObjectStore{values: map[string][]byte{"object-7": []byte("hello")}}
	service, err := artifactplane.NewService("tenant-a", db, objects, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	info := artifact.SessionInfo{AppName: "support", UserID: "owner-1", SessionID: "session-1"}
	record, err := NewArtifactTombstone(info, "report.txt", 7, "text/plain", 24)
	if err != nil {
		t.Fatal(err)
	}
	projector, err := NewArtifactProjector(func(context.Context, string) (ArtifactVersionStore, error) { return service, nil })
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")).WithArgs(sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("UPDATE artifact_versions SET deleted_at=COALESCE").WithArgs(
		"tenant-a", "support", "owner-1", "session-1", "report.txt", 7,
	).WillReturnRows(sqlmock.NewRows([]string{"object_key"}).AddRow("object-7"))
	mock.ExpectCommit()
	if err := projector.Apply(context.Background(), "tenant-a", record); err != nil {
		t.Fatal(err)
	}
	if _, exists := objects.values["object-7"]; exists {
		t.Fatal("projected tombstone left the object body visible")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

type projectionObjectStore struct {
	values  map[string][]byte
	lastKey string
}

func (s *projectionObjectStore) Put(_ context.Context, key, _ string, value []byte) error {
	s.values[key] = append([]byte(nil), value...)
	s.lastKey = key
	return nil
}
func (s *projectionObjectStore) Get(_ context.Context, key string) ([]byte, error) {
	return append([]byte(nil), s.values[key]...), nil
}
func (s *projectionObjectStore) Delete(_ context.Context, key string) error {
	delete(s.values, key)
	return nil
}
