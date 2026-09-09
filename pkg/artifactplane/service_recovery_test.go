package artifactplane

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
)

type faultObjectStore struct {
	bodies         map[string][]byte
	deleteFailures map[string]int
	deleteCalls    []string
	deferDeletion  bool
	pendingDeletes []string
	putError       error
}

func (s *faultObjectStore) Put(_ context.Context, key, _ string, body []byte) error {
	if s.bodies == nil {
		s.bodies = make(map[string][]byte)
	}
	s.bodies[key] = append([]byte(nil), body...)
	return s.putError
}

func (s *faultObjectStore) Get(_ context.Context, key string) ([]byte, error) {
	body, ok := s.bodies[key]
	if !ok {
		return nil, errors.New("object not found")
	}
	return append([]byte(nil), body...), nil
}

func (s *faultObjectStore) Delete(_ context.Context, key string) error {
	s.deleteCalls = append(s.deleteCalls, key)
	if s.deferDeletion {
		s.pendingDeletes = append(s.pendingDeletes, key)
		return errors.New("object deletion outcome unknown")
	}
	if s.deleteFailures[key] > 0 {
		s.deleteFailures[key]--
		return errors.New("object deletion failed")
	}
	delete(s.bodies, key)
	return nil
}

func TestDeleteArtifactRetriesBodyCleanupAfterTombstone(t *testing.T) {
	const key = "artifact-object-1"
	retryRows := sqlmock.NewRows([]string{"object_key"})
	matcher := sqlmock.QueryMatcherFunc(func(expected, actual string) error {
		if expected == "retry tombstone" {
			if !strings.Contains(actual, "deleted_at IS NULL") {
				retryRows.AddRow(key)
			}
			return nil
		}
		return sqlmock.QueryMatcherRegexp.Match(expected, actual)
	})
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	objects := &faultObjectStore{bodies: map[string][]byte{key: []byte("hello")}, deleteFailures: map[string]int{key: 1}}
	service, err := NewService("tenant-a", db, objects, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	info := artifact.SessionInfo{AppName: "support", UserID: "owner-1", SessionID: "session-1"}
	expectArtifactLock(mock)
	mock.ExpectQuery("UPDATE artifact_versions").WillReturnRows(sqlmock.NewRows([]string{"object_key"}).AddRow(key))
	mock.ExpectCommit()
	if err := service.DeleteArtifact(context.Background(), info, "report.txt"); err == nil {
		t.Fatal("first deletion must report the object-store failure")
	}
	expectArtifactLock(mock)
	mock.ExpectQuery("retry tombstone").WillReturnRows(retryRows)
	mock.ExpectCommit()
	if err := service.DeleteArtifact(context.Background(), info, "report.txt"); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if len(objects.bodies) != 0 {
		t.Fatal("deletion retry reported success but left the tombstoned object body")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteArtifactAttemptsEveryBodyAfterPartialFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	objects := &faultObjectStore{
		bodies:         map[string][]byte{"first": []byte("one"), "second": []byte("two"), "third": []byte("three")},
		deleteFailures: map[string]int{"first": 1, "third": 1},
	}
	service, err := NewService("tenant-a", db, objects, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	info := artifact.SessionInfo{AppName: "support", UserID: "owner-1", SessionID: "session-1"}
	expectArtifactLock(mock)
	mock.ExpectQuery("UPDATE artifact_versions").WillReturnRows(sqlmock.NewRows([]string{"object_key"}).AddRow("first").AddRow("second").AddRow("third"))
	mock.ExpectCommit()
	if err := service.DeleteArtifact(context.Background(), info, "report.txt"); err == nil {
		t.Fatal("partial deletion must return an error")
	}
	if _, exists := objects.bodies["second"]; exists {
		t.Fatal("first deletion failure skipped a later healthy object")
	}
	if len(objects.deleteCalls) != 3 {
		t.Fatalf("deletion attempts = %v, want all three versions", objects.deleteCalls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteArtifactRejectsIncompleteTombstoneRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	objects := &faultObjectStore{bodies: map[string][]byte{"first": []byte("one"), "second": []byte("two")}}
	service, err := NewService("tenant-a", db, objects, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	info := artifact.SessionInfo{AppName: "support", UserID: "owner-1", SessionID: "session-1"}
	readErr := errors.New("tombstone result stream interrupted")
	expectArtifactLock(mock)
	mock.ExpectQuery("UPDATE artifact_versions").WillReturnRows(sqlmock.NewRows([]string{"object_key"}).AddRow("first").AddRow("second").RowError(1, readErr))
	mock.ExpectRollback()
	err = service.DeleteArtifact(context.Background(), info, "report.txt")
	if !errors.Is(err, readErr) {
		t.Fatalf("delete error = %v, want interrupted result error", err)
	}
	if len(objects.bodies) != 2 || len(objects.deleteCalls) != 0 {
		t.Fatal("incomplete metadata result allowed object deletion")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func expectArtifactLock(mock sqlmock.Sqlmock) {
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")).
		WithArgs(sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
}

func TestSaveArtifactCommitAcknowledgementLostPreservesCommittedBody(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	objects := &faultObjectStore{}
	service, err := NewService("tenant-a", db, objects, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	info := artifact.SessionInfo{AppName: "support", UserID: "owner-1", SessionID: "session-1"}
	lostAck := errors.New("commit acknowledgement lost")
	expectArtifactLock(mock)
	mock.ExpectQuery("SELECT COALESCE").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(0))
	mock.ExpectExec("INSERT INTO artifact_versions").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit().WillReturnError(lostAck)
	expectArtifactLock(mock)
	mock.ExpectQuery("SELECT 1 FROM artifact_versions").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(1))
	mock.ExpectRollback()

	_, err = service.SaveArtifact(context.Background(), info, "report.txt", &artifact.Artifact{Data: []byte("hello"), MimeType: "text/plain"})
	if !errors.Is(err, lostAck) {
		t.Fatalf("save error = %v, want original commit error", err)
	}
	if len(objects.bodies) != 1 {
		t.Fatal("commit acknowledgement loss deleted the committed object")
	}
	var storedKey string
	for key := range objects.bodies {
		storedKey = key
	}
	mock.ExpectQuery("SELECT object_key,mime_type,size_bytes,content_sha256 FROM artifact_versions").
		WillReturnRows(sqlmock.NewRows([]string{"object_key", "mime_type", "size_bytes", "content_sha256"}).
			AddRow(storedKey, "text/plain", 5, hashBytes([]byte("hello"))))
	version := 0
	loaded, err := service.LoadArtifact(context.Background(), info, "report.txt", &version)
	if err != nil || loaded == nil || string(loaded.Data) != "hello" {
		t.Fatalf("committed artifact cannot be loaded: artifact=%#v error=%v", loaded, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestProjectVersionCommitAcknowledgementLostPreservesCommittedBody(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	objects := &faultObjectStore{}
	service, err := NewService("tenant-a", db, objects, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	info := artifact.SessionInfo{AppName: "support", UserID: "owner-1", SessionID: "session-1"}
	lostAck := errors.New("commit acknowledgement lost")
	expectArtifactLock(mock)
	mock.ExpectQuery("SELECT mime_type,size_bytes,content_sha256,deleted_at").WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO artifact_versions").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit().WillReturnError(lostAck)
	expectArtifactLock(mock)
	mock.ExpectQuery("SELECT 1 FROM artifact_versions").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(1))
	mock.ExpectRollback()

	err = service.ProjectVersion(context.Background(), info, "report.txt", 7, &artifact.Artifact{Data: []byte("hello"), MimeType: "text/plain"})
	if !errors.Is(err, lostAck) {
		t.Fatalf("project error = %v, want original commit error", err)
	}
	if len(objects.bodies) != 1 {
		t.Fatal("commit acknowledgement loss deleted the projected object")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactWriteCleanupPreservesBodyWhenReferenceCheckFails(t *testing.T) {
	for _, projection := range []bool{false, true} {
		name := "save"
		if projection {
			name = "project"
		}
		t.Run(name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			objects := &faultObjectStore{}
			service, err := NewService("tenant-a", db, objects, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			info := artifact.SessionInfo{AppName: "support", UserID: "owner-1", SessionID: "session-1"}
			writeErr := errors.New("commit acknowledgement lost")
			readErr := errors.New("reference database unavailable")
			expectArtifactLock(mock)
			if projection {
				mock.ExpectQuery("SELECT mime_type,size_bytes,content_sha256,deleted_at").WillReturnError(sql.ErrNoRows)
			} else {
				mock.ExpectQuery("SELECT COALESCE").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(7))
			}
			mock.ExpectExec("INSERT INTO artifact_versions").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit().WillReturnError(writeErr)
			expectArtifactLock(mock)
			mock.ExpectQuery("SELECT 1 FROM artifact_versions").WithArgs("tenant-a", "support", "owner-1", "session-1", "report.txt", 7, sqlmock.AnyArg()).WillReturnError(readErr)
			mock.ExpectRollback()
			value := &artifact.Artifact{Data: []byte("hello"), MimeType: "text/plain"}
			if projection {
				err = service.ProjectVersion(context.Background(), info, "report.txt", 7, value)
			} else {
				_, err = service.SaveArtifact(context.Background(), info, "report.txt", value)
			}
			if !errors.Is(err, writeErr) || !errors.Is(err, readErr) {
				t.Fatalf("write/verification errors not preserved: %v", err)
			}
			if len(objects.bodies) != 1 || len(objects.deleteCalls) != 0 {
				t.Fatal("failed reference verification deleted an unverified object")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSaveArtifactDelayedCleanupCannotDeleteReusedVersion(t *testing.T) {
	testDelayedCleanupCannotDeleteReusedVersion(t, false)
}

func TestProjectVersionDelayedCleanupCannotDeleteReusedVersion(t *testing.T) {
	testDelayedCleanupCannotDeleteReusedVersion(t, true)
}

func testDelayedCleanupCannotDeleteReusedVersion(t *testing.T, projection bool) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	objects := &faultObjectStore{deferDeletion: true}
	service, err := NewService("tenant-a", db, objects, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	info := artifact.SessionInfo{AppName: "support", UserID: "owner-1", SessionID: "session-1"}
	expectWrite := func() {
		expectArtifactLock(mock)
		if projection {
			mock.ExpectQuery("SELECT mime_type,size_bytes,content_sha256,deleted_at").WillReturnError(sql.ErrNoRows)
		} else {
			mock.ExpectQuery("SELECT COALESCE").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(0))
		}
		mock.ExpectExec("INSERT INTO artifact_versions").WillReturnResult(sqlmock.NewResult(0, 1))
	}
	write := func(body string) (int, error) {
		value := &artifact.Artifact{Data: []byte(body), MimeType: "text/plain"}
		if projection {
			return 0, service.ProjectVersion(context.Background(), info, "report.txt", 0, value)
		}
		return service.SaveArtifact(context.Background(), info, "report.txt", value)
	}
	expectWrite()
	mock.ExpectCommit().WillReturnError(errors.New("transaction rolled back"))
	expectArtifactLock(mock)
	mock.ExpectQuery("SELECT 1 FROM artifact_versions").WithArgs("tenant-a", "support", "owner-1", "session-1", "report.txt", 0, sqlmock.AnyArg()).WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()
	if _, err := write("first"); err == nil {
		t.Fatal("first write must report transaction and uncertain cleanup failure")
	}
	if len(objects.pendingDeletes) != 1 {
		t.Fatalf("pending cleanups = %v, want the failed write only", objects.pendingDeletes)
	}
	oldKey := objects.pendingDeletes[0]
	expectWrite()
	mock.ExpectCommit()
	version, err := write("second")
	if err != nil || version != 0 {
		t.Fatalf("second write: version=%d error=%v", version, err)
	}
	var newKey string
	for key, body := range objects.bodies {
		if string(body) == "second" {
			newKey = key
		}
	}
	if newKey == "" || newKey == oldKey {
		t.Fatal("a later write reused the uncertain cleanup's object identity")
	}
	for _, key := range objects.pendingDeletes {
		delete(objects.bodies, key)
	}
	mock.ExpectQuery("SELECT object_key,mime_type,size_bytes,content_sha256 FROM artifact_versions").
		WillReturnRows(sqlmock.NewRows([]string{"object_key", "mime_type", "size_bytes", "content_sha256"}).AddRow(newKey, "text/plain", 6, hashBytes([]byte("second"))))
	loaded, err := service.LoadArtifact(context.Background(), info, "report.txt", &version)
	if err != nil || loaded == nil || string(loaded.Data) != "second" {
		t.Fatalf("late cleanup damaged the newer write: artifact=%#v error=%v", loaded, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestProjectVersionRepairsRecordedObjectKey(t *testing.T) {
	for _, writeIdentity := range []string{"", "/ab68b47a-a327-405f-99f4-a4c1e6d10c17"} {
		name := "legacy"
		if writeIdentity != "" {
			name = "unique-write"
		}
		t.Run(name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			objects := &faultObjectStore{}
			service, err := NewService("tenant-a", db, objects, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			info := artifact.SessionInfo{AppName: "support", UserID: "owner-1", SessionID: "session-1"}
			key := objectKey("tenant-a", info, "report.txt", 7) + writeIdentity
			expectArtifactLock(mock)
			mock.ExpectQuery("SELECT mime_type,size_bytes,content_sha256,deleted_at,object_key").
				WillReturnRows(sqlmock.NewRows([]string{"mime_type", "size_bytes", "content_sha256", "deleted_at", "object_key"}).
					AddRow("text/plain", 5, hashBytes([]byte("hello")), nil, key))
			mock.ExpectCommit()
			if err := service.ProjectVersion(context.Background(), info, "report.txt", 7, &artifact.Artifact{Data: []byte("hello"), MimeType: "text/plain"}); err != nil {
				t.Fatalf("repair: %v", err)
			}
			if len(objects.bodies) != 1 || string(objects.bodies[key]) != "hello" {
				t.Fatal("projection did not repair the recorded object key")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestArtifactPutAcknowledgementLostVerifiesBodyBeforeCleanup(t *testing.T) {
	for _, projection := range []bool{false, true} {
		for _, referenceUnavailable := range []bool{false, true} {
			name := "save"
			if projection {
				name = "project"
			}
			if referenceUnavailable {
				name += "/unavailable-reference"
			} else {
				name += "/unreferenced"
			}
			t.Run(name, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				db.SetMaxOpenConns(1)
				putErr := errors.New("put acknowledgement lost")
				readErr := errors.New("reference lookup failed")
				objects := &faultObjectStore{putError: putErr}
				service, err := NewService("tenant-a", db, objects, 1<<20)
				if err != nil {
					t.Fatal(err)
				}
				info := artifact.SessionInfo{AppName: "support", UserID: "owner-1", SessionID: "session-1"}
				expectArtifactLock(mock)
				if projection {
					mock.ExpectQuery("SELECT mime_type,size_bytes,content_sha256,deleted_at").WillReturnError(sql.ErrNoRows)
				} else {
					mock.ExpectQuery("SELECT COALESCE").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(0))
				}
				mock.ExpectRollback()
				expectArtifactLock(mock)
				query := mock.ExpectQuery("SELECT 1 FROM artifact_versions").WithArgs("tenant-a", "support", "owner-1", "session-1", "report.txt", 0, sqlmock.AnyArg())
				if referenceUnavailable {
					query.WillReturnError(readErr)
				} else {
					query.WillReturnError(sql.ErrNoRows)
				}
				mock.ExpectRollback()
				value := &artifact.Artifact{Data: []byte("hello"), MimeType: "text/plain"}
				if projection {
					err = service.ProjectVersion(context.Background(), info, "report.txt", 0, value)
				} else {
					_, err = service.SaveArtifact(context.Background(), info, "report.txt", value)
				}
				if !errors.Is(err, putErr) {
					t.Fatalf("put error was lost: %v", err)
				}
				if referenceUnavailable {
					if !errors.Is(err, readErr) || len(objects.bodies) != 1 || len(objects.deleteCalls) != 0 {
						t.Fatalf("unverified object must remain with both errors: bodies=%d deletes=%v error=%v", len(objects.bodies), objects.deleteCalls, err)
					}
				} else if len(objects.bodies) != 0 {
					t.Fatal("PUT acknowledgement loss left a confirmed unreferenced object")
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestProjectVersionPutAcknowledgementLostNeverCleansExistingBody(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	putErr := errors.New("put acknowledgement lost")
	info := artifact.SessionInfo{AppName: "support", UserID: "owner-1", SessionID: "session-1"}
	key := objectKey("tenant-a", info, "report.txt", 7)
	objects := &faultObjectStore{bodies: map[string][]byte{key: []byte("hello")}, putError: putErr}
	service, err := NewService("tenant-a", db, objects, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	expectArtifactLock(mock)
	mock.ExpectQuery("SELECT mime_type,size_bytes,content_sha256,deleted_at,object_key").
		WillReturnRows(sqlmock.NewRows([]string{"mime_type", "size_bytes", "content_sha256", "deleted_at", "object_key"}).
			AddRow("text/plain", 5, hashBytes([]byte("hello")), nil, key))
	mock.ExpectRollback()
	err = service.ProjectVersion(context.Background(), info, "report.txt", 7, &artifact.Artifact{Data: []byte("hello"), MimeType: "text/plain"})
	if !errors.Is(err, putErr) || len(objects.deleteCalls) != 0 || string(objects.bodies[key]) != "hello" {
		t.Fatalf("existing object must survive failed repair: deletes=%v error=%v", objects.deleteCalls, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
