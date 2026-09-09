package summary

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

var summaryRowColumns = []string{
	"id", "tenant_id", "agent_app_id", "agent_version_id", "session_owner_id", "session_id", "filter_key",
	"target_event_sequence", "status", "lease_owner", "lease_version",
	"lease_until", "attempts", "max_attempts", "next_attempt_at", "last_error",
	"completed_event_sequence", "created_at", "updated_at", "target_resolution_lease_version", "session_incarnation_id",
}

func postgresSummaryRows(now time.Time, status string, owner any, leaseUntil any, attempts int, completed int64) *sqlmock.Rows {
	return sqlmock.NewRows(summaryRowColumns).AddRow(
		int64(7), "tenant-a", "support", "version-1", "owner-1", "session-1", "", int64(4), status,
		owner, int64(3), leaseUntil, attempts, 8, nil, "", completed, now, now, int64(0), "",
	)
}

func TestPostgresStoreClaimRenewComplete(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC()
	leaseUntil := now.Add(time.Minute)
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT " + summaryColumns + " FROM summary_jobs")).WillReturnRows(postgresSummaryRows(now, string(StatusPending), "", nil, 0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("UPDATE summary_jobs")).WithArgs("worker-a", int64(60000), int64(7)).WillReturnRows(postgresSummaryRows(now, string(StatusProcessing), "worker-a", leaseUntil, 1, 0))
	mock.ExpectCommit()
	store := NewPostgresStore(db)
	job, err := store.Claim(context.Background(), "worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != StatusProcessing || job.LeaseVersion != 3 || job.LeaseOwner != "worker-a" {
		t.Fatalf("claimed job=%#v", job)
	}

	renewedUntil := now.Add(2 * time.Minute)
	mock.ExpectQuery(regexp.QuoteMeta("UPDATE summary_jobs")).WithArgs(int64(60000), int64(7), "worker-a", int64(3)).WillReturnRows(postgresSummaryRows(now, string(StatusProcessing), "worker-a", renewedUntil, 1, 0))
	job, err = store.Renew(context.Background(), job, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !job.LeaseUntil.Equal(renewedUntil) {
		t.Fatalf("renewed lease=%v want %v", job.LeaseUntil, renewedUntil)
	}

	mock.ExpectQuery(regexp.QuoteMeta("UPDATE summary_jobs")).WithArgs(int64(4), int64(7), "worker-a", int64(3)).WillReturnRows(postgresSummaryRows(now, string(StatusCompleted), "", nil, 1, 4))
	job, err = store.Complete(context.Background(), job, 4)
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != StatusCompleted || job.CompletedEventSequence != 4 {
		t.Fatalf("completed job=%#v", job)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresStoreEnqueueNewJob(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC()
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO summary_jobs")).WithArgs("tenant-a", "support", "version-1", "owner-1", "session-1", "", int64(9), 8, "").WillReturnRows(postgresSummaryRows(now, string(StatusPending), "", nil, 0, 0))
	mock.ExpectCommit()
	store := NewPostgresStore(db)
	result, err := store.Enqueue(context.Background(), summaryRequest(summaryKey(), 9))
	if err != nil {
		t.Fatal(err)
	}
	if !result.Created || result.Job.Status != StatusPending {
		t.Fatalf("result=%#v", result)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresSinkRejectsStaleAndAcceptsInsert(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC()
	key := summaryKey()
	candidate := candidateFor(key, 3, "three")
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO summary_checkpoints")).WithArgs(
		"tenant-a", "support", "owner-1", "session-1", "", int64(3), "three", HashContent("three"), candidate.CutoffAt, candidate.LastEventID, "",
	).WillReturnRows(sqlmock.NewRows([]string{
		"tenant_id", "agent_app_id", "session_owner_id", "session_id", "filter_key", "max_event_sequence", "content", "content_sha256", "cutoff_at", "last_event_id", "updated_at", "session_incarnation_id",
	}).AddRow("tenant-a", "support", "owner-1", "session-1", "", int64(3), "three", HashContent("three"), candidate.CutoffAt, candidate.LastEventID, now, ""))
	mock.ExpectCommit()
	sink := NewPostgresSink(db)
	result, err := sink.Publish(context.Background(), candidate)
	if err != nil || !result.Applied {
		t.Fatalf("insert result=%#v err=%v", result, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresSinkFencedPublicationChecksJobScope(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC()
	key := summaryKey()
	candidate := candidateFor(key, 3, "three")
	claimed := Job{ID: 7, Key: key, AgentVersionID: "version-1", TargetEventSequence: 3, Status: StatusProcessing,
		LeaseOwner: "worker-a", LeaseVersion: 3, LeaseUntil: now.Add(time.Minute), Attempts: 1, MaxAttempts: 8}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT tenant_id, agent_app_id, session_owner_id, session_id, filter_key").WithArgs(int64(7), "worker-a", int64(3)).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "agent_app_id", "session_owner_id", "session_id", "filter_key", "session_incarnation_id"}).AddRow("tenant-a", "support", "owner-1", "session-1", "", ""))
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO summary_checkpoints")).WithArgs(
		"tenant-a", "support", "owner-1", "session-1", "", int64(3), "three", HashContent("three"), candidate.CutoffAt, candidate.LastEventID, "",
	).WillReturnRows(sqlmock.NewRows([]string{
		"tenant_id", "agent_app_id", "session_owner_id", "session_id", "filter_key", "max_event_sequence", "content", "content_sha256", "cutoff_at", "last_event_id", "updated_at", "session_incarnation_id",
	}).AddRow("tenant-a", "support", "owner-1", "session-1", "", int64(3), "three", HashContent("three"), candidate.CutoffAt, candidate.LastEventID, now, ""))
	mock.ExpectCommit()
	sink := NewPostgresSink(db)
	result, err := sink.PublishFenced(context.Background(), candidate, claimed)
	if err != nil || !result.Applied {
		t.Fatalf("fenced result=%#v err=%v", result, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresStoreReturnsStaleLeaseOnMissingMutation(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC()
	job := Job{ID: 7, Key: summaryKey(), AgentVersionID: "version-1", Status: StatusProcessing, LeaseOwner: "worker-a", LeaseVersion: 3,
		LeaseUntil: now.Add(time.Minute), TargetEventSequence: 1, MaxAttempts: 8}
	mock.ExpectQuery(regexp.QuoteMeta("UPDATE summary_jobs")).WithArgs(int64(60000), int64(7), "worker-a", int64(3)).WillReturnError(sql.ErrNoRows)
	store := NewPostgresStore(db)
	if _, err := store.Renew(context.Background(), job, time.Minute); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("error=%v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
