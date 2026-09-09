package summary

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresStoreCoalescingMatchesMemoryStore(t *testing.T) {
	for _, tc := range []struct {
		name        string
		status      Status
		target      int64
		force       bool
		version     string
		maxAttempts int
	}{
		{name: "completed then new messages", status: StatusCompleted, target: 9, version: "version-2"},
		{name: "processing with higher target preserves lease", status: StatusProcessing, target: 9, version: "version-2"},
		{name: "force completed retry", status: StatusCompleted, target: 4, force: true, version: "version-1"},
		{name: "force exhausted failure", status: StatusFailed, target: 4, force: true, version: "version-1"},
		{name: "lower target preserves boundary", status: StatusCompleted, target: 2, version: "version-2"},
		{name: "increase retry allowance", status: StatusFailed, target: 4, version: "version-1", maxAttempts: 12},
		{name: "completed deferred refresh", status: StatusCompleted, target: 0, version: "version-2"},
		{name: "processing deferred refresh", status: StatusProcessing, target: 0, version: "version-2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			now := time.Now().UTC()
			current := Job{ID: 7, Key: summaryKey(), AgentVersionID: "version-1", TargetEventSequence: 4,
				Status: tc.status, Attempts: 8, MaxAttempts: 8, CreatedAt: now, UpdatedAt: now}
			switch tc.status {
			case StatusCompleted:
				current.CompletedEventSequence = 4
			case StatusProcessing:
				current.Attempts = 1
				current.LeaseOwner = "worker-a"
				current.LeaseVersion = 3
				current.LeaseUntil = now.Add(time.Minute)
			case StatusFailed:
				current.LastError = "retryable failure"
			}
			request := summaryRequest(current.Key, tc.target)
			request.AgentVersionID, request.Force, request.MaxAttempts = tc.version, tc.force, tc.maxAttempts
			memory := NewMemoryStore(func() time.Time { return now })
			memory.byID[current.ID], memory.byKey[current.Key], memory.nextID = current, current.ID, current.ID
			want, err := memory.Enqueue(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			rows := func(job Job) *sqlmock.Rows {
				return sqlmock.NewRows(summaryRowColumns).AddRow([]driver.Value{
					job.ID, job.TenantID, job.AgentAppID, job.AgentVersionID, job.SessionOwnerID, job.SessionID, job.FilterKey,
					job.TargetEventSequence, string(job.Status), job.LeaseOwner, job.LeaseVersion, nullableTime(job.LeaseUntil),
					job.Attempts, job.MaxAttempts, nullableTime(job.NextAttemptAt), job.LastError, job.CompletedEventSequence, job.CreatedAt, job.UpdatedAt, job.TargetResolutionLeaseVersion, job.SessionIncarnationID,
				}...)
			}
			mock.ExpectBegin()
			maxAttempts := request.MaxAttempts
			if maxAttempts == 0 {
				maxAttempts = DefaultMaxAttempts
			}
			mock.ExpectQuery("INSERT INTO summary_jobs").WithArgs(current.TenantID, current.AgentAppID, request.AgentVersionID,
				current.SessionOwnerID, current.SessionID, current.FilterKey, request.TargetEventSequence, maxAttempts, request.SessionIncarnationID).
				WillReturnRows(sqlmock.NewRows(summaryRowColumns))
			mock.ExpectQuery("SELECT .* FROM summary_jobs .* FOR UPDATE").WithArgs(current.TenantID, current.AgentAppID,
				current.SessionOwnerID, current.SessionID, current.FilterKey, request.SessionIncarnationID).WillReturnRows(rows(current))
			if want.Job != current {
				mock.ExpectQuery("UPDATE summary_jobs").WithArgs(want.Job.ID, want.Job.AgentVersionID,
					want.Job.TargetEventSequence, string(want.Job.Status), want.Job.Attempts, want.Job.MaxAttempts,
					nullableTime(want.Job.NextAttemptAt), want.Job.LastError, want.Job.TargetResolutionLeaseVersion).WillReturnRows(rows(want.Job))
			}
			mock.ExpectCommit()
			got, err := NewPostgresStore(db).Enqueue(ctx, request)
			if err != nil {
				t.Fatalf("enqueue must persist the merged job: %v", err)
			}
			// PostgreSQL owns wall-clock timestamps; compare the coordination
			// contract independently from the deterministic in-memory clock.
			got.Job.UpdatedAt = want.Job.UpdatedAt
			if got.Job != want.Job || got.Created || !got.Coalesced {
				t.Fatalf("coalesced result = %#v, want %#v", got, want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
