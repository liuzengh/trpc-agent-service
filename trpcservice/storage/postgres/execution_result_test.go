package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

func TestValidateExecutionCommitRecord(t *testing.T) {
	valid := storage.ExecutionCommitRecord{
		JobID: "job", ExecutionID: "execution", TenantID: "tenant", SessionID: "session", OwnerID: "owner",
		Epoch: 1, FenceToken: 1, ResultJSON: []byte(`{"text":"ok"}`),
	}
	if err := validateExecutionCommitRecord(context.Background(), valid); err != nil {
		t.Fatalf("valid record rejected: %v", err)
	}
	cases := []struct {
		name string
		edit func(*storage.ExecutionCommitRecord)
	}{
		{"missing job", func(r *storage.ExecutionCommitRecord) { r.JobID = "" }},
		{"missing execution", func(r *storage.ExecutionCommitRecord) { r.ExecutionID = "" }},
		{"missing tenant", func(r *storage.ExecutionCommitRecord) { r.TenantID = "" }},
		{"missing session", func(r *storage.ExecutionCommitRecord) { r.SessionID = "" }},
		{"missing owner", func(r *storage.ExecutionCommitRecord) { r.OwnerID = "" }},
		{"missing epoch", func(r *storage.ExecutionCommitRecord) { r.Epoch = 0 }},
		{"missing fence token", func(r *storage.ExecutionCommitRecord) { r.FenceToken = 0 }},
		{"invalid result", func(r *storage.ExecutionCommitRecord) { r.ResultJSON = []byte(`{`) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			record := valid
			tc.edit(&record)
			if err := validateExecutionCommitRecord(context.Background(), record); !errors.Is(err, storage.ErrInvalidArgument) {
				t.Fatalf("error=%v", err)
			}
		})
	}
	if err := validateExecutionCommitRecord(nil, valid); err == nil {
		t.Fatal("nil context was accepted")
	}
}

func TestExecutionCommitSQLRechecksFenceAtWriteBoundary(t *testing.T) {
	for _, required := range []string{
		"s.tenant_id = $1",
		"s.session_id = $4",
		"l.owner_id = $5",
		"l.epoch = $6",
		"l.fencing_token = $7",
		"l.leased_until > clock_timestamp()",
		"ON CONFLICT DO NOTHING",
	} {
		if !strings.Contains(executionCommitInsert, required) {
			t.Fatalf("commit SQL missing %q", required)
		}
	}
}

func TestCommitErrorPreservesSentinelsAndDatabaseErrors(t *testing.T) {
	fence := commitError("fence", storage.ErrFenceRejected)
	if !errors.Is(fence, storage.ErrFenceRejected) {
		t.Fatalf("sentinel was not preserved: %v", fence)
	}
	pgErr := &pgconn.PgError{Code: "40001", Message: "serialization failure"}
	wrapped := commitError("database", pgErr)
	var got *pgconn.PgError
	if !errors.As(wrapped, &got) || got.Code != "40001" {
		t.Fatalf("database error was not preserved: %v", wrapped)
	}
}

func TestNewExecutionResultRepositoryRequiresPool(t *testing.T) {
	if _, err := NewExecutionResultRepository(nil); err == nil {
		t.Fatal("nil pool was accepted")
	}
}
