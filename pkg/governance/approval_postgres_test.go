package governance

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresApprovalStoreConsumeUsesAtomicScopeAndHash(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	request := approvalRequest()
	grantToken := mustApprovalToken(t)
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE tool_approvals")).
		WithArgs(request.TenantID, request.UserID, request.SessionOwnerID, request.SessionID,
			request.ToolName, request.ArgsHash, request.InvocationID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO control_plane_audit")).
		WithArgs(request.TenantID, "system", "tool.approval.consume", request.InvocationID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := NewPostgresApprovalStore(db).Consume(context.Background(), request, grantToken); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresApprovalStoreConsumeGrantedForChallengeBindsChallengeID(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	request := approvalRequest()
	challengeID := "challenge-exact"
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE tool_approvals")).
		WithArgs(challengeID, request.TenantID, request.UserID, request.SessionOwnerID, request.SessionID,
			request.ToolName, request.ArgsHash, request.InvocationID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO control_plane_audit")).
		WithArgs(request.TenantID, "system", "tool.approval.consume", request.InvocationID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := NewPostgresApprovalStore(db).ConsumeGrantedForChallenge(context.Background(), request, challengeID); err != nil {
		t.Fatalf("ConsumeGrantedForChallenge: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresApprovalStoreConsumeDoesNotTreatWrongTokenAsSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	request := approvalRequest()
	wrongToken := mustApprovalToken(t)
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE tool_approvals")).
		WithArgs(request.TenantID, request.UserID, request.SessionOwnerID, request.SessionID,
			request.ToolName, request.ArgsHash, request.InvocationID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT EXISTS(")).
		WithArgs(request.TenantID, request.UserID, request.SessionOwnerID, request.SessionID,
			request.ToolName, request.ArgsHash, request.InvocationID).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectRollback()
	if err := NewPostgresApprovalStore(db).Consume(context.Background(), request, wrongToken); !errors.Is(err, ErrApprovalInvalid) {
		t.Fatalf("Consume error=%v, want ErrApprovalInvalid", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresApprovalStoreConsumeRollsBackWhenAuditWriteFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	request := approvalRequest()
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE tool_approvals")).
		WithArgs(request.TenantID, request.UserID, request.SessionOwnerID, request.SessionID,
			request.ToolName, request.ArgsHash, request.InvocationID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO control_plane_audit")).
		WithArgs(request.TenantID, "system", "tool.approval.consume", request.InvocationID, sqlmock.AnyArg()).
		WillReturnError(errors.New("audit database unavailable"))
	mock.ExpectRollback()
	if err := NewPostgresApprovalStore(db).Consume(context.Background(), request, mustApprovalToken(t)); err == nil {
		t.Fatal("Consume succeeded despite audit failure")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresApprovalStoreRejectsMalformedTokenBeforeDatabase(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := NewPostgresApprovalStore(db).Consume(context.Background(), approvalRequest(), "newline\nsecret"); !errors.Is(err, ErrApprovalInvalid) {
		t.Fatalf("Consume error=%v, want ErrApprovalInvalid", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresApprovalStoreListsPendingChallengesByTenant(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	request := approvalRequest()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT challenge_id, user_id, session_owner_id")).
		WithArgs(request.TenantID, 10).
		WillReturnRows(sqlmock.NewRows([]string{
			"challenge_id", "user_id", "session_owner_id", "session_id", "tool_name", "args_hash", "invocation_id", "expires_at",
		}).AddRow("challenge-1", request.UserID, request.SessionOwnerID, request.SessionID,
			request.ToolName, request.ArgsHash, request.InvocationID, time.Now().Add(time.Minute)))
	items, err := NewPostgresApprovalStore(db).ListChallenges(context.Background(), request.TenantID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].ChallengeID != "challenge-1" {
		t.Fatalf("challenge list=%#v", items)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresApprovalStoreFindActiveApprovalUsesInvocationScope(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	request := approvalRequest()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT challenge_id, tool_name, args_hash, expires_at")).
		WithArgs(request.TenantID, request.UserID, request.SessionOwnerID, request.SessionID, request.InvocationID).
		WillReturnRows(sqlmock.NewRows([]string{"challenge_id", "tool_name", "args_hash", "expires_at"}).
			AddRow("challenge-1", request.ToolName, request.ArgsHash, time.Now().Add(time.Minute)))
	found, err := NewPostgresApprovalStore(db).FindActiveApproval(context.Background(), ApprovalInvocationScope{
		TenantID: request.TenantID, UserID: request.UserID,
		SessionOwnerID: request.SessionOwnerID, SessionID: request.SessionID,
		InvocationID: request.InvocationID,
	})
	if err != nil || found.ChallengeID != "challenge-1" || found.Request.ToolName != request.ToolName {
		t.Fatalf("FindActiveApproval=%#v err=%v", found, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresApprovalStoreIsApprovalGrantedUsesAuthoritativeState(t *testing.T) {
	tests := []struct {
		name      string
		rows      *sqlmock.Rows
		queryErr  error
		want      bool
		wantError error
	}{
		{
			name: "pending",
			rows: sqlmock.NewRows([]string{"granted_at", "consumed_at", "expires_at", "clock_timestamp"}).
				AddRow(nil, nil, time.Now().Add(time.Minute), time.Now()),
			want: false,
		},
		{
			name: "granted",
			rows: sqlmock.NewRows([]string{"granted_at", "consumed_at", "expires_at", "clock_timestamp"}).
				AddRow(time.Now(), nil, time.Now().Add(time.Minute), time.Now()),
			want: true,
		},
		{
			name: "consumed",
			rows: sqlmock.NewRows([]string{"granted_at", "consumed_at", "expires_at", "clock_timestamp"}).
				AddRow(time.Now(), time.Now(), time.Now().Add(time.Minute), time.Now()),
			wantError: ErrApprovalInvalid,
		},
		{
			name: "expired",
			rows: sqlmock.NewRows([]string{"granted_at", "consumed_at", "expires_at", "clock_timestamp"}).
				AddRow(time.Now(), nil, time.Now().Add(-time.Minute), time.Now()),
			wantError: ErrApprovalInvalid,
		},
		{
			name:      "missing",
			rows:      sqlmock.NewRows([]string{"granted_at", "consumed_at", "expires_at", "clock_timestamp"}),
			wantError: ErrApprovalNotFound,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if test.queryErr != nil {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT granted_at, consumed_at, expires_at, clock_timestamp()")).
					WithArgs("challenge-1").WillReturnError(test.queryErr)
			} else {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT granted_at, consumed_at, expires_at, clock_timestamp()")).
					WithArgs("challenge-1").WillReturnRows(test.rows)
			}
			got, err := NewPostgresApprovalStore(db).IsApprovalGranted(context.Background(), "challenge-1")
			if got != test.want {
				t.Fatalf("granted=%v, want %v (err=%v)", got, test.want, err)
			}
			if test.wantError == nil {
				if err != nil {
					t.Fatalf("IsApprovalGranted: %v", err)
				}
			} else if !errors.Is(err, test.wantError) {
				t.Fatalf("error=%v, want %v", err, test.wantError)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPostgresApprovalStoreInspectApprovalResumeReadsGrantWithChallenge(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	request := approvalRequest()
	scope := ApprovalInvocationScope{
		TenantID: request.TenantID, UserID: request.UserID,
		SessionOwnerID: request.SessionOwnerID, SessionID: request.SessionID,
		InvocationID: request.InvocationID,
	}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT challenge_id, tool_name, args_hash, expires_at, granted_at")).
		WithArgs(scope.TenantID, scope.UserID, scope.SessionOwnerID, scope.SessionID, scope.InvocationID).
		WillReturnRows(sqlmock.NewRows([]string{"challenge_id", "tool_name", "args_hash", "expires_at", "granted_at"}).
			AddRow("challenge-atomic", request.ToolName, request.ArgsHash, time.Now().Add(time.Minute), time.Now()))
	state, err := NewPostgresApprovalStore(db).InspectApprovalResume(context.Background(), scope)
	if err != nil {
		t.Fatalf("InspectApprovalResume: %v", err)
	}
	if !state.Granted || state.Challenge.ChallengeID != "challenge-atomic" ||
		state.Challenge.Request.ToolName != request.ToolName {
		t.Fatalf("state=%#v, want granted challenge", state)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresApprovalStoreInspectApprovalResumeFailsClosedOnAmbiguity(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	request := approvalRequest()
	scope := ApprovalInvocationScope{
		TenantID: request.TenantID, UserID: request.UserID,
		SessionOwnerID: request.SessionOwnerID, SessionID: request.SessionID,
		InvocationID: request.InvocationID,
	}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT challenge_id, tool_name, args_hash, expires_at, granted_at")).
		WithArgs(scope.TenantID, scope.UserID, scope.SessionOwnerID, scope.SessionID, scope.InvocationID).
		WillReturnRows(sqlmock.NewRows([]string{"challenge_id", "tool_name", "args_hash", "expires_at", "granted_at"}).
			AddRow("challenge-a", request.ToolName, request.ArgsHash, time.Now().Add(time.Minute), nil).
			AddRow("challenge-b", request.ToolName, request.ArgsHash, time.Now().Add(time.Minute), time.Now()))
	_, err = NewPostgresApprovalStore(db).InspectApprovalResume(context.Background(), scope)
	if !errors.Is(err, ErrApprovalAmbiguous) {
		t.Fatalf("error=%v, want ErrApprovalAmbiguous", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func mustApprovalToken(t *testing.T) string {
	t.Helper()
	token, err := randomToken(approvalTokenBytes)
	if err != nil {
		t.Fatal(err)
	}
	return token
}
