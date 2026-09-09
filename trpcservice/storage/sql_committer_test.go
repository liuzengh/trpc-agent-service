package storage

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/persistence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionfence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
)

func TestPostgresCommitterTransactionAndIdempotentReceipt(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	committer := testCommitter(db, dialectPostgres)
	envelope := testSQLEnvelope(t, tenant.StorageKindPostgres)
	fingerprint, _ := envelope.BackendFingerprint.Digest()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO .*platform_session_heads").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("SELECT last_committed_seq,backend_fingerprint").WithArgs(envelope.SessionCoord).
		WillReturnRows(sqlmock.NewRows([]string{"last_committed_seq", "backend_fingerprint"}).AddRow(0, fingerprint))
	mock.ExpectQuery("SELECT task_id,tenant_id").WithArgs(envelope.TaskID).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "tenant_id", "agent_app_id", "storage_profile_id", "backend_kind", "session_coord", "session_seq", "payload_digest", "envelope_digest"}))
	mock.ExpectExec("INSERT INTO .*session_states").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO .*session_events").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO .*platform_turn_commits").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("UPDATE .*platform_session_heads").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := committer.Commit(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO .*platform_session_heads").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT last_committed_seq,backend_fingerprint").WithArgs(envelope.SessionCoord).
		WillReturnRows(sqlmock.NewRows([]string{"last_committed_seq", "backend_fingerprint"}).AddRow(1, fingerprint))
	mock.ExpectQuery("SELECT task_id,tenant_id").WithArgs(envelope.TaskID).
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "tenant_id", "agent_app_id", "storage_profile_id", "backend_kind", "session_coord", "session_seq", "payload_digest", "envelope_digest"}).
			AddRow(envelope.TaskID, envelope.TenantID, envelope.AgentAppID, envelope.StorageProfileID, string(envelope.BackendKind), envelope.SessionCoord, envelope.SessionSeq, envelope.PayloadDigest, envelope.EnvelopeDigest))
	mock.ExpectCommit()
	if err := committer.Commit(context.Background(), envelope); err != nil {
		t.Fatalf("idempotent commit failed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresCommitterSequenceConflictRollsBack(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	committer := testCommitter(db, dialectPostgres)
	envelope := testSQLEnvelope(t, tenant.StorageKindPostgres)
	fingerprint, _ := envelope.BackendFingerprint.Digest()
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO .*platform_session_heads").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT last_committed_seq,backend_fingerprint").WillReturnRows(sqlmock.NewRows([]string{"last_committed_seq", "backend_fingerprint"}).AddRow(4, fingerprint))
	mock.ExpectQuery("SELECT task_id,tenant_id").WillReturnRows(sqlmock.NewRows([]string{"task_id", "tenant_id", "agent_app_id", "storage_profile_id", "backend_kind", "session_coord", "session_seq", "payload_digest", "envelope_digest"}))
	mock.ExpectRollback()
	if err := committer.Commit(context.Background(), envelope); !errors.Is(err, persistence.ErrSessionSequenceConflict) {
		t.Fatalf("sequence error = %v", err)
	}
}

func TestMySQLCommitterCreatesActiveStateInsideTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	committer := testCommitter(db, dialectMySQL)
	envelope := testSQLEnvelope(t, tenant.StorageKindMySQL)
	envelope.TurnCommit.Events = nil
	envelope.EnvelopeDigest, _ = envelope.CalculateDigest()
	fingerprint, _ := envelope.BackendFingerprint.Digest()
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO .*platform_session_heads.*ON DUPLICATE KEY UPDATE").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("SELECT last_committed_seq,backend_fingerprint").WillReturnRows(sqlmock.NewRows([]string{"last_committed_seq", "backend_fingerprint"}).AddRow(0, fingerprint))
	mock.ExpectQuery("SELECT task_id,tenant_id").WillReturnRows(sqlmock.NewRows([]string{"task_id", "tenant_id", "agent_app_id", "storage_profile_id", "backend_kind", "session_coord", "session_seq", "payload_digest", "envelope_digest"}))
	mock.ExpectQuery("SELECT id FROM .*session_states").WillReturnRows(sqlmock.NewRows([]string{"id"}))
	mock.ExpectExec("INSERT INTO .*session_states").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO .*platform_turn_commits").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("UPDATE .*platform_session_heads").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := committer.Commit(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
}

func TestSQLCommitterRejectsColumnTypeAndTimestampPrecisionDrift(t *testing.T) {
	for _, current := range []struct {
		name      string
		dataType  string
		precision any
	}{
		{name: "wrong type", dataType: "longtext", precision: nil},
		{name: "wrong precision", dataType: "timestamp", precision: int64(0)},
	} {
		t.Run(current.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectBegin()
			tx, err := db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			mock.ExpectQuery("SELECT data_type, datetime_precision FROM information_schema.columns").
				WithArgs("session_states", "created_at").
				WillReturnRows(sqlmock.NewRows([]string{"data_type", "datetime_precision"}).AddRow(current.dataType, current.precision))
			precision6 := int64(6)
			committer := &sqlTurnCommitter{dialect: dialectMySQL}
			err = committer.validateColumnContracts(context.Background(), tx, []sqlColumnContract{{table: "session_states", column: "created_at", dataType: "timestamp", precision: &precision6}})
			if !errors.Is(err, persistence.ErrSchemaIncompatible) {
				t.Fatalf("column drift error = %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func testCommitter(db *sql.DB, dialect sqlDialect) *sqlTurnCommitter {
	committer := &sqlTurnCommitter{dialect: dialect, schema: "public", skipDBInit: true,
		transaction: func(ctx context.Context, fn func(*sql.Tx) error) (err error) {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer func() {
				if err != nil {
					_ = tx.Rollback()
				}
			}()
			if err = fn(tx); err != nil {
				return err
			}
			err = tx.Commit()
			return err
		},
		exec: db.ExecContext,
	}
	if dialect == dialectPostgres {
		committer.stateTable = `"public"."test_session_states"`
		committer.eventTable = `"public"."test_session_events"`
		committer.headTable = `"public"."test_platform_session_heads"`
		committer.commitTable = `"public"."test_platform_turn_commits"`
	} else {
		committer.stateTable = "`test_session_states`"
		committer.eventTable = "`test_session_events`"
		committer.headTable = "`test_platform_session_heads`"
		committer.commitTable = "`test_platform_turn_commits`"
	}
	return committer
}

func testSQLEnvelope(t *testing.T, kind tenant.StorageKind) persistence.Envelope {
	t.Helper()
	task := message.ExecutionTask{SchemaVersion: message.TaskSchemaVersion, TaskID: "task", Channel: "demo", ChannelBindingID: "binding", ExternalAccountID: "account", TenantID: "tenant", AgentAppID: "agent", ConfigVersion: "v1", RunnerUserID: "user", SessionID: "session", PlatformMessageID: "message", ActorUserID: "actor", ConversationID: "conversation", ConversationType: message.ConversationDirect, Text: "hello", RequestID: "request", TraceID: "trace", ReceivedAt: time.Unix(100, 0), Attempt: 1}
	task.PayloadDigest = task.CanonicalDigest()
	database := "db.example:5432/app"
	namespace := "public.test_"
	if kind == tenant.StorageKindMySQL {
		database, namespace = "tcp(db.example:3306)/app", "test_"
	}
	route := persistence.Route{TenantID: task.TenantID, AgentAppID: task.AgentAppID, Fingerprint: persistence.BackendFingerprint{SchemaVersion: persistence.FingerprintSchemaVersion, Kind: kind, StorageProfileID: string(kind), DatabaseIdentity: database, Namespace: namespace}}
	commit := sessionfence.TurnCommit{SessionCoord: "coord", SessionSeq: 1, AppName: tenant.AppName(task.TenantID, task.AgentAppID), UserID: task.RunnerUserID, SessionID: task.SessionID, FinalState: map[string][]byte{"key": []byte("value")}, Events: nil}
	current := message.OutboundMessage{RequestID: task.RequestID, TraceID: task.TraceID, SessionID: task.SessionID, Text: "answer"}
	envelope, err := persistence.NewEnvelope(task, route, commit, current, time.Unix(200, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	// A compact valid event payload is enough to verify ordered event insertion.
	envelope.TurnCommit.Events = append(envelope.TurnCommit.Events, event.Event{})
	envelope.EnvelopeDigest, _ = envelope.CalculateDigest()
	return envelope
}
