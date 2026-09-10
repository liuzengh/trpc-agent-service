package postgres

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp/contracttest"
)

func TestAgentAppRepositoryContractPostgreSQL16(t *testing.T) {
	db := openContractDB(t)
	contracttest.Run(t, func(tb testing.TB, tenantID string) agentapp.Repository {
		tb.Helper()
		tenantKey := "agent-contract-" + strings.ToLower(tenantID[len(tenantID)-3:])
		if _, err := db.ExecContext(context.Background(), `INSERT INTO tenant(tenant_id,tenant_key,display_name) VALUES($1,$2,'Agent Contract')`, tenantID, tenantKey); err != nil {
			tb.Fatal(err)
		}
		insertActiveModelProfile(tb, db, tenantID)
		return New(db)
	})
}

func TestPublishedRevisionDatabaseGuardsPostgreSQL16(t *testing.T) {
	db := openContractDB(t)
	ctx := context.Background()
	tenantID := "t_01ARZ3NDEKTSV4RRFFQ69G5FAE"
	appID := "app_01ARZ3NDEKTSV4RRFFQ69G5FAE"
	if _, err := db.ExecContext(ctx, `INSERT INTO tenant(tenant_id,tenant_key,display_name) VALUES($1,'agent-guard','Agent Guard')`, tenantID); err != nil {
		t.Fatal(err)
	}
	insertActiveModelProfile(t, db, tenantID)
	repository := New(db)
	metadata := agentapp.ChangeMetadata{ActorType: "test", ActorID: "guard", Reason: "guard", CorrelationID: "guard", TraceID: "guard"}
	app, err := repository.Create(ctx, agentapp.CreateInput{App: agentapp.AgentApp{TenantID: tenantID, AgentAppID: appID, AgentAppKey: "guard", DisplayName: "Guard"}, ChangeMetadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	draft, err := repository.CreateDraft(ctx, agentapp.CreateDraftInput{TenantID: tenantID, AgentAppID: appID, ExpectedAppVersion: app.Version, Revision: agentapp.Revision{AgentKind: "llm", Instruction: "guard", ModelProfileID: "model", ModelProfileVersion: 1}, ChangeMetadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Publish(ctx, agentapp.PublishInput{TenantID: tenantID, AgentAppID: appID, Revision: draft.Revision, ExpectedAppVersion: 2, ExpectedDraftVersion: 1, ChangeMetadata: metadata}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE agent_app_revision SET instruction='forged' WHERE tenant_id=$1 AND agent_app_id=$2 AND revision=$3`, tenantID, appID, draft.Revision); sqlState(err) != "55000" {
		t.Fatalf("published update err=%v state=%s", err, sqlState(err))
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO agent_app_revision_tool(tenant_id,agent_app_id,revision,tool_id,tool_version) VALUES($1,$2,$3,'late-tool',1)`, tenantID, appID, draft.Revision); sqlState(err) != "55000" {
		t.Fatalf("published child insert err=%v state=%s", err, sqlState(err))
	}
}

func insertActiveModelProfile(tb testing.TB, db *sql.DB, tenantID string) {
	tb.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `INSERT INTO model_profile(tenant_id,model_profile_id,profile_key,display_name,status) VALUES($1,'model','contract-model','Contract Model','active')`, tenantID); err != nil {
		tb.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO model_profile_revision(tenant_id,model_profile_id,profile_version,schema_version,provider,model_name,content_digest) VALUES($1,'model',1,1,'contract','contract',repeat('a',64))`, tenantID); err != nil {
		tb.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE model_profile SET current_version=1 WHERE tenant_id=$1 AND model_profile_id='model'`, tenantID); err != nil {
		tb.Fatal(err)
	}
}

func sqlState(err error) string {
	type sqlStater interface{ SQLState() string }
	var state sqlStater
	if errors.As(err, &state) {
		return state.SQLState()
	}
	return ""
}

func openContractDB(t *testing.T) *sql.DB {
	t.Helper()
	if os.Getenv("TRPC_MIGRATION_TEST") != "1" {
		t.Skip("requires explicit disposable PostgreSQL migration test")
	}
	dsn := os.Getenv("TRPC_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("TRPC_POSTGRES_TEST_DSN is not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var databaseName string
	var serverMajor int
	if err := db.QueryRowContext(context.Background(), `SELECT current_database(),current_setting('server_version_num')::int/10000`).Scan(&databaseName, &serverMajor); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(databaseName, "trpc_agent_service_test_") || serverMajor != 16 {
		t.Fatalf("refusing database=%q PostgreSQL=%d", databaseName, serverMajor)
	}
	return db
}
