package postgres

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/summary"
)

func TestSummaryContentCompactionPostgreSQL16(t *testing.T) {
	if os.Getenv("TRPC_MIGRATION_TEST") != "1" {
		t.Skip("requires explicit disposable PostgreSQL migration test")
	}
	db, err := sql.Open("pgx", os.Getenv("TRPC_POSTGRES_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var database string
	var major int
	if err := db.QueryRow(`SELECT current_database(),current_setting('server_version_num')::int/10000`).Scan(&database, &major); err != nil || !strings.HasPrefix(database, "trpc_agent_service_test_") || major != 16 {
		t.Fatalf("database=%q major=%d err=%v", database, major, err)
	}
	ctx := context.Background()
	const tenant = "t_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const app = "app_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const session = "summary-content-contract"
	if _, err := db.ExecContext(ctx, `INSERT INTO session_head(tenant_id,agent_app_id,session_id,version,last_fence,last_session_seq,next_input_seq,state_json) VALUES($1,$2,$3,0,0,2,1,'{}')`, tenant, app, session); err != nil {
		t.Fatal(err)
	}
	for _, value := range []struct {
		id  string
		seq int
	}{{"one", 1}, {"two", 2}} {
		if _, err := db.ExecContext(ctx, `INSERT INTO session_summary(tenant_id,agent_app_id,session_id,summary_id,base_session_seq,last_event_id,cutoff_at,content_ref) VALUES($1,$2,$3,$4,$5,$6,now(),$7)`, tenant, app, session, value.id, value.seq, "event-"+value.id, "summary://"+value.id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, `UPDATE session_head SET summary_id='two' WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3`, tenant, app, session); err != nil {
		t.Fatal(err)
	}
	store := New(db)
	one := summary.Body{Key: summary.Key{TenantID: tenant, AgentAppID: app, SessionID: session, SummaryID: "one"}, ContentRef: "summary://one", Content: []byte("old")}
	two := summary.Body{Key: summary.Key{TenantID: tenant, AgentAppID: app, SessionID: session, SummaryID: "two"}, ContentRef: "summary://two", Content: []byte("new")}
	if _, err := store.Put(ctx, one); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, two); err != nil {
		t.Fatal(err)
	}
	if err := store.Supersede(ctx, one.Key, "two", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimSuperseded(ctx, time.Now().UTC(), "worker", time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim=%#v err=%v", claimed, err)
	}
	if err := store.FinishDelete(ctx, claimed[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, one.Key); !errors.Is(err, runtime.ErrNotFound) {
		t.Fatalf("old err=%v", err)
	}
}
