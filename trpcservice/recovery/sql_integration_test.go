package recovery_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/recovery"
	"github.com/liuzengh/trpc-agent-service/trpcservice/repository"
)

func TestPostgresTenantScopedInboxOutboxRecovery(t *testing.T) {
	dsn := os.Getenv("TRPC_AGENT_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set TRPC_AGENT_POSTGRES_TEST_DSN to run PostgreSQL integration tests")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := repository.Migrate(ctx, func(ctx context.Context, script string) error {
		_, err := db.ExecContext(ctx, script)
		return err
	}, repository.DirectionUp); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	store := &recovery.SQLStore{DB: db, Now: func() time.Time { return now }}
	suffix := fmt.Sprint(time.Now().UnixNano())
	for _, tenantID := range []string{"recovery-a-" + suffix, "recovery-b-" + suffix} {
		seedRecoveryTenant(t, ctx, db, tenantID, "shared", now)
	}
	tenantA, tenantB := "recovery-a-"+suffix, "recovery-b-"+suffix

	items, err := store.List(ctx, tenantA, recovery.KindOutbox, []recovery.Status{recovery.StatusDLQ, recovery.StatusUncertain}, 50)
	if err != nil || len(items) != 2 {
		t.Fatalf("list=%+v err=%v", items, err)
	}
	for _, item := range items {
		if item.TenantID != tenantA || strings.Contains(fmt.Sprintf("%+v", item), "secret-canary") {
			t.Fatalf("unsafe list item=%+v", item)
		}
		if item.TraceID != "trace-outbox" {
			t.Fatalf("missing durable trace metadata: %+v", item)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.Redrive(ctx, tenantA, recovery.KindOutbox, "shared-dlq", recovery.StatusDLQ)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	success, conflicts := 0, 0
	for err := range errs {
		switch {
		case err == nil:
			success++
		case errors.Is(err, recovery.ErrConflict):
			conflicts++
		default:
			t.Fatal(err)
		}
	}
	if success != 1 || conflicts != 1 {
		t.Fatalf("success=%d conflicts=%d", success, conflicts)
	}
	// The identical id in tenant B is untouched.
	other, err := store.List(ctx, tenantB, recovery.KindOutbox, []recovery.Status{recovery.StatusDLQ}, 50)
	if err != nil || len(other) != 1 || other[0].ID != "shared-dlq" {
		t.Fatalf("tenant B=%+v err=%v", other, err)
	}
	resolved, err := store.ResolveOutbox(ctx, tenantA, "shared-uncertain", recovery.StatusUncertain, recovery.StatusSent)
	if err != nil || resolved.Status != recovery.StatusSent {
		t.Fatalf("resolve=%+v err=%v", resolved, err)
	}
	if _, err := store.ResolveOutbox(ctx, tenantA, "shared-uncertain", recovery.StatusUncertain, recovery.StatusRetry); !errors.Is(err, recovery.ErrConflict) {
		t.Fatalf("stale resolve=%v", err)
	}
	if inbox, err := store.Redrive(ctx, tenantA, recovery.KindInbox, "shared-inbox", recovery.StatusDLQ); err != nil || inbox.Status != recovery.StatusRetry || inbox.Attempts != 0 {
		t.Fatalf("inbox=%+v err=%v", inbox, err)
	}
}

func seedRecoveryTenant(t *testing.T, ctx context.Context, db *sql.DB, tenantID, prefix string, now time.Time) {
	t.Helper()
	config := []byte("schema_version: 1\ntenants: []\n")
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO tenants(tenant_id,name,enabled,current_config_version) VALUES($1,$1,true,1)`, []any{tenantID}},
		{`INSERT INTO config_versions(tenant_id,version,config_yaml,config_sha256) VALUES($1,1,$2,'hash')`, []any{tenantID, config}},
		{`INSERT INTO agent_apps(tenant_id,app_id,name,enabled,config_version) VALUES($1,'app','app',true,1)`, []any{tenantID}},
		{`INSERT INTO channel_bindings(tenant_id,app_id,binding_id,channel_type,provider_account_id,enabled,config_version) VALUES($1,'app','binding','integration-test',$1,true,1)`, []any{tenantID}},
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	inboxPayload := fmt.Sprintf(`{"tenant_id":%q,"app_id":"app","binding_id":"binding","external_message_id":%q,"user_id":"user","session_id":"session","config_version":1}`, tenantID, prefix+"-external")
	if _, err := db.ExecContext(ctx, `INSERT INTO inbox_messages(tenant_id,binding_id,external_message_id,inbox_id,app_id,user_id,session_id,config_version,inbox_seq,status,attempts,trace_id,last_error,payload_json,created_at) VALUES($1,'binding',$2,$3,'app','user','session',1,1,'dlq',5,'trace-inbox','secret-canary',$4,$5)`, tenantID, prefix+"-external", prefix+"-inbox", inboxPayload, now); err != nil {
		t.Fatal(err)
	}
	eventID := prefix + "-event"
	if _, err := db.ExecContext(ctx, `INSERT INTO session_heads(tenant_id,app_id,user_id,session_id) VALUES($1,'app','user','session')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO message_events(tenant_id,app_id,user_id,session_id,event_id,inbox_id,event_seq,event_type,payload_json,created_at) VALUES($1,'app','user','session',$2,$3,1,'assistant','{}',$4)`, tenantID, eventID, prefix+"-inbox", now); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range []struct{ id, status string }{{prefix + "-dlq", "dlq"}, {prefix + "-uncertain", "uncertain"}} {
		payload := fmt.Sprintf(`{"TenantID":%q,"AppID":"app","BindingID":"binding","OutboxID":%q,"TraceID":"trace-outbox","Text":"secret-canary"}`, tenantID, fixture.id)
		if _, err := db.ExecContext(ctx, `INSERT INTO outbox_messages(tenant_id,outbox_id,dedupe_key,binding_id,app_id,user_id,session_id,source_inbox_id,source_event_id,fence,status,attempts,last_error,payload_json,created_at) VALUES($1,$2,$3,'binding','app','user','session',$4,$5,1,$6,5,'secret-canary',$7,$8)`, tenantID, fixture.id, fixture.id+"-dedupe", prefix+"-inbox", eventID, fixture.status, payload, now); err != nil {
			t.Fatal(err)
		}
	}
}
