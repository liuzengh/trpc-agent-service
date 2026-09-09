package sessioncoord_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/repository"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessioncoord"
)

func TestSQLFenceGuardTurnOrderAndStaleCommit(t *testing.T) {
	dsn := os.Getenv("TRPC_AGENT_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set TRPC_AGENT_POSTGRES_TEST_DSN to run PostgreSQL integration tests")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(4)
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := repository.Migrate(ctx, func(ctx context.Context, script string) error {
		_, err := db.ExecContext(ctx, script)
		return err
	}, repository.DirectionUp); err != nil {
		t.Fatal(err)
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	tenantID := "fence-" + suffix
	if _, err := db.ExecContext(ctx, `INSERT INTO tenants(tenant_id,name,enabled,current_config_version) VALUES($1,$1,FALSE,1)`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO config_versions(tenant_id,version,config_yaml,config_sha256,status) VALUES($1,1,'test','test','published')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO agent_apps(tenant_id,app_id,name,enabled,config_version) VALUES($1,'app','app',TRUE,1)`, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = db.ExecContext(cleanupCtx, `DELETE FROM runtime_session_events WHERE app_name=$1`, "tenant/"+tenantID+"/app/app")
		_, _ = db.ExecContext(cleanupCtx, `DELETE FROM message_events WHERE tenant_id=$1`, tenantID)
		_, _ = db.ExecContext(cleanupCtx, `DELETE FROM session_heads WHERE tenant_id=$1`, tenantID)
		_, _ = db.ExecContext(cleanupCtx, `DELETE FROM agent_apps WHERE tenant_id=$1`, tenantID)
		_, _ = db.ExecContext(cleanupCtx, `DELETE FROM config_versions WHERE tenant_id=$1`, tenantID)
		_, _ = db.ExecContext(cleanupCtx, `DELETE FROM tenants WHERE tenant_id=$1`, tenantID)
	})

	store := &sessioncoord.SQLWriteStore{DB: db}
	key := gateway.SessionKey{TenantID: tenantID, AppID: "app", UserID: "user", SessionID: "session"}
	if err := store.AdvanceFence(ctx, key, 1); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateTurn(ctx, key, 2); !errors.Is(err, sessioncoord.ErrOutOfOrder) {
		t.Fatalf("later turn error=%v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	guardDone := make(chan error, 1)
	go func() {
		guardDone <- store.WithFence(ctx, key, 1, func(context.Context) error {
			// This write deliberately uses the pool rather than the fencing tx,
			// matching the upstream PostgreSQL Session service connection model.
			if _, err := db.ExecContext(ctx, `INSERT INTO runtime_session_events(app_name,user_id,session_id,event) VALUES($1,$2,$3,'{}'::jsonb)`, "tenant/"+tenantID+"/app/app", key.UserID, key.SessionID); err != nil {
				return err
			}
			close(started)
			<-release
			return nil
		})
	}()
	<-started
	takeoverDone := make(chan error, 1)
	go func() { takeoverDone <- store.AdvanceFence(ctx, key, 2) }()
	select {
	case err := <-takeoverDone:
		t.Fatalf("takeover crossed active fenced mutation: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-guardDone; err != nil {
		t.Fatal(err)
	}
	if err := <-takeoverDone; err != nil {
		t.Fatal(err)
	}

	write := sessioncoord.TurnWrite{Key: key, Fence: 1, InboxSeq: 1, InboxID: "inbox", EventID: "event", EventType: "assistant", Payload: "synthetic"}
	if _, err := store.CommitTurn(ctx, write); !errors.Is(err, sessioncoord.ErrStaleFence) {
		t.Fatalf("old fence commit error=%v", err)
	}
	write.Fence = 2
	if _, err := store.CommitTurn(ctx, write); err != nil {
		t.Fatal(err)
	}
}
