package idempotency_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	"github.com/liuzengh/trpc-agent-service/trpcservice/repository"
)

func TestSQLClaimReadyRecoveryConcurrencyOrderAndDLQ(t *testing.T) {
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

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	tenantID, appID, bindingID := "inbox-"+suffix, "app", "binding"
	seedSQLInboxParents(t, ctx, db, tenantID, appID, bindingID)
	now := time.Unix(1_700_000_000, 0).UTC()
	store := &idempotency.SQLStore{DB: db, Now: func() time.Time { return now }, MaxAttempts: 3}

	t.Run("one worker reclaims a crashed claim", func(t *testing.T) {
		message := sqlTestMessage(tenantID, appID, bindingID, "crash", "crash-session", now)
		stale, won, err := store.Claim(ctx, message, "gateway", time.Second)
		if err != nil || !won {
			t.Fatalf("claim=%+v won=%v err=%v", stale, won, err)
		}
		if err := store.SaveExecution(ctx, stale, idempotency.ExecutionRecord{Stage: idempotency.ExecutionRunnerCommitted, Reply: "answer", EventID: "event"}); err != nil {
			t.Fatal(err)
		}
		now = now.Add(2 * time.Second)
		var wg sync.WaitGroup
		results := make(chan []idempotency.Claim, 2)
		errorsCh := make(chan error, 2)
		for _, owner := range []string{"worker-a", "worker-b"} {
			wg.Add(1)
			go func(owner string) {
				defer wg.Done()
				claims, err := store.ClaimReady(ctx, owner, time.Minute, 10)
				results <- claims
				errorsCh <- err
			}(owner)
		}
		wg.Wait()
		close(results)
		close(errorsCh)
		for err := range errorsCh {
			if err != nil {
				t.Fatal(err)
			}
		}
		var recovered []idempotency.Claim
		for claims := range results {
			recovered = append(recovered, claims...)
		}
		if len(recovered) != 1 || recovered[0].Attempt != 2 {
			t.Fatalf("recovered=%+v", recovered)
		}
		execution, err := store.GetExecution(ctx, recovered[0])
		if err != nil || execution.Stage != idempotency.ExecutionRunnerCommitted || execution.Reply != "answer" {
			t.Fatalf("execution=%+v err=%v", execution, err)
		}
		if err := store.Complete(ctx, stale); !errors.Is(err, idempotency.ErrClaimOwner) {
			t.Fatalf("stale completion error=%v", err)
		}
		if err := store.Complete(ctx, recovered[0]); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("session sequence blocks later work", func(t *testing.T) {
		firstMessage := sqlTestMessage(tenantID, appID, bindingID, "ordered-1", "ordered-session", now)
		first, _, err := store.Claim(ctx, firstMessage, "gateway", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		secondMessage := sqlTestMessage(tenantID, appID, bindingID, "ordered-2", "ordered-session", now.Add(time.Millisecond))
		second, _, err := store.Claim(ctx, secondMessage, "gateway", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		now = now.Add(2 * time.Second)
		ready, err := store.ClaimReady(ctx, "worker", time.Minute, 10)
		if err != nil || len(ready) != 1 || ready[0].InboxID != first.InboxID {
			t.Fatalf("first ready=%+v err=%v", ready, err)
		}
		if err := store.Complete(ctx, ready[0]); err != nil {
			t.Fatal(err)
		}
		ready, err = store.ClaimReady(ctx, "worker", time.Minute, 10)
		if err != nil || len(ready) != 1 || ready[0].InboxID != second.InboxID {
			t.Fatalf("second ready=%+v err=%v", ready, err)
		}
		if err := store.Complete(ctx, ready[0]); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("attempt limit moves work to dlq", func(t *testing.T) {
		dlqStore := &idempotency.SQLStore{DB: db, Now: func() time.Time { return now }, MaxAttempts: 1}
		message := sqlTestMessage(tenantID, appID, bindingID, "dlq", "dlq-session", now)
		original, _, err := dlqStore.Claim(ctx, message, "gateway", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		now = now.Add(2 * time.Second)
		ready, err := dlqStore.ClaimReady(ctx, "worker", time.Minute, 10)
		if err != nil || len(ready) != 0 {
			t.Fatalf("ready=%+v err=%v", ready, err)
		}
		duplicate, won, err := dlqStore.Claim(ctx, message, "other", time.Minute)
		if err != nil || won || duplicate.Status != idempotency.StatusDLQ || duplicate.ClaimToken != original.ClaimToken {
			t.Fatalf("duplicate=%+v won=%v err=%v", duplicate, won, err)
		}
	})

	t.Run("capacity defer preserves retry budget", func(t *testing.T) {
		message := sqlTestMessage(tenantID, appID, bindingID, "capacity", "capacity-session", now)
		original, won, err := store.Claim(ctx, message, "gateway", time.Minute)
		if err != nil || !won || original.Attempt != 1 {
			t.Fatalf("claim=%+v won=%v err=%v", original, won, err)
		}
		if err := store.Defer(ctx, original, now); err != nil {
			t.Fatal(err)
		}
		ready, err := store.ClaimReady(ctx, "worker", time.Minute, 10)
		if err != nil || len(ready) != 1 || ready[0].InboxID != original.InboxID || ready[0].Attempt != 1 {
			t.Fatalf("deferred ready=%+v err=%v", ready, err)
		}
		if err := store.Complete(ctx, ready[0]); err != nil {
			t.Fatal(err)
		}
	})
}

func seedSQLInboxParents(t *testing.T, ctx context.Context, db *sql.DB, tenantID, appID, bindingID string) {
	t.Helper()
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO tenants (tenant_id,name,enabled,current_config_version) VALUES ($1,$2,FALSE,1)`, []any{tenantID, tenantID}},
		{`INSERT INTO config_versions (tenant_id,version,config_yaml,config_sha256,status) VALUES ($1,1,$2,$3,'published')`, []any{tenantID, []byte("test"), "test"}},
		{`INSERT INTO agent_apps (tenant_id,app_id,name,enabled,config_version) VALUES ($1,$2,$2,TRUE,1)`, []any{tenantID, appID}},
		{`INSERT INTO channel_bindings (tenant_id,app_id,binding_id,channel_type,provider_account_id,enabled,config_version) VALUES ($1,$2,$3,'http',$3,TRUE,1)`, []any{tenantID, appID, bindingID}},
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
}

func sqlTestMessage(tenantID, appID, bindingID, externalID, sessionID string, receivedAt time.Time) gateway.InboundMessage {
	return gateway.InboundMessage{
		TenantID: tenantID, AppID: appID, BindingID: bindingID,
		ExternalMessageID: externalID, UserID: "user", SessionID: sessionID,
		Text: "hello", ConfigVersion: 1, ReceivedAt: receivedAt,
	}
}
