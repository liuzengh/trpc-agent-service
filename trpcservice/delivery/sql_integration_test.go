package delivery_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/delivery"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/repository"
)

func TestSQLStoreClaimsOutboxAcrossWorkersAndRecovers(t *testing.T) {
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
	tenantID, appID := "delivery-"+suffix, "app"
	seedDeliveryParents(t, ctx, db, tenantID, appID, "wecom", "http")
	now := time.Unix(1_700_100_000, 0).UTC()
	store := &delivery.SQLStore{DB: db, Now: func() time.Time { return now }, MaxAttempts: 3}
	wecomBinding := []delivery.BindingKey{{TenantID: tenantID, BindingID: "wecom"}}

	t.Run("binding scope and skip locked produce one winner", func(t *testing.T) {
		wecom := seedOutbox(t, ctx, db, tenantID, appID, "wecom", "first", now)
		_ = seedOutbox(t, ctx, db, tenantID, appID, "http", "not-for-delivery", now)
		var wg sync.WaitGroup
		results := make(chan []delivery.Claim, 2)
		errorsCh := make(chan error, 2)
		for _, owner := range []string{"delivery-a", "delivery-b"} {
			wg.Add(1)
			go func(owner string) {
				defer wg.Done()
				claims, err := store.ClaimReady(ctx, wecomBinding, owner, time.Second, 10)
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
		var claimed []delivery.Claim
		for claims := range results {
			claimed = append(claimed, claims...)
		}
		if len(claimed) != 1 || claimed[0].Message.OutboxID != wecom.OutboxID {
			t.Fatalf("claims=%+v", claimed)
		}
		now = now.Add(2 * time.Second)
		recovered, err := store.ClaimReady(ctx, wecomBinding, "recovery", time.Minute, 10)
		if err != nil || len(recovered) != 1 || recovered[0].Attempt != 2 {
			t.Fatalf("recovered=%+v err=%v", recovered, err)
		}
		if err := store.BeginSend(ctx, claimed[0]); !errors.Is(err, delivery.ErrClaimOwner) {
			t.Fatalf("stale mark sent error=%v", err)
		}
		if err := store.BeginSend(ctx, recovered[0]); err != nil {
			t.Fatal(err)
		}
		if err := store.MarkSent(ctx, recovered[0]); err != nil {
			t.Fatal(err)
		}
		assertOutboxStatus(t, ctx, db, tenantID, "outbox:not-for-delivery", delivery.StatusPending)
	})

	t.Run("retry schedule and permanent failure", func(t *testing.T) {
		message := seedOutbox(t, ctx, db, tenantID, appID, "wecom", "retry", now)
		claims, err := store.ClaimReady(ctx, wecomBinding, "worker", time.Minute, 10)
		if err != nil || len(claims) != 1 || claims[0].Message.OutboxID != message.OutboxID {
			t.Fatalf("claims=%+v err=%v", claims, err)
		}
		if err := store.BeginSend(ctx, claims[0]); err != nil {
			t.Fatal(err)
		}
		retryAt := now.Add(time.Minute)
		status, err := store.Fail(ctx, claims[0], errors.New("temporary\nprovider error"), retryAt, true)
		if err != nil || status != delivery.StatusRetry {
			t.Fatalf("status=%q err=%v", status, err)
		}
		if early, err := store.ClaimReady(ctx, wecomBinding, "early", time.Minute, 10); err != nil || len(early) != 0 {
			t.Fatalf("early=%+v err=%v", early, err)
		}
		now = retryAt
		claims, err = store.ClaimReady(ctx, wecomBinding, "worker-2", time.Minute, 10)
		if err != nil || len(claims) != 1 || claims[0].Attempt != 2 {
			t.Fatalf("retry claims=%+v err=%v", claims, err)
		}
		if err := store.BeginSend(ctx, claims[0]); err != nil {
			t.Fatal(err)
		}
		status, err = store.Fail(ctx, claims[0], errors.New("invalid recipient"), time.Time{}, false)
		if err != nil || status != delivery.StatusDLQ {
			t.Fatalf("status=%q err=%v", status, err)
		}
		assertOutboxStatus(t, ctx, db, tenantID, message.OutboxID, delivery.StatusDLQ)
	})

	t.Run("attempt limit goes directly to dlq", func(t *testing.T) {
		limited := &delivery.SQLStore{DB: db, Now: func() time.Time { return now }, MaxAttempts: 1}
		message := seedOutbox(t, ctx, db, tenantID, appID, "wecom", "exhausted", now)
		claims, err := limited.ClaimReady(ctx, wecomBinding, "worker", time.Minute, 10)
		if err != nil || len(claims) != 1 {
			t.Fatalf("claims=%+v err=%v", claims, err)
		}
		if err := limited.BeginSend(ctx, claims[0]); err != nil {
			t.Fatal(err)
		}
		status, err := limited.Fail(ctx, claims[0], errors.New("rate limited"), now.Add(time.Second), true)
		if err != nil || status != delivery.StatusDLQ {
			t.Fatalf("status=%q err=%v", status, err)
		}
		assertOutboxStatus(t, ctx, db, tenantID, message.OutboxID, delivery.StatusDLQ)
	})

	t.Run("expired in-flight send becomes uncertain", func(t *testing.T) {
		message := seedOutbox(t, ctx, db, tenantID, appID, "wecom", "uncertain", now)
		claims, err := store.ClaimReady(ctx, wecomBinding, "worker", time.Second, 10)
		if err != nil || len(claims) != 1 {
			t.Fatalf("claims=%+v err=%v", claims, err)
		}
		if err := store.BeginSend(ctx, claims[0]); err != nil {
			t.Fatal(err)
		}
		now = now.Add(2 * time.Second)
		ready, err := store.ClaimReady(ctx, wecomBinding, "other", time.Minute, 10)
		if err != nil || len(ready) != 0 {
			t.Fatalf("ready=%+v err=%v", ready, err)
		}
		assertOutboxStatus(t, ctx, db, tenantID, message.OutboxID, delivery.StatusUncertain)
	})
}

func seedDeliveryParents(t *testing.T, ctx context.Context, db *sql.DB, tenantID, appID string, bindings ...string) {
	t.Helper()
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO tenants (tenant_id,name,enabled,current_config_version) VALUES ($1,$1,FALSE,1)`, []any{tenantID}},
		{`INSERT INTO config_versions (tenant_id,version,config_yaml,config_sha256,status) VALUES ($1,1,$2,'test','published')`, []any{tenantID, []byte("test")}},
		{`INSERT INTO agent_apps (tenant_id,app_id,name,enabled,config_version) VALUES ($1,$2,$2,TRUE,1)`, []any{tenantID, appID}},
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	for _, binding := range bindings {
		// Keep test rows outside the production Delivery Worker route query so
		// this integration suite can run beside a live Compose service without
		// that worker claiming the fixture Outbox first.
		channelType := "integration-test"
		if _, err := db.ExecContext(ctx, `INSERT INTO channel_bindings (tenant_id,app_id,binding_id,channel_type,provider_account_id,enabled,config_version) VALUES ($1,$2,$3,$4,$3,TRUE,1)`, tenantID, appID, binding, channelType); err != nil {
			t.Fatal(err)
		}
	}
}

func seedOutbox(t *testing.T, ctx context.Context, db *sql.DB, tenantID, appID, bindingID, suffix string, now time.Time) gateway.OutboundMessage {
	t.Helper()
	sessionID, inboxID, eventID, outboxID := "session-"+suffix, "inbox-"+suffix, "event-"+suffix, "outbox:"+suffix
	inbound, _ := json.Marshal(gateway.InboundMessage{TenantID: tenantID, AppID: appID, BindingID: bindingID, ExternalMessageID: "external-" + suffix, UserID: "user", SessionID: sessionID, Text: "hello", ConfigVersion: 1})
	message := gateway.OutboundMessage{TenantID: tenantID, AppID: appID, BindingID: bindingID, OutboxID: outboxID, DedupeKey: "reply:" + suffix, UserID: "user", SessionID: sessionID, ExternalUserID: "external-user", Text: "reply", SourceInboxID: inboxID, SourceEventID: eventID, CreatedAt: now}
	payload, _ := json.Marshal(message)
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO session_heads (tenant_id,app_id,user_id,session_id,last_event_seq,last_fence) VALUES ($1,$2,'user',$3,1,1)`, []any{tenantID, appID, sessionID}},
		{`INSERT INTO inbox_messages (tenant_id,binding_id,external_message_id,app_id,user_id,session_id,status,attempts,payload_json,inbox_id,config_version,inbox_seq) VALUES ($1,$2,$3,$4,'user',$5,'completed',1,$6,$7,1,1)`, []any{tenantID, bindingID, "external-" + suffix, appID, sessionID, inbound, inboxID}},
		{`INSERT INTO message_events (tenant_id,app_id,user_id,session_id,event_id,event_seq,event_type,payload_json,inbox_id) VALUES ($1,$2,'user',$3,$4,1,'message','{}',$5)`, []any{tenantID, appID, sessionID, eventID, inboxID}},
		{`INSERT INTO outbox_messages (tenant_id,outbox_id,dedupe_key,binding_id,app_id,user_id,session_id,status,payload_json,source_inbox_id,source_event_id,fence,created_at) VALUES ($1,$2,$3,$4,$5,'user',$6,'pending',$7,$8,$9,1,$10)`, []any{tenantID, outboxID, message.DedupeKey, bindingID, appID, sessionID, payload, inboxID, eventID, now}},
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	return message
}

func assertOutboxStatus(t *testing.T, ctx context.Context, db *sql.DB, tenantID, outboxID string, expected delivery.Status) {
	t.Helper()
	var status delivery.Status
	if err := db.QueryRowContext(ctx, `SELECT status FROM outbox_messages WHERE tenant_id=$1 AND outbox_id=$2`, tenantID, outboxID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != expected {
		t.Fatalf("status=%q want=%q", status, expected)
	}
}
