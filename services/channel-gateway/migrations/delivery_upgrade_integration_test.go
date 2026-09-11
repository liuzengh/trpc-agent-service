package migrations_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/migrations"
	"strings"
	"sync"
	"testing"
	"time"
)

func embeddedMigrationCount(t *testing.T) int {
	t.Helper()
	entries, e := migrations.Files.ReadDir(".")
	if e != nil {
		t.Fatal(e)
	}
	n := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sql") {
			n++
		}
	}
	return n
}
func deliveryUpgradeDatabase(t *testing.T) (*pgxpool.Pool, *pgxpool.Pool) {
	t.Helper()
	a, b := connectionUpgradeDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, e := a.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(context.Background())
	body, e := migrations.Files.ReadFile("0005_connection.sql")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = tx.Exec(ctx, string(body)); e != nil {
		t.Fatal(e)
	}
	if _, e = tx.Exec(ctx, `INSERT INTO gateway_schema_migrations(version,digest,applied_at) VALUES('0005_connection.sql',$1,'2026-09-01T00:00:00Z')`, fmt.Sprintf("%x", sha256.Sum256(body))); e != nil {
		t.Fatal(e)
	}
	// Preserve both an active owner and a permanently isolated revision.
	if _, e = tx.Exec(ctx, `INSERT INTO gateway_connection_accounts(account_id,bot_id,credential_ref,revision,enabled,instance_id,epoch,lease_until,blocked_revision,updated_at) VALUES
 ('active-account','active-bot','FIXTURE_ACTIVE',4,true,'active-owner',9,'2030-01-01T00:00:00Z',0,'2026-09-01T00:00:00Z'),
 ('replaced-account','replaced-bot','FIXTURE_REPLACED',7,true,'',11,NULL,7,'2026-09-01T00:00:00Z')`); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	return a, b
}
func deliveryLegacyFacts(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]string {
	t.Helper()
	facts := legacyConnectionFacts(t, ctx, pool)
	for _, table := range []string{"gateway_schema_migrations", "gateway_connection_accounts"} {
		filter := ""
		if table == "gateway_schema_migrations" {
			filter = " WHERE version<'0006'"
		}
		var snapshot string
		if e := pool.QueryRow(ctx, `SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY to_jsonb(row)::text),'[]'::jsonb)::text FROM `+pgx.Identifier{table}.Sanitize()+` row`+filter).Scan(&snapshot); e != nil {
			t.Fatal(e)
		}
		facts[table] = snapshot
	}
	return facts
}
func TestDeliveryMigrationUpgradesReal0005WithoutRewritingFacts(t *testing.T) {
	a, b := deliveryUpgradeDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	before := deliveryLegacyFacts(t, ctx, a)
	var existed bool
	if e := a.QueryRow(ctx, `SELECT to_regclass('gateway_delivery_intents') IS NOT NULL`).Scan(&existed); e != nil || existed {
		t.Fatalf("fixture was not a real 0005 database: %v %v", existed, e)
	}
	start := make(chan struct{})
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			<-start
			pool := a
			if i%2 == 1 {
				pool = b
			}
			errs <- migrations.Apply(ctx, pool)
		})
	}
	close(start)
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	assertConnectionFacts(t, before, deliveryLegacyFacts(t, ctx, a))
	var count int
	if e := a.QueryRow(ctx, `SELECT count(*) FROM gateway_schema_migrations`).Scan(&count); e != nil || count != embeddedMigrationCount(t) {
		t.Fatalf("migration count=%d err=%v", count, e)
	}
	for _, name := range []string{"0001_gateway.sql", "0002_admission_budget.sql", "0003_routing_replay.sql", "0004_routing_apply_lag.sql", "0005_connection.sql", "0006_delivery.sql"} {
		body, e := migrations.Files.ReadFile(name)
		if e != nil {
			t.Fatal(e)
		}
		var digest string
		if e = a.QueryRow(ctx, `SELECT digest FROM gateway_schema_migrations WHERE version=$1`, name).Scan(&digest); e != nil || digest != fmt.Sprintf("%x", sha256.Sum256(body)) {
			t.Fatalf("SHA ledger changed %s: %v", name, e)
		}
	}
	var historicalNull bool
	if e := a.QueryRow(ctx, `SELECT bool_and(reply_origin IS NULL) FROM gateway_admissions`).Scan(&historicalNull); e != nil || !historicalNull {
		t.Fatalf("historical origin invented: %v %v", historicalNull, e)
	}
	if e := migrations.Apply(ctx, b); e != nil {
		t.Fatal(e)
	}
	assertConnectionFacts(t, before, deliveryLegacyFacts(t, ctx, a))
	t.Log("DELIVERY_UPGRADE_VERIFIED: real 0005 to 0006, two pools/eight migrators, all prior facts and migration digests unchanged, historical reply_origin NULL, repeated startup idempotent")
}
func TestDeliveryMigrationFailureRollsBackDDLOriginAndLedger(t *testing.T) {
	a, b := deliveryUpgradeDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	before := deliveryLegacyFacts(t, ctx, a)
	if _, e := a.Exec(ctx, `CREATE FUNCTION reject_delivery_ledger() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.version='0006_delivery.sql' THEN RAISE EXCEPTION 'fixture rejects Delivery ledger'; END IF; RETURN NEW; END $$;
 CREATE TRIGGER reject_delivery_ledger BEFORE INSERT ON gateway_schema_migrations FOR EACH ROW EXECUTE FUNCTION reject_delivery_ledger()`); e != nil {
		t.Fatal(e)
	}
	if e := migrations.Apply(ctx, b); e == nil || !strings.Contains(e.Error(), "fixture rejects Delivery ledger") {
		t.Fatalf("expected post-DDL rollback: %v", e)
	}
	assertConnectionFacts(t, before, deliveryLegacyFacts(t, ctx, a))
	var tables, origin bool
	if e := a.QueryRow(ctx, `SELECT to_regclass('gateway_delivery_intents') IS NOT NULL,EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='gateway_admissions' AND column_name='reply_origin')`).Scan(&tables, &origin); e != nil || tables || origin {
		t.Fatalf("DDL escaped rollback: tables=%v origin=%v err=%v", tables, origin, e)
	}
	var count int
	if e := a.QueryRow(ctx, `SELECT count(*) FROM gateway_schema_migrations`).Scan(&count); e != nil || count != 5 {
		t.Fatalf("failure advanced ledger: %d %v", count, e)
	}
	if _, e := a.Exec(ctx, `DROP TRIGGER reject_delivery_ledger ON gateway_schema_migrations; DROP FUNCTION reject_delivery_ledger()`); e != nil {
		t.Fatal(e)
	}
	if e := migrations.Apply(ctx, b); e != nil {
		t.Fatal(e)
	}
	assertConnectionFacts(t, before, deliveryLegacyFacts(t, ctx, a))
	t.Log("DELIVERY_UPGRADE_ROLLBACK_VERIFIED: post-DDL error retains real 0005 facts and removes all 0006 DDL/reply_origin/ledger; subsequent startup succeeds")
}
