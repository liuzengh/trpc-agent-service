package migrations_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/migrations"
)

func runtimeUpgradeDatabase(t *testing.T) (*pgxpool.Pool, *pgxpool.Pool) {
	t.Helper()
	a, b := deliveryUpgradeDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := a.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	body, err := migrations.Files.ReadFile("0006_delivery.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, string(body)); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO gateway_schema_migrations(version,digest,applied_at) VALUES('0006_delivery.sql',$1,'2026-09-02T00:00:00Z')`, fmt.Sprintf("%x", sha256.Sum256(body))); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO gateway_delivery_intents(intent_id,run_id,digest,intent,target,provider,account_id,deadline,part_count,created_at)
 VALUES('historical-final','historical-run','historical-digest','{"immutable":"intent"}','{"immutable":"target"}','telegram','historical-account','2030-01-01T00:00:00Z',2,'2026-09-02T00:00:00Z');
 INSERT INTO gateway_delivery_parts(part_id,intent_id,part_index,body,state,attempt_number,current_attempt_id,next_attempt_at,updated_at)
 VALUES('historical-first','historical-final',0,'first body','UNKNOWN',1,'historical-attempt',NULL,'2026-09-02T01:00:00Z'),
 ('historical-second','historical-final',1,'second body','PENDING',0,NULL,'2026-09-02T00:00:00Z','2026-09-02T00:00:00Z');
 INSERT INTO gateway_delivery_attempts(attempt_id,part_id,attempt_number,claim_token,instance_id,request_id,request_digest,evidence_hash,calling_until,result,created_at,finished_at)
 VALUES('historical-attempt','historical-first',1,'historical-claim','historical-instance','historical-request','historical-request-digest','historical-capability-hash','2026-09-02T00:00:30Z','{"Certainty":"UNKNOWN"}','2026-09-02T00:00:00Z','2026-09-02T01:00:00Z');
 INSERT INTO gateway_delivery_observations(observation_id,attempt_id,request_id,request_digest,result,observed_at)
 VALUES('historical-observation','historical-attempt','historical-request','historical-request-digest','{"Certainty":"ACCEPTED","ProviderMessageID":"historical-message"}','2026-09-02T02:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return a, b
}

// Compare exactly the original 0006 facts; 0009 adds empty, non-authorizing
// proof columns, separately exercised by Control A1/A2 upgrade tests.
func runtimeLegacyFacts(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]string {
	t.Helper()
	facts := deliveryLegacyFacts(t, ctx, pool)
	for _, table := range []string{"gateway_schema_migrations", "gateway_delivery_intents", "gateway_delivery_parts", "gateway_delivery_attempts", "gateway_delivery_observations"} {
		filter := ""
		if table == "gateway_schema_migrations" {
			filter = " WHERE version<'0007'"
		}
		var snapshot string
		if err := pool.QueryRow(ctx, `SELECT COALESCE(jsonb_agg(to_jsonb(row)-'claim_use_binding'-'account_use_binding' ORDER BY to_jsonb(row)::text),'[]'::jsonb)::text FROM `+pgx.Identifier{table}.Sanitize()+` row`+filter).Scan(&snapshot); err != nil {
			t.Fatal(err)
		}
		facts[table] = snapshot
	}
	return facts
}

var runtimeIndexes = []string{"gateway_delivery_runtime_accounts", "gateway_delivery_runtime_deadlines", "gateway_delivery_runtime_pending", "gateway_delivery_runtime_unknown", "gateway_delivery_runtime_attempts"}

func TestRuntimeMigrationUpgradesReal0006PreservingFacts(t *testing.T) {
	a, b := runtimeUpgradeDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	before := runtimeLegacyFacts(t, ctx, a)
	start := make(chan struct{})
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	for n := 0; n < 8; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			<-start
			pool := a
			if n%2 == 1 {
				pool = b
			}
			errs <- migrations.Apply(ctx, pool)
		}(n)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	assertConnectionFacts(t, before, runtimeLegacyFacts(t, ctx, a))
	for _, name := range runtimeIndexes {
		var valid bool
		if err := a.QueryRow(ctx, `SELECT COALESCE((SELECT indisvalid FROM pg_index WHERE indexrelid=to_regclass($1)),false)`, name).Scan(&valid); err != nil || !valid {
			t.Fatalf("runtime index %s valid=%v err=%v", name, valid, err)
		}
	}
	var count int
	if err := a.QueryRow(ctx, `SELECT count(*) FROM gateway_schema_migrations`).Scan(&count); err != nil || count != embeddedMigrationCount(t) {
		t.Fatalf("migration count=%d err=%v", count, err)
	}
	var digest string
	body, err := migrations.Files.ReadFile("0007_delivery_runtime.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err = a.QueryRow(ctx, `SELECT digest FROM gateway_schema_migrations WHERE version='0007_delivery_runtime.sql'`).Scan(&digest); err != nil || digest != fmt.Sprintf("%x", sha256.Sum256(body)) {
		t.Fatalf("runtime migration SHA: %s %v", digest, err)
	}
	if err = migrations.Apply(ctx, b); err != nil {
		t.Fatal(err)
	}
	assertConnectionFacts(t, before, runtimeLegacyFacts(t, ctx, a))
	t.Log("RUNTIME_UPGRADE_VERIFIED: real 0006, two pools/eight migrators; all prior route/admission/owner/Final/part/attempt/late-observation facts and old migration SHA/timestamps unchanged; five valid indexes; idempotent restart")
}

func TestRuntimeMigrationFailureRollsBackIndexesAndLedger(t *testing.T) {
	a, b := runtimeUpgradeDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	before := runtimeLegacyFacts(t, ctx, a)
	if _, err := a.Exec(ctx, `CREATE FUNCTION reject_runtime_migration() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.version='0007_delivery_runtime.sql' THEN RAISE EXCEPTION 'fixture rejects runtime migration'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_runtime_migration BEFORE INSERT ON gateway_schema_migrations FOR EACH ROW EXECUTE FUNCTION reject_runtime_migration()`); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Apply(ctx, b); err == nil || !strings.Contains(err.Error(), "fixture rejects runtime migration") {
		t.Fatalf("expected post-index failure: %v", err)
	}
	assertConnectionFacts(t, before, runtimeLegacyFacts(t, ctx, a))
	for _, name := range runtimeIndexes {
		var exists bool
		if err := a.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, name).Scan(&exists); err != nil || exists {
			t.Fatalf("runtime index escaped rollback: %s %v %v", name, exists, err)
		}
	}
	var count int
	if err := a.QueryRow(ctx, `SELECT count(*) FROM gateway_schema_migrations`).Scan(&count); err != nil || count != 6 {
		t.Fatalf("failure advanced migration ledger: %d %v", count, err)
	}
	if _, err := a.Exec(ctx, `DROP TRIGGER reject_runtime_migration ON gateway_schema_migrations; DROP FUNCTION reject_runtime_migration()`); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Apply(ctx, b); err != nil {
		t.Fatal(err)
	}
	assertConnectionFacts(t, before, runtimeLegacyFacts(t, ctx, a))
	t.Log("RUNTIME_UPGRADE_ROLLBACK_VERIFIED: post-DDL failure leaves real 0006 business facts and SHA ledger intact; all five new indexes absent; retry upgrades successfully")
}
