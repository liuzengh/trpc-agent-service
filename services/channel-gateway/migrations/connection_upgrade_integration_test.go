package migrations_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	connectionpg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/postgres"
	connectiondomain "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/migrations"
)

var beforeConnectionMigrations = []string{
	"0001_gateway.sql", "0002_admission_budget.sql", "0003_routing_replay.sql", "0004_routing_apply_lag.sql",
}

// Reconstruct a real 0004 database from its immutable SQL and SHA ledger, rather
// than first installing 0005 and then pretending that it is an upgrade fixture.
func connectionUpgradeDatabase(t *testing.T) (*pgxpool.Pool, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("GATEWAY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("GATEWAY_TEST_DATABASE_URL required for real PostgreSQL upgrade")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	var id [8]byte
	if _, err = rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}
	schema := pgx.Identifier{"gateway_connection_upgrade_" + hex.EncodeToString(id[:])}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, err := admin.Exec(cleanup, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	newPool := func() *pgxpool.Pool {
		cfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			t.Fatal(err)
		}
		cfg.ConnConfig.RuntimeParams["search_path"] = schema
		cfg.MaxConns = 8
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		return pool
	}
	p1, p2 := newPool(), newPool()
	tx, err := p1.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = tx.Rollback(cleanup)
	}()
	if _, err = tx.Exec(ctx, `CREATE TABLE gateway_schema_migrations(version text PRIMARY KEY,digest text NOT NULL,applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		t.Fatal(err)
	}
	for _, name := range beforeConnectionMigrations {
		body, err := migrations.Files.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, string(body)); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, `INSERT INTO gateway_schema_migrations(version,digest,applied_at) VALUES($1,$2,'2026-09-01T00:00:00Z')`, name, fmt.Sprintf("%x", sha256.Sum256(body))); err != nil {
			t.Fatal(err)
		}
	}
	// Include records whose retention is safety-critical: an old disabled route,
	// consumed and quarantined stream positions, an ongoing lag episode, a first
	// Receipt/Admission, claimed unpublished Outbox work, and the shared budget.
	_, err = tx.Exec(ctx, `
INSERT INTO gateway_route_projections(provider,account_id,generation,enabled,snapshot,digest,updated_at)
 VALUES('telegram','retired-account',7,false,'{"provider":"telegram","account_id":"retired-account","generation":7}','sha256:'||repeat('a',64),'2026-09-01T00:00:00Z');
INSERT INTO gateway_route_receipts(event_id,digest,received_at)
 VALUES('control-event-1','sha256:'||repeat('b',64),'2026-09-01T00:00:00Z');
INSERT INTO gateway_route_replay_state(singleton,stream_name,stream_id,target_sequence,contiguous_sequence,highest_sequence,last_observed_at,updated_at,apply_lag_since)
 VALUES(true,'ROUTES','2026-09-01T00:00:00Z',1,1,3,'2026-09-01T00:00:02Z','2026-09-01T00:00:02Z','2026-09-01T00:00:01Z');
INSERT INTO gateway_route_stream_receipts(stream_name,stream_id,sequence,event_id,digest,status,reason,received_at)
 VALUES('ROUTES','2026-09-01T00:00:00Z',1,'control-event-1','sha256:'||repeat('b',64),'APPLIED','','2026-09-01T00:00:00Z'),
 ('ROUTES','2026-09-01T00:00:00Z',3,NULL,'sha256:'||repeat('c',64),'QUARANTINED','schema','2026-09-01T00:00:02Z');
INSERT INTO gateway_route_quarantines(stream_name,stream_id,sequence,reason,digest,observed_at)
 VALUES('ROUTES','2026-09-01T00:00:00Z',3,'schema','sha256:'||repeat('c',64),'2026-09-01T00:00:02Z');
INSERT INTO gateway_inbox(provider,account_id,event_id,source_digest,receipt,received_at)
 VALUES('telegram','retired-account','message-1',repeat('d',64),'{"decision":"admit-run","admission_id":"admission-1","run_id":"run-1"}','2026-09-01T00:00:00Z');
INSERT INTO gateway_admissions(admission_id,run_id,tenant_id,provider,account_id,event_id,route,input,created_at)
 VALUES('admission-1','run-1','tenant-1','telegram','retired-account','message-1','{"generation":6,"binding_id":"previous-binding"}','{"text":"retained fixture"}','2026-09-01T00:00:00Z');
INSERT INTO gateway_outbox(event_id,subject,payload,created_at,claim_token,claimed_until,attempts,next_attempt_at)
 VALUES('admission-1','execution.run-requested.v1','{"event_id":"admission-1","fixture":"retained"}','2026-09-01T00:00:00Z','old-claim','2026-09-01T00:00:30Z',3,'2026-09-01T00:00:10Z');
UPDATE gateway_admission_budget SET window_started_at='2026-09-01T00:00:00Z',new_events=9 WHERE singleton;
`)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return p1, p2
}

func legacyConnectionFacts(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]string {
	t.Helper()
	tables := []string{"gateway_schema_migrations", "gateway_route_projections", "gateway_route_receipts", "gateway_route_replay_state", "gateway_route_stream_receipts", "gateway_route_quarantines", "gateway_inbox", "gateway_admissions", "gateway_outbox", "gateway_admission_budget"}
	result := make(map[string]string, len(tables))
	for _, table := range tables {
		filter := ""
		if table == "gateway_schema_migrations" {
			filter = " WHERE version < '0005'"
		}
		rowJSON := "to_jsonb(row)"
		if table == "gateway_admissions" {
			rowJSON += " - 'reply_origin'"
		} // 0006 adds nullable local metadata, not a rewrite of prior facts.
		query := `SELECT COALESCE(jsonb_agg(` + rowJSON + ` ORDER BY (` + rowJSON + `)::text),'[]'::jsonb)::text FROM ` + pgx.Identifier{table}.Sanitize() + ` row` + filter
		var snapshot string
		if err := pool.QueryRow(ctx, query).Scan(&snapshot); err != nil {
			t.Fatal(err)
		}
		result[table] = snapshot
	}
	return result
}

func assertConnectionFacts(t *testing.T, before, after map[string]string) {
	t.Helper()
	for table, snapshot := range before {
		want, err := legacyFactsWithoutNullTrace(table, snapshot)
		if err != nil {
			t.Fatal(err)
		}
		got, err := legacyFactsWithoutNullTrace(table, after[table])
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("migration rewrote %s, before=%s after=%s", table, snapshot, after[table])
		}
	}
}

func TestIntegrationConnectionMigrationUpgradesExistingFactsConcurrently(t *testing.T) {
	p1, p2 := connectionUpgradeDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	before := legacyConnectionFacts(t, ctx, p1)
	var exists bool
	if err := p1.QueryRow(ctx, `SELECT to_regclass('gateway_connection_accounts') IS NOT NULL`).Scan(&exists); err != nil || exists {
		t.Fatalf("fixture already contains Connection: %v %v", exists, err)
	}
	start := make(chan struct{})
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	for i := range 8 {
		pool := []*pgxpool.Pool{p1, p2}[i%2]
		wg.Go(func() { <-start; errs <- migrations.Apply(ctx, pool) })
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	assertConnectionFacts(t, before, legacyConnectionFacts(t, ctx, p1))
	var count int
	var digest string
	if err := p1.QueryRow(ctx, `SELECT count(*) FROM gateway_schema_migrations`).Scan(&count); err != nil || count != embeddedMigrationCount(t) {
		t.Fatalf("migration ledger count=%d err=%v", count, err)
	}
	body, err := migrations.Files.ReadFile("0005_connection.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err = p1.QueryRow(ctx, `SELECT digest FROM gateway_schema_migrations WHERE version='0005_connection.sql'`).Scan(&digest); err != nil || digest != fmt.Sprintf("%x", sha256.Sum256(body)) {
		t.Fatalf("Connection migration digest=%s err=%v", digest, err)
	}
	s := connectionpg.NewStore(p1)
	a := connectiondomain.Account{ID: "new-wecom-account", BotID: "bot-1", CredentialRef: "FIXTURE_REFERENCE", Revision: 1, Enabled: true}
	g, err := s.ApplyAndAcquire(ctx, a, "instance-1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.MarkReplaced(ctx, g); err != nil {
		t.Fatal(err)
	}
	if err = migrations.Apply(ctx, p2); err != nil {
		t.Fatal(err)
	}
	if _, err = connectionpg.NewStore(p2).ApplyAndAcquire(ctx, a, "instance-2", time.Second); !errors.Is(err, connectiondomain.ErrReplaced) {
		t.Fatalf("idempotent startup lost persistent replacement fence: %v", err)
	}
	a.Revision++
	next, err := s.ApplyAndAcquire(ctx, a, "instance-2", time.Second)
	if err != nil || next.Epoch != g.Epoch+1 {
		t.Fatalf("upgraded lease failed revision recovery: %+v %v", next, err)
	}
	assertConnectionFacts(t, before, legacyConnectionFacts(t, ctx, p1))
	t.Log("CONNECTION_UPGRADE_VERIFIED: 0001..0004 SHA ledger and all 9 legacy fact tables unchanged; two pools/eight migrators; new lease usable; repeated startup retains replacement fence")
}

func TestIntegrationConnectionMigrationFailureRollsBackTableAndLedger(t *testing.T) {
	p1, p2 := connectionUpgradeDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	before := legacyConnectionFacts(t, ctx, p1)
	_, err := p1.Exec(ctx, `CREATE FUNCTION reject_connection_ledger() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.version='0005_connection.sql' THEN RAISE EXCEPTION 'fixture rejects Connection ledger'; END IF; RETURN NEW; END $$;
CREATE TRIGGER reject_connection_ledger BEFORE INSERT ON gateway_schema_migrations FOR EACH ROW EXECUTE FUNCTION reject_connection_ledger()`)
	if err != nil {
		t.Fatal(err)
	}
	if err = migrations.Apply(ctx, p2); err == nil || !strings.Contains(err.Error(), "fixture rejects Connection ledger") {
		t.Fatalf("expected injected ledger failure after CREATE TABLE: %v", err)
	}
	assertConnectionFacts(t, before, legacyConnectionFacts(t, ctx, p1))
	var exists bool
	var count int
	if err = p1.QueryRow(ctx, `SELECT to_regclass('gateway_connection_accounts') IS NOT NULL`).Scan(&exists); err != nil || exists {
		t.Fatalf("failed migration left Connection table: %v %v", exists, err)
	}
	if err = p1.QueryRow(ctx, `SELECT count(*) FROM gateway_schema_migrations`).Scan(&count); err != nil || count != 4 {
		t.Fatalf("failed migration advanced ledger: %d %v", count, err)
	}
	if _, err = p1.Exec(ctx, `DROP TRIGGER reject_connection_ledger ON gateway_schema_migrations; DROP FUNCTION reject_connection_ledger()`); err != nil {
		t.Fatal(err)
	}
	if err = migrations.Apply(ctx, p2); err != nil {
		t.Fatal(err)
	}
	assertConnectionFacts(t, before, legacyConnectionFacts(t, ctx, p1))
	t.Log("CONNECTION_UPGRADE_ROLLBACK_VERIFIED: injected post-DDL failure leaves no 0005 table/ledger; 0004 facts preserved; later retry succeeds")
}

// Nullable observation columns do not rewrite historical business facts. Only
// absent/NULL trace columns are equivalent: a non-NULL backfill must still fail.
func legacyFactsWithoutNullTrace(table, snapshot string) (string, error) {
	if table != "gateway_outbox" && table != "gateway_delivery_intents" {
		return snapshot, nil
	}
	var rows []map[string]json.RawMessage
	if err := json.Unmarshal([]byte(snapshot), &rows); err != nil {
		return "", err
	}
	for _, row := range rows {
		for _, key := range []string{"traceparent", "tracestate"} {
			if string(row[key]) == "null" {
				delete(row, key)
			}
		}
	}
	body, err := json.Marshal(rows)
	return string(body), err
}
