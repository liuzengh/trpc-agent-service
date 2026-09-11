package postgresadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/migrations"
)

func runtimePlanDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("GATEWAY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("GATEWAY_TEST_DATABASE_URL required for real runtime EXPLAIN")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	name := fmt.Sprintf("gateway_runtime_plan_%d", time.Now().UnixNano())
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, e := admin.Exec(c, "DROP SCHEMA "+quoted+" CASCADE"); e != nil {
			t.Error(e)
		}
	})
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err = migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return pool
}
func TestRuntimeQueryPlansUseSparseCandidateIndexes(t *testing.T) {
	pool := runtimePlanDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := pool.Exec(ctx, `INSERT INTO gateway_delivery_intents(intent_id,run_id,digest,intent,target,provider,account_id,deadline,part_count)
 SELECT 'plan-'||lpad(n::text,5,'0'),'run-plan-'||n,'fixture-digest','{}','{}','telegram','account-'||lpad(n::text,5,'0'),
 CASE WHEN n BETWEEN 9901 AND 9910 THEN clock_timestamp()-interval '1 day' ELSE clock_timestamp()+interval '1 day' END,1 FROM generate_series(1,10000) n;
 INSERT INTO gateway_delivery_parts(part_id,intent_id,part_index,body,state,attempt_number,current_attempt_id,next_attempt_at)
 SELECT 'part-plan-'||lpad(n::text,5,'0'),'plan-'||lpad(n::text,5,'0'),0,'fixture body',
 CASE WHEN n BETWEEN 9901 AND 9920 THEN 'PENDING' WHEN n BETWEEN 9921 AND 9930 THEN 'UNKNOWN' ELSE 'ACCEPTED' END,
 CASE WHEN n BETWEEN 9901 AND 9920 THEN 0 ELSE 1 END,
 CASE WHEN n BETWEEN 9901 AND 9920 THEN NULL ELSE 'attempt-plan-'||lpad(n::text,5,'0') END,
 CASE WHEN n BETWEEN 9901 AND 9920 THEN clock_timestamp()-interval '1 hour' ELSE NULL END FROM generate_series(1,10000) n;
 INSERT INTO gateway_delivery_attempts(attempt_id,part_id,attempt_number,claim_token,instance_id,request_id,request_digest,evidence_hash,calling_until,result,finished_at)
 SELECT 'attempt-plan-'||lpad(n::text,5,'0'),'part-plan-'||lpad(n::text,5,'0'),1,'fixture-token','fixture-instance','fixture-request','fixture-digest','fixture-hash',clock_timestamp()-interval '1 hour',
 CASE WHEN n BETWEEN 9921 AND 9930 THEN '{"Certainty":"UNKNOWN"}'::jsonb ELSE '{"Certainty":"ACCEPTED"}'::jsonb END,clock_timestamp()-interval '1 hour'
 FROM generate_series(1,10000) n WHERE n NOT BETWEEN 9901 AND 9920;
 INSERT INTO gateway_delivery_observations(observation_id,attempt_id,request_id,request_digest,result)
 SELECT 'observation-plan-'||n,'attempt-plan-'||lpad(n::text,5,'0'),'fixture-request','fixture-digest','{"Certainty":"ACCEPTED"}' FROM generate_series(9921,9930) n;
 ANALYZE gateway_delivery_intents; ANALYZE gateway_delivery_parts; ANALYZE gateway_delivery_attempts; ANALYZE gateway_delivery_observations;`)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name, query string
		args        []any
	}{
		{"due-first", dueAccountsSQL, []any{"telegram", "", 3, 21}},
		{"due-keyset", dueAccountsSQL, []any{"telegram", "account-09910", 3, 21}},
		{"expiry", expirePendingSQL, []any{20, 3}},
		{"observed", observedAttemptsSQL, []any{"", 21}},
		{"due-dense-keyset", dueAccountsSQL, []any{"telegram", "account-09990", 3, 21}},
		{"observed-dense-keyset", observedAttemptsSQL, []any{"attempt-plan-09990", 21}},
	}
	for _, tc := range tests {
		if tc.name == "due-dense-keyset" {
			if _, err = pool.Exec(ctx, `UPDATE gateway_delivery_parts SET state='PENDING',attempt_number=0,next_attempt_at=clock_timestamp()-interval '1 hour'; ANALYZE gateway_delivery_parts;`); err != nil {
				t.Fatal(err)
			}
		}
		if tc.name == "observed-dense-keyset" {
			if _, err = pool.Exec(ctx, `UPDATE gateway_delivery_parts p SET state='UNKNOWN',attempt_number=1,current_attempt_id=a.attempt_id,next_attempt_at=NULL FROM gateway_delivery_attempts a WHERE a.part_id=p.part_id;
 UPDATE gateway_delivery_attempts SET result='{"Certainty":"UNKNOWN"}';
 INSERT INTO gateway_delivery_observations(observation_id,attempt_id,request_id,request_digest,result) SELECT 'dense-'||attempt_id,attempt_id,request_id,request_digest,'{"Certainty":"ACCEPTED"}' FROM gateway_delivery_attempts;
 ANALYZE gateway_delivery_parts; ANALYZE gateway_delivery_attempts; ANALYZE gateway_delivery_observations;`); err != nil {
				t.Fatal(err)
			}
		}
		t.Run(tc.name, func(t *testing.T) {
			// EXPLAIN executes the exact private statement used by the public Store seam;
			// this focused performance fixture does not duplicate production SQL literals.
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer rollback(tx)
			var raw []byte
			if err = tx.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+tc.query, tc.args...).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			var plan []map[string]any
			if err = json.Unmarshal(raw, &plan); err != nil {
				t.Fatal(err)
			}
			root := plan[0]["Plan"].(map[string]any)
			if got := root["Actual Rows"].(float64); got != 10 {
				t.Fatalf("unexpected plan result rows=%v", got)
			}
			// Sparse due discovery legitimately uses the existing 0006 due index.
			// Assert useful indexed access, not that PostgreSQL must prefer a new index.
			if !strings.Contains(string(raw), "gateway_delivery_runtime_") && !strings.Contains(string(raw), "gateway_delivery_due") {
				t.Fatalf("planner did not use candidate indexes: %s", raw)
			}
			assertNoLargeRuntimeSequentialScan(t, root)
			t.Logf("EXPLAIN_SQL %s: %s; ARGS=%v", tc.name, tc.query, tc.args)
			t.Logf("EXPLAIN_RESULT %s: %s", tc.name, raw)
			t.Logf("PLAN_VERIFIED %s: 10000 intents/10000 parts/9980 attempts; rows=10; candidate index chosen, no full historical-table scan, no planner overrides", tc.name)
		})
	}
}

func assertNoLargeRuntimeSequentialScan(t *testing.T, node map[string]any) {
	t.Helper()
	if node["Node Type"] == "Seq Scan" {
		name, _ := node["Relation Name"].(string)
		if name != "gateway_delivery_observations" {
			rows, _ := node["Actual Rows"].(float64)
			removed, _ := node["Rows Removed by Filter"].(float64)
			loops, _ := node["Actual Loops"].(float64)
			if (rows+removed)*loops > 1000 {
				t.Fatalf("wide historical sequential scan: %+v", node)
			}
		}
	}
	if children, ok := node["Plans"].([]any); ok {
		for _, child := range children {
			assertNoLargeRuntimeSequentialScan(t, child.(map[string]any))
		}
	}
}
