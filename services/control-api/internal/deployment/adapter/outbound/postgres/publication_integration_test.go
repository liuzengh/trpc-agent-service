package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/migrations"
)

func TestPublicationTransactionAgainstPostgreSQL(t *testing.T) {
	store, pool := deploymentPostgres(t)
	ctx := context.Background()

	create := createCommitFixture()
	create.Deployment.ID = "dep_1"
	create.Receipt.DeploymentID = "dep_1"
	create.Receipt.Result, _ = json.Marshal(createReceiptResult{Deployment: create.Deployment})
	created, err := store.CreateDeployment(ctx, create)
	if err != nil || !created.Created {
		t.Fatalf("CreateDeployment() = %#v, %v", created, err)
	}
	published, err := store.CommitPublication(ctx, publicationCommitFixture())
	if err != nil || !published.Created {
		t.Fatalf("CommitPublication() = %#v, %v", published, err)
	}
	replayed, err := store.CommitPublication(ctx, publicationCommitFixture())
	if err != nil || replayed.Created || replayed.Receipt.DeploymentRevisionID != "dpr_1" {
		t.Fatalf("CommitPublication() replay = %#v, %v", replayed, err)
	}

	conflict := publicationCommitFixture()
	conflict.Receipt.RequestDigest = "sha256:" + repeatHex("b")
	if _, err := store.CommitPublication(ctx, conflict); !errors.Is(err, application.ErrIdempotencyConflict) {
		t.Fatalf("same key with changed request = %v", err)
	}
	stale := publicationCommitFixture()
	stale.Receipt.KeyHash = "sha256:" + repeatHex("d")
	if _, err := store.CommitPublication(ctx, stale); !errors.Is(err, application.ErrLatestRevisionConflict) {
		t.Fatalf("different key with stale expected latest = %v", err)
	}

	full, err := store.GetPublishedRevision(ctx, "tnt_1", "dep_1", 1)
	if err != nil || full.Revision.ID != "dpr_1" || full.ManifestID != "rmf_1" ||
		full.ManifestDigest != full.Manifest.ContentDigest {
		t.Fatalf("GetPublishedRevision() = %#v, %v", full, err)
	}
	page, err := store.ListRevisionSummaries(
		ctx, "tnt_1", "dep_1", application.Page{Limit: 10},
	)
	if err != nil || page.Total != 1 || len(page.Revisions) != 1 ||
		page.Revisions[0].ManifestID != "rmf_1" {
		t.Fatalf("ListRevisionSummaries() = %#v, %v", page, err)
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM control_outbox WHERE tenant_id='tnt_1' AND id='evt_1'`).Scan(&status); err != nil || status != "PENDING" {
		t.Fatalf("outbox status = %q, %v", status, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE deployment_revisions SET input_digest='sha256:' || repeat('f',64) WHERE tenant_id='tnt_1' AND id='dpr_1'`); err == nil {
		t.Fatal("immutable deployment revision accepted an update")
	}
}

func TestConcurrentSameKeyPublicationReturnsOneWinnerAgainstPostgreSQL(t *testing.T) {
	store, pool := deploymentPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	create := createCommitFixture()
	create.Deployment.ID = "dep_1"
	create.Receipt.DeploymentID = "dep_1"
	create.Receipt.Result, _ = json.Marshal(createReceiptResult{Deployment: create.Deployment})
	if result, err := store.CreateDeployment(ctx, create); err != nil || !result.Created {
		t.Fatalf("CreateDeployment() = %#v, %v", result, err)
	}

	// Hold the owner row before either publication starts. The lock inspection
	// below proves both callers are waiting inside the publication transaction,
	// rather than relying on goroutine scheduling or an arbitrary sleep.
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(context.Background()) }()
	if _, err := blocker.Exec(ctx, `SELECT id FROM deployments WHERE tenant_id='tnt_1' AND id='dep_1' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	type outcome struct {
		result application.PublicationCommitResult
		err    error
	}
	outcomes := make(chan outcome, 2)
	for range 2 {
		go func() {
			<-start
			result, err := store.CommitPublication(ctx, publicationCommitFixture())
			outcomes <- outcome{result: result, err: err}
		}()
	}
	close(start)
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		var waiters int
		err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM pg_stat_activity
			WHERE application_name = $1
			  AND wait_event_type = 'Lock'
			  AND cardinality(pg_blocking_pids(pid)) > 0
			  AND query LIKE '%FROM deployments%'
			  AND query LIKE '%FOR UPDATE%'
		`, pool.Config().ConnConfig.RuntimeParams["application_name"]).Scan(&waiters)
		if err != nil {
			t.Fatalf("inspect publication lock waiters: %v", err)
		}
		select {
		case early := <-outcomes:
			t.Fatalf("publication returned while its owner row remained locked: %#v, %v", early.result, early.err)
		default:
		}
		if waiters == 2 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("two publication lock waiters were not observed: %v", ctx.Err())
		case <-poll.C:
		}
	}
	if err := blocker.Commit(ctx); err != nil {
		t.Fatalf("release publication owner row: %v", err)
	}
	created, replayed := 0, 0
	for range 2 {
		var outcome outcome
		select {
		case outcome = <-outcomes:
		case <-ctx.Done():
			t.Fatalf("publication did not finish after lock release: %v", ctx.Err())
		}
		if outcome.err != nil {
			t.Fatalf("CommitPublication() error = %v", outcome.err)
		}
		if outcome.result.Created {
			created++
		} else {
			replayed++
		}
		if outcome.result.Receipt.DeploymentRevisionID != "dpr_1" ||
			outcome.result.Receipt.RuntimeManifestID != "rmf_1" {
			t.Fatalf("CommitPublication() result = %#v", outcome.result)
		}
	}
	if created != 1 || replayed != 1 {
		t.Fatalf("created=%d replayed=%d, want exactly one of each", created, replayed)
	}
	t.Log("observed two blocked publishers; released owner lock; one created and one replayed")
	for table, want := range map[string]int64{
		"deployment_revisions": 1,
		"runtime_manifests":    1,
		"control_outbox":       1,
	} {
		var count int64
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{table}.Sanitize()).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != want {
			t.Fatalf("%s count = %d, want %d", table, count, want)
		}
	}
}

func TestConcurrentSameKeyCreationReturnsOneWinnerAgainstPostgreSQL(t *testing.T) {
	store, pool := deploymentPostgres(t)
	ctx := context.Background()
	commits := []application.CreateCommit{createCommitFixture(), createCommitFixture()}
	commits[0].Deployment.ID = "dep_a"
	commits[1].Deployment.ID = "dep_b"
	for index := range commits {
		commits[index].Receipt.DeploymentID = commits[index].Deployment.ID
		commits[index].Receipt.Result, _ = json.Marshal(createReceiptResult{
			Deployment: commits[index].Deployment,
		})
	}

	start := make(chan struct{})
	type outcome struct {
		result application.CreateCommitResult
		err    error
	}
	outcomes := make(chan outcome, 2)
	for index := range commits {
		commit := commits[index]
		go func() {
			<-start
			result, err := store.CreateDeployment(ctx, commit)
			outcomes <- outcome{result: result, err: err}
		}()
	}
	close(start)
	created := 0
	winner := ""
	for range 2 {
		outcome := <-outcomes
		if outcome.err != nil {
			t.Fatalf("CreateDeployment() error = %v", outcome.err)
		}
		if outcome.result.Created {
			created++
		}
		if winner == "" {
			winner = outcome.result.Receipt.DeploymentID
		} else if outcome.result.Receipt.DeploymentID != winner {
			t.Fatalf("winner ids differ: %q and %q", winner, outcome.result.Receipt.DeploymentID)
		}
	}
	if created != 1 || (winner != "dep_a" && winner != "dep_b") {
		t.Fatalf("created=%d winner=%q", created, winner)
	}
	for table, want := range map[string]int64{
		"deployments":                 1,
		"deployment_command_receipts": 1,
	} {
		var count int64
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{table}.Sanitize()).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != want {
			t.Fatalf("%s count = %d, want %d", table, count, want)
		}
	}
}

func deploymentPostgres(t *testing.T) (*Store, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("CONTROL_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("CONTROL_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("deployment_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := admin.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		if err != nil {
			t.Errorf("cleanup schema: %v", err)
		}
		admin.Close()
	})
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	config.ConnConfig.RuntimeParams["application_name"] = schema
	// Deterministic lock tests use one blocker, two publishers and one observer.
	if config.MaxConns < 4 {
		config.MaxConns = 4
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	baseline, err := migrations.Files.ReadFile("0001_baseline.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(baseline)); err != nil {
		t.Fatalf("apply isolated baseline: %v", err)
	}
	const setup = `
		INSERT INTO user_accounts(id,username,normalized_username)
		VALUES ('usr_1','user1','user1');
		INSERT INTO tenants(id,slug,name,created_at,updated_at)
		VALUES ('tnt_1','tenant1','Tenant 1',now(),now());
		INSERT INTO agents(tenant_id,id,name,created_by,created_at,updated_at)
		VALUES ('tnt_1','agt_1','Agent','usr_1',now(),now());
		INSERT INTO agent_versions(
			tenant_id,id,agent_id,version_number,source_draft_revision,
			schema_version,spec_jsonb,spec_digest,published_by,published_at
		) VALUES (
			'tnt_1','agv_2','agt_1',2,1,'v1','{}',
			'sha256:' || repeat('a',64),'usr_1',now()
		);
		INSERT INTO runtime_profiles(tenant_id,id,name,created_by,created_at,updated_at)
		VALUES ('tnt_1','rpf_1','Profile','usr_1',now(),now());
		INSERT INTO runtime_profile_revisions(
			tenant_id,id,profile_id,revision_number,source_draft_revision,
			schema_version,spec_jsonb,spec_digest,published_by,published_at
		) VALUES (
			'tnt_1','rpr_3','rpf_1',3,1,'v1','{}',
			'sha256:' || repeat('b',64),'usr_1',now()
		);
	`
	if _, err := pool.Exec(ctx, setup); err != nil {
		t.Fatal(err)
	}
	return NewStore(pool), pool
}

func repeatHex(value string) string {
	result := ""
	for range 64 {
		result += value
	}
	return result
}

func TestPublicationWriteFailuresRollbackEveryRecordAgainstPostgreSQL(t *testing.T) {
	for _, stage := range []struct{ table, operation string }{
		{"deployment_revisions", "INSERT"},
		{"runtime_manifests", "INSERT"},
		{"control_outbox", "INSERT"},
		{"deployment_command_receipts", "INSERT"},
		{"deployments", "UPDATE"},
	} {
		t.Run(stage.table, func(t *testing.T) {
			store, pool := deploymentPostgres(t)
			ctx := context.Background()
			create := createCommitFixture()
			create.Deployment.ID = "dep_1"
			create.Receipt.DeploymentID = "dep_1"
			var err error
			create.Receipt.Result, err = json.Marshal(createReceiptResult{Deployment: create.Deployment})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.CreateDeployment(ctx, create); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, `CREATE FUNCTION fail_publication_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected publication failure'; END $$`); err != nil {
				t.Fatal(err)
			}
			statement := "CREATE TRIGGER injected_failure BEFORE " + stage.operation + " ON " + pgx.Identifier{stage.table}.Sanitize() + " FOR EACH ROW EXECUTE FUNCTION fail_publication_write()"
			if _, err := pool.Exec(ctx, statement); err != nil {
				t.Fatal(err)
			}
			if _, err := store.CommitPublication(ctx, publicationCommitFixture()); err == nil {
				t.Fatal("injected failure unexpectedly committed")
			}
			for _, table := range []string{"deployment_revisions", "runtime_manifests", "control_outbox"} {
				var count int
				if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{table}.Sanitize()).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 0 {
					t.Fatalf("%s retained %d rows after rollback", table, count)
				}
			}
			var receipts int
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM deployment_command_receipts WHERE operation='publish'").Scan(&receipts); err != nil {
				t.Fatal(err)
			}
			if receipts != 0 {
				t.Fatalf("publish receipts = %d after rollback", receipts)
			}
			var latest *int64
			if err := pool.QueryRow(ctx, "SELECT latest_revision_number FROM deployments WHERE tenant_id='tnt_1' AND id='dep_1'").Scan(&latest); err != nil {
				t.Fatal(err)
			}
			if latest != nil {
				t.Fatalf("latest advanced after rollback: %d", *latest)
			}
			if _, err := pool.Exec(ctx, "DROP TRIGGER injected_failure ON "+pgx.Identifier{stage.table}.Sanitize()); err != nil {
				t.Fatal(err)
			}
			if result, err := store.CommitPublication(ctx, publicationCommitFixture()); err != nil || !result.Created {
				t.Fatalf("retry after failure removal = %#v, %v", result, err)
			}
		})
	}
}

func TestPublicationDatabaseImmutabilityAgainstPostgreSQL(t *testing.T) {
	store, pool := deploymentPostgres(t)
	ctx := context.Background()
	create := createCommitFixture()
	create.Deployment.ID = "dep_1"
	create.Receipt.DeploymentID = "dep_1"
	var err error
	create.Receipt.Result, err = json.Marshal(createReceiptResult{Deployment: create.Deployment})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateDeployment(ctx, create); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CommitPublication(ctx, publicationCommitFixture()); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"deployment_revisions", "runtime_manifests", "deployment_command_receipts"} {
		for _, operation := range []string{"UPDATE " + pgx.Identifier{table}.Sanitize() + " SET tenant_id=tenant_id", "DELETE FROM " + pgx.Identifier{table}.Sanitize()} {
			if _, err := pool.Exec(ctx, operation); err == nil {
				t.Fatalf("immutable table accepted %s", operation)
			}
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE control_outbox SET payload_jsonb='{}'`); err == nil {
		t.Fatal("outbox payload mutation was accepted")
	}
	if _, err := pool.Exec(ctx, `UPDATE control_outbox SET status='IN_FLIGHT', attempt_count=1, claimed_by='relay', claimed_until=now()+interval '1 minute'`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE control_outbox SET status='PUBLISHED', published_at=now(), claimed_by=NULL, claimed_until=NULL`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE control_outbox SET updated_at=now()`); err == nil {
		t.Fatal("published outbox record accepted an update")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM control_outbox`); err != nil {
		t.Fatal(err)
	}
	replay, err := store.CommitPublication(ctx, publicationCommitFixture())
	if err != nil || replay.Created || replay.Receipt.DeploymentRevisionID != "dpr_1" {
		t.Fatalf("receipt replay after outbox cleanup = %#v, %v", replay, err)
	}
}

func TestConcurrentDifferentKeyPublicationHasOneCASWinnerAgainstPostgreSQL(t *testing.T) {
	store, pool := deploymentPostgres(t)
	ctx := context.Background()
	create := createCommitFixture()
	create.Deployment.ID = "dep_1"
	create.Receipt.DeploymentID = "dep_1"
	var err error
	create.Receipt.Result, err = json.Marshal(createReceiptResult{Deployment: create.Deployment})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateDeployment(ctx, create); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errorsFound := make(chan error, 2)
	for _, key := range []string{"a", "d"} {
		commit := publicationCommitFixture()
		commit.Receipt.KeyHash = "sha256:" + repeatHex(key)
		go func() {
			<-start
			_, err := store.CommitPublication(ctx, commit)
			errorsFound <- err
		}()
	}
	close(start)
	winners, conflicts := 0, 0
	for range 2 {
		err := <-errorsFound
		switch {
		case err == nil:
			winners++
		case errors.Is(err, application.ErrLatestRevisionConflict):
			conflicts++
		default:
			t.Fatalf("concurrent publication error = %v", err)
		}
	}
	if winners != 1 || conflicts != 1 {
		t.Fatalf("winners=%d conflicts=%d", winners, conflicts)
	}
	var revisions, receipts int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM deployment_revisions").Scan(&revisions); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM deployment_command_receipts WHERE operation='publish'").Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if revisions != 1 || receipts != 1 {
		t.Fatalf("revisions=%d publish receipts=%d", revisions, receipts)
	}
}

func TestRevisionWithoutManifestIsIntegrityFailureAgainstPostgreSQL(t *testing.T) {
	store, pool := deploymentPostgres(t)
	ctx := context.Background()
	create := createCommitFixture()
	create.Deployment.ID = "dep_1"
	create.Receipt.DeploymentID = "dep_1"
	create.Receipt.Result, _ = json.Marshal(createReceiptResult{Deployment: create.Deployment})
	if _, err := store.CreateDeployment(ctx, create); err != nil {
		t.Fatal(err)
	}
	// Intentionally insert only the revision to model a corrupted historical
	// record. Production CommitPublication always inserts the Manifest too.
	if err := insertDeploymentRevision(ctx, pool, publicationCommitFixture().Revision); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetPublishedRevision(ctx, "tnt_1", "dep_1", 1); !errors.Is(err, application.ErrPublicationIntegrity) {
		t.Fatalf("GetPublishedRevision() = %v, want integrity failure", err)
	}
	if _, err := store.ListRevisionSummaries(ctx, "tnt_1", "dep_1", application.Page{Limit: 10}); !errors.Is(err, application.ErrPublicationIntegrity) {
		t.Fatalf("ListRevisionSummaries() = %v, want integrity failure", err)
	}
	if _, err := store.GetPublishedRevision(ctx, "tnt_1", "dep_1", 2); !errors.Is(err, application.ErrDeploymentRevisionNotFound) {
		t.Fatalf("truly absent revision = %v, want not found", err)
	}
}
