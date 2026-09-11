package postgresadapter

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/adapter/outbound/credentialcrypto"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
	tenantpostgres "github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/control-api/migrations"
)

const testScope = "gateway_pool"
const testEpoch = "00000000-0000-4000-8000-000000000001"

var testActor = application.Actor{TenantID: "tnt_a", UserID: "usr_owner"}

type allowOwner struct{}

func (allowOwner) IsActiveMember(context.Context, string, string) (bool, error) { return true, nil }
func (allowOwner) IsActiveOwner(context.Context, string, string) (bool, error)  { return true, nil }

// Seeded targets below are persistence fixtures, not executable manifests. The
// separate HTTP integration exercises real publication and owner integrity reads.
type targetFixture struct{}

func (targetFixture) ReadExact(_ context.Context, tenant, actor string, s domain.TargetSelector) (domain.PublishedTarget, error) {
	if tenant != "tnt_a" || s.RevisionNumber != 1 || (s.DeploymentID != "dpl_a" && s.DeploymentID != "dpl_b") {
		return domain.PublishedTarget{}, application.ErrTargetNotFound
	}
	return domain.PublishedTarget{TenantID: tenant, DeploymentID: s.DeploymentID, RevisionNumber: 1, DeploymentRevisionID: "dpr_" + s.DeploymentID, ManifestID: "rmf_" + s.DeploymentID, ManifestDigest: "sha256:" + strings.Repeat("a", 64)}, nil
}
func channelPG(t *testing.T, baselineOnly ...bool) (*Store, *application.Service, *pgxpool.Pool) {
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
	nonce := make([]byte, 6)
	if _, err = rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	schema := "channel_test_" + hex.EncodeToString(nonce)
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := admin.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		if err != nil {
			t.Error(err)
		}
		admin.Close()
	})
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	config.ConnConfig.RuntimeParams["application_name"] = schema
	config.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	raw, err := migrations.Files.ReadFile("0001_baseline.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(raw)); err != nil {
		t.Fatalf("apply Channel baseline: %v", err)
	}
	if len(baselineOnly) == 0 || !baselineOnly[0] {
		upgrade, err := migrations.Files.ReadFile("0003_telegram_receive_modes.sql")
		if err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, string(upgrade)); err != nil {
			t.Fatal("apply receive modes upgrade", err)
		}
		rollout, err := migrations.Files.ReadFile("0009_channel_binding_traffic_rollout.sql")
		if err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, string(rollout)); err != nil {
			t.Fatal("apply traffic rollout upgrade", err)
		}
	}
	if _, err = pool.Exec(ctx, `INSERT INTO user_accounts(id,username,normalized_username) VALUES('usr_owner','owner','owner'),('usr_other','other','other');
 INSERT INTO tenants(id,slug,name,created_at,updated_at) VALUES('tnt_a','a','A',now(),now()),('tnt_b','b','B',now(),now());
 INSERT INTO tenant_memberships(id,tenant_id,user_id,role,created_by,created_at) VALUES('mem_a','tnt_a','usr_owner','OWNER','usr_owner',now()),('mem_b','tnt_b','usr_other','OWNER','usr_other',now());`); err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(pool, tenantpostgres.TransactionAuthorizer{}, Options{ScopeID: testScope, SourceEpoch: testEpoch})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.EnsureCatalog(ctx); err != nil {
		t.Fatal(err)
	}
	keyring, err := credentialcrypto.New("k1", map[string]credentialcrypto.Key{"k1": {Encryption: bytes.Repeat([]byte{1}, 32), MAC: bytes.Repeat([]byte{2}, 32)}})
	if err != nil {
		t.Fatal(err)
	}
	var sequence atomic.Int64
	service, err := application.NewService(application.Dependencies{Commands: store, Queries: store, TenantAccess: allowOwner{}, Targets: targetFixture{}, Cipher: keyring, ScopeID: testScope, NewID: func(prefix string) (string, error) { return fmt.Sprintf("%s_%d", prefix, sequence.Add(1)), nil }, Now: func() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }})
	if err != nil {
		t.Fatal(err)
	}
	return store, service, pool
}
func accountInput() application.CreateAccountInput {
	token, secret := "TEST_ONLY_CHANNEL_TOKEN", "TEST_ONLY_WEBHOOK_SECRET"
	return application.CreateAccountInput{Config: &application.AccountConfigInput{ReceiveMode: domain.Webhook}, Provider: domain.Telegram, ProviderAccountID: "123", Name: "test", Credentials: map[string]domain.CredentialEdit{domain.TelegramBotToken: {Action: "replace", Value: &token}, domain.TelegramWebhookSecret: {Action: "replace", Value: &secret}}}
}
func createAccount(t *testing.T, s *application.Service) application.CommandResult {
	t.Helper()
	r, err := s.CreateAccount(context.Background(), testActor, "create", accountInput())
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func seedTargets(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx, `INSERT INTO agents(tenant_id,id,name,created_by,created_at,updated_at) VALUES('tnt_a','agt_a','A','usr_owner',now(),now());
 INSERT INTO agent_versions(tenant_id,id,agent_id,version_number,source_draft_revision,schema_version,spec_jsonb,spec_digest,published_by,published_at) VALUES('tnt_a','agv_a','agt_a',1,1,'v1','{}','sha256:'||repeat('a',64),'usr_owner',now());
 INSERT INTO runtime_profiles(tenant_id,id,name,created_by,created_at,updated_at) VALUES('tnt_a','rpf_a','P','usr_owner',now(),now());
 INSERT INTO runtime_profile_revisions(tenant_id,id,profile_id,revision_number,source_draft_revision,schema_version,spec_jsonb,spec_digest,published_by,published_at) VALUES('tnt_a','rpr_a','rpf_a',1,1,'v1','{}','sha256:'||repeat('a',64),'usr_owner',now());`)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"dpl_a", "dpl_b"} {
		_, err = pool.Exec(ctx, `INSERT INTO deployments(tenant_id,id,name,metadata_revision,latest_revision_number,created_by,created_at,updated_at) VALUES('tnt_a',$1,'D',1,1,'usr_owner',now(),now())`, id)
		if err != nil {
			t.Fatal(err)
		}
		_, err = pool.Exec(ctx, `INSERT INTO deployment_revisions(tenant_id,id,deployment_id,revision_number,schema_version,input_jsonb,input_digest,agent_id,agent_version_id,agent_version_number,agent_schema_version,agent_spec_digest,profile_id,profile_revision_id,profile_revision_number,profile_schema_version,profile_spec_digest,published_by,published_at) VALUES('tnt_a',$1,$2,1,'v1','{}','sha256:'||repeat('a',64),'agt_a','agv_a',1,'v1','sha256:'||repeat('a',64),'rpf_a','rpr_a',1,'v1','sha256:'||repeat('a',64),'usr_owner',now())`, "dpr_"+id, id)
		if err != nil {
			t.Fatal(err)
		}
		_, err = pool.Exec(ctx, `INSERT INTO runtime_manifests(tenant_id,id,deployment_revision_id,schema_version,compiler_version,runtime_contract_version,content_jsonb,content_digest,published_at) VALUES('tnt_a',$1,$2,'v1','test','test','{}','sha256:'||repeat('a',64),now())`, "rmf_"+id, "dpr_"+id)
		if err != nil {
			t.Fatal(err)
		}
	}
}
func TestChannelAccountPersistenceAndSnapshotAgainstPostgreSQL(t *testing.T) {
	store, service, pool := channelPG(t)
	created := createAccount(t, service)
	id := created.Account.ID
	got, err := store.GetAccount(context.Background(), "tnt_a", id)
	if err != nil || got.Account.Revision != 1 || len(got.Credentials) != 2 {
		t.Fatal("read account", err)
	}
	for _, c := range got.Credentials {
		if c.Ciphertext != nil || c.KeyID != "" {
			t.Fatal("ordinary query retrieved ciphertext")
		}
	}
	_, err = store.GetAccount(context.Background(), "tnt_b", id)
	if !errors.Is(err, application.ErrAccountNotFound) {
		t.Fatal("cross-tenant account visible", err)
	}
	snapshot, err := store.ReadSnapshot(context.Background(), testScope)
	if err != nil || snapshot.Revision != 2 || len(snapshot.Accounts) != 1 {
		t.Fatal("snapshot", err)
	}
	raw, _ := json.Marshal(snapshot)
	if _, err = domain.DecodeSnapshot(raw); err != nil {
		t.Fatal("snapshot digest", err)
	}
	if bytes.Contains(raw, []byte("TEST_ONLY")) || bytes.Contains(raw, []byte("ciphertext")) {
		t.Fatal("snapshot leaked credentials")
	}
	var leaked int
	if err = pool.QueryRow(context.Background(), `SELECT count(*) FROM channel_command_receipts WHERE result_jsonb::text LIKE '%TEST_ONLY%' OR result_jsonb::text LIKE '%credential_id%'`).Scan(&leaked); err != nil || leaked != 0 {
		t.Fatal("secret-bearing receipt", err)
	}
	again, err := service.CreateAccount(context.Background(), testActor, "create", accountInput())
	if err != nil || again.Account.ID != id {
		t.Fatal("receipt retry", err)
	}
	snapshot, err = store.ReadSnapshot(context.Background(), testScope)
	if err != nil || snapshot.Revision != 2 {
		t.Fatal("receipt advanced catalog", err)
	}
}
func TestChannelRouteFloorAtomicityAgainstPostgreSQL(t *testing.T) {
	store, service, pool := channelPG(t)
	seedTargets(t, pool)
	ctx := context.Background()
	created := createAccount(t, service)
	id := created.Account.ID
	_, err := service.SetAccountEnabled(ctx, testActor, id, "account-on", application.AccountEnabledInput{ExpectedAccountRevision: 1, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := service.CreateBinding(ctx, testActor, "bind", application.CreateBindingInput{AccountID: id, Target: domain.TargetSelector{DeploymentID: "dpl_a", RevisionNumber: 1}})
	if err != nil {
		t.Fatal("create binding", err)
	}
	_, err = service.SetBindingEnabled(ctx, testActor, binding.Binding.ID, "binding-on", application.BindingEnabledInput{ExpectedBindingRevision: 1, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.SetAccountEnabled(ctx, testActor, id, "account-off", application.AccountEnabledInput{ExpectedAccountRevision: 2, Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.ReadSnapshot(ctx, testScope)
	if err != nil {
		t.Fatal(err)
	}
	moved, err := service.SetBindingTarget(ctx, testActor, binding.Binding.ID, "retarget", application.BindingTargetInput{ExpectedBindingRevision: 2, Target: domain.TargetSelector{DeploymentID: "dpl_b", RevisionNumber: 1}})
	if err != nil || moved.EventID != "" {
		t.Fatal(err)
	}
	after, err := store.ReadSnapshot(ctx, testScope)
	if err != nil || before.Digest != after.Digest {
		t.Fatal("disabled retarget modified account catalog", err)
	}
	restored, err := service.SetAccountEnabled(ctx, testActor, id, "restore", application.AccountEnabledInput{ExpectedAccountRevision: 3, Enabled: true})
	if err != nil || restored.RouteGeneration != 4 || restored.Account.MinRouteGeneration != 4 {
		t.Fatal(err)
	}
	after, err = store.ReadSnapshot(ctx, testScope)
	if err != nil || after.Revision != before.Revision+1 || after.Accounts[0].MinRouteGeneration != 4 {
		t.Fatal("account and route double-advanced catalog", err)
	}
	rows, err := pool.Query(ctx, `SELECT aggregate_revision,schema_version,payload_jsonb,payload_digest,status FROM control_outbox WHERE event_type=$1 ORDER BY aggregate_revision`, domain.RouteEventType)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
		var generation int64
		var schema, digest, status string
		var raw []byte
		if err = rows.Scan(&generation, &schema, &raw, &digest, &status); err != nil {
			t.Fatal(err)
		}
		p, err := domain.ValidateStoredRoute(raw, digest)
		if err != nil || generation != int64(count) || schema != "1" || status != "PENDING" {
			t.Fatal("outbox mapping", err)
		}
		if count == 2 && p.Route.ManifestRef != "rmf_dpl_a" || count == 4 && p.Route.ManifestRef != "rmf_dpl_b" {
			t.Fatal("old target modified")
		}
		if bytes.Contains(raw, []byte("TEST_ONLY")) {
			t.Fatal("secret in Outbox")
		}
	}
	if err = rows.Err(); err != nil || count != 4 {
		t.Fatal("outbox count", count, err)
	}
}

func TestChannelTrafficPolicyPersistsAndProjectsAgainstPostgreSQL(t *testing.T) {
	store, service, pool := channelPG(t)
	seedTargets(t, pool)
	ctx := context.Background()
	created := createAccount(t, service)
	if _, err := service.SetAccountEnabled(ctx, testActor, created.Account.ID, "account-on", application.AccountEnabledInput{ExpectedAccountRevision: 1, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	binding, err := service.CreateBinding(ctx, testActor, "bind", application.CreateBindingInput{AccountID: created.Account.ID, Target: domain.TargetSelector{DeploymentID: "dpl_a", RevisionNumber: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.SetBindingEnabled(ctx, testActor, binding.Binding.ID, "binding-on", application.BindingEnabledInput{ExpectedBindingRevision: 1, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	rollout, err := service.SetBindingTraffic(ctx, testActor, binding.Binding.ID, "traffic", application.BindingTrafficInput{
		ExpectedBindingRevision: 2,
		Target:                  domain.TargetSelector{DeploymentID: "dpl_b", RevisionNumber: 1},
		PercentageBasisPoints:   2750,
		CanarySubjects:          []string{"usr_other", "usr_owner"},
	})
	if err != nil || rollout.Binding.Traffic == nil || rollout.RouteGeneration != 3 {
		t.Fatalf("set traffic: %+v err=%v", rollout, err)
	}

	loaded, err := store.GetBinding(ctx, testActor.TenantID, binding.Binding.ID)
	if err != nil || loaded.Binding == nil || loaded.Binding.Traffic == nil {
		t.Fatalf("load traffic: %+v err=%v", loaded, err)
	}
	if loaded.Binding.Traffic.ID != rollout.Binding.Traffic.ID || loaded.Binding.Traffic.Target.DeploymentRevisionID != "dpr_dpl_b" || loaded.Binding.Traffic.PercentageBasisPoints != 2750 {
		t.Fatalf("stored policy changed: %+v", loaded.Binding.Traffic)
	}

	var raw []byte
	var digest string
	if err = pool.QueryRow(ctx, `SELECT payload_jsonb,payload_digest FROM control_outbox WHERE event_type=$1 AND aggregate_revision=3`, domain.RouteEventType).Scan(&raw, &digest); err != nil {
		t.Fatal(err)
	}
	projection, err := domain.ValidateStoredRoute(raw, digest)
	if err != nil || projection.Route.Traffic == nil || projection.Route.Traffic.Target.ManifestID != "rmf_dpl_b" {
		t.Fatalf("outbox policy: %+v err=%v", projection.Route.Traffic, err)
	}
	if bytes.Contains(raw, []byte("TEST_ONLY")) {
		t.Fatal("route policy leaked account credentials")
	}
}
func TestChannelWriteFailureRollsBackAllRecordsAgainstPostgreSQL(t *testing.T) {
	for _, table := range []string{"channel_accounts", "channel_account_credentials", "channel_account_route_states", "channel_command_receipts", "channel_account_catalog"} {
		t.Run(table, func(t *testing.T) {
			store, service, pool := channelPG(t)
			ctx := context.Background()
			before, err := store.ReadSnapshot(ctx, testScope)
			if err != nil {
				t.Fatal(err)
			}
			_, err = pool.Exec(ctx, `CREATE FUNCTION channel_injected_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected'; END $$;`)
			if err != nil {
				t.Fatal(err)
			}
			_, err = pool.Exec(ctx, "CREATE TRIGGER injected BEFORE INSERT OR UPDATE ON "+pgx.Identifier{table}.Sanitize()+" FOR EACH ROW EXECUTE FUNCTION channel_injected_failure()")
			if err != nil {
				t.Fatal(err)
			}
			_, err = service.CreateAccount(ctx, testActor, "create", accountInput())
			if err == nil {
				t.Fatal("injected failure committed")
			}
			after, err := store.ReadSnapshot(ctx, testScope)
			if err != nil || after.Digest != before.Digest {
				t.Fatal("partial snapshot survived", err)
			}
			var records int
			if err = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM channel_accounts)+(SELECT count(*) FROM channel_account_credentials)+(SELECT count(*) FROM channel_command_receipts)+(SELECT count(*) FROM channel_account_route_states)+(SELECT count(*) FROM control_outbox)`).Scan(&records); err != nil || records != 0 {
				t.Fatal("partial rows survived", records, err)
			}
		})
	}
}
func waitForLock(t *testing.T, pool *pgxpool.Pool, pattern string, count int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var n int
		err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE application_name=current_setting('application_name') AND wait_event_type='Lock' AND query LIKE $1`, pattern).Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		if n >= count {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("transaction did not reach PostgreSQL lock barrier")
		}
	}
}
func TestChannelSameKeyHasOneCommitAgainstPostgreSQL(t *testing.T) {
	store, service, pool := channelPG(t)
	ctx := context.Background()
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(blocker)
	if _, err = blocker.Exec(ctx, `SELECT scope_id FROM channel_account_catalog WHERE scope_id=$1 FOR UPDATE`, testScope); err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		r application.CommandResult
		e error
	}
	done := make(chan outcome, 2)
	for range 2 {
		go func() { r, e := service.CreateAccount(ctx, testActor, "same", accountInput()); done <- outcome{r, e} }()
	}
	waitForLock(t, pool, "%FROM channel_account_catalog%", 2)
	if err = blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	a, b := <-done, <-done
	if a.e != nil || b.e != nil || a.r.Account.ID != b.r.Account.ID {
		t.Fatal("concurrent receipt", a.e, b.e)
	}
	snapshot, err := store.ReadSnapshot(ctx, testScope)
	if err != nil || snapshot.Revision != 2 || len(snapshot.Accounts) != 1 {
		t.Fatal("more than one commit", err)
	}
}
func TestChannelOwnerRevocationAtCommitAgainstPostgreSQL(t *testing.T) {
	_, service, pool := channelPG(t)
	ctx := context.Background()
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(blocker)
	if _, err = blocker.Exec(ctx, `UPDATE tenant_memberships SET role='MEMBER' WHERE tenant_id='tnt_a' AND user_id='usr_owner'`); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := service.CreateAccount(ctx, testActor, "denied", accountInput()); done <- err }()
	waitForLock(t, pool, "%FROM tenant_memberships%", 1)
	if err = blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-done; !errors.Is(err, application.ErrPermissionDenied) {
		t.Fatal("revoked Owner committed", err)
	}
	var count int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM channel_accounts`).Scan(&count); err != nil || count != 0 {
		t.Fatal("revoked operation persisted", err)
	}
}
func TestChannelRepeatableReadSnapshotAgainstPostgreSQL(t *testing.T) {
	store, service, pool := channelPG(t)
	created := createAccount(t, service)
	ctx := context.Background()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	first, err := store.snapshot(ctx, tx)
	if err != nil {
		t.Fatal(err)
	}
	name := "new name"
	_, err = service.UpdateAccount(ctx, testActor, created.Account.ID, "rename", application.UpdateAccountInput{ExpectedAccountRevision: 1, Name: &name})
	if err != nil {
		t.Fatal(err)
	}
	same, err := store.snapshot(ctx, tx)
	if err != nil || same.Digest != first.Digest {
		t.Fatal("torn snapshot transaction", err)
	}
	fresh, err := store.ReadSnapshot(ctx, testScope)
	if err != nil || fresh.Revision != first.Revision+1 || fresh.Accounts[0].AccountRevision != 2 || fresh.Accounts[0].ConnectionRevision != 1 {
		t.Fatal("fresh metadata-only snapshot", err)
	}
}

func runtimeService(t *testing.T, store *Store) (*application.RuntimeService, application.WorkloadPrincipal) {
	t.Helper()
	k, err := credentialcrypto.New("k1", map[string]credentialcrypto.Key{"k1": {Encryption: bytes.Repeat([]byte{1}, 32), MAC: bytes.Repeat([]byte{2}, 32)}})
	if err != nil {
		t.Fatal(err)
	}
	service, err := application.NewRuntimeService(store, k, testScope, testEpoch)
	if err != nil {
		t.Fatal(err)
	}
	return service, application.WorkloadPrincipal{PrincipalID: "spiffe://trpc-agent-service/gateway/gw-1", ScopeID: testScope, InstanceID: "gw-1", Audience: application.WorkloadAudience, Consumers: []string{"telegram_registration", "telegram_webhook", "telegram_delivery"}}
}
func registrationRequest(t *testing.T, store *Store) application.ResolveRequest {
	t.Helper()
	s, err := store.ReadSnapshot(context.Background(), testScope)
	if err != nil || len(s.Accounts) != 1 {
		t.Fatal("registration snapshot", err)
	}
	a := s.Accounts[0]
	uses := []domain.CredentialUse{}
	for _, c := range a.Credentials {
		uses = append(uses, domain.CredentialUse{Purpose: c.Purpose, ID: c.ID, Version: c.Version})
	}
	return application.ResolveRequest{SchemaVersion: 1, ScopeID: testScope, SourceEpoch: testEpoch, ConnectionRevision: a.ConnectionRevision, Uses: uses, Consumer: domain.Consumer{Kind: "telegram_registration", InstanceID: "gw-1"}}
}
func TestChannelCredentialResolveAndRotationAgainstPostgreSQL(t *testing.T) {
	store, service, _ := channelPG(t)
	created := createAccount(t, service)
	ctx := context.Background()
	id := created.Account.ID
	_, err := service.SetAccountEnabled(ctx, testActor, id, "on", application.AccountEnabledInput{ExpectedAccountRevision: 1, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	runtime, p := runtimeService(t, store)
	input := registrationRequest(t, store)
	response, err := runtime.ResolveCredentials(ctx, p, "tnt_a", id, input)
	if err != nil || len(response.Values) != 2 || response.ConnectionRevision != 2 {
		t.Fatal("registration resolve", err)
	}
	if response.Values[0].Value != "TEST_ONLY_CHANNEL_TOKEN" || response.Values[1].Value != "TEST_ONLY_WEBHOOK_SECRET" {
		t.Fatal("wrong credential set")
	}
	_, err = runtime.ResolveCredentials(ctx, p, "tnt_b", id, input)
	if !errors.Is(err, application.ErrAccountNotFound) {
		t.Fatal("cross tenant resolve", err)
	}
	wrong := input
	wrong.Consumer.InstanceID = "gw-other"
	_, err = runtime.ResolveCredentials(ctx, p, "tnt_a", id, wrong)
	if !errors.Is(err, application.ErrWorkloadDenied) {
		t.Fatal("forged instance", err)
	}
	token := "ROTATED_TEST_ONLY_TOKEN"
	rotated, err := service.UpdateCredential(ctx, testActor, id, domain.TelegramBotToken, "rotate", application.UpdateCredentialInput{ExpectedAccountRevision: 2, ExpectedCredentialVersion: 1, CredentialEdit: domain.CredentialEdit{Action: "replace", Value: &token}})
	if err != nil || rotated.Account.ConnectionRevision != 3 {
		t.Fatal("rotate", err)
	}
	_, err = runtime.ResolveCredentials(ctx, p, "tnt_a", id, input)
	var d *domain.Error
	if !errors.As(err, &d) || d.Code != domain.CredentialVersionConflict {
		t.Fatal("old connection resolved", err)
	}
	response, err = runtime.ResolveCredentials(ctx, p, "tnt_a", id, registrationRequest(t, store))
	if err != nil || response.Values[0].Value != token || response.Values[0].Version != 2 || response.Values[1].Version != 1 {
		t.Fatal("mixed revision response", err)
	}
}
func TestChannelResolveSerializesWithDisableAgainstPostgreSQL(t *testing.T) {
	store, service, pool := channelPG(t)
	created := createAccount(t, service)
	ctx := context.Background()
	id := created.Account.ID
	_, err := service.SetAccountEnabled(ctx, testActor, id, "on", application.AccountEnabledInput{ExpectedAccountRevision: 1, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	resolved := make(chan error, 1)
	go func() {
		resolved <- store.WithCredentials(ctx, testScope, "tnt_a", id, func(a domain.Account, records []domain.CredentialRecord, epoch string) error {
			close(entered)
			<-release
			if !a.Enabled || a.ConnectionRevision != 2 || len(records) != 2 {
				return integrity()
			}
			return nil
		})
	}()
	<-entered
	disabled := make(chan error, 1)
	go func() {
		_, err := service.SetAccountEnabled(ctx, testActor, id, "off", application.AccountEnabledInput{ExpectedAccountRevision: 2, Enabled: false})
		disabled <- err
	}()
	waitForLock(t, pool, "%FROM channel_account_catalog%", 1)
	close(release)
	if err = <-resolved; err != nil {
		t.Fatal(err)
	}
	if err = <-disabled; err != nil {
		t.Fatal(err)
	}
	runtime, p := runtimeService(t, store)
	_, err = runtime.ResolveCredentials(ctx, p, "tnt_a", id, registrationRequest(t, store))
	var d *domain.Error
	if !errors.As(err, &d) || d.Code != domain.AccountDisabled {
		t.Fatal("disabled account resolved", err)
	}
}
func TestChannelObservationsUseServerTimeAndSequenceAgainstPostgreSQL(t *testing.T) {
	store, service, pool := channelPG(t)
	created := createAccount(t, service)
	ctx := context.Background()
	id := created.Account.ID
	_, err := service.SetAccountEnabled(ctx, testActor, id, "on", application.AccountEnabledInput{ExpectedAccountRevision: 1, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	runtime, p := runtimeService(t, store)
	o := application.Observation{ScopeID: testScope, SourceEpoch: testEpoch, TenantID: "tnt_a", AccountID: id, Provider: domain.Telegram, ConnectionRevision: 2, InstanceID: "gw-1", InstanceEpoch: testEpoch, ReportSequence: 1, State: "READY", ReasonCode: "NONE", ObservedAt: time.Now().Add(24 * time.Hour).UTC()}
	request := application.ObservationsRequest{SchemaVersion: 1, Observations: []application.Observation{o}}
	if err = runtime.ReportObservations(ctx, p, request); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE channel_account_observations SET received_at=clock_timestamp()-interval '2 minutes'`); err != nil {
		t.Fatal(err)
	}
	if err = runtime.ReportObservations(ctx, p, request); err != nil {
		t.Fatal("same-sequence replay", err)
	}
	details, err := store.ReadAccount(ctx, "tnt_a", id)
	if err != nil || len(details.Observations) != 1 || details.Observations[0].EffectiveState != "STALE" {
		t.Fatal("client future time or replay renewed freshness", err)
	}
	request.Observations[0].State = "ERROR"
	err = runtime.ReportObservations(ctx, p, request)
	var d *domain.Error
	if !errors.As(err, &d) || d.Code != domain.RevisionConflict {
		t.Fatal("same seq different content accepted", err)
	}
	request.Observations[0] = o
	request.Observations[0].ReportSequence = 2
	if err = runtime.ReportObservations(ctx, p, request); err != nil {
		t.Fatal(err)
	}
	request.Observations[0] = o
	if err = runtime.ReportObservations(ctx, p, request); err != nil {
		t.Fatal("older report", err)
	}
	details, err = store.ReadAccount(ctx, "tnt_a", id)
	if err != nil || details.Observations[0].ReportSequence != 2 || details.Observations[0].EffectiveState != "READY" || details.GatewayApplication != "UNKNOWN" {
		t.Fatal("old report replaced newer or READY implied route applied", err)
	}
	request.Observations[0].InstanceEpoch = "00000000-0000-4000-8000-000000000002"
	if err = runtime.ReportObservations(ctx, p, request); err != nil {
		t.Fatal(err)
	}
	details, err = store.ReadAccount(ctx, "tnt_a", id)
	if err != nil || len(details.Observations) != 2 {
		t.Fatal("new startup replaced old startup view", err)
	}
	raw, _ := json.Marshal(details)
	if bytes.Contains(raw, []byte("credential_id")) || bytes.Contains(raw, []byte("TEST_ONLY")) || bytes.Contains(raw, []byte("source_epoch")) {
		t.Fatal("public detail leaked internal material")
	}
}
func TestChannelQuotaAndCredentialNullGuardsAgainstPostgreSQL(t *testing.T) {
	store, service, pool := channelPG(t)
	store.options.MaxTenantAccounts = 1
	created := createAccount(t, service)
	input := accountInput()
	input.ProviderAccountID = "456"
	_, err := service.CreateAccount(context.Background(), testActor, "second", input)
	var d *domain.Error
	if !errors.As(err, &d) || d.Code != "CHANNEL_LIMIT_EXCEEDED" {
		t.Fatal("quota not enforced", err)
	}
	_, err = pool.Exec(context.Background(), `UPDATE channel_account_credentials SET ciphertext=NULL WHERE tenant_id='tnt_a' AND account_id=$1`, created.Account.ID)
	if err == nil {
		t.Fatal("configured credential accepted NULL ciphertext")
	}
	other, err := NewStore(pool, tenantpostgres.TransactionAuthorizer{}, Options{ScopeID: testScope, SourceEpoch: "00000000-0000-4000-8000-000000000002"})
	if err != nil {
		t.Fatal(err)
	}
	if err = other.EnsureCatalog(context.Background()); !errors.Is(err, application.ErrEpochMismatch) {
		t.Fatal("bootstrap silently replaced source epoch", err)
	}
}
