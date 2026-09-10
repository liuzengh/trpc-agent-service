package postgres

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	agentpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/agentapp/postgres"
	deliverypostgres "github.com/liuzengh/trpc-agent-service/trpcservice/channels/delivery/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	tenantpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/tenant/postgres"
)

func TestConfigPublishCASRollbackAndImmutabilityPostgreSQL16(t *testing.T) {
	db := openContractDB(t)
	ctx := context.Background()
	const tenantID = "t_01ARZ3NDEKTSV4RRFFQ69G5FAF"
	const appID = "app_01ARZ3NDEKTSV4RRFFQ69G5FAF"
	metadata := tenant.ChangeMetadata{ActorType: "test", ActorID: "config", ReasonCode: "contract", CorrelationID: "contract", TraceID: "contract"}
	tenantRepository := tenantpostgres.New(db)
	created, err := tenantRepository.Create(ctx, tenant.CreateInput{Tenant: tenant.Tenant{TenantID: tenantID, TenantKey: "config-contract", DisplayName: "Config Contract"}, ChangeMetadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO model_profile(tenant_id,model_profile_id,profile_key,display_name,status) VALUES($1,'model','config-model','Config Model','active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO model_profile_revision(tenant_id,model_profile_id,profile_version,schema_version,provider,model_name,content_digest) VALUES($1,'model',1,1,'contract','contract',repeat('a',64))`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE model_profile SET current_version=1 WHERE tenant_id=$1 AND model_profile_id='model'`, tenantID); err != nil {
		t.Fatal(err)
	}
	appRepository := agentpostgres.New(db)
	appMetadata := agentapp.ChangeMetadata{ActorType: "test", ActorID: "config", Reason: "contract", CorrelationID: "contract", TraceID: "contract"}
	app, err := appRepository.Create(ctx, agentapp.CreateInput{App: agentapp.AgentApp{TenantID: tenantID, AgentAppID: appID, AgentAppKey: "config-contract", DisplayName: "Config Contract"}, ChangeMetadata: appMetadata})
	if err != nil {
		t.Fatal(err)
	}
	draft, err := appRepository.CreateDraft(ctx, agentapp.CreateDraftInput{TenantID: tenantID, AgentAppID: appID, ExpectedAppVersion: app.Version, Revision: agentapp.Revision{AgentKind: "llm", Instruction: "contract", ModelProfileID: "model", ModelProfileVersion: 1}, ChangeMetadata: appMetadata})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := appRepository.Publish(ctx, agentapp.PublishInput{TenantID: tenantID, AgentAppID: appID, Revision: draft.Revision, ExpectedAppVersion: 2, ExpectedDraftVersion: 1, ChangeMetadata: appMetadata}); err != nil {
		t.Fatal(err)
	}
	repository := New(db, tenantRepository)
	payload := func(policy int64) config.ConfigV1 {
		return config.ConfigV1{SchemaVersion: 1, DefaultAgentAppID: appID, PolicyVersion: policy, ChannelBindings: []config.ChannelBinding{{
			BindingID: "feishu-main", Channel: "feishu", ExternalAccountID: "cli_contract", AgentAppID: appID,
			SecretRef: secrets.SecretRef{Ref: "verify", Version: 1}, SendSecretRef: secrets.SecretRef{Ref: "send", Version: 2},
		}}}
	}
	first, err := repository.Publish(ctx, config.PublishInput{TenantID: tenantID, ExpectedTenantVersion: created.Version, Payload: payload(1), Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	var sendRef string
	var sendVersion int64
	if err := db.QueryRowContext(ctx, `SELECT send_secret_ref,send_secret_version FROM channel_binding
WHERE tenant_id=$1 AND config_version=$2 AND binding_id='feishu-main'`, tenantID, first.Snapshot.ConfigVersion).Scan(&sendRef, &sendVersion); err != nil || sendRef != "send" || sendVersion != 2 {
		t.Fatalf("send credential ref=%q version=%d err=%v", sendRef, sendVersion, err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO channel_binding(tenant_id,config_version,binding_id,channel,external_account_id,agent_app_id,secret_ref,secret_version,send_secret_ref,send_secret_version)
VALUES($1,$2,'bypass','wecom','corp-bypass',$3,'verify',1,'forged',1)`, tenantID, first.Snapshot.ConfigVersion, appID); sqlState(err) != "23514" {
		t.Fatalf("direct send credential bypass err=%v state=%s", err, sqlState(err))
	}
	deliveryCatalog, err := deliverypostgres.New(db, &feishu.Adapter{}, &wecom.Adapter{})
	if err != nil {
		t.Fatal(err)
	}
	destinations, err := deliveryCatalog.ListDeliveryDestinations(ctx)
	if err != nil || len(destinations) != 1 || destinations[0].TenantID != tenantID || destinations[0].ChannelBindingID != "feishu-main" || destinations[0].ConfigVersion != 0 {
		t.Fatalf("delivery destinations=%#v err=%v", destinations, err)
	}
	adapter, err := deliveryCatalog.ResolveVersionedAdapter(ctx, tenantID, "feishu-main", first.Snapshot.ConfigVersion)
	if err != nil || adapter.ID() != "feishu" {
		t.Fatalf("delivery adapter=%T err=%v", adapter, err)
	}
	results := make(chan error, 2)
	var wait sync.WaitGroup
	start := make(chan struct{})
	for policy := int64(2); policy <= 3; policy++ {
		policy := policy
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, publishErr := repository.Publish(ctx, config.PublishInput{TenantID: tenantID, ExpectedTenantVersion: first.Tenant.Version, Payload: payload(policy), Metadata: metadata})
			results <- publishErr
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	successes, conflicts := 0, 0
	for publishErr := range results {
		switch {
		case publishErr == nil:
			successes++
		case errors.Is(publishErr, config.ErrVersionConflict):
			conflicts++
		default:
			t.Fatalf("publish err=%v", publishErr)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}
	currentTenant, err := tenantRepository.Get(ctx, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	currentSnapshot, err := repository.GetCurrent(ctx, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Publish(ctx, config.PublishInput{TenantID: tenantID, ExpectedTenantVersion: first.Tenant.Version, Payload: payload(9), Metadata: metadata}); !errors.Is(err, config.ErrVersionConflict) {
		t.Fatalf("stale publish=%v", err)
	}
	unchanged, err := repository.GetCurrent(ctx, tenantID)
	if err != nil || unchanged.ConfigVersion != currentSnapshot.ConfigVersion {
		t.Fatalf("failed publish moved current=%#v err=%v", unchanged, err)
	}
	rolled, err := repository.Rollback(ctx, config.RollbackInput{TenantID: tenantID, ExpectedTenantVersion: currentTenant.Version, TargetVersion: first.Snapshot.ConfigVersion, Metadata: metadata})
	if err != nil || rolled.Snapshot.ConfigVersion <= currentSnapshot.ConfigVersion || rolled.Snapshot.Payload.PolicyVersion != 1 {
		t.Fatalf("rollback=%#v err=%v", rolled, err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE config_snapshot SET payload='{}'::jsonb WHERE tenant_id=$1 AND config_version=$2`, tenantID, rolled.Snapshot.ConfigVersion); sqlState(err) != "55000" {
		t.Fatalf("immutable update err=%v state=%s", err, sqlState(err))
	}
	candidate, err := repository.Stage(ctx, config.StageInput{TenantID: tenantID, ExpectedTenantVersion: rolled.Tenant.Version, Payload: payload(4), Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	unchangedTenant, err := tenantRepository.Get(ctx, tenantID)
	if err != nil || unchangedTenant.ActiveConfigVersion != rolled.Snapshot.ConfigVersion || unchangedTenant.Version != rolled.Tenant.Version {
		t.Fatalf("stage changed tenant=%#v err=%v", unchangedTenant, err)
	}
	release, err := repository.CreateRelease(ctx, config.ReleaseCreateInput{ReleaseID: "release-contract", Percentage: 100, Salt: "0123456789abcdef", Metadata: metadata, Targets: []config.ReleaseTarget{{
		TenantID: tenantID, BaselineConfigVersion: rolled.Snapshot.ConfigVersion, CandidateConfigVersion: candidate.ConfigVersion,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	selected, err := repository.SelectEffective(ctx, tenantID, rolled.Snapshot.ConfigVersion)
	if err != nil || selected.ConfigVersion != candidate.ConfigVersion {
		t.Fatalf("release selected=%#v err=%v", selected, err)
	}
	if _, err = repository.RollbackRelease(ctx, config.ReleaseRollbackInput{ReleaseID: release.ReleaseID, ExpectedVersion: release.Version, Metadata: metadata}); err != nil {
		t.Fatal(err)
	}
	selected, err = repository.SelectEffective(ctx, tenantID, rolled.Snapshot.ConfigVersion)
	if err != nil || selected.ConfigVersion != rolled.Snapshot.ConfigVersion {
		t.Fatalf("rollback selected=%#v err=%v", selected, err)
	}
}

func openContractDB(t *testing.T) *sql.DB {
	t.Helper()
	if os.Getenv("TRPC_MIGRATION_TEST") != "1" {
		t.Skip("requires explicit disposable PostgreSQL migration test")
	}
	dsn := os.Getenv("TRPC_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("TRPC_POSTGRES_TEST_DSN is not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var databaseName string
	var serverMajor int
	if err := db.QueryRowContext(context.Background(), `SELECT current_database(),current_setting('server_version_num')::int/10000`).Scan(&databaseName, &serverMajor); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(databaseName, "trpc_agent_service_test_") || serverMajor != 16 {
		t.Fatalf("refusing database=%q PostgreSQL=%d", databaseName, serverMajor)
	}
	return db
}

func sqlState(err error) string {
	type sqlStater interface{ SQLState() string }
	var state sqlStater
	if errors.As(err, &state) {
		return state.SQLState()
	}
	return ""
}
