package postgresadapter

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	channelv1 "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
	"github.com/liuzengh/trpc-agent-service/services/control-api/migrations"
)

func receiveModeLedger(t *testing.T, pool *pgxpool.Pool) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, table := range []string{"channel_account_credentials", "channel_bindings", "channel_account_route_states", "channel_command_receipts", "control_outbox"} {
		var d string
		if err := pool.QueryRow(context.Background(), `SELECT md5(COALESCE(string_agg(to_jsonb(r)::text,',' ORDER BY to_jsonb(r)::text),'')) FROM `+pgx.Identifier{table}.Sanitize()+` r`).Scan(&d); err != nil {
			t.Fatal(err)
		}
		out[table] = d
	}
	return out
}
func TestReceiveModeMigrationBackfillsAndPreservesLedger(t *testing.T) {
	ctx := context.Background()
	store, service, pool := channelPG(t, true)
	a := createAccount(t, service)
	_, err := pool.Exec(ctx, `UPDATE channel_accounts SET config_jsonb=config_jsonb-'receive_mode'; INSERT INTO channel_account_observations(tenant_id,account_id,scope_id,source_epoch,provider,connection_revision,instance_id,instance_epoch,report_sequence,state,reason_code,observed_at,observation_digest) SELECT tenant_id,id,scope_id,$1,'telegram',1,'gw-1',$1,1,'DISABLED','NONE',now(),'sha256:'||repeat('a',64) FROM channel_accounts`, pgx.QueryExecModeSimpleProtocol, testEpoch)
	if err != nil {
		t.Fatal(err)
	}
	before := receiveModeLedger(t, pool)
	var revision int64
	if err = pool.QueryRow(ctx, `SELECT snapshot_revision FROM channel_account_catalog WHERE scope_id=$1`, testScope).Scan(&revision); err != nil {
		t.Fatal(err)
	}
	raw, err := migrations.Files.ReadFile("0003_telegram_receive_modes.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(raw)); err != nil {
		t.Fatal(err)
	}
	rollout, err := migrations.Files.ReadFile("0009_channel_binding_traffic_rollout.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(rollout)); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetAccount(ctx, "tnt_a", a.Account.ID)
	if err != nil || got.Account.Config.ReceiveMode != domain.Webhook || got.Account.Revision != 2 || got.Account.ConnectionRevision != 2 {
		t.Fatal("old account not explicitly webhook with CAS advanced", err)
	}
	snap, err := store.ReadSnapshot(ctx, testScope)
	if err != nil || snap.Revision != revision+1 || snap.Accounts[0].Config.ReceiveMode != domain.Webhook {
		t.Fatal("catalog backfill", err)
	}
	if !reflect.DeepEqual(before, receiveModeLedger(t, pool)) {
		t.Fatal("migration changed credentials, receipts, routes or outbox")
	}
	var mode, digest string
	if err = pool.QueryRow(ctx, `SELECT receive_mode,observation_digest FROM channel_account_observations`).Scan(&mode, &digest); err != nil || mode != domain.Webhook || digest != "sha256:"+strings.Repeat("a", 64) {
		t.Fatal("legacy observation altered", err)
	}
}
func TestReceiveModeMigrationDuplicateBotFailsAtomically(t *testing.T) {
	ctx := context.Background()
	_, service, pool := channelPG(t, true)
	createAccount(t, service)
	_, err := pool.Exec(ctx, `UPDATE channel_accounts SET config_jsonb=config_jsonb-'receive_mode'; INSERT INTO channel_account_catalog(scope_id,source_epoch,account_count) VALUES('other_pool',$1,1); INSERT INTO channel_accounts SELECT tenant_id,'cha_duplicate','other_pool',provider,provider_account_id,name,description,account_revision,connection_revision,min_route_generation,enabled,'{"webhook_path":"/v1/telegram/cha_duplicate"}',created_by,created_at,updated_at FROM channel_accounts LIMIT 1`, pgx.QueryExecModeSimpleProtocol, testEpoch)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := migrations.Files.ReadFile("0003_telegram_receive_modes.sql")
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, string(raw))
	if err == nil {
		t.Fatal("duplicate Bot selected an implicit winner")
	}
	_ = tx.Rollback(ctx)
	var unchanged bool
	if err = pool.QueryRow(ctx, `SELECT count(*)=2 AND bool_and(NOT(config_jsonb?'receive_mode') AND account_revision=1 AND connection_revision=1) FROM channel_accounts`).Scan(&unchanged); err != nil || !unchanged {
		t.Fatal("failed migration partly backfilled", err)
	}
	var columnCount int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='channel_account_observations' AND column_name='receive_mode'`).Scan(&columnCount); err != nil || columnCount != 0 {
		t.Fatal("failed migration left schema change", err)
	}
}
func TestPollingOptionalSecretReceiverAndObservation(t *testing.T) {
	ctx := context.Background()
	store, service, pool := channelPG(t)
	in := accountInput()
	in.Config = nil
	delete(in.Credentials, domain.TelegramWebhookSecret)
	created, err := service.CreateAccount(ctx, testActor, "lp", in)
	if err != nil {
		t.Fatal(err)
	}
	id := created.Account.ID
	enabled, err := service.SetAccountEnabled(ctx, testActor, id, "lp-enable", application.AccountEnabledInput{ExpectedAccountRevision: 1, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	runtime, p := runtimeService(t, store)
	p.Consumers = append(p.Consumers, "telegram_receiver")
	req := registrationRequest(t, store)
	// Registration cannot resolve a polling account even when the BotToken exists.
	if _, err = runtime.ResolveCredentials(ctx, p, "tnt_a", id, req); err == nil {
		t.Fatal("polling registration credential leak")
	}
	req.Uses = req.Uses[:0]
	a, err := store.GetAccount(ctx, "tnt_a", id)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range a.Credentials {
		if c.Meta.Purpose == domain.TelegramBotToken {
			req.Uses = append(req.Uses, domain.CredentialUse{Purpose: c.Meta.Purpose, ID: c.Meta.ID, Version: c.Meta.Version})
		}
	}
	owner := int64(7)
	req.Consumer = domain.Consumer{Kind: "telegram_receiver", InstanceID: "gw-1", OwnerEpoch: &owner}
	resolved, err := runtime.ResolveCredentials(ctx, p, "tnt_a", id, req)
	if err != nil || len(resolved.Values) != 1 {
		t.Fatal("receiver BotToken-only resolve", err)
	}
	req.Consumer.OwnerEpoch = nil
	if _, err = runtime.ResolveCredentials(ctx, p, "tnt_a", id, req); err == nil {
		t.Fatal("receiver accepted absent epoch")
	}
	observation := application.Observation{ScopeID: testScope, SourceEpoch: testEpoch, TenantID: "tnt_a", AccountID: id, Provider: domain.Telegram, ReceiveMode: domain.LongPolling, ConnectionRevision: enabled.Account.ConnectionRevision, InstanceID: "gw-1", InstanceEpoch: testEpoch, ReportSequence: 1, State: "READY", ReasonCode: "NONE", ObservedAt: time.Now().UTC()}
	report := application.ObservationsRequest{SchemaVersion: 1, Observations: []application.Observation{observation}}
	if err = runtime.ReportObservations(ctx, p, report); err == nil {
		t.Fatal("polling READY without owner")
	}
	report.Observations[0].OwnerEpoch = &owner
	if err = runtime.ReportObservations(ctx, p, report); err != nil {
		t.Fatal(err)
	}
	report.Observations[0].ReceiveMode = domain.Webhook
	report.Observations[0].ReportSequence = 2
	if err = runtime.ReportObservations(ctx, p, report); err == nil {
		t.Fatal("wrong mode report accepted")
	}
	var mode string
	var epoch int64
	if err = pool.QueryRow(ctx, `SELECT receive_mode,owner_epoch FROM channel_account_observations`).Scan(&mode, &epoch); err != nil || mode != domain.LongPolling || epoch != 7 {
		t.Fatal("observation identity", err)
	}
}
func TestPollingPreflightStorageReleasesOnlyIrrelevantOrigin(t *testing.T) {
	store, service, _ := preflightPG(t)
	in := accountInput()
	in.Config = &application.AccountConfigInput{ReceiveMode: domain.LongPolling}
	a, err := service.CreateAccount(context.Background(), testActor, "lp", in)
	if err != nil {
		t.Fatal(err)
	}
	r := insertPreflight(t, store, a.Account.ID, "cpf_polling_store", time.Minute)
	// The manually seeded historical row is immutable in policy; seed new policy
	// directly before loading to model a newly-created, mode-aware task.
	r.View.ReceiveMode = domain.LongPolling
	r.View.DiagnosticPolicy = channelv1.PreflightReceiveModesPolicy
	raw, _ := json.Marshal(r)
	if _, err = store.base.db.Exec(context.Background(), `UPDATE channel_preflights SET record_jsonb=$1 WHERE id=$2`, raw, r.View.PreflightID); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Candidate(context.Background(), testScope); err != nil || found {
		t.Fatal("legacy policy selected new task", err)
	}
	if _, found, err := store.Candidate(context.Background(), testScope, channelv1.PreflightReceiveModesPolicy); err != nil || !found {
		t.Fatal("new policy missed task", err)
	}
	modifyStoredPreflight(t, store, r, func(x *application.PreflightRecord) {
		markPreflightRunning(x)
		x.GatewayPublicOrigin = x.View.ExpectedPublicOrigin
		x.View.ExpectedPublicOrigin = nil
		x.View.EffectiveConfigDigest, _ = channelv1.PreflightEffectiveConfigDigest(x.ScopeID, x.SourceEpoch, x.View.ReceiveMode, x.View.ConnectionRevision, x.GatewayPublicOrigin, x.OriginStatus)
	})
	modifyStoredPreflight(t, store, r, func(x *application.PreflightRecord) {
		x.LeaseEpoch = 2
		x.GatewayPublicOrigin = nil
		x.OriginStatus = "PUBLIC_ORIGIN_INVALID"
		digest, _ := channelv1.PreflightConfigDigest(x.ScopeID, x.SourceEpoch, nil, x.OriginStatus)
		x.View.GatewayConfigDigest = &digest
	})
}
