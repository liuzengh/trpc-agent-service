package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress"
	ingresspostgres "github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	gatewaypostgres "github.com/liuzengh/trpc-agent-service/trpcservice/gateway/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
	preprocesspostgres "github.com/liuzengh/trpc-agent-service/trpcservice/preprocess/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	secretmemory "github.com/liuzengh/trpc-agent-service/trpcservice/secrets/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	messagingpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/postgres"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/session/contracttest"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestAtomicSessionStoreContractPostgreSQL16(t *testing.T) {
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

	contracttest.Run(t, func(tb testing.TB, key sessionstore.SessionKey, prepared map[string]uint64) sessionstore.AtomicSessionStore {
		tb.Helper()
		prepareContractFixture(tb, db, key, prepared)
		return New(db)
	})
}

func TestClaimInboxPrepareDispatchPostgreSQL16(t *testing.T) {
	db := openTestDB(t)
	key := sessionstore.SessionKey{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAV", AgentAppID: "app_01ARZ3NDEKTSV4RRFFQ69G5FAV", SessionID: "prepare-session"}
	prepareContractFixture(t, db, key, map[string]uint64{})
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `UPDATE tenant SET status='active',version=version+1 WHERE tenant_id=$1 AND status='suspended'`, key.TenantID); err != nil {
		t.Fatal(err)
	}
	var tenantVersion int64
	if err := db.QueryRowContext(ctx, `UPDATE tenant SET active_config_version=1,default_agent_app_id=$2,version=version+1
WHERE tenant_id=$1 AND active_config_version IS NULL RETURNING version`, key.TenantID, key.AgentAppID).Scan(&tenantVersion); err != nil {
		if err != sql.ErrNoRows {
			t.Fatal(err)
		}
		if err := db.QueryRowContext(ctx, `SELECT version FROM tenant WHERE tenant_id=$1`, key.TenantID).Scan(&tenantVersion); err != nil {
			t.Fatal(err)
		}
	}
	inboxes := messagingpostgres.New(db)
	inboxKey := messaging.InboxKey{TenantID: key.TenantID, Channel: "fake", ExternalAccountID: "contract-account", ExternalMessageID: "prepare-message"}
	claim := messaging.ClaimInboxRequest{InboxKey: inboxKey, AgentAppID: key.AgentAppID, SessionID: key.SessionID,
		ExternalChatID: "contract-chat", ExternalUserID: "contract-user", PayloadDigest: strings.Repeat("d", 64),
		KeyVersion: 1, InitialState: messaging.InboxDispatchPending}
	first, err := inboxes.ClaimInbox(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	again, err := inboxes.ClaimInbox(ctx, claim)
	if err != nil || again.RequestID != first.RequestID {
		t.Fatalf("duplicate claim=%#v err=%v", again, err)
	}
	claim.PayloadDigest = strings.Repeat("e", 64)
	if _, err := inboxes.ClaimInbox(ctx, claim); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("digest collision=%v", err)
	}
	tasks := gatewaypostgres.NewTaskStore(db)
	request := gateway.PrepareDispatchRequest{
		Tenant:    tenant.Context{TenantID: key.TenantID, TenantVersion: tenantVersion, AgentAppID: key.AgentAppID, SubjectID: "user", Channel: "fake", TrustedSource: "channel_binding:contract"},
		Binding:   tenant.ExecutionBinding{AgentAppVersion: 2, AgentAppRevision: 1, AgentContentDigest: strings.Repeat("a", 64), ConfigVersion: 1, PolicyVersion: 1},
		RequestID: first.RequestID, SessionID: key.SessionID, UserID: "user", PayloadRef: first.PayloadRef,
	}
	prepared, err := tasks.PrepareDispatch(ctx, request)
	if err != nil || !prepared.Accepted || prepared.Envelope.InputSeq != 1 {
		t.Fatalf("prepare=%#v err=%v", prepared, err)
	}
	route, err := inboxes.ResolveReplyRoute(ctx, key.TenantID, first.RequestID)
	if err != nil || route.ChannelBindingID != "contract-binding" || route.ExternalAccountID != "contract-account" ||
		route.ExternalChatID != "contract-chat" || route.ExternalUserID != "contract-user" || route.ConfigVersion != 1 {
		t.Fatalf("reply route=%#v err=%v", route, err)
	}
	ingressStore := ingresspostgres.New(db)
	bindingRoute := ingress.BindingRoute{OpaqueBindingID: "opaque-contract-binding", Channel: "fake", RouteKeyDigest: "route-digest-contract",
		TenantID: key.TenantID, AgentAppID: key.AgentAppID, ChannelBindingID: "contract-binding", TenantVersion: tenantVersion,
		ExternalAccountID: "contract-account", BindingVersion: 1, SecretRef: secrets.SecretRef{Ref: "secret://contract", Version: 1},
		IdentitySecretRef: secrets.SecretRef{Ref: "secret://identity", Version: 1}, SessionSecretRef: secrets.SecretRef{Ref: "secret://session", Version: 1}, Enabled: true}
	if err := ingressStore.PutBindingRoute(ctx, bindingRoute); err != nil {
		t.Fatal(err)
	}
	secretProvider := secretmemory.New()
	secretProvider.Put(secrets.Scope{TenantID: key.TenantID, Subject: "contract-binding", Purpose: secrets.PurposeChannelVerify,
		ResourceID: "contract-binding", ResourceVersion: 1}, bindingRoute.SecretRef, []byte("contract-secret"))
	ingressResolver := ingress.Resolver{Store: ingressStore, Secrets: secretProvider, TTL: time.Minute}
	candidate, err := ingressResolver.ResolveCandidate(ctx, channel.PublicRouteHint{Channel: "fake", RouteKeyDigest: bindingRoute.RouteKeyDigest, IngressAttemptID: "attempt"})
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := ingressResolver.AcquireVerifier(ctx, candidate)
	if err != nil {
		t.Fatal(err)
	}
	_, receipt, err := verifier.Verify(ctx, channel.CallbackRequest{Body: []byte("ciphertext")}, func(_ context.Context, _ channel.CallbackRequest, secret []byte) (channel.VerifiedProtocolPayload, error) {
		if string(secret) != "contract-secret" {
			return channel.VerifiedProtocolPayload{}, runtime.ErrVersionMismatch
		}
		return channel.VerifiedProtocolPayload{Body: []byte("plaintext"), ProtocolIdentityDigest: "identity-digest"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	verifiedBinding, err := ingressResolver.PromoteVerified(ctx, candidate, receipt)
	if err != nil || verifiedBinding.TenantID != key.TenantID || verifiedBinding.ChannelBindingID != "contract-binding" {
		t.Fatalf("verified binding=%#v err=%v", verifiedBinding, err)
	}
	// A process may crash after verification but before promotion. The expiry
	// reconciler must burn that verified candidate instead of retaining it.
	expiryClock := time.Now().UTC().Truncate(time.Microsecond)
	expiryResolver := ingress.Resolver{Store: ingressStore, Secrets: secretProvider, TTL: time.Minute, Now: func() time.Time { return expiryClock }}
	expiringCandidate, err := expiryResolver.ResolveCandidate(ctx, channel.PublicRouteHint{Channel: "fake", RouteKeyDigest: bindingRoute.RouteKeyDigest, IngressAttemptID: "expiry-attempt"})
	if err != nil {
		t.Fatal(err)
	}
	expiringVerifier, err := expiryResolver.AcquireVerifier(ctx, expiringCandidate)
	if err != nil {
		t.Fatal(err)
	}
	_, expiringReceipt, err := expiringVerifier.Verify(ctx, channel.CallbackRequest{}, func(_ context.Context, _ channel.CallbackRequest, secret []byte) (channel.VerifiedProtocolPayload, error) {
		if string(secret) != "contract-secret" {
			return channel.VerifiedProtocolPayload{}, runtime.ErrVersionMismatch
		}
		return channel.VerifiedProtocolPayload{ProtocolIdentityDigest: "expiry-identity"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	expiryClock = expiryClock.Add(2 * time.Minute)
	sweeper := ingress.CandidateExpiryReconciler{Store: ingressStore, Now: func() time.Time { return expiryClock }, BatchSize: 10}
	if count, err := sweeper.RunOnce(ctx); err != nil || count != 1 {
		t.Fatalf("candidate expiry sweep count=%d err=%v", count, err)
	}
	if _, err := expiryResolver.PromoteVerified(ctx, expiringCandidate, expiringReceipt); !errors.Is(err, runtime.ErrVersionConflict) {
		t.Fatalf("expired verified candidate promoted: %v", err)
	}
	preprocessStore := preprocesspostgres.New(db)
	preprocessKey := messaging.InboxKey{TenantID: key.TenantID, Channel: "fake", ExternalAccountID: "contract-account", ExternalMessageID: "preprocess-message"}
	preprocessInbox, originalJob, err := preprocessStore.ClaimInboxAndSchedule(ctx, preprocess.ClaimRequest{
		Inbox: messaging.ClaimInboxRequest{InboxKey: preprocessKey, AgentAppID: key.AgentAppID, SessionID: "preprocess-session",
			ExternalChatID: "contract-chat", ExternalUserID: "contract-user", PayloadDigest: strings.Repeat("9", 64),
			KeyVersion: 1, InitialState: messaging.InboxPreprocessPending},
		TenantVersion: tenantVersion, ConfigVersion: 1, ChannelBindingID: "contract-binding",
		UserID: "derived-user", TraceParent: "preprocess-trace",
	})
	if err != nil || originalJob.State != preprocess.Pending {
		t.Fatalf("preprocess claim inbox=%#v job=%#v err=%v", preprocessInbox, originalJob, err)
	}
	traceRetry := preprocess.ClaimRequest{
		Inbox: messaging.ClaimInboxRequest{InboxKey: preprocessKey, AgentAppID: key.AgentAppID, SessionID: "preprocess-session",
			ExternalChatID: "contract-chat", ExternalUserID: "contract-user", PayloadDigest: strings.Repeat("9", 64),
			KeyVersion: 1, InitialState: messaging.InboxPreprocessPending},
		TenantVersion: tenantVersion, ConfigVersion: 1, ChannelBindingID: "contract-binding",
		UserID: "derived-user", TraceParent: "retry-trace",
	}
	_, retriedJob, err := preprocessStore.ClaimInboxAndSchedule(ctx, traceRetry)
	if err != nil || retriedJob.JobID != originalJob.JobID || retriedJob.TraceParent != originalJob.TraceParent {
		t.Fatalf("trace-only retry job=%#v err=%v", retriedJob, err)
	}
	preprocessRequest := request
	preprocessRequest.RequestID, preprocessRequest.SessionID, preprocessRequest.UserID = preprocessInbox.RequestID, originalJob.SessionID, originalJob.UserID
	preprocessRequest.PayloadRef, preprocessRequest.TraceParent = preprocessInbox.PayloadRef, originalJob.TraceParent
	if _, err := tasks.PrepareDispatch(ctx, preprocessRequest); !errors.Is(err, runtime.ErrPreprocessNotReady) {
		t.Fatalf("preprocess bypass dispatch=%v", err)
	}
	claimClock := time.Now().UTC()
	claimedJobs, err := preprocessStore.ClaimJobs(ctx, preprocess.ClaimOptions{Owner: "preprocess-contract", Now: claimClock, TTL: time.Minute, Limit: 10})
	if err != nil || len(claimedJobs) != 1 || claimedJobs[0].JobID != originalJob.JobID {
		t.Fatalf("claimed preprocess jobs=%#v err=%v", claimedJobs, err)
	}
	readyJob, err := preprocessStore.FinishReady(ctx, claimedJobs[0])
	if err != nil || readyJob.State != preprocess.Ready {
		t.Fatalf("ready preprocess job=%#v err=%v", readyJob, err)
	}
	preprocessPrepared, err := tasks.PrepareDispatch(ctx, preprocessRequest)
	if err != nil || !preprocessPrepared.Accepted {
		t.Fatalf("prepared after preprocess=%#v err=%v", preprocessPrepared, err)
	}
	readyJobs, err := preprocessStore.ClaimReadyForDispatch(ctx, preprocess.ClaimOptions{Owner: "dispatch-contract", Now: claimClock, TTL: time.Minute, Limit: 10})
	if err != nil || len(readyJobs) != 1 {
		t.Fatalf("ready jobs=%#v err=%v", readyJobs, err)
	}
	if _, err := preprocessStore.MarkDispatched(ctx, readyJobs[0], claimClock.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	repeated, err := tasks.PrepareDispatch(ctx, request)
	if err != nil || repeated.Envelope != prepared.Envelope {
		t.Fatalf("repeat prepare=%#v err=%v", repeated, err)
	}
	var dispatchCount int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM outbox WHERE tenant_id=$1 AND kind='dispatch' AND aggregate_id=$2`, key.TenantID, first.RequestID).Scan(&dispatchCount); err != nil || dispatchCount != 1 {
		t.Fatalf("dispatch outbox=%d err=%v", dispatchCount, err)
	}
	claimedOutbox, err := inboxes.ClaimOutbox(ctx, "dispatch", 10, "contract-relay", time.Now().Add(time.Second))
	if err != nil {
		t.Fatalf("claimed outbox=%#v err=%v", claimedOutbox, err)
	}
	targetFound := false
	for _, outboxRecord := range claimedOutbox {
		renewedVersion, renewErr := inboxes.RenewOutboxClaim(ctx, outboxRecord.TenantID, outboxRecord.OutboxID, outboxRecord.Version, "contract-relay", time.Now().Add(2*time.Second))
		if renewErr != nil || renewedVersion <= outboxRecord.Version {
			t.Fatalf("renewed version=%d err=%v", renewedVersion, renewErr)
		}
		if err := inboxes.MarkPublished(ctx, outboxRecord.TenantID, outboxRecord.OutboxID, renewedVersion); err != nil {
			t.Fatal(err)
		}
		if outboxRecord.TenantID == key.TenantID && outboxRecord.AggregateID == first.RequestID {
			targetFound = true
		}
	}
	if !targetFound {
		t.Fatalf("request dispatch was not claimed: %#v", claimedOutbox)
	}
	var suspendedVersion int64
	if err := db.QueryRowContext(ctx, `SELECT transition_tenant_status($1,$2,'suspended','test','contract','test',NULL,'contract','contract',NULL)`, key.TenantID, tenantVersion).Scan(&suspendedVersion); err != nil {
		t.Fatal(err)
	}
	deniedKey := messaging.InboxKey{TenantID: key.TenantID, Channel: "fake", ExternalAccountID: "contract-account", ExternalMessageID: "suspended-message"}
	deniedClaim, err := inboxes.ClaimInbox(ctx, messaging.ClaimInboxRequest{InboxKey: deniedKey, AgentAppID: key.AgentAppID, SessionID: "suspended-session", PayloadDigest: strings.Repeat("f", 64), KeyVersion: 1, InitialState: messaging.InboxDispatchPending})
	if err != nil {
		t.Fatal(err)
	}
	request.Tenant.TenantVersion = suspendedVersion
	request.RequestID, request.SessionID, request.PayloadRef = deniedClaim.RequestID, deniedClaim.SessionID, deniedClaim.PayloadRef
	denied, err := tasks.PrepareDispatch(ctx, request)
	if err != nil || denied.Accepted || denied.TerminalReason != "suspended" {
		t.Fatalf("suspended prepare=%#v err=%v", denied, err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM execution_record WHERE tenant_id=$1 AND request_id=$2`, key.TenantID, deniedClaim.RequestID).Scan(&dispatchCount); err != nil || dispatchCount != 0 {
		t.Fatalf("suspended execution count=%d err=%v", dispatchCount, err)
	}
}

func TestDeliveryLedgerPostgreSQL16(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `TRUNCATE delivery_ledger`); err != nil {
		t.Fatal(err)
	}
	store := messagingpostgres.New(db)
	key := messaging.DeliveryKey{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAV", DeliveryKey: "r1_contract", SegmentNo: 0}
	plan := messaging.DeliveryPlan{RendererVersion: "renderer-v1", FormatVersion: "text-v1", ContentDigest: strings.Repeat("a", 64), SegmentCount: 1}
	claim := messaging.DeliveryClaim{Owner: "adapter-1", TTL: time.Minute}
	claimed, acquired, err := store.ClaimDelivery(ctx, key, plan, claim)
	if err != nil || !acquired || claimed.State != messaging.DeliverySending {
		t.Fatalf("claimed=%#v acquired=%t err=%v", claimed, acquired, err)
	}
	if duplicate, acquired, err := store.ClaimDelivery(ctx, key, plan, claim); err != nil || acquired || duplicate.Version != claimed.Version {
		t.Fatalf("duplicate=%#v acquired=%t err=%v", duplicate, acquired, err)
	}
	claimed, err = store.RenewDeliveryClaim(ctx, claimed, time.Minute)
	if err != nil || claimed.ClaimUntil.Before(time.Now()) {
		t.Fatalf("renewed=%#v err=%v", claimed, err)
	}
	claimed.State = messaging.DeliveryAmbiguous
	claimed.LastErrorClass = "response_lost"
	ambiguous, err := store.FinishDelivery(ctx, claimed, claimed.Version)
	if err != nil || ambiguous.State != messaging.DeliveryAmbiguous {
		t.Fatalf("ambiguous=%#v err=%v", ambiguous, err)
	}
	if replay, acquired, err := store.ClaimDelivery(ctx, key, plan, claim); err != nil || acquired || replay.State != messaging.DeliveryAmbiguous {
		t.Fatalf("replay=%#v acquired=%t err=%v", replay, acquired, err)
	}
	ambiguous.ReconcileAttempt, ambiguous.NotBefore, ambiguous.LastErrorClass = 1, time.Now().Add(time.Second), "reconcile_retryable"
	ambiguous, err = store.DeferDeliveryReconciliation(ctx, ambiguous, ambiguous.Version)
	if err != nil || ambiguous.ReconcileAttempt != 1 {
		t.Fatalf("deferred=%#v err=%v", ambiguous, err)
	}
	ambiguous.State, ambiguous.ProviderMessageID = messaging.DeliverySent, "provider-reconciled"
	reconciled, err := store.ReconcileDelivery(ctx, ambiguous, ambiguous.Version)
	if err != nil || reconciled.State != messaging.DeliverySent || reconciled.ProviderMessageID != "provider-reconciled" {
		t.Fatalf("reconciled=%#v err=%v", reconciled, err)
	}

	crashKey := messaging.DeliveryKey{TenantID: key.TenantID, DeliveryKey: "r1_crash", SegmentNo: 0}
	crashed, acquired, err := store.ClaimDelivery(ctx, crashKey, plan, messaging.DeliveryClaim{Owner: "dead-owner", TTL: time.Microsecond})
	if err != nil || !acquired {
		t.Fatalf("crashed=%#v acquired=%t err=%v", crashed, acquired, err)
	}
	time.Sleep(time.Millisecond)
	recovered, acquired, err := store.ClaimDelivery(ctx, crashKey, plan, messaging.DeliveryClaim{Owner: "new-owner", TTL: time.Minute})
	if err != nil || acquired || recovered.State != messaging.DeliveryAmbiguous || recovered.LastErrorClass != "owner_lost" || recovered.ClientRequestID != crashed.ClientRequestID {
		t.Fatalf("recovered=%#v acquired=%t err=%v", recovered, acquired, err)
	}
}

func openTestDB(t *testing.T) *sql.DB {
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

func prepareContractFixture(tb testing.TB, db *sql.DB, key sessionstore.SessionKey, prepared map[string]uint64) {
	tb.Helper()
	ctx := context.Background()
	statements := []string{
		`TRUNCATE session_event,session_commit,session_summary,execution_record,inbox,session_head CASCADE`,
		`INSERT INTO tenant(tenant_id,tenant_key,display_name) VALUES
('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','atomic-contract','Atomic Contract') ON CONFLICT DO NOTHING`,
		`INSERT INTO agent_app(tenant_id,agent_app_id,agent_app_key,display_name,status,current_revision,next_revision,version)
VALUES('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','app_01ARZ3NDEKTSV4RRFFQ69G5FAV','atomic-contract','Atomic Contract','disabled',NULL,2,1)
ON CONFLICT DO NOTHING`,
		`INSERT INTO model_profile(tenant_id,model_profile_id,profile_key,display_name,status) VALUES
('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','model','atomic-model','Atomic Model','active') ON CONFLICT DO NOTHING`,
		`INSERT INTO model_profile_revision(tenant_id,model_profile_id,profile_version,schema_version,provider,model_name,content_digest) VALUES
('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','model',1,1,'contract','contract',repeat('a',64)) ON CONFLICT DO NOTHING`,
		`UPDATE model_profile SET current_version=1 WHERE tenant_id='t_01ARZ3NDEKTSV4RRFFQ69G5FAV' AND model_profile_id='model' AND current_version IS NULL`,
		`INSERT INTO agent_app_revision(tenant_id,agent_app_id,revision,state,draft_version,agent_kind,schema_version,instruction,model_profile_id,model_profile_version,content_digest,published_at)
VALUES('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','app_01ARZ3NDEKTSV4RRFFQ69G5FAV',1,'published',1,'llm',1,'contract','model',1,repeat('a',64),now())
ON CONFLICT DO NOTHING`,
		`UPDATE agent_app SET status='active',current_revision=1,version=version+1
WHERE tenant_id='t_01ARZ3NDEKTSV4RRFFQ69G5FAV' AND agent_app_id='app_01ARZ3NDEKTSV4RRFFQ69G5FAV' AND current_revision IS NULL`,
		`INSERT INTO config_snapshot(tenant_id,config_version,schema_version,payload,content_digest,state,actor_id,reason_code,correlation_id,trace_id,published_at)
VALUES('t_01ARZ3NDEKTSV4RRFFQ69G5FAV',1,1,'{"policy_version":1}'::jsonb,repeat('b',64),'published','contract','test','contract','contract',now())
ON CONFLICT DO NOTHING`,
		`INSERT INTO channel_binding(tenant_id,config_version,binding_id,channel,external_account_id,agent_app_id,secret_ref,secret_version)
VALUES('t_01ARZ3NDEKTSV4RRFFQ69G5FAV',1,'contract-binding','fake','contract-account','app_01ARZ3NDEKTSV4RRFFQ69G5FAV','secret://contract',1)
ON CONFLICT DO NOTHING`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			tb.Fatalf("fixture statement: %v", err)
		}
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO session_head(tenant_id,agent_app_id,session_id,last_allocated_input_seq)
VALUES($1,$2,$3,$4)`, key.TenantID, key.AgentAppID, key.SessionID, maxInput(prepared)); err != nil {
		tb.Fatal(err)
	}
	for requestID, inputSeq := range prepared {
		externalID := fmt.Sprintf("%s-%d", key.SessionID, inputSeq)
		if _, err := db.ExecContext(ctx, `INSERT INTO inbox(tenant_id,channel,external_account_id,external_message_id,request_id,agent_app_id,session_id,input_seq,state,payload_ref,payload_digest,key_version)
VALUES($1,'fake','contract',$2,$3,$4,$5,$6,'dispatch_ready',$7,repeat('c',64),1)`,
			key.TenantID, externalID, requestID, key.AgentAppID, key.SessionID, inputSeq, "payload://"+requestID); err != nil {
			tb.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO execution_record(tenant_id,request_id,tenant_version,agent_app_id,agent_app_version,agent_app_revision,agent_content_digest,config_version,policy_version,session_id,user_id,channel,input_seq,payload_ref)
VALUES($1,$2,1,$3,2,1,repeat('a',64),1,1,$4,'user','fake',$5,$6)`,
			key.TenantID, requestID, key.AgentAppID, key.SessionID, inputSeq, "payload://"+requestID); err != nil {
			tb.Fatal(err)
		}
	}
}

func maxInput(prepared map[string]uint64) uint64 {
	var result uint64
	for _, input := range prepared {
		if input > result {
			result = input
		}
	}
	return result
}
