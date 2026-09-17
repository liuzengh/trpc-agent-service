package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	messagingpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/postgres"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
	sessionpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session/postgres"
)

func TestPostgreSQLDurableConfirmationContract(t *testing.T) {
	if os.Getenv("TRPC_MIGRATION_TEST") != "1" {
		t.Skip("TRPC_MIGRATION_TEST=1 is required")
	}
	dsn := os.Getenv("TRPC_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("TRPC_POSTGRES_TEST_DSN is required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	const tenantID = "t_01ARZ3NDEKTSV4RRFFQ69G5FCF"
	const appID = "app_01ARZ3NDEKTSV4RRFFQ69G5FCF"
	for _, statement := range []string{
		`TRUNCATE interaction_payload,tool_result_payload,tool_attempt,confirmation_grant,confirmation,outbox,session_event,session_commit,execution_record,inbox,session_head CASCADE`,
		`INSERT INTO tenant(tenant_id,tenant_key,display_name) VALUES('` + tenantID + `','confirmation-contract','Confirmation Contract') ON CONFLICT DO NOTHING`,
		`INSERT INTO agent_app(tenant_id,agent_app_id,agent_app_key,display_name,status,current_revision,next_revision,version) VALUES('` + tenantID + `','` + appID + `','confirmation-contract','Confirmation Contract','disabled',NULL,2,1) ON CONFLICT DO NOTHING`,
		`INSERT INTO model_profile(tenant_id,model_profile_id,profile_key,display_name,status) VALUES('` + tenantID + `','model','confirmation-model','Confirmation Model','active') ON CONFLICT DO NOTHING`,
		`INSERT INTO model_profile_revision(tenant_id,model_profile_id,profile_version,schema_version,provider,model_name,content_digest) VALUES('` + tenantID + `','model',1,1,'contract','contract',repeat('a',64)) ON CONFLICT DO NOTHING`,
		`UPDATE model_profile SET current_version=1 WHERE tenant_id='` + tenantID + `' AND model_profile_id='model' AND current_version IS NULL`,
		`INSERT INTO agent_app_revision(tenant_id,agent_app_id,revision,state,draft_version,agent_kind,schema_version,instruction,model_profile_id,model_profile_version,content_digest,published_at) VALUES('` + tenantID + `','` + appID + `',1,'published',1,'llm',1,'contract','model',1,repeat('a',64),now()) ON CONFLICT DO NOTHING`,
		`UPDATE agent_app SET status='active',current_revision=1,version=version+1 WHERE tenant_id='` + tenantID + `' AND agent_app_id='` + appID + `' AND current_revision IS NULL`,
		`INSERT INTO config_snapshot(tenant_id,config_version,schema_version,payload,content_digest,state,actor_id,reason_code,correlation_id,trace_id,published_at) VALUES('` + tenantID + `',1,1,'{"policy_version":1}'::jsonb,repeat('b',64),'published','contract','test','contract','contract',now()) ON CONFLICT DO NOTHING`,
		`INSERT INTO session_head(tenant_id,agent_app_id,session_id,last_allocated_input_seq) VALUES('` + tenantID + `','` + appID + `','session',1)`,
		`INSERT INTO inbox(tenant_id,channel,external_account_id,external_message_id,request_id,agent_app_id,session_id,input_seq,state,payload_ref,payload_digest,key_version) VALUES('` + tenantID + `','fake','account','message','request','` + appID + `','session',1,'dispatch_ready','payload://request',repeat('c',64),7)`,
		`INSERT INTO execution_record(tenant_id,request_id,tenant_version,agent_app_id,agent_app_version,agent_app_revision,agent_content_digest,config_version,policy_version,session_id,user_id,channel,input_seq,payload_ref) VALUES('` + tenantID + `','request',1,'` + appID + `',2,1,repeat('a',64),1,1,'session','user','fake',1,'payload://request')`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatalf("fixture: %v", err)
		}
	}
	key := sessionstore.SessionKey{TenantID: tenantID, AgentAppID: appID, SessionID: "session"}
	sessions := sessionpostgres.New(db)
	head, err := sessions.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: key, RequestID: "request", InputSeq: 1, Fence: 9})
	if err != nil {
		t.Fatal(err)
	}
	commit := sessionstore.CommitTurnRequest{SessionKey: key, RequestID: "request", CommitID: "request:waiting:call", Stage: "waiting", InputSeq: 1, Fence: 9,
		ExpectedVersion: head.Version, Outcome: runtime.OutcomeWaitingConfirmation, ResultRef: "continuation://request/call",
		Outbox: []sessionstore.OutboxEvent{{Kind: "reply", IdempotencyKey: "confirmation-reply", PayloadRef: "confirmation://tenant/conf", EventSeq: 1}}}
	coordinator := New(db)
	in := governance.SuspensionRequest{ConfirmationID: "conf_0123456789abcdef0123456789abcdef", TenantID: tenantID, RequestID: "request", AgentAppID: appID,
		SessionID: "session", InputSeq: 1, Fence: 9, SubjectID: "user", ChannelBindingID: "binding", Tool: governance.VersionedRef{ID: "danger", Version: 1},
		ToolCallID: "call", ArgsDigest: hex.EncodeToString(sha256.New().Sum(nil)), CheckpointRef: commit.ResultRef, PolicyVersion: 1,
		Usage: governance.Usage{InputTokens: 3, OutputTokens: 2}, ExpiresAt: time.Now().UTC().Add(time.Hour)}
	first, err := coordinator.Suspend(ctx, commit, in)
	if err != nil || first.State != governance.ConfirmationPending {
		t.Fatalf("suspend=%#v err=%v", first, err)
	}
	replay, err := coordinator.Suspend(ctx, commit, in)
	if err != nil || replay.ConfirmationID != first.ConfirmationID {
		t.Fatalf("replay=%#v err=%v", replay, err)
	}
	if _, err := coordinator.Decide(ctx, governance.ConfirmationDecision{TenantID: tenantID, ConfirmationID: in.ConfirmationID, SubjectID: "other", Approve: true, ExpectedVersion: 1, DecidedAt: time.Now().UTC()}); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("subject mismatch=%v", err)
	}
	approved, err := coordinator.Decide(ctx, governance.ConfirmationDecision{TenantID: tenantID, ConfirmationID: in.ConfirmationID, SubjectID: "user", Approve: true, ExpectedVersion: 1, DecidedAt: time.Now().UTC()})
	if err != nil || approved.State != governance.ConfirmationApproved {
		t.Fatalf("approved=%#v err=%v", approved, err)
	}
	grant, err := coordinator.GetGrantByConfirmation(ctx, tenantID, in.ConfirmationID)
	if err != nil {
		t.Fatal(err)
	}
	claim := governance.GrantClaim{TenantID: tenantID, GrantID: grant.GrantID, RequestID: "request", SubjectID: "user", Tool: in.Tool, ToolCallID: "call", ArgsDigest: in.ArgsDigest, PolicyVersion: 1, ExpectedVersion: grant.Version}
	if _, err := coordinator.ConsumeGrant(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.ConsumeGrant(ctx, claim); !errors.Is(err, runtime.ErrVersionConflict) {
		t.Fatalf("grant replay=%v", err)
	}
	attempt, err := coordinator.GetToolAttempt(ctx, tenantID, grant.GrantID)
	if err != nil || attempt.State != governance.ToolAttemptEffectUnknown {
		t.Fatalf("attempt=%#v err=%v", attempt, err)
	}
	if _, err := coordinator.FinishToolAttempt(ctx, governance.FinishToolAttemptRequest{TenantID: tenantID, GrantID: grant.GrantID, State: governance.ToolAttemptSucceeded, ResultRef: "tool-result://one"}); err != nil {
		t.Fatal(err)
	}
	payloads := messagingpostgres.NewWithPayloadKey(db, bytes.Repeat([]byte{0x71}, 32), 7)
	prompt := []byte(`{"kind":"tool_confirmation"}`)
	sum := sha256.Sum256(prompt)
	record := messaging.InteractionRecord{TenantID: tenantID, RequestID: "request", ContentRef: "confirmation://tenant/conf", ContentDigest: hex.EncodeToString(sum[:]), Content: prompt, KeyVersion: 7}
	if err := payloads.PutInteraction(ctx, record); err != nil {
		t.Fatal(err)
	}
	stored, err := payloads.GetReplyContent(ctx, tenantID, "request", record.ContentRef)
	if err != nil || !bytes.Equal(stored.Content, prompt) {
		t.Fatalf("stored=%#v err=%v", stored, err)
	}
	var ciphertext []byte
	if err := db.QueryRowContext(ctx, `SELECT content_ciphertext FROM interaction_payload WHERE tenant_id=$1 AND request_id='request'`, tenantID).Scan(&ciphertext); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(ciphertext, prompt) {
		t.Fatal("confirmation prompt stored in plaintext")
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO confirmation(tenant_id,confirmation_id,request_id,request_digest,agent_app_id,session_id,input_seq,subject_id,
channel_binding_id,tool_id,tool_version,tool_call_id,args_digest,policy_version,checkpoint_ref,state,expires_at)
VALUES($1,'conf_ffffffffffffffffffffffffffffffff','request',repeat('d',64),$2,'session',1,'user','binding','danger',1,'expired-call',repeat('e',64),1,'continuation://expired','pending',$3)`,
		tenantID, appID, time.Now().UTC().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	expired, err := coordinator.ExpireDue(ctx, time.Now().UTC(), 10)
	if err != nil || len(expired) != 1 || expired[0].State != governance.ConfirmationExpired {
		t.Fatalf("expired=%#v err=%v", expired, err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE execution_record SET cancel_requested_at=now(),cancel_version=1 WHERE tenant_id=$1 AND request_id='request'`, tenantID); err != nil {
		t.Fatal(err)
	}
	const cancelledConfirmation = "conf_cccccccccccccccccccccccccccccccc"
	if _, err := db.ExecContext(ctx, `INSERT INTO confirmation(tenant_id,confirmation_id,request_id,request_digest,agent_app_id,session_id,input_seq,subject_id,
channel_binding_id,tool_id,tool_version,tool_call_id,args_digest,policy_version,checkpoint_ref,state,expires_at)
VALUES($1,$2,'request',repeat('d',64),$3,'session',1,'user','binding','danger',1,'cancel-call',repeat('f',64),1,'continuation://cancel','pending',$4)`,
		tenantID, cancelledConfirmation, appID, time.Now().UTC().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	cancelled, err := coordinator.Decide(ctx, governance.ConfirmationDecision{TenantID: tenantID, ConfirmationID: cancelledConfirmation, SubjectID: "user", Approve: true, ExpectedVersion: 1, DecidedAt: time.Now().UTC()})
	if err != nil || cancelled.State != governance.ConfirmationDenied {
		t.Fatalf("cancelled decision=%#v err=%v", cancelled, err)
	}
	if _, err := coordinator.GetGrantByConfirmation(ctx, tenantID, cancelledConfirmation); !errors.Is(err, runtime.ErrNotFound) {
		t.Fatalf("cancelled grant=%v", err)
	}
	var confirmationAudit int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM outbox WHERE tenant_id=$1 AND kind='audit' AND idempotency_key LIKE 'confirmation-audit:%'`, tenantID).Scan(&confirmationAudit); err != nil {
		t.Fatal(err)
	}
	if confirmationAudit != 4 {
		t.Fatalf("confirmation audit intents=%d", confirmationAudit)
	}
}
