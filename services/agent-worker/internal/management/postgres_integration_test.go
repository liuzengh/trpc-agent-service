package management_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/postgresadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/management"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/migrations"
)

func TestManagementProjectionPostgres(t *testing.T) {
	migrationURL, runtimeURL := os.Getenv("WORKER_MANAGEMENT_TEST_MIGRATION_URL"), os.Getenv("WORKER_MANAGEMENT_TEST_RUNTIME_URL")
	if migrationURL == "" || runtimeURL == "" {
		t.Skip("dedicated Worker management PostgreSQL fixture required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	open := func(raw string) *pgxpool.Pool {
		pool, err := pgxpool.New(ctx, raw)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		return pool
	}
	migrator, runtime := open(migrationURL), open(runtimeURL)
	if err := migrations.ApplyForRuntime(ctx, migrator, "worker_runtime"); err != nil {
		t.Fatal(err)
	}
	ledger := postgresadapter.New(runtime)
	now := time.Now().UTC()
	requested := domain.Requested{EventID: "event", EventDigest: domain.Digest([]byte("event")), RunDigest: domain.Digest([]byte("run")), RunID: "run", AdmissionID: "admission", Route: domain.Route{TenantID: "tenant", Provider: "telegram", AccountID: "account", BindingID: "binding", DeploymentRevisionID: "revision", ManifestRef: "manifest", ManifestDigest: domain.Digest([]byte("manifest melt")), Generation: 1}, Input: domain.Input{ConversationID: "conversation", SenderID: "sender", Text: "not returned", SourceDigest: domain.Digest([]byte("source")), ReceivedAt: now}}
	policy := domain.Policy{Version: "v1", MaxRunAge: time.Hour, MaxReplyAge: time.Hour, MaxFutureSkew: time.Minute, LeaseTTL: 10 * time.Second, RenewalInterval: time.Second, RetryBackoff: time.Second, MaxAttempts: 2}
	if _, err := ledger.Accept(ctx, requested, policy, domain.IntakeLimits{MaxQueuedRuns: 10, MaxRetainedRuns: 100}); err != nil {
		t.Fatal(err)
	}
	grant, err := ledger.Claim(ctx, domain.ClaimRequest{TenantID: "tenant", RunID: "run", WorkerID: "worker", MaxRunSeconds: 60, MaxActive: 2})
	if err != nil {
		t.Fatal(err)
	}
	completion, err := ledger.Complete(ctx, domain.Finish{Grant: grant, Status: domain.Succeeded, Candidate: domain.Candidate{Ref: "candidate", Digest: domain.Digest([]byte("candidate")), Parent: grant.Parent}, FinalText: "not returned"})
	if err != nil {
		t.Fatal(err)
	}
	reader := management.NewReader(runtime)
	page, err := reader.List(ctx, "tenant", 0, 25)
	if err != nil || page.Total != 1 || len(page.Runs) != 1 || page.Runs[0].ReplyStatus != "PENDING" || page.Runs[0].UsageStatus != "UNAVAILABLE" {
		t.Fatal(page, err)
	}
	if _, err = runtime.Exec(ctx, `INSERT INTO execution_model_usage(operation_id,tenant_id,run_id,attempt_id,input_tokens,output_tokens,total_tokens,result_digest) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, "usage", "tenant", "run", grant.AttemptID, 12, 4, 16, domain.Digest([]byte("usage"))); err != nil {
		t.Fatal(err)
	}
	page, err = reader.List(ctx, "tenant", 0, 25)
	if err != nil || page.Runs[0].UsageStatus != "COMPLETE" || page.Runs[0].InputTokens != 12 || page.Runs[0].OutputTokens != 4 || page.Runs[0].TotalTokens != 16 {
		t.Fatal(page, err)
	}
	detail, err := reader.Get(ctx, "tenant", "run")
	if err != nil || detail.ManifestRef != "manifest" || detail.AdmissionID != "admission" || len(detail.AttemptsLog) != 1 || detail.SessionHead == "" || detail.UsageStatus != "COMPLETE" || detail.TotalTokens != 16 {
		t.Fatal(detail, err)
	}
	if detail.Stage != "REPLY_PUBLISH" {
		t.Fatal(detail.Stage)
	}
	final, err := ledger.Final(ctx, completion.FinalIntentID)
	if err != nil {
		t.Fatal(err)
	}
	if err = ledger.MarkReplyPublished(ctx, completion.FinalIntentID, final.Digest); err != nil {
		t.Fatal(err)
	}
	detail, err = reader.Get(ctx, "tenant", "run")
	if err != nil || detail.Stage != "REPLY_HANDOFF" || detail.ReplyStatus != "HANDED_OFF" {
		t.Fatal(detail, err)
	}
	if _, err = reader.Get(ctx, "other", "run"); err != management.ErrNotFound {
		t.Fatal("cross tenant", err)
	}
	audit, err := reader.Audit(ctx, "tenant", 0, 25)
	if err != nil || audit.Total != 4 || len(audit.Events) != 4 {
		t.Fatal(audit, err)
	}
}
