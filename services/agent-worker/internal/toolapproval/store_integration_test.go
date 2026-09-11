package toolapproval_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	approvalv1 "github.com/liuzengh/trpc-agent-service/api/runtime/approval/v1"
	postgresadapter "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/postgresadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/toolapproval"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/migrations"
)

func TestApprovalPostgresLifecycle(t *testing.T) {
	migrationURL, runtimeURL := os.Getenv("WORKER_APPROVAL_TEST_MIGRATION_URL"), os.Getenv("WORKER_APPROVAL_TEST_RUNTIME_URL")
	if migrationURL == "" || runtimeURL == "" {
		t.Skip("requires dedicated Worker PostgreSQL fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	migration, err := pgxpool.New(ctx, migrationURL)
	if err != nil {
		t.Fatal(err)
	}
	defer migration.Close()
	runtime, err := pgxpool.New(ctx, runtimeURL)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if err = migrations.ApplyForRuntime(ctx, migration, "worker_runtime"); err != nil {
		t.Fatal(err)
	}
	ledger := postgresadapter.New(runtime)
	request := domain.Requested{EventID: "evt", RunID: "run", AdmissionID: "adm", EventDigest: domain.Digest([]byte("event")), RunDigest: domain.Digest([]byte("run")), Route: domain.Route{TenantID: "tenant-a", Provider: "telegram", AccountID: "account", BindingID: "binding", DeploymentRevisionID: "revision", ManifestRef: "manifest", ManifestDigest: domain.Digest([]byte("manifest")), Generation: 1}, Input: domain.Input{ConversationID: "conversation", SenderID: "sender", Text: "close test ticket", ReceivedAt: time.Now().UTC()}}
	policy := domain.Policy{Version: "v1", MaxRunAge: time.Hour, MaxReplyAge: time.Hour, MaxFutureSkew: time.Minute, LeaseTTL: 10 * time.Second, RenewalInterval: time.Second, RetryBackoff: time.Second, MaxAttempts: 1}
	if _, err = ledger.Accept(ctx, request, policy, domain.IntakeLimits{MaxQueuedRuns: 10, MaxRetainedRuns: 100}); err != nil {
		t.Fatal(err)
	}
	grant, err := ledger.Claim(ctx, domain.ClaimRequest{TenantID: "tenant-a", RunID: "run", WorkerID: "worker", MaxRunSeconds: 600, MaxActive: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err = ledger.MarkExecuting(ctx, grant); err != nil {
		t.Fatal(err)
	}
	store, _ := toolapproval.New(runtime)
	proposal := toolapproval.Proposal{TenantID: "tenant-a", RunID: "run", AttemptID: grant.AttemptID, InvocationID: "inv-1", ToolCallID: "call-1", NodeID: "assistant", ToolName: "mcp_ticket", ToolResource: "ticket", Capability: approvalv1.CapabilityTestTicketStatusUpdate, Arguments: []byte(`{"ticket_id":"TEST-42","status":"closed"}`), TTL: time.Minute}
	op, err := store.Propose(ctx, proposal)
	if err != nil || op.Status != approvalv1.StatusPending || op.Target != "TEST-42" {
		t.Fatal(op, err)
	}
	changed := proposal
	changed.Arguments = []byte(`{"ticket_id":"TEST-42","status":"resolved"}`)
	if _, err = store.Propose(ctx, changed); !errors.Is(err, toolapproval.ErrConflict) {
		t.Fatal("changed arguments reused approval", err)
	}
	decision, err := store.Decide(ctx, "tenant-a", op.OperationID, "owner", "approve", "verified target", op.ArgumentsDigest)
	if err != nil || decision.Outcome != "DECIDED" {
		t.Fatal(decision, err)
	}
	replay, err := store.Decide(ctx, "tenant-a", op.OperationID, "owner", "approve", "verified target", op.ArgumentsDigest)
	if err != nil || replay.Outcome != "REPLAY" {
		t.Fatal(replay, err)
	}
	executing, result, err := store.AwaitAndBegin(ctx, "tenant-a", op.OperationID, time.Millisecond)
	if err != nil || result != nil || executing.Status != approvalv1.StatusExecuting {
		t.Fatal(executing, string(result), err)
	}
	if err = store.Finish(ctx, "tenant-a", op.OperationID, []byte(`{"updated":true}`), nil); err != nil {
		t.Fatal(err)
	}
	final, err := store.Get(ctx, "tenant-a", op.OperationID)
	if err != nil || final.Status != approvalv1.StatusSucceeded || final.ResultDigest == "" {
		t.Fatal(final, err)
	}
	page, err := store.List(ctx, "tenant-a", 0, 25)
	if err != nil || page.Total != 1 || len(page.Operations) != 1 {
		t.Fatal(page, err)
	}
	other, err := store.List(ctx, "tenant-b", 0, 25)
	if err != nil || other.Total != 0 {
		t.Fatal(other, err)
	}

	interruptedRequest := request
	interruptedRequest.EventID = "evt-interrupted"
	interruptedRequest.RunID = "run-interrupted"
	interruptedRequest.AdmissionID = "adm-interrupted"
	interruptedRequest.EventDigest = domain.Digest([]byte("event-interrupted"))
	interruptedRequest.RunDigest = domain.Digest([]byte("run-interrupted"))
	interruptedRequest.Route.TenantID = "tenant-interrupted"
	if _, err = ledger.Accept(ctx, interruptedRequest, policy, domain.IntakeLimits{MaxQueuedRuns: 10, MaxRetainedRuns: 100}); err != nil {
		t.Fatal(err)
	}
	interruptedGrant, err := ledger.Claim(ctx, domain.ClaimRequest{TenantID: interruptedRequest.Route.TenantID, RunID: interruptedRequest.RunID, WorkerID: "worker-restarted", MaxRunSeconds: 600, MaxActive: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err = ledger.MarkExecuting(ctx, interruptedGrant); err != nil {
		t.Fatal(err)
	}
	interruptedProposal := proposal
	interruptedProposal.TenantID = interruptedRequest.Route.TenantID
	interruptedProposal.RunID = interruptedRequest.RunID
	interruptedProposal.AttemptID = interruptedGrant.AttemptID
	interruptedProposal.InvocationID = "inv-interrupted"
	interruptedProposal.ToolCallID = "call-interrupted"
	interrupted, err := store.Propose(ctx, interruptedProposal)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.ReconcileInterrupted(ctx, "another-worker"); err != nil {
		t.Fatal(err)
	}
	unchanged, err := store.Get(ctx, interrupted.TenantID, interrupted.OperationID)
	if err != nil || unchanged.Status != approvalv1.StatusPending {
		t.Fatal("another worker reconciled this operation", unchanged, err)
	}
	if err = store.ReconcileInterrupted(ctx, "worker-restarted"); err != nil {
		t.Fatal(err)
	}
	reconciled, err := store.Get(ctx, interrupted.TenantID, interrupted.OperationID)
	if err != nil || reconciled.Status != approvalv1.StatusUnknown {
		t.Fatal("interrupted approval was not fenced", reconciled, err)
	}
	var waitReason string
	if err = runtime.QueryRow(ctx, `SELECT wait_reason FROM execution_runs WHERE tenant_id=$1 AND run_id=$2`, interrupted.TenantID, interrupted.RunID).Scan(&waitReason); err != nil || waitReason != "TOOL_APPROVAL_RECONCILIATION_REQUIRED" {
		t.Fatal("run does not expose reconciliation requirement", waitReason, err)
	}
	if _, err = runtime.Exec(ctx, `UPDATE execution_attempts SET lease_until=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND attempt_id=$2`, interrupted.TenantID, interruptedGrant.AttemptID); err != nil {
		t.Fatal(err)
	}
	ready, err := ledger.Ready(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range ready {
		if candidate.Request.RunID == interrupted.RunID {
			t.Fatal("interrupted approval run became retryable")
		}
	}
}
