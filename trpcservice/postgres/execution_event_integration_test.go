//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestTerminalEventAndExecutionStatusAreCommittedTogether(t *testing.T) {
	p := newIM05Fixture(t, newIntegrationTargetProtector(t, "v1"))
	claim, exec := claimIM05Execution(t, p, "message-terminal-atomic", "request-terminal-atomic")
	exec.TerminalStatus = queue.CompletionSucceeded

	journal, err := platformpostgres.NewExecutionEventJournal(
		p.store,
		platformpostgres.WithReplyEventBuilder(func(_ context.Context, exec worker.Execution, sequence int64, _ *event.Event) ([]channels.Reply, error) {
			reply := channels.Reply{
				TenantID:        exec.Tenant.TenantID,
				AppID:           exec.Tenant.AppID,
				RequestID:       exec.RequestID,
				SourceEventID:   fmt.Sprintf("%s:%d", exec.RequestID, sequence),
				Channel:         channels.Channel(exec.Tenant.Channel),
				BindingID:       exec.Tenant.BindingID,
				BindingRevision: exec.Tenant.BindingRevision,
				Revision:        sequence,
				Target:          channels.ReplyTarget{Kind: channels.TargetKindUser, InternalEntityID: exec.Tenant.UserID},
				Text:            "done",
			}
			return []channels.Reply{reply}, nil
		}),
	)
	if err != nil {
		t.Fatalf("new execution event journal: %v", err)
	}
	ctx, err := worker.ContextWithJobLease(p.ctx, claim.Lease)
	if err != nil {
		t.Fatalf("attach execution lease: %v", err)
	}
	if err := journal.HandleRunnerEvent(ctx, exec, terminalCompletionEvent()); err != nil {
		t.Fatalf("append terminal event: %v", err)
	}

	var status string
	var eventCount, replyCount int
	if err := p.pool.QueryRow(p.ctx, `
SELECT e.status, (
    SELECT count(*) FROM platform.execution_event
    WHERE tenant_id = e.tenant_id AND app_id = e.app_id AND request_id = e.request_id
), (
    SELECT count(*) FROM platform.reply_outbox
    WHERE tenant_id = e.tenant_id AND app_id = e.app_id AND request_id = e.request_id
)
FROM platform.execution e
WHERE e.tenant_id = $1 AND e.app_id = $2 AND e.request_id = $3`,
		p.scope.TenantID, p.scope.AppID, claim.Job.RequestID()).Scan(&status, &eventCount, &replyCount); err != nil {
		t.Fatalf("read terminal projection: %v", err)
	}
	if status != string(queue.CompletionSucceeded) || eventCount != 1 || replyCount != 1 {
		t.Fatalf("terminal projection = status:%q events:%d replies:%d, want status:%q events:1 replies:1", status, eventCount, replyCount, queue.CompletionSucceeded)
	}
	if err := journal.HandleRunnerEvent(ctx, exec, terminalCompletionEvent()); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("replayed terminal event error = %v, want lease lost", err)
	}
	if err := p.pool.QueryRow(p.ctx, `
SELECT count(*) FROM platform.reply_outbox
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3`,
		p.scope.TenantID, p.scope.AppID, claim.Job.RequestID()).Scan(&replyCount); err != nil {
		t.Fatalf("read replayed reply count: %v", err)
	}
	if replyCount != 1 {
		t.Fatalf("replayed reply count = %d, want 1", replyCount)
	}
}

func TestTerminalEventRollbackLeavesExecutionRunnable(t *testing.T) {
	p := newIM05Fixture(t, newIntegrationTargetProtector(t, "v1"))
	claim, exec := claimIM05Execution(t, p, "message-terminal-rollback", "request-terminal-rollback")
	exec.TerminalStatus = queue.CompletionSucceeded

	journal, err := platformpostgres.NewExecutionEventJournal(
		p.store,
		platformpostgres.WithReplyEventBuilder(func(context.Context, worker.Execution, int64, *event.Event) ([]channels.Reply, error) {
			return nil, errors.New("reply projection failed")
		}),
	)
	if err != nil {
		t.Fatalf("new execution event journal: %v", err)
	}
	ctx, err := worker.ContextWithJobLease(p.ctx, claim.Lease)
	if err != nil {
		t.Fatalf("attach execution lease: %v", err)
	}
	if err := journal.HandleRunnerEvent(ctx, exec, terminalCompletionEvent()); err == nil {
		t.Fatal("terminal event succeeded with failing reply projection")
	}

	var status string
	var eventCount int
	if err := p.pool.QueryRow(p.ctx, `
SELECT e.status, (
    SELECT count(*) FROM platform.execution_event
    WHERE tenant_id = e.tenant_id AND app_id = e.app_id AND request_id = e.request_id
)
FROM platform.execution e
WHERE e.tenant_id = $1 AND e.app_id = $2 AND e.request_id = $3`,
		p.scope.TenantID, p.scope.AppID, claim.Job.RequestID()).Scan(&status, &eventCount); err != nil {
		t.Fatalf("read rolled back projection: %v", err)
	}
	if status != "RUNNING" || eventCount != 0 {
		t.Fatalf("rolled back projection = status:%q events:%d, want status:RUNNING events:0", status, eventCount)
	}
}

func TestApprovalPauseKeepsExecutionEventStreamOpen(t *testing.T) {
	p := newIM05Fixture(t, newIntegrationTargetProtector(t, "v1"))
	claim, exec := claimIM05Execution(t, p, "message-approval-stream", "request-approval-stream")

	journal, err := platformpostgres.NewExecutionEventJournal(p.store)
	if err != nil {
		t.Fatalf("new execution event journal: %v", err)
	}
	leaseContext, err := worker.ContextWithJobLease(p.ctx, claim.Lease)
	if err != nil {
		t.Fatalf("attach execution lease: %v", err)
	}
	if err := journal.HandleRunnerEvent(leaseContext, exec, terminalCompletionEvent()); err != nil {
		t.Fatalf("append approval pause completion: %v", err)
	}
	if _, err := p.pool.Exec(p.ctx, `
UPDATE platform.execution
SET status = 'WAITING_APPROVAL', lease_owner = NULL, run_token = NULL,
    lease_until = NULL, updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3`,
		p.scope.TenantID, p.scope.AppID, claim.Job.RequestID()); err != nil {
		t.Fatalf("park execution for approval: %v", err)
	}

	streamCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := journal.SubscribeExecutionEvents(streamCtx, p.scope, claim.Job.RequestID(), 0)
	if err != nil {
		t.Fatalf("subscribe execution events: %v", err)
	}
	select {
	case item, ok := <-stream:
		if !ok || item.Event == nil || !item.Event.IsRunnerCompletion() {
			t.Fatalf("first approval event = %#v, open=%t", item, ok)
		}
	case <-time.After(time.Second):
		t.Fatal("approval completion was not streamed")
	}
	select {
	case _, ok := <-stream:
		if !ok {
			t.Fatal("execution stream closed while status was WAITING_APPROVAL")
		}
		t.Fatal("unexpected event before approval continuation")
	case <-time.After(450 * time.Millisecond):
	}

	if _, err := p.pool.Exec(p.ctx, `
UPDATE platform.execution
SET status = 'SUCCEEDED', updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3`,
		p.scope.TenantID, p.scope.AppID, claim.Job.RequestID()); err != nil {
		t.Fatalf("complete approval continuation: %v", err)
	}
	select {
	case _, ok := <-stream:
		if ok {
			t.Fatal("execution stream emitted an event after terminal status")
		}
	case <-time.After(time.Second):
		t.Fatal("execution stream did not close after terminal status")
	}
}

func claimIM05Execution(t *testing.T, p im05Fixture, externalMessageID, requestID string) (queue.Claim, worker.Execution) {
	t.Helper()
	admitted, err := p.store.Admit(p.ctx, newIM05Request(t, p.route, p.binding, externalMessageID, requestID, channels.MessageTypeText, "terminal"))
	if err != nil {
		t.Fatalf("admit execution: %v", err)
	}
	var dispatch queue.Dispatch
	if err := p.pool.QueryRow(p.ctx, `
SELECT o.outbox_id, o.tenant_id, o.app_id, o.request_id, e.trace_parent, e.trace_state
FROM platform.dispatch_outbox o
JOIN platform.execution e USING (tenant_id, app_id, request_id)
WHERE o.tenant_id = $1 AND o.app_id = $2 AND o.request_id = $3`,
		p.scope.TenantID, p.scope.AppID, admitted.RequestID).Scan(
		&dispatch.OutboxID, &dispatch.TenantID, &dispatch.AppID, &dispatch.RequestID,
		&dispatch.TraceParent, &dispatch.TraceState); err != nil {
		t.Fatalf("read execution dispatch: %v", err)
	}
	claim, found, err := p.store.Claim(p.ctx, dispatch, queue.ClaimRequest{Owner: "terminal-test-worker", LeaseDuration: time.Minute})
	if err != nil || !found {
		t.Fatalf("claim execution = %#v, found=%t, err=%v", claim, found, err)
	}
	if claim.Job.RequestID() != admitted.RequestID {
		t.Fatalf("claimed request id = %q, want %q", claim.Job.RequestID(), admitted.RequestID)
	}
	exec, err := worker.New(p.store, nil, nil, nil, nil).Prepare(p.ctx, claim.Job)
	if err != nil {
		t.Fatalf("prepare execution: %v", err)
	}
	return claim, exec
}

func terminalCompletionEvent() *event.Event {
	return &event.Event{Response: &model.Response{Object: model.ObjectTypeRunnerCompletion, Done: true}}
}
