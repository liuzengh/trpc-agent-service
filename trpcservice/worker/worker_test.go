package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	agentruntime "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/approval"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/background"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
)

func TestWorkerCompletesDurableRun(t *testing.T) {
	journal := gateway.NewMemoryJournal()
	queue := workqueue.NewMemoryQueue(4)
	jobs := background.NewMemoryRepository()
	runtime := agentruntime.NewDemoRuntime()
	t.Cleanup(func() {
		_ = runtime.Close()
		_ = queue.Close()
		_ = journal.Close()
		_ = jobs.Close()
	})
	scope, _ := runtimecontext.NewScope(
		"tenant-a", "app-a", "revision-a", "http", "binding-a",
	)
	accepted, err := journal.Accept(context.Background(), gateway.InboundRequest{
		Scope:             scope,
		ExternalMessageID: "worker-message",
		UserID:            "alice",
		SessionID:         "session",
		ChatType:          "direct",
		Text:              "hello",
	})
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	relay, err := gateway.NewOutboxRelay(journal, queue, gateway.RelayOptions{
		WorkerID:     "relay",
		BatchSize:    10,
		ClaimLease:   time.Second,
		PollInterval: time.Second,
		RetryDelay:   time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new relay: %v", err)
	}
	if _, err := relay.RelayOnce(context.Background()); err != nil {
		t.Fatalf("relay: %v", err)
	}
	worker, err := New(queue, journal, runtime, Options{
		WorkerID:    "worker-1",
		MaxAttempts: 3,
		RetryDelay:  time.Millisecond,
		Audit:       audit.NewMemoryWriter(),
		Jobs:        jobs,
	})
	if err != nil {
		t.Fatalf("new Worker: %v", err)
	}
	processed, err := worker.ProcessOne(context.Background())
	if err != nil || !processed {
		t.Fatalf("processed=%t err=%v", processed, err)
	}
	status, result, ok := journal.RunStatus(accepted.RequestID)
	if !ok || status != "completed" || result.Reply == "" || result.FencingToken == 0 {
		t.Fatalf("status=%q result=%+v ok=%t", status, result, ok)
	}
	events := worker.opts.Audit.(*audit.MemoryWriter).Events()
	if len(events) != 1 || events[0].Decision != "run_completed" ||
		events[0].RequestID != accepted.RequestID {
		t.Fatalf("audit events=%+v", events)
	}
	firstJob, err := jobs.Claim(context.Background(), "jobs", time.Second)
	if err != nil || (firstJob.Type != background.JobSummary && firstJob.Type != background.JobMemoryExtract) {
		t.Fatalf("first background job=%+v err=%v", firstJob, err)
	}
	if err := jobs.Complete(context.Background(), firstJob.ID, "jobs"); err != nil {
		t.Fatalf("complete first background job: %v", err)
	}
	secondJob, err := jobs.Claim(context.Background(), "jobs", time.Second)
	if err != nil || secondJob.Type == firstJob.Type {
		t.Fatalf("second background job=%+v err=%v", secondJob, err)
	}
}

func TestAppendApprovalInstructions(t *testing.T) {
	reply := appendApprovalInstructions("waiting", []approval.Record{{
		ApprovalID: "apr_0123456789abcdef0123456789abcdef", ToolName: "dangerous_demo",
	}})
	if !strings.Contains(reply, "批准 apr_0123456789abcdef0123456789abcdef") ||
		!strings.Contains(reply, "拒绝 apr_0123456789abcdef0123456789abcdef") {
		t.Fatalf("reply=%q", reply)
	}
}
