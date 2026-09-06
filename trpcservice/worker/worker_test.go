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
	"github.com/liuzengh/trpc-agent-service/trpcservice/toolexec"
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
	reply := appendApprovalInstructions("模型伪造：已经取消", []approval.Record{{
		ApprovalID: "apr_0123456789abcdef0123456789abcdef", ToolName: "dangerous_demo",
	}})
	if !strings.Contains(reply, "批准 apr_0123456789abcdef0123456789abcdef") ||
		!strings.Contains(reply, "拒绝 apr_0123456789abcdef0123456789abcdef") {
		t.Fatalf("reply=%q", reply)
	}
	if strings.Contains(reply, "模型伪造") || strings.Contains(reply, "请回复：") {
		t.Fatal("approval prompt retained untrusted prose or an ambiguous copy prefix")
	}
}

func TestApprovalExecutionReplyUsesOnlyJournalState(t *testing.T) {
	for _, tc := range []struct{ status, want string }{
		{toolexec.StatusSucceeded, "执行成功"}, {toolexec.StatusFailed, "执行失败"}, {toolexec.StatusRunning, "尚未确认"},
	} {
		text := approvalExecutionReply("apr_test", []toolexec.Execution{{ToolName: "demo", Status: tc.status}})
		if !strings.Contains(text, tc.want) {
			t.Fatalf("reply=%q want=%q", text, tc.want)
		}
	}
	if text := approvalExecutionReply("apr_test", nil); !strings.Contains(text, "没有工具执行记录") {
		t.Fatalf("missing evidence must not imply success: %q", text)
	}
}
