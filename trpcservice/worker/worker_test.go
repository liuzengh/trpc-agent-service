package worker

import (
	"context"
	"testing"
	"time"

	agentruntime "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
)

func TestWorkerCompletesDurableRun(t *testing.T) {
	journal := gateway.NewMemoryJournal()
	queue := workqueue.NewMemoryQueue(4)
	runtime := agentruntime.NewDemoRuntime()
	t.Cleanup(func() {
		_ = runtime.Close()
		_ = queue.Close()
		_ = journal.Close()
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
}
