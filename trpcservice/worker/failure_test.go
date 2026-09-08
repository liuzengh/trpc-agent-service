package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	agentruntime "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
)

type failingRuntime struct{ calls int }

func (r *failingRuntime) ChatWithScope(context.Context, agentruntime.ChatInput) (agentruntime.ChatResult, error) {
	r.calls++
	return agentruntime.ChatResult{}, errors.New("private-provider-error api_key=canary-model-secret")
}

type failNoticeJournal struct {
	*gateway.MemoryJournal
	calls                 int
	failOnce, afterCommit bool
}

func (j *failNoticeJournal) TerminalFailRun(ctx context.Context, task workqueue.AgentTask, result gateway.RunResult) (bool, error) {
	j.calls++
	if j.failOnce && j.calls == 1 {
		if j.afterCommit {
			if _, err := j.MemoryJournal.TerminalFailRun(ctx, task, result); err != nil {
				return false, err
			}
		}
		return false, errors.New("temporary notice write failure")
	}
	return j.MemoryJournal.TerminalFailRun(ctx, task, result)
}

func TestFailureNoticeIsDurableIdempotentAndStopsModelRetries(t *testing.T) {
	for _, mode := range []string{"normal", "write-failed", "commit-response-lost"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			journal := &failNoticeJournal{MemoryJournal: gateway.NewMemoryJournal(), failOnce: mode != "normal", afterCommit: mode == "commit-response-lost"}
			defer func(closer interface{ Close() error }) { _ = closer.Close() }(journal)
			accepted, err := journal.Accept(ctx, gateway.InboundRequest{Scope: runtimecontext.TutorialScope(), ExternalMessageID: "failure-message", UserID: "alice", SessionID: "demo", ChatType: "direct", Text: "hello"})
			if err != nil {
				t.Fatal(err)
			}
			task := journal.Tasks()[0]
			queue := workqueue.NewMemoryQueue(8)
			defer func(closer interface{ Close() error }) { _ = closer.Close() }(queue)
			_ = queue.Publish(ctx, task)
			runtime := &failingRuntime{}
			worker, err := New(queue, journal, runtime, Options{WorkerID: "failure-worker", MaxAttempts: 2, RetryDelay: time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if processed, _ := worker.ProcessOne(ctx); !processed {
					t.Fatal("message not processed")
				}
			}
			if mode != "normal" {
				if processed, _ := worker.ProcessOne(ctx); !processed {
					t.Fatal("notice repair not processed")
				}
			}
			if runtime.calls != 2 {
				t.Fatalf("runtime reran after retry budget: %d", runtime.calls)
			}
			status, _, _ := journal.RunStatus(accepted.RequestID)
			if status != "dead" {
				t.Fatalf("status=%s", status)
			}
			items, err := journal.ClaimOutbound(ctx, "sender", 10, time.Minute)
			if err != nil || len(items) != 1 {
				t.Fatalf("notices=%d err=%v", len(items), err)
			}
			if !strings.Contains(items[0].Text, accepted.RequestID) || strings.Contains(items[0].Text, "canary") || !strings.Contains(items[0].Text, "不代表工具已经回滚") {
				t.Fatalf("unsafe notice: %s", items[0].Text)
			}
			_ = journal.MarkOutboundSent(ctx, items[0].ID, "sender", "receipt")
			_ = queue.Publish(ctx, task) // Original delivery reclaimed after ACK was lost.
			if processed, err := worker.ProcessOne(ctx); !processed || err != nil {
				t.Fatalf("terminal replay=%t %v", processed, err)
			}
			if runtime.calls != 2 {
				t.Fatal("dead task invoked model")
			}
			items, _ = journal.ClaimOutbound(ctx, "sender", 10, time.Minute)
			if len(items) != 0 {
				t.Fatal("duplicate failure notice")
			}
		})
	}
}
