package attachments

import (
	"context"
	"testing"
	"time"

	agentservice "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
)

type noModelRuntime struct{ t *testing.T }

func (r noModelRuntime) ChatWithScope(context.Context, agentservice.ChatInput) (agentservice.ChatResult, error) {
	r.t.Error("attachment automatically reached model")
	return agentservice.ChatResult{}, nil
}
func TestAttachmentTravelsDurableQueueAndCompletesWithoutModel(t *testing.T) {
	s, task := attachmentFixture(t)
	ctx := context.Background()
	journal := gateway.NewMemoryJournal()
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(journal)
	queue := workqueue.NewMemoryQueue(4)
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(queue)
	_, err := journal.Accept(ctx, gateway.InboundRequest{Scope: task.Scope, ExternalMessageID: task.MessageID, UserID: task.UserID, SessionID: task.SessionID, ChatType: "direct", Text: "uploaded attachment", ReplyTarget: "target", Media: task.Media})
	if err != nil {
		t.Fatal(err)
	}
	relay, _ := gateway.NewOutboxRelay(journal, queue, gateway.RelayOptions{WorkerID: "relay", BatchSize: 10, ClaimLease: time.Second, PollInterval: time.Millisecond, RetryDelay: time.Millisecond})
	if _, err := relay.RelayOnce(ctx); err != nil {
		t.Fatal(err)
	}
	w, err := worker.New(queue, journal, noModelRuntime{t}, worker.Options{WorkerID: "worker", MaxAttempts: 3, RetryDelay: time.Millisecond, Attachments: s})
	if err != nil {
		t.Fatal(err)
	}
	if processed, err := w.ProcessOne(ctx); err != nil || !processed {
		t.Fatalf("process=%t err=%v", processed, err)
	}
	items, err := journal.ClaimOutbound(ctx, "sender", 10, time.Second)
	if err != nil || len(items) != 1 {
		t.Fatal("attachment completion did not produce durable reply")
	}
}
