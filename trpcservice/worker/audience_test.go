package worker

import (
	"context"
	"testing"
	"time"

	agentruntime "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
)

type audienceRuntime struct{ got string }

func (r *audienceRuntime) ChatWithScope(_ context.Context, input agentruntime.ChatInput) (agentruntime.ChatResult, error) {
	r.got = input.ChatType
	return agentruntime.ChatResult{Reply: "synthetic reply"}, nil
}

func TestQueueCarriesVerifiedAudienceToRuntime(t *testing.T) {
	for _, audience := range []string{"direct", "group"} {
		t.Run(audience, func(t *testing.T) {
			ctx := context.Background()
			journal := gateway.NewMemoryJournal()
			defer func(closer interface{ Close() error }) { _ = closer.Close() }(journal)
			queue := workqueue.NewMemoryQueue(2)
			defer func(closer interface{ Close() error }) { _ = closer.Close() }(queue)
			if _, err := journal.Accept(ctx, gateway.InboundRequest{Scope: runtimecontext.TutorialScope(), ExternalMessageID: "audience", UserID: "user", SessionID: "session", ChatType: audience, Text: "test"}); err != nil {
				t.Fatal(err)
			}
			relay, err := gateway.NewOutboxRelay(journal, queue, gateway.RelayOptions{WorkerID: "relay", BatchSize: 1, ClaimLease: time.Second, PollInterval: time.Millisecond, RetryDelay: time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := relay.RelayOnce(ctx); err != nil {
				t.Fatal(err)
			}
			runtime := &audienceRuntime{}
			worker, err := New(queue, journal, runtime, Options{WorkerID: "worker", MaxAttempts: 1, RetryDelay: time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			if done, err := worker.ProcessOne(ctx); err != nil || !done {
				t.Fatalf("done=%t err=%v", done, err)
			}
			if runtime.got != audience {
				t.Fatalf("lost audience: %q", runtime.got)
			}
		})
	}
}
