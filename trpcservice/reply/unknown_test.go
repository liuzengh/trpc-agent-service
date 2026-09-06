package reply

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
)

type unknownAdapter struct{ calls int }

func (a *unknownAdapter) Type() string { return "http" }
func (a *unknownAdapter) Capabilities() channels.Capabilities {
	return channels.Capabilities{MaxTextRunes: 4000}
}
func (a *unknownAdapter) Send(context.Context, controlplane.ChannelBinding, channels.OutboundMessage) (channels.DeliveryReceipt, error) {
	a.calls++
	return channels.DeliveryReceipt{}, &channels.DeliveryError{Cause: errors.New("outcome unknown"), Unknown: true, Retryable: true}
}

func TestUnknownDeliveryAndSuspendedTenantStopSender(t *testing.T) {
	for _, suspended := range []bool{false, true} {
		data := controlplane.DefaultBootstrapData()
		if suspended {
			data.Tenants[0].Status = controlplane.StatusSuspended
		}
		repo := controlplane.NewMemoryRepository(data)
		journal := gateway.NewMemoryJournal()
		writer := audit.NewMemoryWriter()
		adapter := &unknownAdapter{}
		registry, _ := channels.NewRegistry(adapter)
		t.Cleanup(func() { _ = repo.Close(); _ = journal.Close(); _ = writer.Close() })
		ctx := context.Background()
		accepted, err := journal.Accept(ctx, gateway.InboundRequest{Scope: runtimecontext.TutorialScope(), ExternalMessageID: "test", UserID: "alice", SessionID: "test", ChatType: "direct", Text: "hello"})
		if err != nil {
			t.Fatal(err)
		}
		if err := journal.MarkRunRunning(ctx, accepted.RequestID, "worker"); err != nil {
			t.Fatal(err)
		}
		if err := journal.CompleteRun(ctx, journal.Tasks()[0], gateway.RunResult{WorkerID: "worker", Reply: "reply"}); err != nil {
			t.Fatal(err)
		}
		sender, _ := New(journal, repo, registry, Options{WorkerID: "sender", BatchSize: 1, ClaimLease: time.Second, PollInterval: time.Second, RetryDelay: time.Second, MaxAttempts: 3, Audit: writer})
		if n, err := sender.ProcessOnce(ctx); err == nil || n != 0 {
			t.Fatal("delivery unexpectedly succeeded")
		}
		if n, err := sender.ProcessOnce(ctx); err != nil || n != 0 {
			t.Fatal("terminal delivery retried")
		}
		wantCalls, decision := 1, "reply_delivery_unknown"
		if suspended {
			wantCalls, decision = 0, "reply_failed"
		}
		events := writer.Events()
		if adapter.calls != wantCalls || len(events) != 1 || events[0].Decision != decision || events[0].Details["terminal"] != true {
			t.Fatal("delivery audit/terminal decision incorrect")
		}
		id, _ := events[0].Details["outbound_id"].(string)
		if status, ok := journal.OutboundStatus(id); !ok || status != "dead" {
			t.Fatal("unknown or suspended delivery was left retryable")
		}
	}
}
