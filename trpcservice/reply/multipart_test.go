package reply

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
)

type multipartAdapter struct {
	calls []string
	fail  bool
	delay time.Duration
}

func (a *multipartAdapter) Type() string { return "http" }
func (a *multipartAdapter) Capabilities() channels.Capabilities {
	return channels.Capabilities{MaxTextRunes: 3}
}
func (a *multipartAdapter) Send(ctx context.Context, _ controlplane.ChannelBinding, m channels.OutboundMessage) (channels.DeliveryReceipt, error) {
	a.calls = append(a.calls, m.Text)
	if a.delay > 0 {
		select {
		case <-time.After(a.delay):
		case <-ctx.Done():
			return channels.DeliveryReceipt{}, &channels.DeliveryError{Unknown: true, Cause: ctx.Err()}
		}
	}
	if a.fail && m.Text == "def" {
		a.fail = false
		return channels.DeliveryReceipt{}, &channels.DeliveryError{Cause: errors.New("rate limited"), Retryable: true, RetryAfter: time.Millisecond}
	}
	return channels.DeliveryReceipt{ProviderMessageID: m.Text, SentAt: time.Now()}, nil
}
func multipartFixture(t *testing.T, j gateway.Journal, a channels.Adapter) (*controlplane.MemoryRepository, *Sender) {
	t.Helper()
	repo := controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData())
	t.Cleanup(func() { repo.Close() })
	ctx := context.Background()
	accepted, err := j.Accept(ctx, gateway.InboundRequest{Scope: runtimecontext.TutorialScope(), ExternalMessageID: "multipart", UserID: "user", SessionID: "session", ChatType: "direct", Text: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	items, err := j.ClaimQueueOutbox(ctx, "relay", 1, time.Second)
	if err != nil || len(items) != 1 {
		t.Fatal(err)
	}
	if err = j.MarkRunRunning(ctx, accepted.RequestID, "worker"); err != nil {
		t.Fatal(err)
	}
	if err = j.CompleteRun(ctx, items[0].Task, gateway.RunResult{WorkerID: "worker", Reply: "abcdefghi"}); err != nil {
		t.Fatal(err)
	}
	registry, _ := channels.NewRegistry(a)
	s, err := New(j, repo, registry, Options{WorkerID: "sender", BatchSize: 1, ClaimLease: 150 * time.Millisecond, PollInterval: time.Millisecond, RetryDelay: time.Millisecond, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	return repo, s
}
func TestMultipartRetryResumesAtFirstUnsentPart(t *testing.T) {
	j := gateway.NewMemoryJournal()
	defer j.Close()
	a := &multipartAdapter{fail: true}
	repo, s := multipartFixture(t, j, a)
	ctx := context.Background()
	if n, err := s.ProcessOnce(ctx); n != 0 || err == nil {
		t.Fatal("expected second part failure")
	}
	time.Sleep(3 * time.Millisecond)
	registry, _ := channels.NewRegistry(a)
	restarted, _ := New(j, repo, registry, s.opts)
	if n, err := restarted.ProcessOnce(ctx); n != 1 || err != nil {
		t.Fatalf("resume=%d %v", n, err)
	}
	want := []string{"abc", "def", "def", "ghi"}
	if len(a.calls) != len(want) {
		t.Fatalf("calls=%v", a.calls)
	}
	for i := range want {
		if a.calls[i] != want[i] {
			t.Fatalf("replayed delivered prefix: %v", a.calls)
		}
	}
}

type losePartAck struct {
	gateway.Journal
	lose bool
}

func (j *losePartAck) FinishPart(ctx context.Context, p gateway.OutboundPart, status, id string) error {
	if j.lose {
		j.lose = false
		return errors.New("response lost")
	}
	return j.Journal.FinishPart(ctx, p, status, id)
}
func TestMultipartUnknownStopsWithoutRepeatingExternalSend(t *testing.T) {
	base := gateway.NewMemoryJournal()
	defer base.Close()
	j := &losePartAck{Journal: base, lose: true}
	a := &multipartAdapter{}
	_, s := multipartFixture(t, j, a)
	if _, err := s.ProcessOnce(context.Background()); err == nil {
		t.Fatal("missing persistence failure")
	}
	if _, err := s.ProcessOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(a.calls) != 1 {
		t.Fatal("unknown send repeated")
	}
}
func TestMultipartHeartbeatProtectsLongDelivery(t *testing.T) {
	j := gateway.NewMemoryJournal()
	defer j.Close()
	a := &multipartAdapter{delay: 200 * time.Millisecond}
	_, s := multipartFixture(t, j, a)
	if n, err := s.ProcessOnce(context.Background()); n != 1 || err != nil {
		t.Fatalf("long delivery=%d %v", n, err)
	}
}
