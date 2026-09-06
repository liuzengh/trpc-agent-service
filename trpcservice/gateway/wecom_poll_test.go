package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecommcp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/routing"
)

type windowReaderFunc func(context.Context, controlplane.ChannelBinding, string, time.Time, time.Time) ([]channels.InboundEnvelope, error)

func (f windowReaderFunc) ReadWindow(ctx context.Context, b controlplane.ChannelBinding, c string, from, to time.Time) ([]channels.InboundEnvelope, error) {
	return f(ctx, b, c, from, to)
}

type polledIntakeFunc func(context.Context, controlplane.ChannelBinding, channels.InboundEnvelope) error

func (f polledIntakeFunc) AcceptPolled(ctx context.Context, b controlplane.ChannelBinding, m channels.InboundEnvelope) error {
	return f(ctx, b, m)
}

func pollFixture(t *testing.T, reader WeComWindowReader, intake PolledIntake, state wecommcp.Store) (*WeComPoller, *controlplane.MemoryRepository, controlplane.ChannelBinding, time.Time) {
	t.Helper()
	start := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	data := controlplane.DefaultBootstrapData()
	b := &data.ChannelBindings[0]
	b.ChannelType = wecommcp.ChannelType
	b.SecretRef = "env://MCP"
	b.Config = json.RawMessage(`{"allowed_chat_ids":["group"],"allowed_user_ids":["human"],"mention_prefix":"@bot","timezone":"UTC","start_at":"2026-09-06T00:00:00Z","dedupe_mode":"fingerprint-v1"}`)
	repo := controlplane.NewMemoryRepository(data)
	coord := coordination.NewLocalCoordinator()
	t.Cleanup(func() { _ = repo.Close(); _ = coord.Close() })
	p, err := NewWeComPoller(repo, reader, intake, state, coord, WeComPollOptions{Targets: []config.WeComMCPTarget{{TenantID: b.TenantID, BindingID: b.ID}}, Interval: time.Second, Window: time.Minute, Overlap: time.Minute, SettleDelay: 5 * time.Second, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	p.now = func() time.Time { return start.Add(2 * time.Minute) }
	return p, repo, *b, start
}

func TestPollingRestartOverlapAndPartialAcceptance(t *testing.T) {
	state := wecommcp.NewMemoryStore()
	calls := 0
	accepted := map[string]int{}
	fail := true
	reader := windowReaderFunc(func(_ context.Context, _ controlplane.ChannelBinding, chat string, from, to time.Time) ([]channels.InboundEnvelope, error) {
		calls++
		if chat != "group" || to.Sub(from) > 2*time.Minute {
			t.Error("window escaped scope")
		}
		return []channels.InboundEnvelope{{ExternalMessageID: "one"}, {ExternalMessageID: "two"}}, nil
	})
	intake := polledIntakeFunc(func(_ context.Context, _ controlplane.ChannelBinding, m channels.InboundEnvelope) error {
		if m.ExternalMessageID == "two" && fail {
			return errors.New("inbox unavailable")
		}
		accepted[m.ExternalMessageID]++
		return nil
	})
	p, _, _, _ := pollFixture(t, reader, intake, state)
	if n, err := p.ProcessOnce(context.Background()); err == nil || n != 1 {
		t.Fatal("partial failure advanced window")
	}
	fail = false
	// Reconstruct receiver while keeping durable state; the first accepted row
	// is not accepted again and the unaccepted second row is not lost.
	second, _, _, _ := pollFixture(t, reader, intake, state)
	if n, err := second.ProcessOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("resume: %d %v", n, err)
	}
	if n, err := second.ProcessOnce(context.Background()); err != nil || n != 0 {
		t.Fatalf("overlap: %d %v", n, err)
	}
	if accepted["one"] != 1 || accepted["two"] != 1 || calls != 3 {
		t.Fatal("checkpoint/seen recovery failed")
	}
}

func TestPollingReadFailureCancellationAndDisabledBinding(t *testing.T) {
	for _, kind := range []string{"read_failure", "cancel", "disabled", "old_checkpoint"} {
		t.Run(kind, func(t *testing.T) {
			reads, accepts := 0, 0
			reader := windowReaderFunc(func(ctx context.Context, _ controlplane.ChannelBinding, _ string, _, _ time.Time) ([]channels.InboundEnvelope, error) {
				reads++
				if kind == "cancel" {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return nil, errors.New("source failed")
			})
			p, repo, b, start := pollFixture(t, reader, polledIntakeFunc(func(context.Context, controlplane.ChannelBinding, channels.InboundEnvelope) error {
				accepts++
				return nil
			}), wecommcp.NewMemoryStore())
			if kind == "disabled" {
				_, err := repo.UpdateChannelBinding(context.Background(), b.TenantID, b.ID, b.Config, controlplane.StatusDisabled, b.Version)
				if err != nil {
					t.Fatal(err)
				}
			}
			if kind == "old_checkpoint" {
				p.now = func() time.Time { return start.Add(8 * 24 * time.Hour) }
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			_, err := p.ProcessOnce(ctx)
			if kind != "disabled" && err == nil {
				t.Fatal("failure hidden")
			}
			if accepts != 0 || ((kind == "disabled" || kind == "old_checkpoint") && reads != 0) {
				t.Fatal("disabled/out-of-retention data read")
			}
		})
	}
}

func TestPolledIngressUsesApprovalPathAndRejectsStaleBinding(t *testing.T) {
	p, repo, b, _ := pollFixture(t, windowReaderFunc(func(context.Context, controlplane.ChannelBinding, string, time.Time, time.Time) ([]channels.InboundEnvelope, error) {
		return nil, nil
	}), polledIntakeFunc(func(context.Context, controlplane.ChannelBinding, channels.InboundEnvelope) error { return nil }), wecommcp.NewMemoryStore())
	_ = p
	journal := NewMemoryJournal()
	t.Cleanup(func() { _ = journal.Close() })
	resolver, _ := routing.NewControlPlaneResolver(repo)
	intake, _ := NewIntake(resolver, journal)
	registry, _ := channels.NewRegistry(channels.NewTestAdapter())
	decisions := &approvalDecisionTestHandler{}
	g, _ := NewCallbackGateway(repo, registry, intake, WithApprovalDecisionHandler(decisions))
	message := channels.InboundEnvelope{ExternalMessageID: "fp", ExternalUserID: "human", ExternalChatID: "group", ChatType: "group", MessageType: "text", Text: "批准 apr_test", ReplyTarget: "group"}
	if err := g.AcceptPolled(context.Background(), b, message); err != nil || decisions.calls != 1 || len(journal.Tasks()) != 0 {
		t.Fatal("polled approval bypassed shared handling")
	}
	_, _ = repo.UpdateChannelBinding(context.Background(), b.TenantID, b.ID, b.Config, controlplane.StatusDisabled, b.Version)
	if err := g.AcceptPolled(context.Background(), b, message); err == nil || decisions.calls != 1 {
		t.Fatal("stale subscription accepted")
	}
}

func TestPollerRunStopsOnContextCancellation(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan error, 1)
	reader := windowReaderFunc(func(ctx context.Context, _ controlplane.ChannelBinding, _ string, _, _ time.Time) ([]channels.InboundEnvelope, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	p, _, _, _ := pollFixture(t, reader, polledIntakeFunc(func(context.Context, controlplane.ChannelBinding, channels.InboundEnvelope) error { return nil }), wecommcp.NewMemoryStore())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { finished <- p.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("receiver did not start")
	}
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("receiver goroutine did not exit")
	}
}

type rejectedBatchReader struct{ windowReaderFunc }

func (r rejectedBatchReader) ReadWindowBatch(_ context.Context, _ controlplane.ChannelBinding, _ string, from, to time.Time) (wecommcp.WindowBatch, error) {
	return wecommcp.WindowBatch{Messages: []channels.InboundEnvelope{{ExternalMessageID: "normal-text"}}, Rejected: []wecommcp.RejectedMessage{{Fingerprint: "bad-record", Reason: "unsupported_type", WindowFrom: from, WindowTo: to}}}, nil
}

type failRejectionStore struct {
	wecommcp.Store
	fail bool
}

func (s *failRejectionStore) RecordRejection(ctx context.Context, k wecommcp.PollKey, r wecommcp.RejectedMessage) (bool, error) {
	if s.fail {
		return false, errors.New("rejection storage unavailable")
	}
	return s.Store.RecordRejection(ctx, k, r)
}

func TestRejectedRecordsMustPersistBeforeWindowAdvances(t *testing.T) {
	state := &failRejectionStore{Store: wecommcp.NewMemoryStore(), fail: true}
	accepted := 0
	reader := rejectedBatchReader{windowReaderFunc(func(context.Context, controlplane.ChannelBinding, string, time.Time, time.Time) ([]channels.InboundEnvelope, error) {
		t.Fatal("legacy reader used")
		return nil, nil
	})}
	p, _, b, _ := pollFixture(t, reader, polledIntakeFunc(func(context.Context, controlplane.ChannelBinding, channels.InboundEnvelope) error {
		accepted++
		return nil
	}), state)
	if _, err := p.ProcessOnce(context.Background()); err == nil || accepted != 0 {
		t.Fatal("window advanced without durable rejected record")
	}
	state.fail = false
	if n, err := p.ProcessOnce(context.Background()); err != nil || n != 1 || accepted != 1 {
		t.Fatal("normal text blocked by rejected message")
	}
	if n, err := p.ProcessOnce(context.Background()); err != nil || n != 0 || accepted != 1 {
		t.Fatal("replay re-executed normal text")
	}
	rejected, err := state.ListRejections(context.Background(), b.TenantID, b.ID, 100)
	if err != nil || len(rejected) != 1 {
		t.Fatal("rejection was lost or duplicated")
	}
}
