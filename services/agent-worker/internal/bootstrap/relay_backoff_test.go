package bootstrap

import (
	"context"
	"errors"
	app "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/nats-io/nats.go"
	"os"
	"sync/atomic"
	"testing"
	"time"

	codec "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/infra/natsadapter"
	"github.com/nats-io/nats.go/jetstream"
)

// This unit gate covers the real App loop and ReplyRelay, not broker timing.
// The PG/NATS capacity gate separately proves actual saturation and recovery.
func TestReplyRelayUnavailableBackoffAndCancellation(t *testing.T) {
	raw, err := os.ReadFile("../../../../api/events/execution/v1/fixtures/reply-intent-final.valid.json")
	if err != nil {
		t.Fatal(err)
	}
	event, err := codec.DecodeReplyIntent(raw)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := codec.ReplyIntentDigest(event)
	if err != nil {
		t.Fatal(err)
	}
	source := &backoffOutbox{row: domain.OutboxItem{IntentID: event.IntentID, Digest: digest, Payload: raw}}
	publisher := &backoffPublisher{calls: make(chan time.Time, 64)}
	relay, err := natsadapter.NewReplyRelay(source, publisher, 1)
	if err != nil {
		t.Fatal(err)
	}
	const interval = 250 * time.Millisecond
	a := &App{reply: relay, config: Config{Timing: Timing{PollInterval: Duration(interval), OperationTimeout: Duration(time.Second)}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	failures := make(chan error, 1)
	go func() {
		defer close(done)
		a.relay(ctx, failures)
	}()
	// Always reap the actual loop, including assertion-failure paths.
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("relay loop did not terminate after cancellation")
		}
	})
	next := func() time.Time {
		t.Helper()
		select {
		case at := <-publisher.calls:
			return at
		case <-time.After(2 * time.Second):
			t.Fatal("reply relay did not retry unavailable publish")
			return time.Time{}
		}
	}
	first, second := next(), next()
	if elapsed := second.Sub(first); elapsed < interval {
		t.Fatalf("unavailable publish busy-loop: interval=%s observed=%s", interval, elapsed)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancellation did not interrupt retry wait")
	}
	if source.reads.Load() != 2 || publisher.count.Load() != 2 || source.marks.Load() != 0 {
		t.Fatalf("unexpected retries/marks: reads=%d publishes=%d marks=%d", source.reads.Load(), publisher.count.Load(), source.marks.Load())
	}
	if a.replyHealthy.Load() {
		t.Fatal("unavailable reply publisher remained healthy")
	}
	select {
	case err := <-failures:
		t.Fatalf("transient outage became terminal: %v", err)
	default:
	}
	t.Logf("real App.relay + ReplyRelay: unavailable attempts=2 marks=0 interval=%s observed=%s cancellation=PASS (unit dependencies)", interval, second.Sub(first))
}

type backoffOutbox struct {
	row          domain.OutboxItem
	reads, marks atomic.Int32
}

func (s *backoffOutbox) PendingReplies(context.Context, int) ([]domain.OutboxItem, error) {
	s.reads.Add(1)
	return []domain.OutboxItem{s.row}, nil
}
func (s *backoffOutbox) MarkReplyPublished(context.Context, string, string) error {
	s.marks.Add(1)
	return nil
}

type backoffPublisher struct {
	calls chan time.Time
	count atomic.Int32
}

func (p *backoffPublisher) PublishMsg(context.Context, *nats.Msg, ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	p.count.Add(1)
	select {
	case p.calls <- time.Now():
	default:
	}
	return nil, errors.New("explicit unit fixture broker unavailable")
}

func (o *backoffOutbox) PendingTracedReplies(ctx context.Context, limit int) ([]app.TracedReply, error) {
	rows, err := o.PendingReplies(ctx, limit)
	out := make([]app.TracedReply, 0, len(rows))
	for _, row := range rows {
		out = append(out, app.TracedReply{OutboxItem: row})
	}
	return out, err
}
