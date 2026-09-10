package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

func TestReplySenderUsesProviderRetryAfter(t *testing.T) {
	delivery := testReplyDelivery()
	outbox := &replyOutboxStub{deliveries: []ReplyDelivery{delivery}}
	sender, err := NewReplySender(
		outbox,
		func(context.Context, ReplyDelivery) (string, error) { return "target", nil },
		func(context.Context, ReplyDelivery) (ReplyProvider, error) {
			return ReplyProvider{Client: replyClientFunc(func(context.Context, channels.Reply, string) (channels.ProviderReceipt, error) {
				return channels.ProviderReceipt{}, retryAfterTestError{delay: 8 * time.Second}
			})}, nil
		},
		ReplySenderOptions{Owner: "worker-1"},
	)
	if err != nil {
		t.Fatalf("new sender: %v", err)
	}
	if _, err := sender.SendBatch(context.Background()); err != nil {
		t.Fatalf("send batch: %v", err)
	}
	if !outbox.retried || outbox.delay < 8*time.Second || outbox.failed {
		t.Fatalf("retried=%t delay=%s failed=%t", outbox.retried, outbox.delay, outbox.failed)
	}
}

func TestReplySenderPersistsKnownFailureAfterShutdown(t *testing.T) {
	delivery := testReplyDelivery()
	outbox := &replyOutboxStub{deliveries: []ReplyDelivery{delivery}}
	sender, err := NewReplySender(
		outbox,
		func(context.Context, ReplyDelivery) (string, error) { return "target", nil },
		func(context.Context, ReplyDelivery) (ReplyProvider, error) {
			return ReplyProvider{Client: replyClientFunc(func(context.Context, channels.Reply, string) (channels.ProviderReceipt, error) {
				return channels.ProviderReceipt{}, retryAfterTestError{delay: time.Second}
			})}, nil
		},
		ReplySenderOptions{Owner: "worker-1"},
	)
	if err != nil {
		t.Fatalf("new sender: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := sender.SendBatch(ctx); err != nil {
		t.Fatalf("send batch after shutdown: %v", err)
	}
	if !outbox.retried || outbox.failed {
		t.Fatalf("retried=%t failed=%t", outbox.retried, outbox.failed)
	}
}

func TestReplySenderMarksCanceledProviderSendUncertain(t *testing.T) {
	delivery := testReplyDelivery()
	outbox := &replyOutboxStub{deliveries: []ReplyDelivery{delivery}}
	sender, err := NewReplySender(
		outbox,
		func(context.Context, ReplyDelivery) (string, error) { return "target", nil },
		func(context.Context, ReplyDelivery) (ReplyProvider, error) {
			return ReplyProvider{Client: replyClientFunc(func(context.Context, channels.Reply, string) (channels.ProviderReceipt, error) {
				return channels.ProviderReceipt{}, context.Canceled
			})}, nil
		},
		ReplySenderOptions{Owner: "worker-1"},
	)
	if err != nil {
		t.Fatalf("new sender: %v", err)
	}
	if _, err := sender.SendBatch(context.Background()); err != nil {
		t.Fatalf("send batch: %v", err)
	}
	if !outbox.uncertain || outbox.retried || outbox.failed {
		t.Fatalf("uncertain=%t retried=%t failed=%t", outbox.uncertain, outbox.retried, outbox.failed)
	}
}

func TestReplySenderBoundsProviderRetryAfter(t *testing.T) {
	delivery := testReplyDelivery()
	outbox := &replyOutboxStub{deliveries: []ReplyDelivery{delivery}}
	sender, err := NewReplySender(
		outbox,
		func(context.Context, ReplyDelivery) (string, error) { return "target", nil },
		func(context.Context, ReplyDelivery) (ReplyProvider, error) {
			return ReplyProvider{Client: replyClientFunc(func(context.Context, channels.Reply, string) (channels.ProviderReceipt, error) {
				return channels.ProviderReceipt{}, retryAfterTestError{delay: 24 * time.Hour}
			})}, nil
		},
		ReplySenderOptions{Owner: "worker-1"},
	)
	if err != nil {
		t.Fatalf("new sender: %v", err)
	}
	if _, err := sender.SendBatch(context.Background()); err != nil {
		t.Fatalf("send batch: %v", err)
	}
	if !outbox.retried || outbox.delay != replyRetryMax {
		t.Fatalf("retried=%t delay=%s, want bounded delay %s", outbox.retried, outbox.delay, replyRetryMax)
	}
}

func TestReplyRetryDelayUsesBoundedExponentialJitter(t *testing.T) {
	for attempt, base := range map[int]time.Duration{
		1: time.Second,
		2: 2 * time.Second,
		3: 4 * time.Second,
		6: 32 * time.Second,
		7: time.Minute,
		9: time.Minute,
	} {
		for range 10 {
			delay := retryDelay(attempt)
			if delay < base/2 || delay > base {
				t.Fatalf("attempt=%d delay=%s, want [%s,%s]", attempt, delay, base/2, base)
			}
		}
	}
}

func TestReplySenderStopsRetryingAtMaxAttempts(t *testing.T) {
	delivery := testReplyDelivery()
	delivery.Attempt = 8
	outbox := &replyOutboxStub{deliveries: []ReplyDelivery{delivery}}
	sender, err := NewReplySender(
		outbox,
		func(context.Context, ReplyDelivery) (string, error) { return "target", nil },
		func(context.Context, ReplyDelivery) (ReplyProvider, error) {
			return ReplyProvider{Client: replyClientFunc(func(context.Context, channels.Reply, string) (channels.ProviderReceipt, error) {
				return channels.ProviderReceipt{}, retryAfterTestError{delay: time.Second}
			})}, nil
		},
		ReplySenderOptions{Owner: "worker-1", MaxAttempts: 8},
	)
	if err != nil {
		t.Fatalf("new sender: %v", err)
	}
	if _, err := sender.SendBatch(context.Background()); err != nil {
		t.Fatalf("send batch: %v", err)
	}
	if outbox.retried || !outbox.failed {
		t.Fatalf("retry bounded result retried=%t failed=%t", outbox.retried, outbox.failed)
	}
}

func TestReplySenderMarksUncertainProviderResult(t *testing.T) {
	delivery := testReplyDelivery()
	outbox := &replyOutboxStub{deliveries: []ReplyDelivery{delivery}}
	sender, err := NewReplySender(
		outbox,
		func(context.Context, ReplyDelivery) (string, error) { return "target", nil },
		func(context.Context, ReplyDelivery) (ReplyProvider, error) {
			return ReplyProvider{Client: replyClientFunc(func(context.Context, channels.Reply, string) (channels.ProviderReceipt, error) {
				return channels.ProviderReceipt{}, uncertainReplyError{}
			})}, nil
		},
		ReplySenderOptions{Owner: "worker-1"},
	)
	if err != nil {
		t.Fatalf("new sender: %v", err)
	}
	if _, err := sender.SendBatch(context.Background()); err != nil {
		t.Fatalf("send batch: %v", err)
	}
	if !outbox.uncertain || outbox.retried || outbox.failed {
		t.Fatalf("uncertain=%t retried=%t failed=%t", outbox.uncertain, outbox.retried, outbox.failed)
	}
}

func TestReplySenderMarksCompletionFailureUncertain(t *testing.T) {
	delivery := testReplyDelivery()
	outbox := &replyOutboxStub{deliveries: []ReplyDelivery{delivery}}
	sender, err := NewReplySender(
		outbox,
		func(context.Context, ReplyDelivery) (string, error) { return "target", nil },
		func(context.Context, ReplyDelivery) (ReplyProvider, error) {
			return ReplyProvider{Client: replyClientFunc(func(context.Context, channels.Reply, string) (channels.ProviderReceipt, error) {
				return channels.ProviderReceipt{ProviderMessageID: "provider-1"}, nil
			})}, nil
		},
		ReplySenderOptions{Owner: "worker-1"},
	)
	if err != nil {
		t.Fatalf("new sender: %v", err)
	}
	if _, err := sender.SendBatch(context.Background()); err != nil {
		t.Fatalf("send batch: %v", err)
	}
	if !outbox.uncertain || outbox.retried || outbox.failed {
		t.Fatalf("uncertain=%t retried=%t failed=%t", outbox.uncertain, outbox.retried, outbox.failed)
	}
}

func TestReplySenderDoesNotClaimLaterRowsAfterSendFailure(t *testing.T) {
	first := testReplyDelivery()
	second := testReplyDelivery()
	second.Reply.ReplyID = "reply-2"
	outbox := &replyOutboxStub{deliveries: []ReplyDelivery{first, second}, retryErr: errors.New("persist retry failed")}
	sender, err := NewReplySender(
		outbox,
		func(context.Context, ReplyDelivery) (string, error) { return "target", nil },
		func(context.Context, ReplyDelivery) (ReplyProvider, error) {
			return ReplyProvider{Client: replyClientFunc(func(context.Context, channels.Reply, string) (channels.ProviderReceipt, error) {
				return channels.ProviderReceipt{}, retryAfterTestError{delay: time.Second}
			})}, nil
		},
		ReplySenderOptions{Owner: "worker-1", BatchSize: 2},
	)
	if err != nil {
		t.Fatalf("new sender: %v", err)
	}
	if _, err := sender.SendBatch(context.Background()); err == nil {
		t.Fatal("send batch succeeded after retry persistence failure")
	}
	if outbox.claims != 1 || len(outbox.deliveries) != 1 || !outbox.retried {
		t.Fatalf("claims=%d remaining=%d retried=%t", outbox.claims, len(outbox.deliveries), outbox.retried)
	}
}

type replyOutboxStub struct {
	deliveries []ReplyDelivery
	claims     int
	retried    bool
	failed     bool
	uncertain  bool
	delay      time.Duration
	retryErr   error
}

func (s *replyOutboxStub) ClaimReplies(context.Context, string, time.Duration, int) ([]ReplyDelivery, error) {
	s.claims++
	if len(s.deliveries) == 0 {
		return nil, nil
	}
	delivery := s.deliveries[0]
	s.deliveries = s.deliveries[1:]
	return []ReplyDelivery{delivery}, nil
}
func (*replyOutboxStub) CompleteReply(context.Context, ReplyDelivery, channels.ProviderReceipt) error {
	return errors.New("unexpected completion")
}
func (s *replyOutboxStub) MarkReplyUncertain(context.Context, ReplyDelivery, string, error) error {
	s.uncertain = true
	return nil
}
func (s *replyOutboxStub) RetryReply(_ context.Context, _ ReplyDelivery, _ string, delay time.Duration, _ error) error {
	s.retried = true
	s.delay = delay
	return s.retryErr
}
func (s *replyOutboxStub) FailReply(context.Context, ReplyDelivery, string, error) error {
	s.failed = true
	return nil
}
func (*replyOutboxStub) RecoverReplyLeases(context.Context) error { return nil }

type replyClientFunc func(context.Context, channels.Reply, string) (channels.ProviderReceipt, error)

func (f replyClientFunc) SendOnce(ctx context.Context, reply channels.Reply, target string) (channels.ProviderReceipt, error) {
	return f(ctx, reply, target)
}

type retryAfterTestError struct{ delay time.Duration }

func (e retryAfterTestError) Error() string             { return "provider throttled" }
func (e retryAfterTestError) IsRetryable() bool         { return true }
func (e retryAfterTestError) RetryAfter() time.Duration { return e.delay }

type uncertainReplyError struct{}

func (uncertainReplyError) Error() string               { return "provider result is unknown" }
func (uncertainReplyError) IsSideEffectUncertain() bool { return true }

func testReplyDelivery() ReplyDelivery {
	reply := channels.Reply{
		TenantID:        "tenant-a",
		AppID:           "app-a",
		RequestID:       "request-1",
		SourceEventID:   "event-1",
		Channel:         channels.ChannelWeCom,
		BindingID:       "binding-1",
		BindingRevision: 1,
		ReplyID:         "reply-1",
		Revision:        1,
		Target: channels.ReplyTarget{
			Kind:             channels.TargetKindMessage,
			InternalEntityID: "request-1",
		},
		Text: "hello",
	}
	return ReplyDelivery{Reply: reply, Attempt: 1, LeaseOwner: "worker-1", LeaseUntil: time.Now().Add(time.Minute)}
}
