package application

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
)

type outboxStub struct {
	message                              OutboxMessage
	found                                bool
	claimErr, errorRetry, errorPublished error
	retry, published                     int
}

func (o *outboxStub) Claim(context.Context) (OutboxMessage, bool, error) {
	return o.message, o.found, o.claimErr
}
func (o *outboxStub) Retry(context.Context, OutboxMessage) error { o.retry++; return o.errorRetry }
func (o *outboxStub) Published(context.Context, OutboxMessage) error {
	o.published++
	return o.errorPublished
}

type publishFunc func(context.Context, string, string, []byte) error

func (f publishFunc) PublishMessage(ctx context.Context, subject, id string, payload []byte, _ tracecontext.Carrier) error {
	return f(ctx, subject, id, payload)
}
func TestRelayPublishesBeforeLedgerCompletion(t *testing.T) {
	ledger := &outboxStub{message: OutboxMessage{EventID: "event", Subject: "execution.run-requested.v1", ClaimToken: "claim", Payload: []byte("payload")}, found: true}
	relay := NewRelay(ledger, publishFunc(func(ctx context.Context, subject, id string, payload []byte) error {
		if id != "event" || subject != ledger.message.Subject || string(payload) != "payload" {
			t.Fatal("message identity changed")
		}
		if ledger.published != 0 {
			t.Fatal("marked before broker acknowledgement")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("publish has no deadline")
		}
		return nil
	}))
	found, err := relay.PublishNext(context.Background())
	if !found || err != nil || ledger.published != 1 || ledger.retry != 0 {
		t.Fatalf("found=%v err=%v published=%d retry=%d", found, err, ledger.published, ledger.retry)
	}
}
func TestRelayFailurePreservesRetryAndClaimLoss(t *testing.T) {
	publishErr := errors.New("broker timeout")
	retryErr := errors.New("retry CAS failed")
	ledger := &outboxStub{found: true, errorRetry: retryErr}
	relay := NewRelay(ledger, publishFunc(func(context.Context, string, string, []byte) error { return publishErr }))
	found, err := relay.PublishNext(context.Background())
	if !found || !errors.Is(err, publishErr) || !errors.Is(err, retryErr) || ledger.retry != 1 || ledger.published != 0 {
		t.Fatalf("failure result: %v %v %+v", found, err, ledger)
	}
	completeErr := errors.New("claim lost")
	ledger = &outboxStub{found: true, errorPublished: completeErr}
	relay = NewRelay(ledger, publishFunc(func(context.Context, string, string, []byte) error { return nil }))
	if _, err = relay.PublishNext(context.Background()); !errors.Is(err, completeErr) {
		t.Fatalf("published CAS failure discarded: %v", err)
	}
}
func TestRelayNoClaimDoesNotPublish(t *testing.T) {
	for _, failure := range []error{nil, errors.New("store down")} {
		ledger := &outboxStub{claimErr: failure}
		relay := NewRelay(ledger, publishFunc(func(context.Context, string, string, []byte) error {
			t.Fatal("no claim yet publish called")
			return nil
		}))
		found, err := relay.PublishNext(context.Background())
		if found || !errors.Is(err, failure) {
			t.Fatalf("claim result: %v %v", found, err)
		}
	}
}
func TestRelayRunExitsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	relay := NewRelay(&outboxStub{}, publishFunc(func(context.Context, string, string, []byte) error { return nil }))
	if err := relay.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel result: %v", err)
	}
}
