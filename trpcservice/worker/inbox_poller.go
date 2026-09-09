package worker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
)

// RequestSubmitter accepts a reclaimed durable Worker request.
type RequestSubmitter interface {
	Submit(gateway.RunRequest) error
}

// InboxPollerConfig bounds polling, claim leases, and each database batch.
type InboxPollerConfig struct {
	Owner        string
	PollInterval time.Duration
	ClaimTTL     time.Duration
	BatchSize    int
}

// InboxPoller continuously recovers due or lease-expired Inbox work.
type InboxPoller struct {
	store     idempotency.ReadyStore
	submitter RequestSubmitter
	config    InboxPollerConfig
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	closeOnce sync.Once
}

// NewInboxPoller starts a background durable Inbox recovery loop.
func NewInboxPoller(parent context.Context, store idempotency.ReadyStore, submitter RequestSubmitter, config InboxPollerConfig) (*InboxPoller, error) {
	if parent == nil || store == nil || submitter == nil || config.Owner == "" {
		return nil, errors.New("worker: inbox poller context, store, submitter, and owner are required")
	}
	if config.PollInterval <= 0 {
		config.PollInterval = time.Second
	}
	if config.ClaimTTL <= 0 {
		config.ClaimTTL = 30 * time.Second
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 32
	}
	ctx, cancel := context.WithCancel(parent)
	poller := &InboxPoller{store: store, submitter: submitter, config: config, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	go poller.run()
	return poller, nil
}

func (poller *InboxPoller) run() {
	defer close(poller.done)
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-poller.ctx.Done():
			return
		case <-timer.C:
			poller.poll()
			timer.Reset(poller.config.PollInterval)
		}
	}
}

func (poller *InboxPoller) poll() {
	claims, err := poller.store.ClaimReady(poller.ctx, poller.config.Owner, poller.config.ClaimTTL, poller.config.BatchSize)
	if err != nil {
		return
	}
	for _, claim := range claims {
		if err := poller.submitter.Submit(claim.RunRequest()); err != nil {
			retryAt := time.Now().UTC().Add(poller.config.PollInterval)
			failCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			_ = poller.store.Fail(failCtx, claim, err, retryAt)
			cancel()
		}
	}
}

// Close stops new claims and waits for the polling goroutine.
func (poller *InboxPoller) Close(ctx context.Context) error {
	if poller == nil || ctx == nil {
		return errors.New("worker: inbox poller and close context are required")
	}
	poller.closeOnce.Do(poller.cancel)
	select {
	case <-poller.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
